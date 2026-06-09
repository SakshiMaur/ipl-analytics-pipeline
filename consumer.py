import json
import time
import psycopg2
import os
from kafka import KafkaConsumer
from minio import Minio

# MinIO Setup
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

# Postgres Connection
DB_CONFIG = {"host": "127.0.0.1", "database": "ipl_db", "user": "user", "password": "password", "port": "5433"}

def get_db_connection():
    while True:
        try:
            return psycopg2.connect(**DB_CONFIG)
        except Exception as e:
            print(f"Connection fail... Error: {e}")
            time.sleep(5)

# Kafka Consumer
consumer = KafkaConsumer('ipl_match', bootstrap_servers=['localhost:9092'], 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')), api_version=(0, 10, 1))

conn = get_db_connection()
cursor = conn.cursor()

# Logic Variables
TARGET_SCORE, current_score, total_balls, balls_bowled = 180, 0, 120, 0
batch_data = []
count = 0

print("Consumer ready hai...")

try:
    for message in consumer:
        delivery = message.value
        count += 1
        balls_bowled += 1
        
        # Win Prob Logic
        runs = int(delivery.get('runs', {}).get('total', 0))
        current_score += runs
        balls_remaining = total_balls - balls_bowled
        
        if balls_remaining > 0:
            req_run_rate = (TARGET_SCORE - current_score) / (balls_remaining / 6)
            win_prob = max(0, min(100, 100 - (req_run_rate * 5)))
        else:
            win_prob = 100 if current_score >= TARGET_SCORE else 0

        # SQL Insert
        cursor.execute("""
            INSERT INTO ipl_deliveries (batter, bowler, runs_off_bat, total_runs, delivery_data, win_probability)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (str(delivery.get('batter', 'Unknown')), str(delivery.get('bowler', 'Unknown')), 
              int(delivery.get('runs', {}).get('batter', 0)), int(delivery.get('runs', {}).get('total', 0)), 
              json.dumps(delivery), float(win_prob)))
        conn.commit()

        # Phase 6: Data Lake (MinIO) Logic
        batch_data.append(delivery)
        if count % 100 == 0:
            filename = f"batch_{count}.json"
            with open(filename, 'w') as f:
                json.dump(batch_data, f)
            minio_client.fput_object("ipl-raw-data", filename, filename)
            print(f"--- Data Lake: Uploaded {filename} to MinIO ---")
            os.remove(filename)
            batch_data = []

        print(f"Saved: {delivery.get('batter')} vs {delivery.get('bowler')} | Win Prob: {win_prob:.2f}%")

except Exception as e:
    print(f"Loop error: {e}")
finally:
    cursor.close()
    conn.close()