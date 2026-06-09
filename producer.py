import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# 1. Topic create karne ka admin client
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))

try:
    topic = NewTopic(name="ipl_match", num_partitions=1, replication_factor=1)
    admin_client.create_topics(new_topics=[topic])
    print("Topic created successfully!")
except TopicAlreadyExistsError:
    print("Topic already exists, continuing...")

# 2. Producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 10, 1)
)

file_path = 'ipl_json/1529296.json' 

try:
    with open(file_path, 'r') as f:
        data = json.load(f)
        for inning in data['innings']:
            for over in inning['overs']:
                for delivery in over['deliveries']:
                    producer.send('ipl_match', value=delivery)
                    print(f"Sent ball: {delivery}")
                    time.sleep(0.5) 
    producer.flush()
    print("Streaming complete!")
except Exception as e:
    print(f"Error: {e}")