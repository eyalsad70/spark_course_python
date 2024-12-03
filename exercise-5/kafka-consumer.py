from kafka import KafkaConsumer
import json

# Topics/Brokers
topic1 = 'gps-user-review-source'
brokers = ['course-kafka:19092']

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    topic1,  # Subscribe to your topic
    bootstrap_servers=brokers,  # Kafka broker addresses
    group_id='my_consumer_group',  # Consumer group id
    auto_offset_reset='earliest',     # Start from the earliest available message
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON message
)

# Consume messages from Kafka
for message in consumer:
    # Print the message value (JSON object)
    print("Received message: ", message.value)
