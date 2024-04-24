from kafka import KafkaConsumer
import pandas as pd

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest', 
    enable_auto_commit=False
)

# Subscribe to a specific topic
consumer.subscribe(topics=['twitter'])

for message in consumer:
    print("Received message: ", message.value)