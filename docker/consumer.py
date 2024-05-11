from kafka import KafkaConsumer
import json
import requests

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

# Subscribe to a specific topic
consumer.subscribe(topics=['twitter'])

# Define the URL of the Flask API endpoint
API_URL = 'http://localhost:5000/predict'

for message in consumer:
    try:
        # Deserialize JSON message to a Python dictionary
        print(message.value.decode('utf-8'))

        # Send HTTP POST request to the Flask API endpoint with the message data
        response = requests.post(API_URL, json=message.value.decode('utf-8'))


        # Check if the request was successful
        if response.status_code == 200:
            print("INFO: Data processed and saved successfully")
        else:
            print("ERROR: Failed to process data:", response.text)
    except Exception as e:
        print("ERROR:", str(e))

