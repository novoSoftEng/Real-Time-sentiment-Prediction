# Define the Kafka broker address and topic name
from kafka import KafkaProducer
from Helpers import Reader, Serializer


kafka_broker = "localhost:9092"
topic_name = "twitter"

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=[kafka_broker])

# Function to read data from the Reader class and send it to Kafka
def send_data_to_kafka(reader):
    serializer = Serializer()

    for data in reader.read_with_sleep(drop=True):
        serialized_data = serializer.serialize(data)
        print(serialized_data)
        producer.send(topic_name, value=serialized_data)
# Example file path
csv_file_path = "./twitter_training.csv"

# Create a Reader instance
reader = Reader(csv_file_path)

# Send data to Kafka
send_data_to_kafka(reader)