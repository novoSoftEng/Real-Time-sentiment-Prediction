from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"]
)

producer.send("twitter", value="Hello, World!".encode("utf-8"))