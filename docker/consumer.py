import json
from kafka import KafkaConsumer
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pymongo import MongoClient
print("INFO : started Consumer")
# Establish a connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['twitter_database']
collection = db['twitter_collection']

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .getOrCreate()


# Load the pre-trained model
model = PipelineModel.load('pipeline_model/')

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

# Subscribe to a specific topic
consumer.subscribe(topics=['twitter'])
for message in consumer:
    print("INFO: Received message")
    # Deserialize JSON message to a Python dictionary
    message_dict = json.loads(message.value)

    # Convert dictionary to a Pandas DataFrame
    df = pd.DataFrame([message_dict])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Transform the DataFrame using the model
    transformed_data = model.transform(spark_df)

    # Convert Spark DataFrame to Pandas DataFrame
    transformed_df = transformed_data.toPandas()

    # Perform further processing with the transformed data
    records = transformed_df.to_dict(orient='records')
    for record in records:
        print("INFO : inserting records")
        collection.insert_one(record)
