import json
from kafka import KafkaConsumer
import pandas as pd
from pyspark.ml import PipelineModel
import pandas as pd
from pymongo import MongoClient

# Établir une connexion à MongoDB
client = MongoClient('localhost', 27017)
db = client['twitter_database']
collection = db['twitter_collection']

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
    # Deserialize JSON message to a Python dictionary
    message_dict = json.loads(message.value)

    # Convert dictionary to a Pandas DataFrame
    df = pd.DataFrame(message_dict)

    # Transform the DataFrame using the model
    transformed_data = model.transform(df)

    # Perform further processing with the transformed data
    data = df.to_dict(orient='records')
    data['Sentiment']=transformed_data
    print(data)
    collection.insert_one(data)