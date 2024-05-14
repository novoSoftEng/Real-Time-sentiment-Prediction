import time
from flask import Flask, jsonify, request
from pyspark.ml import PipelineModel
import json
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import pandas as pd
from pyspark.ml import PipelineModel
from pymongo import MongoClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
app = Flask(__name__)
client = MongoClient('mongodb', 27017)
db = client['twitter_database']
collection = db['twitter_collection']
# Load the pre-trained model
try:
    model = PipelineModel.load('pipeline_model/')
except Exception as e:
    print(f"Error loading model: {e}")
    model = None
# Création de la session Spark
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .getOrCreate()


@app.route('/predict', methods=['POST'])
def predict():
    if model is None:
        return jsonify({'error': 'Model not loaded'}), 500

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Invalid request'}), 400
    schema = StructType([
        StructField("Tweet ID", IntegerType(), True),
        StructField("Entity", StringType(), True),
        StructField("Tweet content", StringType(), True)
        ])
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame([json.loads(data)],schema=schema) 
    print(spark_df.show())

    # Transform the DataFrame using the model
    transformed_data = model.transform(spark_df)

    # Convert Spark DataFrame to Pandas DataFrame
    transformed_df = transformed_data.toLocalIterator()
    def convert(lst):
        # Convert the list of Row objects into a list of dictionaries
        data_dict_list = []

        for row in lst:
            #data_dict = row.asDict()
            # Extract only the required fields
            tweet_id = row["Tweet ID"]
            tweet_entity = row["Entity"]
            tweet_content = row["Tweet content"]
            
            prediction = row["prediction"]
        
        # Create a dictionary with the extracted fields
            data_dict = {"Tweet ID": tweet_id,"Entity":tweet_entity, "Tweet content": tweet_content, "Sentiment": prediction, "timestamp": time.time()}
            data_dict_list.append(data_dict)

        print(data_dict_list[0])
        return data_dict_list
    # Perform further processing with the transformed data
    #records =pd.DataFrame(list(transformed_df)).to_dict()
    res = convert(list(transformed_df))
    print(list(transformed_df))
    collection.insert_one(res[0])
    
    return "success"
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
