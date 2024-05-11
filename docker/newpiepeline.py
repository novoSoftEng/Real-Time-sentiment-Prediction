#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when
import nltk
from nltk.corpus import stopwords


# In[2]:


# Configure NLTK
nltk.download('stopwords')
stop_words = stopwords.words('english')


# In[3]:


# Create Spark session
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .getOrCreate()


# In[4]:


# Define schema for the CSV file
schema = StructType([
    StructField("Tweet ID", IntegerType(), True),
    StructField("Entity", StringType(), True),
    StructField("Sentiment", StringType(), True),
    StructField("Tweet content", StringType(), True)
])


# In[5]:


# Load CSV data with specified schema
df = spark.read.csv("twitter_training.csv", header=True, schema=schema)


# In[6]:


# Replace null values in the 'Tweet content' column with an empty string
df_cleaned = df.withColumn('Tweet content', when(col('Tweet content').isNull(), '').otherwise(col('Tweet content')))


# In[7]:


# Define preprocessing stages
tokenizer = Tokenizer(inputCol="Tweet content", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")


# In[8]:


# Indexer for Sentiment column (in preprocessing)
sentiment_indexer = StringIndexer(inputCol="Sentiment", outputCol="label")


# In[9]:


# Create the preprocessing pipeline (with indexer)
preprocessing_pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf, sentiment_indexer])


# In[10]:


# Fit and transform the preprocessing pipeline
preprocessed_data = preprocessing_pipeline.fit(df_cleaned).transform(df_cleaned)


# In[11]:


# Split data into training and test sets
train_data, test_data = preprocessed_data.randomSplit([0.8, 0.2], seed=123)


# In[12]:


# Assemble features for the final pipeline (without indexer)
final_assembler = VectorAssembler(inputCols=["features"], outputCol="final_features")


# In[13]:


# Logistic Regression model
lr = LogisticRegression(featuresCol='final_features', labelCol='label')


# In[14]:


# Create the final pipeline (without indexer)
final_pipeline = Pipeline(stages=[final_assembler, lr])


# In[15]:


# Train the final pipeline
pipeline_model = final_pipeline.fit(train_data)


# In[16]:


# Make predictions on the test data
predictions = pipeline_model.transform(test_data)


# In[17]:


