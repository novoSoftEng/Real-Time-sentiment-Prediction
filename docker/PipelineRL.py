#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when
import nltk
from nltk.corpus import stopwords


# In[4]:


# Configuration de NLTK
nltk.download('stopwords')
stop_words = stopwords.words('english')


# In[5]:


# Création de la session Spark
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .getOrCreate()


# In[6]:


# Définir le schéma pour le fichier CSV
schema = StructType([
    StructField("Tweet ID", IntegerType(), True),
    StructField("Entity", StringType(), True),
    StructField("Sentiment", StringType(), True),
    StructField("Tweet content", StringType(), True)
])


# In[7]:


# Charger les données CSV avec le schéma spécifié
df = spark.read.csv("twitter_training.csv", header=True, schema=schema)


# In[8]:


# Remplacer les valeurs nulles dans la colonne 'Tweet content' par une chaîne vide
df_cleaned = df.withColumn('Tweet content', when(col('Tweet content').isNull(), '').otherwise(col('Tweet content')))


# In[9]:


# Définir les étapes de la pipeline
tokenizer = Tokenizer(inputCol="Tweet content", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")
indexer = StringIndexer(inputCol="Sentiment", outputCol="label",handleInvalid="skip")
assembler = VectorAssembler(inputCols=["features"], outputCol="final_features")
lr = LogisticRegression(featuresCol='final_features', labelCol='label')

indexer_model = indexer.fit(df_cleaned)
indexer_model.save('indexer_model')
df_cleaned= indexer_model.transform(df_cleaned)
# In[10]:


# Créer la pipeline
pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf, assembler, lr])


# In[11]:





# In[12]:


# Entraînement de la pipeline
pipeline_model = pipeline.fit(df_cleaned)
pipeline_model.save('pipeline_model')







