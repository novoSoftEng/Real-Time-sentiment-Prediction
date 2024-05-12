from application import app
from flask import render_template
from pymongo import MongoClient
from collections import Counter

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['twitter_database']
collection = db['twitter_collection']
entries = list(collection.find())

# Prepare data for sentiment chart
sentiments = [entry['Sentiment'] for entry in entries]
sentiment_counts = Counter(sentiments)

@app.route('/')
def index():
    return render_template('index.html', title='index', entries=entries)

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html', title='dashboard', sentiment_counts=sentiment_counts)

