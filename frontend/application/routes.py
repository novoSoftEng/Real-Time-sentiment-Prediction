from application import app, socketio
from flask import render_template
from pymongo import MongoClient
from collections import Counter
from flask import jsonify  
from bson import ObjectId
from datetime import datetime
from collections import defaultdict

# Connect to MongoDB
client = MongoClient('localhost', 27018)
db = client['twitter_database']
collection = db['twitter_collection']

@app.route('/')
def index():
    entries = list(collection.find())
    return render_template('index.html', title='index', entries=entries)




@app.route('/dashboard')
def dashboard():
    entries = list(collection.find())
    sentiments = [entry['Sentiment'] for entry in entries]
    sentiment_map = {
        0.0: 'Negative',
        1.0: 'Positive',
        2.0: 'Neutral',
        3.0: 'Irrelevant'
    }
    sentiment_categories = [sentiment_map[sentiment] for sentiment in sentiments]
    sentiment_counts = Counter(sentiment_categories)

    timestamps = [entry['timestamp'] for entry in entries]
    tweet_counts_by_timestamp = Counter([entry['timestamp'] for entry in entries])
    

    entities = [entry['Entity'] for entry in entries]
    entity_counts = Counter(entities)


    return render_template('dashboard.html', title='dashboard', sentiment_counts=sentiment_counts, timestamps=timestamps, entity_counts=entity_counts, tweet_counts_by_timestamp=tweet_counts_by_timestamp)






@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('request_table_update')

@socketio.on('request_table_update')
def handle_table_update():
    print('Received request_table_update event')
    entries = list(collection.find())
    sentiments = [entry['Sentiment'] for entry in entries]
    sentiment_map = {
        0.0: 'Negative',
        1.0: 'Positive',
        2.0: 'Neutral',
        3.0: 'Irrelevant'
    }
    sentiment_categories = [sentiment_map[sentiment] for sentiment in sentiments]
    sentiment_counts = Counter(sentiment_categories)
    emit('update_table', {'entries': entries, 'sentiment_counts': sentiment_counts})


@app.route('/update_table')
def update_table():
    entries = list(collection.find())
    # Convert ObjectId to string for JSON serialization
    for entry in entries:
        entry['_id'] = str(entry['_id'])
    return jsonify({'entries': entries})


if __name__ == '__main__':
    socketio.run(app)
