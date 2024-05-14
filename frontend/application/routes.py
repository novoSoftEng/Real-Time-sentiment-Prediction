from application import app, socketio
from flask import render_template
from pymongo import MongoClient
from collections import Counter
from flask_socketio import SocketIO, emit



# Connect to MongoDB
client = MongoClient('localhost', 27018)
db = client['twitter_database']
collection = db['twitter_collection']
entries = list(collection.find())

# Prepare data for sentiment chart
sentiments = [entry['Sentiment'] for entry in entries]

# Map sentiment values to categories
sentiment_map = {
    0.0: 'Negative',
    1.0: 'Positive',
    2.0: 'Neutral',
    3.0: 'Irrelevant'
}
sentiment_categories = [sentiment_map[sentiment] for sentiment in sentiments]

sentiment_counts = Counter(sentiment_categories)

@app.route('/')
def index():
    entries = list(collection.find())
    return render_template('index.html', title='index', entries=entries)


@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html', title='dashboard', sentiment_counts=sentiment_counts)


@socketio.on('connect')
def handle_connect():
    emit('request_table_update')

@socketio.on('request_table_update')
def handle_table_update():
    app.logger.info('Received request_table_update event')
    entries = list(collection.find())
    sentiment_counts = Counter(entry['Sentiment'] for entry in entries)
    emit('update_table', {'entries': entries, 'sentiment_counts': sentiment_counts})


if __name__ == '__main__':
    socketio.run(app)

