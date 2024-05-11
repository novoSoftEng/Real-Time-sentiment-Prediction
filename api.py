from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO
from pymongo import MongoClient

app = Flask(__name__)
socketio = SocketIO(app)

# Connexion à MongoDB
client = MongoClient('localhost', 27017)
db = client['twitter_database']
collection = db['twitter_collection']

# Route pour récupérer les données depuis MongoDB
@app.route('/')
def index():
    return render_template('a.html')

@socketio.on('connect')
def handle_connect():
    tweets = list(collection.find({}, {'_id': 0}))
    socketio.emit('tweets', tweets)

# Point d'entrée de l'application
if __name__ == '__main__':
    socketio.run(app, debug=True,allow_unsafe_werkzeug=True)
