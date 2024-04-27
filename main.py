from flask import Flask, request, jsonify
import json
import websockets
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)

# Database configuration
db_config = {
    'dbname': 'your_database',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost'
}

# Function to create a database connection
def get_db_connection():
    conn = psycopg2.connect(**db_config)
    return conn
# Function to save tick data to the database
def save_tick_data(price, epoch):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO tick_data (price, epoch) VALUES (%s, %s)", (price, epoch))
    conn.commit()
    cursor.close()
    conn.close()

# Function to handle WebSocket connection and messages
async def websocket_task():
    uri = 'wss://ws.derivws.com/websockets/v3?app_id=1089'
    async with websockets.connect(uri) as ws:
        await ws.send('{ "ticks_history": "R_50", "adjust_start_time": 1, "count": 10, "end": "latest", "start": 1, "style": "ticks"}')
        message = await ws.recv()
        message_data = json.loads(message)
        # Assuming the message data contains 'tick' with 'price' and 'epoch'
        if 'tick' in message_data:
            price = message_data['tick']['quote']
            epoch = message_data['tick']['epoch']
            save_tick_data(price, epoch)
        await ws.close()

# Scheduler to run the WebSocket task every 10 minutes
scheduler = BackgroundScheduler()
scheduler.add_job(websocket_task, 'interval', minutes=10)
scheduler.start()

# Flask route to test database connection
@app.route('/', methods=['GET'])
def test_db():
    conn = get_db_connection()
    # Perform database operations
    conn.close()
    return jsonify({'status': 'success'})

# Start the Flask app
if __name__ == '__main__':
    app.run(debug=True)
