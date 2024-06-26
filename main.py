import json
import websockets
import psycopg2
import asyncio
from urllib.parse import urlparse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.asyncio import AsyncIOExecutor
from fastapi import FastAPI
import logging

logging.basicConfig(level=logging.INFO)

# Database URL parsing
db_url = urlparse('postgres://king:pV7dmgZHFTL8vPmH05p7LxlygdU8h10g@dpg-combr9ol6cac73d4tvd0-a/king_l0v7')

# Database configuration
db_config = {
    'dbname': db_url.path[1:],
    'user': db_url.username,
    'password': db_url.password,
    'host': db_url.hostname,
    'port': db_url.port
}

# Database connection pool
from psycopg2 import pool
db_pool = pool.SimpleConnectionPool(1, 10, **db_config)

def get_latest_epoch():
    """Retrieve the latest epoch value from the database."""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(epoch) FROM tick_data")
            result = cursor.fetchone()
            if result[0] is not None:
                return result[0]
    except psycopg2.Error as e:
        logging.error(f"Failed to get latest epoch: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)
    return 0

def save_tick_data_batch(tick_data):
    """Save a batch of tick data to the database."""
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            # Filter out data that is older than the latest epoch in the database
            latest_epoch = get_latest_epoch()
            new_data = [(price, epoch) for price, epoch in tick_data if epoch > latest_epoch]
            if new_data:
                cursor.executemany("INSERT INTO tick_data (price, epoch) VALUES (%s, %s)", new_data)
                conn.commit()
                logging.info("New tick data saved.")
    except psycopg2.Error as e:
        logging.error(f"Failed to save tick data batch: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)

async def websocket_task():
    uri = 'wss://ws.derivws.com/websockets/v3?app_id=1089'
    try:
        async with websockets.connect(uri) as ws:
            await ws.send('{ "ticks_history": "R_50", "adjust_start_time": 1, "count": 120, "end": "latest", "start": 1, "style": "ticks"}')
            message = await ws.recv()
            message_data = json.loads(message)
            if message_data["msg_type"] == "history":
                prices = message_data['history']['prices']
                times = message_data['history']['times']
                # Use execute many for batch insertion
                tick_data = [(price, epoch) for price, epoch in zip(prices, times)]
                save_tick_data_batch(tick_data)
    except websockets.WebSocketException as e:
        logging.error(f"WebSocket connection error: {e}")

scheduler = AsyncIOScheduler(executors={'default': AsyncIOExecutor()})
scheduler.add_job(websocket_task, 'interval', minutes=1)
scheduler.start()

app = FastAPI()

@app.get("/start_data_collection")
async def start_data_collection():
    # Start the data collection task
    scheduler.add_job(websocket_task, 'interval', minutes=1)
    return {"message": "Data collection started."}

@app.get("/stop_data_collection")
async def stop_data_collection():
    # Stop the data collection task
    scheduler.remove_job('interval')
    return {"message": "Data collection stopped."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
            
