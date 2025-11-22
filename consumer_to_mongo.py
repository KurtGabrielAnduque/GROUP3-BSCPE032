"""
Storage Consumer
Responsibility: This consumer must consume the data that is produce by our producer
and it must send it into the mongoDB
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
KAFKA_TOPIC = "streaming-data"
KAFKA_BROKER = "localhost:9092"
# I will hide my password
MONGO_URI = "mongodb+srv://qkgaanduque_db_user:<dbpassword>@groceryinventorysystem.ntr788c.mongodb.net/?appName=GroceryInventorySystem"
DB_NAME = "BIGDATA_PROJECT"
COLLECTION_NAME = "air_quality"

def get_mongo_collection():
    """Connects to MongoDB and returns the collection"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Trigger a connection to check if server is active
        client.server_info()
        db = client[DB_NAME]
        return db[COLLECTION_NAME]
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return None

def process_message(message_data):
    """
    Prepares data for insertion. 
    Crucial: Converts timestamp string to actual Date object for efficient querying.
    """
    if 'timestamp' in message_data:
        try:
            # Convert string timestamp to datetime object for MongoDB
            ts_str = message_data['timestamp']
            if ts_str.endswith('Z'):
                ts_str = ts_str[:-1]
            message_data['timestamp'] = datetime.fromisoformat(ts_str)
        except ValueError:
            pass # Keep as string if parsing fails
    return message_data

def main():
    print("💾 Starting Storage Consumer...")
    
    # 1. Connect to MongoDB
    collection = get_mongo_collection()
    if not collection:
        print("Exiting: MongoDB not available.")
        return

    # 2. Connect to Kafka
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='storage-group', # Important: Distinct group ID so it doesn't steal msgs from Dashboard
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ Connected to Kafka!")
        except NoBrokersAvailable:
            print("⏳ Kafka broker unavailable. Retrying in 5s...")
            time.sleep(5)

    # 3. Consumption Loop
    print("🚀 Listening for messages to save...")
    try:
        for message in consumer:
            data = message.value
            if data:
                # Process and Insert
                clean_data = process_message(data)
                collection.insert_one(clean_data)
                print(f"📝 Saved record: {clean_data.get('timestamp')} | Sensor: {clean_data.get('sensor_id')}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()