from confluent_kafka import Consumer
from pymongo import MongoClient
import json

mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["warehouse"]
collection = db["product_view"]

conf = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
    'group.id': 'mongo-saver-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['product_view'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue

        data = json.loads(msg.value().decode('utf-8'))
        collection.insert_one(data)
        print("Inserted 1 document to MongoDB")
finally:
    consumer.close()