import json
import logging
from confluent_kafka import Consumer
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError

class KafkaToMongoWorker:
    def __init__(self, kafka_conf, mongo_uri, db_name, coll_name, topic, batch_size=1000):
        self.consumer = Consumer(kafka_conf)
        self.client = MongoClient(mongo_uri)
        self.collection = self.client[db_name][coll_name]
        self.topic = topic
        self.batch_size = batch_size
        self.batch = []

    def _flush_batch(self):
        if not self.batch:
            return
        try:
            result = self.collection.bulk_write(self.batch)
            logging.info(f"✅ Bulk write: {result.upserted_count} upserted, {result.modified_count} modified")
            self.batch.clear()
        except BulkWriteError as bwe:
            logging.error(f"❌ MongoDB Bulk Write Error: {bwe.details}")
            self.batch.clear()

    def run(self):
        self.consumer.subscribe([self.topic])
        logging.info(f"🚀 Started KafkaToMongoWorker on topic: {self.topic}")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self._flush_batch()
                    continue

                if msg.error():
                    logging.error(f"❌ Consumer error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    if '_id' in data:
                        self.batch.append(ReplaceOne({'_id': data['_id']}, data, upsert=True))
                        if len(self.batch) >= self.batch_size:
                            self._flush_batch()
                    else:
                        logging.warning("⚠️ Warning: Document missing '_id', skipping...")
                except Exception as e:
                    logging.error(f"❌ Unexpected Error: {e}")
        finally:
            self._flush_batch()
            self.consumer.close()
            self.client.close()