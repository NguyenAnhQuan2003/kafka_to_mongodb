import os
import sys
import argparse
from dotenv import load_dotenv

current_path = os.path.abspath(__file__)
project_root = os.path.dirname(current_path)
sys.path.append(project_root)

from logs.config import setup_logging
from scripts.kafka_to_kafka import KafkaToKafkaWorker
from scripts.kafka_to_mongodb import KafkaToMongoWorker

load_dotenv()
setup_logging()
import logging

def start_kafka_sync():
    source_conf = {
        'bootstrap.servers': os.getenv('SOURCE_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('SOURCE_USER'),
        'sasl.password': os.getenv('SOURCE_PASS'),
        'group.id': os.getenv('SOURCE_GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }
    dest_conf = {
        'bootstrap.servers': os.getenv('DEST_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('DEST_USER'),
        'sasl.password': os.getenv('DEST_PASS'),
        'linger.ms': 10,
    }
    worker = KafkaToKafkaWorker(source_conf, dest_conf, 'product_view')
    worker.run()

def start_mongo_saver():
    kafka_conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_USER'),
        'sasl.password': os.getenv('KAFKA_PASS'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }
    worker = KafkaToMongoWorker(
        kafka_conf=kafka_conf,
        mongo_uri=os.getenv('MONGO_URI'),
        db_name=os.getenv('MONGO_DB'),
        coll_name=os.getenv('MONGO_COLLECTION'),
        topic='product_view'
    )
    worker.run()

def main():
    parser = argparse.ArgumentParser(description="Data Pipeline Runner")
    parser.add_argument(
        'worker',
        choices=['sync', 'mongo'],
        help="Chọn worker để chạy: 'sync' (Kafka to Kafka) hoặc 'mongo' (Kafka to MongoDB)"
    )

    args = parser.parse_args()

    #chạy file kèm hashtag sync hoặc mongo

    if args.worker == 'sync':
        logging.info("--- Starting Kafka to Kafka Sync Pipeline ---")
        start_kafka_sync()
    elif args.worker == 'mongo':
        logging.info("--- Starting Kafka to MongoDB Pipeline ---")
        start_mongo_saver()

if __name__ == "__main__":
    main()