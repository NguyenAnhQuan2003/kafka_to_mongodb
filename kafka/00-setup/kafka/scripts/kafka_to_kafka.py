from confluent_kafka import Consumer, Producer
import json

source_conf = {
    'bootstrap.servers': '46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'quan-product-view-sync-group',
    'auto.offset.reset': 'earliest'
}

dest_conf = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024'
}

consumer = Consumer(source_conf)
producer = Producer(dest_conf)

consumer.subscribe(['product_view'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        producer.produce('product_view', value=msg.value())
        producer.flush()
        print(f"Forwarded message: {msg.value().decode('utf-8')[:50]}...")
finally:
    consumer.close()