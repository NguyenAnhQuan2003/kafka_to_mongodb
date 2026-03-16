import logging
from confluent_kafka import Consumer, Producer


class KafkaToKafkaWorker:
    def __init__(self, source_conf, dest_conf, topic):
        self.consumer = Consumer(source_conf)
        self.producer = Producer(dest_conf)
        self.topic = topic

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"❌ Gửi tin nhắn thất bại: {err}")

    def run(self):
        self.consumer.subscribe([self.topic])
        logging.info(f"🚀 Started KafkaToKafkaWorker on topic: {self.topic}")
        try:
            count = 0
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

                self.producer.produce(
                    self.topic,
                    value=msg.value(),
                    callback=self.delivery_report
                )
                self.producer.poll(0)

                count += 1
                if count % 1000 == 0:
                    logging.info(f"✅ Data Sync: {count} messages...")
        finally:
            self.producer.flush(timeout=10)
            self.consumer.close()