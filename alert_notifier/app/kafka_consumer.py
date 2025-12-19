from kafka import KafkaConsumer
import json
import time
import logging
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "to-notifier"

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        logging.info(f"Connesso a Kafka: {TOPIC} su {KAFKA_BOOTSTRAP_SERVERS}")
    except NoBrokersAvailable:
        logging.warning("Kafka non disponibile, ritento in 5 secondi...")
        time.sleep(5)
