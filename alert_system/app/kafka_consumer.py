import os
import json
import time
from kafka import KafkaConsumer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Retry loop fino a quando Kafka non è pronto
while True:
    try:
        consumer = KafkaConsumer(
            "to-alert-system",
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset='earliest',
            group_id='alert-system-group'
        )
        print("[Kafka Consumer] Connesso a Kafka!")
        break
    except Exception as e:
        print(f"[Kafka Consumer] Kafka non disponibile, riprovo tra 5 secondi... ({e})")
        time.sleep(5)
