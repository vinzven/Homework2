import os
import json
import time
from kafka import KafkaProducer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Retry loop fino a quando Kafka non è pronto
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("[Kafka Producer] Connesso a Kafka!")
        break
    except Exception as e:
        print(f"[Kafka Producer] Kafka non disponibile, riprovo tra 5 secondi... ({e})")
        time.sleep(5)

def notify_user(email, airport, condition):
    producer.send("to-notifier", {
        "email": email,
        "airport": airport,
        "condition": condition
    })
    producer.flush()
