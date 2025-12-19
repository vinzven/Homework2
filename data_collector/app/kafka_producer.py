from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def notify_alert_system(email, airport_code, total_flights):
    """
    Invia un messaggio a Kafka topic 'to-alert-system'
    """
    msg = {
        "email": email,
        "airport_code": airport_code,
        "total_flights": total_flights
    }
    producer.send("to-alert-system", msg)
