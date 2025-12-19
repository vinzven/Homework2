import logging
from app.kafka_consumer import consumer
from app.notifier import send_email

logging.basicConfig(level=logging.INFO)
logging.info("Avvio AlertNotifier...")

try:
    for msg in consumer:
        data = msg.value
        email = data.get("email")
        airport = data.get("airport")
        condition = data.get("condition")

        if not all([email, airport, condition]):
            logging.warning(f"Messaggio incompleto: {data}")
            continue

        subject = f"Aeroporto {airport}"
        body = f"Condizione superamento soglia: {condition}"
        logging.info(f"Invio email a {email} per {airport}")
        send_email(email, subject, body)

except KeyboardInterrupt:
    logging.info("AlertNotifier terminato manualmente.")
except Exception as e:
    logging.exception(f"Errore inatteso in AlertNotifier: {e}")
