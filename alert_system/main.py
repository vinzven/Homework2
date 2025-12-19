import logging
from app.database import get_profiles_for_airport
from app.thresholds import check_threshold
from app.kafka_producer import notify_user
from app.kafka_consumer import consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logging.info("Avvio AlertSystem...")

try:
    for msg in consumer:
        data = msg.value
        airport = data.get("airport_code")
        total = data.get("total_flights")

        if airport is None or total is None:
            logging.warning(f"Messaggio Kafka malformato: {data}")
            continue

        profiles = get_profiles_for_airport(airport)
        for p in profiles:
            email = p.get("email")
            high = p.get("high_value")
            low = p.get("low_value")

            condition = check_threshold(total, high, low)
            if condition:
                logging.info(f"Invio alert a {email} per {airport}: {condition}")
                notify_user(email, airport, condition)

except KeyboardInterrupt:
    logging.info("AlertSystem terminato manualmente.")
except Exception as e:
    logging.exception(f"Errore inatteso in AlertSystem: {e}")
