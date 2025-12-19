import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from app.database import get_connection
from app.grpc_client import UserManagerClient
from app.opensky import OpenSkyClient


# Configura logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

client = OpenSkyClient()

def collect_data():
    logging.info("Inizio raccolta dati OpenSky")
    conn = get_connection()
    cur = conn.cursor()

    grpc_client = UserManagerClient()

    # utenti con aeroporti
    cur.execute("SELECT DISTINCT email FROM airports")
    users = cur.fetchall()

    for u in users:
        email = u["email"]
        logging.info(f"Controllo utente via gRPC: {email}")

        if not grpc_client.check_user(email):
            logging.warning(f"Utente non trovato: {email}")
            continue

        # aeroporti dell’utente
        cur.execute("SELECT airport_code FROM airports WHERE email=%s", (email,))
        airports = cur.fetchall()

        for a in airports:
            airport = a["airport_code"]
            logging.info(f"Raccolta voli per aeroporto {airport} dell'utente {email}")

             # delay per non superare il rate limit
            time.sleep(60)  # 1 minuto tra ogni richiesta

            dep = client.get_departures(airport)
            arr = client.get_arrivals(airport)

            # salva partenze
            for f in dep:
                cur.execute("""
                    INSERT INTO flights(
                        email, airport_code, flight_type, icao24, callsign, first_seen, origin_country
                    ) VALUES (%s,%s,'departure',%s,%s,%s,%s)
                """, (
                    email, airport,
                    f.get("icao24"), f.get("callsign"),
                    f.get("firstSeen"), f.get("originCountry")
                ))
                logging.info(f"Salvata partenza: {f.get('callsign')} da {airport}")

            # salva arrivi
            for f in arr:
                cur.execute("""
                    INSERT INTO flights(
                        email, airport_code, flight_type, icao24, callsign, last_seen, origin_country
                    ) VALUES (%s,%s,'arrival',%s,%s,%s,%s)
                """, (
                    email, airport,
                    f.get("icao24"), f.get("callsign"),
                    f.get("lastSeen"), f.get("originCountry")
                ))
                logging.info(f"Salvato arrivo: {f.get('callsign')} a {airport}")

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Raccolta dati completata")


def start_scheduler():
    logging.info("Avvio scheduler")
    scheduler = BackgroundScheduler()
    scheduler.add_job(collect_data, "interval", hours=12)  # in produzione mettere hours=12
    scheduler.start()
    logging.info("Scheduler avviato")
