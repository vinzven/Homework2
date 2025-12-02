import logging
from flask import Flask, request, jsonify
from app.database import get_connection
from app.scheduler import start_scheduler
from app.grpc_client import UserManagerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)
grpc_client = UserManagerClient()

logging.info("Avvio Data Collector...")
start_scheduler()
logging.info("Scheduler avviato")

# ------------------- ENDPOINTS -------------------

@app.post("/add_airport")
def add_airport():
    data = request.get_json() or {}
    email = data.get("email")
    airport_code = data.get("airport_code")

    if not email or not airport_code:
        return {"error": "email and airport_code are required"}, 400

    if not grpc_client.check_user(email):
        return {"error": f"User {email} does not exist"}, 404

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO airports(email, airport_code)
            VALUES (%s, %s)
        """, (email, airport_code))
        conn.commit()
        cur.close()
        return {"status": "added"}
    finally:
        conn.close()

@app.delete("/remove_airport")
def remove_airport():
    data = request.get_json() or {}
    email = data.get("email")
    airport_code = data.get("airport_code")

    if not email or not airport_code:
        return {"error": "email and airport_code are required"}, 400

    if not grpc_client.check_user(email):
        return {"error": f"User {email} does not exist"}, 404

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM airports WHERE email=%s AND airport_code=%s", (email, airport_code))
        conn.commit()
        cur.close()
        return {"status": "removed"}
    finally:
        conn.close()

@app.get("/airports/<email>")
def list_airports(email):
    if not grpc_client.check_user(email):
        return {"error": f"User {email} does not exist"}, 404

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT airport_code FROM airports WHERE email=%s", (email,))
        rows = cur.fetchall()
        cur.close()
        return {"email": email, "airports": [r["airport_code"] for r in rows]}
    finally:
        conn.close()

@app.get("/last_flight/<airport_code>")
def last_flight(airport_code):
    data = request.args
    email = data.get('email')

    if not email:
        return {"error": "email is required"}, 400

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM flights
            WHERE airport_code=%s AND email=%s
            ORDER BY created_at DESC
            LIMIT 1
        """, (airport_code, email))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"last_flight": row}
        else:
            return {"error": "No flights found for this airport and user."}, 404
    finally:
        conn.close()

@app.get("/avg/<airport_code>/<int:days>")
def avg_flights(airport_code, days):
    data = request.args
    email = data.get('email')

    if not email:
        return {"error": "email is required"}, 400

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) AS count
            FROM flights
            WHERE airport_code = %s
            AND email = %s
            AND created_at >= CURRENT_DATE - INTERVAL '%s days'
        """, (airport_code, email, days))
        count = cur.fetchone()["count"]
        cur.close()
        if count:
            return {"airport": airport_code, "days": days, "average_flights": count / days}
        else:
            return {"error": "No flights found for this airport and user."}, 404
    finally:
        conn.close()

@app.get("/health")
def health():
    return {"status": "data collector ok"}

# ------------------- RUN SERVER -------------------
if __name__ == "__main__":
    logging.info("Avvio Flask server su 0.0.0.0:8082")
    app.run(host="0.0.0.0", port=8082)
