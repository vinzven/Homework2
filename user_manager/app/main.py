import json
import threading
from flask import Flask, request, jsonify
from app.database import get_connection
from app.grpc_service import start_grpc_server

app = Flask(__name__)

# Avvia gRPC in background
grpc_thread = threading.Thread(target=lambda: start_grpc_server(block=True), daemon=True)
grpc_thread.start()

@app.get("/health")
def health():
    return {"status": "user manager ok"}

# -------- AT-MOST-ONCE ----------
def get_request_log(conn, request_id):
    cur = conn.cursor()
    cur.execute("SELECT request_id FROM processed_requests WHERE request_id = %s", (request_id,))
    row = cur.fetchone()
    cur.close()
    return row is not None

@app.post("/register")
def register():
    data = request.get_json() or {}
    email = data.get("email")
    fiscal_code = data.get("fiscal_code")
    bank_account = data.get("bank_account")
    request_id = data.get("request_id")

    if not email or not request_id:
        return {"error": "email and request_id are required"}, 400

    conn = get_connection()
    try:
        if get_request_log(conn, request_id):
            return {"status": "already_processed"}, 200

        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()

        if user:
            result = {"status": "exists", "email": email, "fiscal_code": user["fiscal_code"], "bank_account": user["bank_account"]}
        else:
            cur.execute(
                "INSERT INTO users(email, fiscal_code, bank_account) VALUES (%s, %s, %s)",
                (email, fiscal_code, bank_account)
            )
            result = {"status": "created", "email": email, "fiscal_code": fiscal_code, "bank_account": bank_account}

        cur.execute("INSERT INTO processed_requests(request_id) VALUES (%s)", (request_id,))
        conn.commit()
        cur.close()
        return result, 201 if result["status"] == "created" else 200

    finally:
        conn.close()

@app.delete("/user/<email>")
def delete_user(email):
    data = request.get_json() or {}
    request_id = data.get("request_id")

    if not request_id:
        return {"error": "request_id is required"}, 400

    conn = get_connection()
    try:
        if get_request_log(conn, request_id):
            return {"status": "already_processed"}, 200

        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()

        if not user:
            result = {"status": "not_found", "email": email}
        else:
            cur.execute("DELETE FROM users WHERE email = %s", (email,))
            result = {"status": "deleted", "email": email}

        cur.execute("INSERT INTO processed_requests(request_id) VALUES (%s)", (request_id,))
        conn.commit()
        cur.close()

        return result, 200

    finally:
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
