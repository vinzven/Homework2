import psycopg2
import psycopg2.extras
import os

DB_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@datadb:5432/datadb")

def get_connection():
    """
    Restituisce una connessione PostgreSQL con cursore a dizionario.
    """
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn

def get_profiles_for_airport(airport_code):
    """
    Restituisce tutti i profili utente interessati a questo aeroporto con le soglie.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT email, high_value, low_value
            FROM airports
            WHERE airport_code = %s
        """, (airport_code,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()
