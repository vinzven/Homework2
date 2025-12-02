# app/database.py

import os
import psycopg2
import psycopg2.extras

# Legge la variabile d'ambiente DATABASE_URL, fallback per lo sviluppo locale
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@datadb:5432/datadb"
)

def get_connection():
    """
    Restituisce una connessione al database PostgreSQL.
    Il cursore restituito sarà di tipo dizionario (RealDictCursor).
    """
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor
    )
