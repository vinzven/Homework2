# Usa Python slim per ridurre la dimensione
FROM python:3.11-slim

WORKDIR /app

# Copia requirements e installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice dell'app
COPY . .

# Comando di default per avviare il servizio
CMD ["python", "main.py"]
