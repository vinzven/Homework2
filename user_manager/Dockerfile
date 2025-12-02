FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia intera cartella app dentro /app
COPY app ./app

EXPOSE 8081 50051

ENV PYTHONPATH=/app
CMD ["python", "app/main.py"]


