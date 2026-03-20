FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY kafka_to_postgres_sink.py /app/kafka_to_postgres_sink.py

CMD ["python", "kafka_to_postgres_sink.py"]
