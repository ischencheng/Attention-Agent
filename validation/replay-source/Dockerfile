FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY jetstream_replay_server.py /app/jetstream_replay_server.py

CMD ["python", "/app/jetstream_replay_server.py"]
