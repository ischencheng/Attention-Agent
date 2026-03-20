FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY llm_worker.py /app/llm_worker.py

CMD ["python", "llm_worker.py"]
