FROM python:3.12-slim

WORKDIR /app

COPY backend-fastapi/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend-fastapi/app.py /app/app.py
COPY backend-fastapi/trace_api.py /app/trace_api.py
COPY skills/evidence.py /app/evidence.py
COPY skills/source_context.py /app/source_context.py
COPY skills/runtime_traces.py /app/runtime_traces.py

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
