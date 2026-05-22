FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn --no-cache-dir

COPY upstream_server.py .

EXPOSE 8000

CMD ["python", "upstream_server.py", "--port", "8000", "--latency", "15", "--id", "s0"]
