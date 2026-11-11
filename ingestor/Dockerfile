FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install runtime dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY detectors.py ingestor.py ./

# Healthcheck verifying libraries can be imported
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import influxdb_client, paho.mqtt, sklearn; print('OK')" || exit 1

CMD ["python", "ingestor.py"]
