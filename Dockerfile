# MarketData collector + API — Railway deployment image.
# One service runs both (collector in a background thread) so they share
# the single Railway volume mounted at /data.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_ROOT=/data \
    TZ=Asia/Kolkata

# git is needed because neo-api-client installs from GitHub.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway injects $PORT; default 8000 for local runs.
# --ws none: our FastAPI app has no WebSocket routes, and uvicorn's
# auto-detection otherwise imports websockets.frames at boot, which
# doesn't exist in websockets==8.1 (hard-pinned by neo_api_client) —
# that import failure silently kills the server before it binds the
# port, which is why /health was timing out.
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000} --ws none"]
