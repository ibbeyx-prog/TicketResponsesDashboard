# Coverage Eye — one image, two run modes (dashboard + bot).
#
# Build:
#   docker build -t coverage-eye:latest .
#   docker tag coverage-eye:latest coverage-eye:$(git rev-parse --short HEAD)
#
# Run (dashboard):
#   docker run --rm -p 8501:8501 --env-file .env coverage-eye:latest
#
# Run (bot):
#   docker run --rm -p 8000:8000 --env-file .env coverage-eye:latest python bot.py
#
# Both services:
#   docker compose up --build

# --- Build Python dependencies (compilers stay in this stage only) ---
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# --- Runtime image (no build-essential) ---
FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TICKETS_TABLE=tickets_active \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    PORT=8000

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --create-home --shell /usr/sbin/nologin appuser

COPY --from=builder /install /usr/local
COPY . .

RUN mkdir -p /app/logs \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8501 8000

# Default service: Streamlit dashboard (override CMD for bot — see docker-compose.yml).
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
