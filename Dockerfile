# Aureon MCX - observation-only research observer with a read-only status API on :1250.
# Single process: python main_aureon.py runs the market observer, the scanner, Discord and
# the supervised Uvicorn status server under one event loop (never Uvicorn workers).
FROM python:3.12-slim AS runtime

ARG GIT_SHA=unknown
ARG GIT_BRANCH=main
ARG BUILD_NUMBER=0
ARG APP_VERSION=0.2.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg \
    AUREON_GIT_SHA=${GIT_SHA} \
    AUREON_GIT_BRANCH=${GIT_BRANCH} \
    AUREON_BUILD_NUMBER=${BUILD_NUMBER} \
    AUREON_CONFIG_DIR=/app/config \
    AUREON_LOCAL_DB_PATH=/data/sqlite/aureon_mcx.db \
    AUREON_PARQUET_DIR=/data/parquet \
    AUREON_API_HOST=0.0.0.0 \
    AUREON_API_PORT=1250

# tini forwards SIGTERM to python so the observer shuts down gracefully (persisted crash state, closed sockets)
RUN apt-get update && apt-get install -y --no-install-recommends tini curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 aureon \
    && useradd --system --uid 10001 --gid aureon --home-dir /app --shell /usr/sbin/nologin aureon

WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY aureon_mcx ./aureon_mcx
COPY config ./config
COPY main_aureon.py pyproject.toml README.md ./

# persistent research data lives outside the image: mount /data/sqlite, /data/parquet, /data/logs
RUN mkdir -p /data/sqlite /data/parquet /data/logs /data/cache \
    && chown -R aureon:aureon /app /data
VOLUME ["/data/sqlite", "/data/parquet", "/data/logs"]

USER aureon
EXPOSE 1250

# liveness: the process is alive when /api/v1/health answers (readiness is a field in the body)
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD curl -fsS http://127.0.0.1:1250/api/v1/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main_aureon.py"]
