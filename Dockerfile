FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_ENV=container

WORKDIR /app
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*
RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml README.md ./
COPY src ./src
COPY web ./web
# Packaging helpers are removed after installation to reduce runtime attack surface.
RUN python -m pip install \
      --index-url https://download.pytorch.org/whl/cpu \
      "torch>=2.4,<3" && \
    python -m pip install ".[nlp,streaming]" && \
    python -m pip install --upgrade "setuptools>=82.0.1" && \
    python -m pip uninstall -y wheel jaraco.context && \
    mkdir -p artifacts && \
    chown -R app:app /app

USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')"]
# The local API materializes Kafka campaign scores in memory, so one worker keeps
# HTTP reads and the consumer on the same process. Kubernetes must use a shared
# persistent repository before enabling the consumer on multiple replicas.
CMD ["uvicorn", "bot_campaign.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
