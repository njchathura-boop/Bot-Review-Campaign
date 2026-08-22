FROM python:3.11-slim AS runtime

ARG RELEASE_VERSION=dev
ARG SOURCE_COMMIT=unknown

LABEL org.opencontainers.image.title="Detectra API and UI" \
      org.opencontainers.image.description="Ecommerce review and bot-campaign detection API" \
      org.opencontainers.image.source="https://github.com/njchathura-boop/Bot-Review-Campaign" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    APP_ENV=container \
    WEB_DIR=/app/web \
    REVIEW_TRANSFORMER_PATH=/models/review_distilbert \
    CAMPAIGN_MODEL_PATH=/models/campaign_model

WORKDIR /app
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir --no-compile \
            --index-url https://download.pytorch.org/whl/cpu \
            "torch>=2.4,<3" && \
        python -m pip install --no-cache-dir --no-compile ".[nlp,streaming]" && \
        python -m pip install --no-cache-dir --no-compile --upgrade \
            "jaraco.context>=6.1.0" \
            "wheel>=0.46.2"
COPY web ./web
COPY artifacts/review_distilbert /models/review_distilbert
COPY artifacts/campaign_model /models/campaign_model
RUN test -s /models/review_distilbert/model/model.safetensors && \
    test -s /models/campaign_model/model_state.pt && \
    mkdir -p artifacts /tmp/detectra && \
    chown -R app:app /app /models /tmp/detectra

USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')"]
# One worker intentionally owns the in-process campaign materializer. Scale-out
# requires PostgreSQL/Redis-backed shared state before increasing replicas.
CMD ["uvicorn", "bot_campaign.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
