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
RUN python -m pip install \
      --index-url https://download.pytorch.org/whl/cpu \
      "torch>=2.4,<3" && \
    python -m pip install ".[nlp]" && \
    python -m pip install --upgrade "wheel>=0.46.2" "jaraco.context>=6.1.0" && \
    mkdir -p artifacts && \
    chown -R app:app /app

USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')"]
CMD ["uvicorn", "bot_campaign.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
