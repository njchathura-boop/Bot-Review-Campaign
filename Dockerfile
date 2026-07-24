FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_ENV=container

WORKDIR /app
RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml README.md ./
COPY src ./src
COPY web ./web
COPY data/sample ./data/sample
RUN python -m pip install . && \
    python -m bot_campaign.cli train \
      --data data/sample/labeled_reviews.jsonl \
      --output artifacts/review_model.joblib && \
    chown -R app:app /app

USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')"]
CMD ["uvicorn", "bot_campaign.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
