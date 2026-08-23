FROM rayproject/ray:2.49.2-py311

ARG RELEASE_VERSION=dev
ARG SOURCE_COMMIT=unknown

LABEL org.opencontainers.image.title="Detectra jobs" \
      org.opencontainers.image.description="Ray training and DVC ETL runtime for Detectra" \
      org.opencontainers.image.source="https://github.com/njchathura-boop/Bot-Review-Campaign" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}"

USER root
WORKDIR /opt/project
COPY pyproject.toml README.md ./
COPY src ./src
COPY training ./training
COPY scripts ./scripts
COPY orchestration ./orchestration
COPY dvc.yaml dvc.lock .dvcignore ./
COPY data/raw.dvc ./data/raw.dvc
RUN pip install --no-cache-dir ".[campaign-training,streaming,mlops]" \
    && dvc init --no-scm \
    && mkdir -p data/raw data/processed data/checkpoints artifacts reports/generated \
    && chown -R ray:users /opt/project
USER ray

ENV PYTHONPATH=/opt/project/src \
    MLFLOW_TRACKING_URI=http://detectra-mlflow:5000

CMD ["ray", "start", "--head", "--port=6379", "--dashboard-host=0.0.0.0", "--metrics-export-port=8080", "--block"]
