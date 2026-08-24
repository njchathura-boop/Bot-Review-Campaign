# test commit
FROM apache/airflow:3.1.3

ARG RELEASE_VERSION=dev
ARG SOURCE_COMMIT=unknown

LABEL org.opencontainers.image.title="Detectra Airflow" \
      org.opencontainers.image.description="Airflow 3 runtime with reviewed Detectra orchestration DAGs" \
      org.opencontainers.image.source="https://github.com/njchathura-boop/Bot-Review-Campaign" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_COMMIT}"

COPY --chown=airflow:root orchestration/dags /opt/airflow/dags/bot_campaign

USER airflow
