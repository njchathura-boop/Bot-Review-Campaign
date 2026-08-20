# Airflow training scheduler

Airflow is the outer workflow scheduler. It decides **when** training runs and submits
jobs to Ray. Ray Tune's ASHA scheduler remains inside each trainer and decides which
hyperparameter trials continue or stop. MLflow records every trial and selected model.

There are two manual DAGs:

- `bot_campaign_model_retraining` rebuilds data and trains candidate models through Ray.
- `bot_campaign_streaming_smoke` publishes a cross-product replay and succeeds only
  when Kafka, Spark, the hybrid model, and FastAPI complete the round trip.

Airflow is not used as the process supervisor for the never-ending Kafka consumers or
Spark query. Docker Compose manages them locally; Kubernetes should manage them in a
production environment.

The DAG is `dags/bot_campaign_training.py`. It runs these tasks sequentially:

```text
submit temporal ETL Ray Job
  -> wait without occupying an Airflow worker slot
  -> submit review Ray Job
  -> wait without occupying an Airflow worker slot
  -> submit campaign Ray Job
  -> wait without occupying an Airflow worker slot
```

The first job runs `dvc repro build_temporal_bundle build_dataset_bundle` inside the
project Ray image. `dvc.yaml` is therefore the single source of truth for all Amazon
categories, labelled inputs, seeds, scenario counts, dependencies, and outputs. When a
DVC remote is configured, the job restores `data/raw.dvc` first and pushes successful
outputs. This keeps Airflow as the workflow orchestrator without duplicating ETL settings
inside the DAG.

It defaults to manual triggering because full DistilBERT training is expensive. Set a
cron only after a full manual run passes acceptance checks. The DAG registers model
versions but deliberately does not promote or deploy them automatically.

Scheduled outputs are written below `artifacts/candidates/`, not over the bundles
mounted by FastAPI and the campaign scorer. After evaluation, promote an approved
MLflow model version through the deployment workflow; do not copy a candidate into a
production mount merely because its training job succeeded.

## Mount the DAG into Airflow 3.1

For a self-contained local scheduler, run the repository's pinned Airflow stack from
this directory:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml up airflow-init
docker compose -f orchestration/docker-compose.airflow.yml up -d airflow-dag-processor airflow-scheduler airflow-api-server
```

Open `http://localhost:8084` and sign in with the configured admin credentials (the
development defaults are `admin`/`admin`). The Compose file mounts this repository's DAG
and project root directly; no second copy of the DAG is required. Set a strong Fernet
key and admin password through environment variables before sharing the stack.

Add this read-only volume to the common Airflow service volumes in the Airflow Compose
file so the DAG processor, scheduler and workers see the same file:

```yaml
volumes:
  - "C:/Users/njcha/Desktop/My Files/IITM/SEM3/MLOPS/Bot_Campaign_Project/orchestration/dags:/opt/airflow/dags/bot_campaign:ro"
```

Add these environment values to the common Airflow environment:

```yaml
environment:
  BOT_CAMPAIGN_RAY_JOBS_URL: http://host.docker.internal:8265
  BOT_CAMPAIGN_MLFLOW_URI: http://mlflow:5000
  BOT_CAMPAIGN_GPUS_PER_TRIAL: "1"
  BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS: "1"
  # Leave unset for manual-only runs. Example weekly Sunday at 02:00 UTC:
  # BOT_CAMPAIGN_RETRAIN_CRON: "0 2 * * 0"
```

Recreate the Airflow DAG processor, scheduler and worker after changing their Compose
file. In the Airflow UI, enable `bot_campaign_model_retraining` and trigger it manually.

To validate the online path, first start the stream services using the command in
`docs/LIVE_STREAMING_PIPELINE.md`, then enable and trigger
`bot_campaign_streaming_smoke`. The CLI equivalents are:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_streaming_smoke

docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_model_retraining
```

## Required services

Before triggering the DAG:

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml `
  up -d mlflow ray-head ray-worker
```

Verify Ray at `http://localhost:8265` and MLflow at `http://localhost:5001`. The Airflow
container reaches Ray through `host.docker.internal`; the submitted Ray driver reaches
MLflow through the internal `mlflow:5000` Compose hostname.
