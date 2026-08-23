"""Airflow 3 DAG that submits memory-safe model training to the Ray Jobs API."""

from __future__ import annotations

import json
import os
import shlex
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

RAY_JOBS_URL = os.getenv(
    "BOT_CAMPAIGN_RAY_JOBS_URL", "http://host.docker.internal:8265"
).rstrip("/")
RETRAIN_CRON = os.getenv("BOT_CAMPAIGN_RETRAIN_CRON") or None
GPUS_PER_TRIAL = os.getenv("BOT_CAMPAIGN_GPUS_PER_TRIAL", "0")
MAX_CONCURRENT_TRIALS = os.getenv("BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS", "1")
PROJECT_ROOT = "/opt/project"
MLFLOW_URI = os.getenv("BOT_CAMPAIGN_MLFLOW_URI", "http://mlflow:5000").rstrip("/")


def _request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        f"{RAY_JOBS_URL}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Ray Jobs API is unavailable at {RAY_JOBS_URL}: {exc}") from exc


def _entrypoint(
    script: str,
    train_data: str,
    validation_data: str,
    test_data: str,
    output: str,
) -> str:
    arguments = [
        "python",
        f"{PROJECT_ROOT}/training/{script}",
        "--ray-address",
        "auto",
        "--mlflow-uri",
        MLFLOW_URI,
        "--ray-storage-path",
        f"{PROJECT_ROOT}/artifacts/ray_results",
        "--train-data",
        f"{PROJECT_ROOT}/{train_data}",
        "--validation-data",
        f"{PROJECT_ROOT}/{validation_data}",
        "--test-data",
        f"{PROJECT_ROOT}/{test_data}",
        "--output",
        f"{PROJECT_ROOT}/{output}",
        "--gpus-per-trial",
        GPUS_PER_TRIAL,
        "--max-concurrent-trials",
        MAX_CONCURRENT_TRIALS,
    ]
    return shlex.join(arguments)


def _data_entrypoint() -> str:
    """Reproduce the authoritative DVC data stages on the Ray workspace.

    `dvc.yaml` is the single source of truth for all Amazon categories, labelled
    inputs, scenario counts, seeds, dependencies, and outputs. The optional
    Kubernetes storage secret is inherited by the Ray job and restores/pushes
    versioned objects when a shared DVC remote is configured.
    """
    return " && ".join(
        [
            f"cd {shlex.quote(PROJECT_ROOT)}",
            "if [ -n \"${DVC_REMOTE_URL:-}\" ]; then "
            "dvc remote add --local --force \"${DVC_REMOTE_NAME:-kubernetes}\" "
            "\"$DVC_REMOTE_URL\" && "
            "dvc remote default \"${DVC_REMOTE_NAME:-kubernetes}\" && "
            "dvc pull data/raw.dvc; fi",
            "test -f data/raw/product_reviews.jsonl",
            "dvc repro build_temporal_bundle build_dataset_bundle",
            "if [ -n \"${DVC_REMOTE_URL:-}\" ]; then dvc push; fi",
        ]
    )


def submit_ray_job(*, model: str) -> str:
    if model == "data":
        entrypoint = _data_entrypoint()
    elif model == "review":
        entrypoint = _entrypoint(
            "ray_review_train.py",
            "data/processed/dataset_bundle/text/training.jsonl",
            "data/processed/dataset_bundle/text/real/validation.jsonl",
            "data/processed/dataset_bundle/text/real/test.jsonl",
            "artifacts/candidates/review_distilbert",
        )
    elif model == "campaign":
        entrypoint = _entrypoint(
            "ray_train.py",
            "data/processed/temporal_bundle/campaign/train.jsonl",
            "data/processed/temporal_bundle/campaign/validation.jsonl",
            "data/processed/temporal_bundle/campaign/test.jsonl",
            "artifacts/candidates/campaign_model",
        )
    else:
        raise ValueError(f"Unsupported model role: {model}")

    response = _request(
        "POST",
        "/api/jobs/",
        {
            "entrypoint": entrypoint,
            "metadata": {"orchestrator": "airflow", "model_role": model},
        },
    )
    submission_id = response.get("submission_id") or response.get("job_id")
    if not submission_id:
        raise RuntimeError(f"Ray did not return a submission ID: {response}")
    return str(submission_id)


def ray_job_finished(*, submission_id: str) -> bool:
    response = _request("GET", f"/api/jobs/{submission_id}")
    status = str(response.get("status", "UNKNOWN")).upper()
    if status == "SUCCEEDED":
        return True
    if status in {"FAILED", "STOPPED"}:
        message = response.get("message", "No Ray failure message was returned")
        raise RuntimeError(f"Ray job {submission_id} ended as {status}: {message}")
    return False


with DAG(
    dag_id="bot_campaign_model_retraining",
    description="Temporal ETL validation/split generation followed by Ray Tune retraining",
    schedule=RETRAIN_CRON,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ml-platform", "retries": 0},
    tags=["bot-campaign", "airflow", "spark", "ray", "mlflow", "retraining"],
) as dag:
    submit_data = PythonOperator(
        task_id="submit_temporal_etl",
        python_callable=submit_ray_job,
        op_kwargs={"model": "data"},
    )
    wait_data = PythonSensor(
        task_id="wait_for_temporal_etl",
        python_callable=ray_job_finished,
        op_kwargs={"submission_id": "{{ ti.xcom_pull(task_ids='submit_temporal_etl') }}"},
        poke_interval=60,
        timeout=timedelta(hours=24),
        mode="reschedule",
    )
    submit_review = PythonOperator(
        task_id="submit_review_training",
        python_callable=submit_ray_job,
        op_kwargs={"model": "review"},
    )
    wait_review = PythonSensor(
        task_id="wait_for_review_training",
        python_callable=ray_job_finished,
        op_kwargs={
            "submission_id": "{{ ti.xcom_pull(task_ids='submit_review_training') }}"
        },
        poke_interval=60,
        timeout=timedelta(hours=24),
        mode="reschedule",
    )
    submit_campaign = PythonOperator(
        task_id="submit_campaign_training",
        python_callable=submit_ray_job,
        op_kwargs={"model": "campaign"},
    )
    wait_campaign = PythonSensor(
        task_id="wait_for_campaign_training",
        python_callable=ray_job_finished,
        op_kwargs={
            "submission_id": "{{ ti.xcom_pull(task_ids='submit_campaign_training') }}"
        },
        poke_interval=60,
        timeout=timedelta(hours=48),
        mode="reschedule",
    )

    submit_data >> wait_data >> submit_review >> wait_review >> submit_campaign >> wait_campaign
