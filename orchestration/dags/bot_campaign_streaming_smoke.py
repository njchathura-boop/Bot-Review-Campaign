"""Manual Airflow DAG that verifies the live Kafka-to-API campaign path."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG


API_URL = os.getenv(
    "BOT_CAMPAIGN_API_URL", "http://host.docker.internal:8000"
).rstrip("/")


def _request(
    method: str, path: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        f"{API_URL}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Bot campaign API is unavailable at {API_URL}: {exc}") from exc


def submit_stream_replay() -> str:
    response = _request(
        "POST",
        "/v1/demo/replay",
        {"scenario": "coordinated-cross-product", "mode": "stream"},
    )
    job_id = response.get("job_id")
    if not job_id or response.get("status") != "queued":
        raise RuntimeError(f"API did not queue a streaming replay: {response}")
    return str(job_id)


def stream_replay_finished(*, job_id: str) -> bool:
    response = _request("GET", f"/v1/demo/replay/{job_id}")
    status = str(response.get("status", "unknown")).lower()
    if status == "completed":
        return True
    if status == "failed":
        raise RuntimeError(f"Streaming replay failed: {response}")
    return False


with DAG(
    dag_id="bot_campaign_streaming_smoke",
    description="Publish a cross-product replay and wait for Kafka/Spark/model/API materialization",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ml-platform", "retries": 0},
    tags=["bot-campaign", "kafka", "spark", "streaming", "smoke"],
) as dag:
    submit = PythonOperator(
        task_id="publish_cross_product_replay",
        python_callable=submit_stream_replay,
    )
    wait = PythonSensor(
        task_id="wait_for_campaign_materialization",
        python_callable=stream_replay_finished,
        op_kwargs={
            "job_id": "{{ ti.xcom_pull(task_ids='publish_cross_product_replay') }}"
        },
        poke_interval=10,
        timeout=timedelta(minutes=15),
        mode="reschedule",
    )
    submit >> wait
