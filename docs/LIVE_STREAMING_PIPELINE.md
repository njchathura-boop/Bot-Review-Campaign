# Live Kafka to campaign API pipeline

This is the implemented online path:

```text
FastAPI stream replay or platform producer
  -> Kafka reviews.raw.v1
  -> Spark Structured Streaming
  -> Kafka reviews.analysis-windows.v1
  -> hybrid DistilBERT campaign scorer
  -> Kafka reviews.campaign-scores.v1
  -> FastAPI campaign materializer
  -> GET /v1/campaigns and the campaign UI
```

The long-running Kafka, Spark, scorer, and API processes are managed by Docker Compose
locally and by a process orchestrator in production. Airflow does not supervise these
never-ending processes. Airflow can trigger and verify a bounded streaming replay using
the manual `bot_campaign_streaming_smoke` DAG.

## 1. Start the live path

Run from the repository root:

```powershell
docker compose --profile stream up -d --build `
  kafka kafka-init spark-master spark-worker spark-stream campaign-scorer api
```

`kafka-init` creates these versioned topics before consumers start:

| Topic | Producer | Consumer |
|---|---|---|
| `reviews.raw.v1` | API replay or `streaming/producer.py` | Spark |
| `reviews.analysis-windows.v1` | Spark | campaign scorer |
| `reviews.campaign-scores.v1` | campaign scorer | FastAPI materializer |
| `reviews.campaign-scores.dlq.v1` | campaign scorer | operator investigation |

Confirm the processes and topics:

```powershell
docker compose ps
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --list
```

Open the UI at `http://localhost:8000` and Spark at `http://localhost:8082`.

## 2. Trigger the flow without Airflow

This publishes four linked reviews across multiple products. It does not call the local
campaign fallback.

```powershell
$body = @{
  scenario = "coordinated-cross-product"
  mode = "stream"
} | ConvertTo-Json

$job = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/v1/demo/replay" `
  -ContentType "application/json" `
  -Body $body

$job
```

Poll the returned job until Spark and the model have materialized a campaign:

```powershell
do {
  Start-Sleep -Seconds 5
  $result = Invoke-RestMethod `
    -Uri "http://localhost:8000/v1/demo/replay/$($job.job_id)"
  $result
} while ($result.status -eq "queued")

Invoke-RestMethod -Uri "http://localhost:8000/v1/campaigns"
Invoke-RestMethod -Uri "http://localhost:8000/v1/monitoring/summary"
```

The completed replay contains a campaign ID. Its evidence contains
`transport=kafka-spark-streaming` and `campaign_scope=cross_product`.

To publish an existing canonical JSONL file instead:

```powershell
python streaming/producer.py `
  --input data/sample/campaign_reviews.jsonl `
  --bootstrap-servers localhost:9092 `
  --topic reviews.raw.v1 `
  --rate 5
```

## 3. Trigger and verify it with Airflow

Start the Airflow metadata database, scheduler, and API server:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml up airflow-init
docker compose -f orchestration/docker-compose.airflow.yml up -d `
  airflow-scheduler airflow-api-server
```

Open `http://localhost:8080`, sign in with the local development credentials
`admin`/`admin`, find `bot_campaign_streaming_smoke`, unpause it, and select
**Trigger DAG**.

The equivalent CLI trigger is:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_streaming_smoke
```

Inspect its runs:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags list-runs -d bot_campaign_streaming_smoke
```

The first task calls the API stream-replay endpoint. The sensor then polls the replay
status. It succeeds only after the scored Kafka event returns to FastAPI.

The expensive ETL and retraining DAG remains separate:

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d `
  mlflow ray-head ray-worker

docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_model_retraining
```

## 4. Observe each handoff

```powershell
docker compose logs -f spark-stream campaign-scorer api
```

Inspect a topic directly:

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic reviews.campaign-scores.v1 `
  --from-beginning `
  --max-messages 5
```

Useful endpoints:

```text
GET http://localhost:8000/v1/campaigns
GET http://localhost:8000/v1/monitoring/summary
GET http://localhost:8000/v1/ops/summary
GET http://localhost:8000/metrics
```

Prometheus exposes `campaign_stream_scores_total` and
`campaign_stream_consumer_errors_total`.

## 5. Processing guarantees and safety

The stream is at-least-once. The scorer commits an input offset only after its output is
delivered. A crash between delivery and offset commit can produce the same score twice;
FastAPI upserts using the stable graph `group_id`, making the materialization idempotent.

Malformed Spark windows go to the dead-letter topic and are committed so one poison
record cannot block a Kafka partition. Non-candidate model results are observed but are
not inserted into the campaign console. A candidate creates evidence only; it does not
apply a soft limit. Moderator confirmation remains a separate API action.

The Compose API intentionally uses one Uvicorn worker because its repository is in
memory. Before enabling the Kafka materializer on multiple Kubernetes replicas, replace
the repository with shared PostgreSQL storage and use a single consumer group.

## 6. Stop the local services

```powershell
docker compose --profile stream stop `
  api campaign-scorer spark-stream spark-worker spark-master kafka

docker compose -f orchestration/docker-compose.airflow.yml stop `
  airflow-scheduler airflow-api-server postgres
```
