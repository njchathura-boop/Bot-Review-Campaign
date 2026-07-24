# Kafka, Spark, Ray, and MLflow pipeline

## Dataset decision

Amazon Reviews 2023 is the primary behavioral dataset. It has 571.54 million reviews
and is about 750 GB in full, so development defaults to a streamed, bounded sample of
`All_Beauty`. It supplies review text, rating, user ID, parent product ID, fine-grained
timestamp, helpful votes, verification status, item metadata, and graph links.
The adapter pins Hugging Face revision `269765a` so upstream file changes do not silently
alter an experiment; record a content checksum when promoting a larger snapshot.

It does **not** supply a defensible fake-review label. The pipeline therefore uses:

- Amazon Reviews 2023 for Kafka replay, Spark behavioral/temporal features, campaign
  discovery, anomaly detection, and realistic scale/load tests.
- Ott Deceptive Opinion Spam Corpus for the classic English fake/truthful benchmark.
- MAiDE-up for modern AI-generated and multilingual deception robustness.

Keep a `source` field and report per-source results. Never merge these datasets and
randomly split rows: deduplicate first and use source/group/temporal holdouts.

## Data flow

```text
Hugging Face bounded stream -> canonical JSONL -> Kafka reviews.raw.v1
                                                   |
                                                   v
                                  Spark Structured Streaming
                                  validation + event-time windows
                                                   |
                                                   v
                                      Parquet feature snapshots

Ott / MAiDE-up labeled adapters -> Ray distributed trials -> MLflow runs/registry
Amazon feature snapshots --------> campaign/anomaly model -> MLflow runs/registry
```

Spark uses a two-hour watermark, one-hour windows sliding every ten minutes, a durable
checkpoint, and Parquet output. Kafka production is idempotent and keyed by product.
For production, add a schema registry, explicit topic creation/retention, TLS/SASL,
dead-letter topics, object storage, and multiple Kafka brokers; the local single broker
is a development environment, not a high-availability deployment.

## Run locally

Use the container stack for Ray and Spark because the host currently uses Python 3.14
and Java 26, while ecosystem support is safer on the pinned Python 3.11/Java 17 images.

```powershell
# 1. Install only the lightweight download/producer dependencies on the host
python -m pip install -e ".[streaming]"

# 2. Stream a bounded sample from the repository's converted Parquet file
python -m bot_campaign.cli download-amazon --category All_Beauty --limit 10000

# 3. Start Kafka, Spark, MLflow, and Ray
docker compose up -d kafka spark-master spark-worker mlflow ray-head ray-worker

# 4. Start the Spark streaming query
docker compose --profile stream up -d spark-stream

# 5. Replay reviews into Kafka
python streaming/producer.py --input data/raw/amazon_all_beauty.jsonl --rate 100

# 6. Run Ray trials against labeled data and register the selected model
docker compose --profile train run --rm ray-trainer
```

Dashboards:

- Spark master: `http://localhost:8082`
- Spark worker: `http://localhost:8083`
- Ray: `http://localhost:8265`
- MLflow: `http://localhost:5001` (host port 5000 was already occupied locally)

The checked-in labeled file is only a smoke test. Replace it with licensed, documented
Ott/MAiDE-up adapters before interpreting MLflow metrics.

## Scale-up gates

1. Validate licensing and record source URL, revision, checksum, category, and row count.
2. Start with one category and estimate throughput/storage before adding categories.
3. Separate raw immutable events, validated canonical events, window features, and labels.
4. Measure Kafka lag, malformed-event rate, Spark input/processed rows per second,
   watermark drops, checkpoint recovery, Ray task failures, and MLflow logging failures.
5. Promote a model only after PR-AUC, calibration, per-source slices, temporal holdout,
   latency, and false-positive review gates pass.
