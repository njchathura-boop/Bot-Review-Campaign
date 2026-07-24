# Bot Review Campaign Detection

An end-to-end ecommerce trust demo that keeps two decisions separate:

- **Review risk** classifies one review from labeled text.
- **Campaign risk** detects coordinated reviews from text similarity, shared products,
  distinct users, rating consistency, and time proximity.

Neither score proves that a person is a bot. Individual review risk never triggers an
automatic restriction. A campaign may be temporarily soft-limited only after moderator
confirmation, and that action is reversible and audited.

## What is runnable now

- Canonical CSV/JSON/JSONL validation and labeled-dataset adapters.
- Leakage-aware TF-IDF + Logistic Regression baseline.
- Explainable campaign detection.
- Versioned FastAPI endpoints and responsive review-scanning UI.
- Deterministic 500-record sample and 50,000-record full synthetic presets.
- Capped real/synthetic training-set builder.
- Kafka, Spark, Ray, and MLflow development scaffolding.
- Docker Compose, Prometheus/Grafana, Kubernetes, and GitHub Actions definitions.

The runnable local classifier is the TF-IDF baseline. DistilBERT/TensorRT is a later
model promotion, not something this repository falsely reports as already trained.
Likewise, external services shown as `configured` have environment configuration;
that label is not a network health claim.

## Project structure

```text
src/bot_campaign/
  api.py               FastAPI application factory
  routes/              Review, campaign, demo, and operations HTTP routes
  runtime.py           Application service layer
  repository.py        Thread-safe local repository interface
  observability.py     Metrics aggregation and Prometheus rendering
  config.py            Environment-based runtime configuration
  model.py             Baseline training and scoring
  campaign.py          Coordination detection
  data.py              Canonical data loading and validation
  labeled_datasets.py  Ott/MAiDE adapters and safe training mix
  synthetic.py         Reproducible scenario generation
  workflow.py          Bounded end-to-end sample workflow
web/                   Dependency-free review-scanning UI
tests/                 Unit, API, UI-contract, and end-to-end tests
```

## Prerequisites

- Python 3.11 recommended.
- Git.
- Docker Desktop only for the container stack.

Ray and Spark should run in the pinned containers. They are not expected to support
every host Python/Java combination.

## 1. Install

From the repository root in PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,synthetic]"
```

If `py -3.11` is unavailable, use a Python 3.11 executable installed on the machine.

## 2. Run the bounded end-to-end test first

```powershell
python -m pytest -q -p no:cacheprovider -p no:tmpdir
python -m ruff check src tests
python -m bot_campaign.cli e2e-demo
```

`e2e-demo` performs one reproducible workflow:

1. Generates 500 controlled records.
2. Validates every record.
3. Trains the baseline.
4. Scores a held-out partition.
5. Detects one known campaign.
6. Writes `artifacts/e2e/report.json`.

Do not move to full data until this command reports `"status": "passed"`.

With DVC installed, the equivalent reproducible targets are:

```powershell
python -m pip install -e ".[mlops]"
dvc repro generate_sample
dvc repro e2e_demo
```

## 3. Train and start the UI/API

```powershell
python -m bot_campaign.cli train
python -m uvicorn bot_campaign.api:app --reload
```

Open:

- UI: `http://127.0.0.1:8000`
- API documentation: `http://127.0.0.1:8000/docs`
- Readiness: `http://127.0.0.1:8000/health/ready`
- Prometheus metrics: `http://127.0.0.1:8000/metrics`

Recommended UI test:

1. Scan the genuine example.
2. Scan the promotional example.
3. Select a coordinated campaign example and click **Replay campaign**.
4. Inspect campaign evidence.
5. Confirm, dismiss, and restore the campaign.
6. Verify monitoring and version-lineage pages update.

## 4. Test the sample data manually

```powershell
python -m bot_campaign.cli generate-synthetic --preset sample
python -m bot_campaign.cli validate data/processed/synthetic_sample.jsonl --labeled
python -m bot_campaign.cli campaigns --data data/sample/campaign_reviews.jsonl
```

The sample preset has 500 rows. Generated files and manifests are intentionally ignored
by Git.

## 5. Prepare real labeled data

Install streaming/dataset adapters:

```powershell
python -m pip install -e ".[streaming]"
```

MAiDE-up:

```powershell
python -m bot_campaign.cli download-maide `
  --output data/processed/maide_labeled.jsonl
```

Ott corpus, after downloading and extracting it according to its license:

```powershell
python -m bot_campaign.cli prepare-ott `
  --input C:\path\to\op_spam_v1.4 `
  --output data/processed/ott_labeled.jsonl
```

Amazon Reviews 2023 is behavior/campaign data, not fake-review ground truth:

```powershell
python -m bot_campaign.cli download-amazon `
  --category All_Beauty `
  --limit 10000 `
  --output data/raw/amazon_all_beauty.jsonl
```

## 6. Generate and validate the full controlled dataset

```powershell
python -m bot_campaign.cli generate-synthetic --preset full
python -m bot_campaign.cli validate `
  data/processed/synthetic_campaigns_50k.jsonl `
  --labeled
```

Expected full counts:

| Scenario | Rows |
|---|---:|
| Organic simulations | 20,000 |
| Coordinated positive | 8,000 |
| Coordinated negative | 5,000 |
| Paraphrased campaigns | 5,000 |
| Slow-drip campaigns | 3,000 |
| Multi-product campaigns | 3,000 |
| Legitimate burst controls | 6,000 |

Synthetic scenario labels are not production truth.

## 7. Build the capped training set

Synthetic rows are limited to 25% and only the generator's `train` split is eligible:

```powershell
python -m bot_campaign.cli build-training-set `
  --real data/processed/ott_labeled.jsonl data/processed/maide_labeled.jsonl `
  --synthetic data/processed/synthetic_campaigns_50k.jsonl `
  --max-synthetic-fraction 0.25 `
  --output data/processed/training_labeled.jsonl

python -m bot_campaign.cli validate `
  data/processed/training_labeled.jsonl `
  --labeled

python -m bot_campaign.cli train `
  --data data/processed/training_labeled.jsonl `
  --output artifacts/review_model.joblib
```

Keep an untouched real-only test partition for the final report. Report real-only and
synthetic results separately.

## 8. Run the infrastructure stack

Start Docker Desktop first:

```powershell
docker compose config --quiet
docker compose up -d api prometheus grafana
docker compose ps
```

Services:

- Application: `http://localhost:8000`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- MLflow, when the distributed profile is started: `http://localhost:5001`

Distributed Kafka → Spark and Ray → MLflow instructions are in
[docs/DISTRIBUTED_PIPELINE.md](docs/DISTRIBUTED_PIPELINE.md).

## API surface

- `POST /v1/reviews/score`
- `POST /v1/reviews/batch-score`
- `GET /v1/reviews/recent`
- `GET /v1/reviews/{review_id}/trust`
- `GET /v1/reviews/{review_id}/lineage`
- `POST /v1/demo/replay`
- `GET /v1/demo/replay/{job_id}`
- `GET /v1/campaigns`
- `GET /v1/campaigns/{campaign_id}`
- `POST /v1/campaigns/{campaign_id}/decision`
- `GET /v1/operations`
- `GET /v1/monitoring`
- `GET /v1/lineage`
- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

## Versioning and deployment

Git versions source code, DVC versions dataset manifests, MLflow versions models, and
immutable Git-SHA tags version Docker images. See [docs/OPERATIONS.md](docs/OPERATIONS.md)
for branch, commit, release, deployment, monitoring, and rollback commands.
