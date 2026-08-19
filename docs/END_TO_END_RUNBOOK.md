# Bot Campaign Detection — End-to-End Runbook

This is the single execution order for the project. Run commands from the repository
root in PowerShell. The production path uses Linux containers for Ray because Windows
Application Control can block Ray's native `raylet.exe`.

## 0. What each dataset is used for

| Dataset | Model/step | Current full split |
|---|---|---:|
| `data/processed/dataset_bundle/text/training.jsonl` | Individual review DistilBERT training | 49,946 rows |
| `data/processed/dataset_bundle/text/real/validation.jsonl` | Review threshold/calibration | 9,260 rows |
| `data/processed/dataset_bundle/text/real/test.jsonl` | Final review evaluation | 9,168 rows |
| `data/processed/temporal_bundle/campaign_v3/train.jsonl` | Hybrid campaign training | 571,312 groups |
| `data/processed/temporal_bundle/campaign_v3/validation.jsonl` | Campaign threshold selection | 122,405 groups |
| `data/processed/temporal_bundle/campaign_v3/test.jsonl` | Final campaign evaluation | 122,499 groups |

The campaign files are group-level examples generated from Amazon behavioral timestamps;
they are not the same schema as the individual review files.

## 1. Preflight checks

### Use only the canonical checkout

This machine has had two clones with the same Compose project name (`bot-campaign`).
Running `docker compose` from the older clone can replace the API container and make the
UI appear to lose its palette or recent features. The current checkout is:

```text
C:\Users\njcha\Desktop\My Files\IITM\SEM3\MLOPS\Bot_Campaign_Project
```

Confirm the served page before a demo:

```powershell
Set-Location 'C:\Users\njcha\Desktop\My Files\IITM\SEM3\MLOPS\Bot_Campaign_Project'
$page = Invoke-WebRequest http://localhost:8000 -UseBasicParsing
[regex]::Match($page.Content, '<title>(.*?)</title>').Groups[1].Value
$page.Content.Contains('Run Detectra on your machine')
```

The expected values are `Detectra | Review Intelligence` and `True`. Use
`scripts/start_detectra.ps1` for subsequent starts because it resolves the canonical
repository path and validates a current-UI marker before invoking Compose.

```powershell
git switch feature/njc
git pull --ff-only
python --version
docker version
docker compose version
Get-PSDrive C
```

Use Python 3.11 or 3.12. Give Docker Desktop at least 12–16 GB memory and keep at
least 20 GB free disk space for full DistilBERT training. Confirm the GPU:

```powershell
nvidia-smi
```

Confirm generated datasets:

```powershell
Test-Path data/processed/dataset_bundle/text/training.jsonl
Test-Path data/processed/temporal_bundle/campaign_v3/train.jsonl
```

Do not regenerate or redownload data if these files exist and their manifests are
correct.

### Fresh-clone developer fast path

The Git repository stores code and DVC metadata, not the multi-gigabyte datasets. A new
developer must first configure the shared DVC object-store URL supplied by the team.
The current `local` remote points to a folder outside this repository and is only useful
on the machine that owns that folder.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,mlops,streaming,ui]"

# Replace this URL with the actual shared S3, MinIO, Azure, GCS, or SSH remote.
python -m dvc remote modify local url <team-dvc-storage-url>
python -m dvc pull
```

With the trained model bundles restored, start serving, streaming, and observability in
one command. Ray and Airflow are intentionally separate because Ray is a training
service and Airflow is an orchestration service, not an online inference dependency.

```powershell
docker compose --profile stream --profile observability up -d `
  api kafka kafka-init spark-master spark-worker spark-stream campaign-scorer `
  mlflow prometheus grafana elasticsearch kibana
```

Open `http://localhost:8000` and verify actual readiness rather than only checking that
containers exist:

```powershell
docker compose ps
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-RestMethod http://localhost:8000/v1/ops/summary
```

### Safe recovery when Docker fills the C: drive

Inspect usage first. Reclaim unused build layers, which can always be rebuilt:

```powershell
docker system df
docker builder prune --all --force
docker system df
```

Do not run `docker volume prune` or add `--volumes` during routine cleanup. Named
volumes may contain MLflow, Grafana, Prometheus, Elasticsearch, PostgreSQL, and other
persistent state. If an image is genuinely stale, `docker image prune --all --force`
removes only images unused by containers, but those images must be downloaded or rebuilt
later.

If DVC generation completed but reported `No space left on device` while caching output,
do not regenerate the dataset. On NTFS, complete the cache using machine-local hard
links and verify the stage:

```powershell
python -m dvc config --local cache.type hardlink,copy
python -m dvc commit --force build_temporal_bundle
python -m dvc status build_temporal_bundle
```

The expected final message is `Data and pipelines are up to date.` Commit `dvc.lock` to
Git, then use `python -m dvc push` only after the shared remote has enough free capacity.

If the laptop or Docker restarts during Ray Tune, keep the existing
`artifacts/ray_results/review-risk-distilbert` directory. Use the review command in
section 4 with `--resume`; completed trials are retained and unfinished trials restore
their latest checkpoint. Do not delete `artifacts/ray_results` before resuming.

## 2. Build data only when missing or changed

Install the local package once:

```powershell
python -m pip install -e ".[dev,mlops]"
```

For a fresh temporal bundle, add every downloaded `amazon_*.jsonl` file to
`--behavioral-input`:

```powershell
python -m bot_campaign.cli build-temporal-bundle `
  --behavioral-input data/raw/amazon_all_beauty.jsonl `
    data/raw/amazon_amazon_fashion.jsonl `
    data/raw/amazon_appliances.jsonl `
  --output-dir data/processed/temporal_bundle `
  --campaign-scenario-count 2000 `
  --seed 42

python -m bot_campaign.cli generate-campaign-splits `
  --profile data/processed/temporal_bundle/behavior/profile.json `
  --products data/processed/temporal_bundle/behavior/products.jsonl `
  --output-dir data/processed/temporal_bundle/campaign_v3 `
  --count 816216 `
  --seed 42
```

Do not pass campaign files to the text-label loader.

## 3. Start MLflow and Ray

Run one Ray trial at a time because the Ray head and worker share the Docker memory
pool:

```powershell
$env:RAY_MAX_CONCURRENT_TRIALS = "1"
docker compose -f docker-compose.yml -f docker-compose.gpu.yml `
  up -d --build mlflow ray-head ray-worker
docker compose ps
```

Open Ray at http://localhost:8265 and MLflow at http://localhost:5001.

## 4. Train the complete individual-review model

This uses every review-training row because there is no `--smoke` or `--max-*` option:

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml `
  --profile train-review run --rm --entrypoint ray ray-review-trainer job submit `
  --address http://ray-head:8265 --no-wait -- `
  python /opt/project/training/ray_review_train.py `
  --ray-address auto `
  --mlflow-uri http://mlflow:5000 `
  --ray-storage-path /opt/project/artifacts/ray_results `
  --train-data /opt/project/data/processed/dataset_bundle/text/training.jsonl `
  --validation-data /opt/project/data/processed/dataset_bundle/text/real/validation.jsonl `
  --test-data /opt/project/data/processed/dataset_bundle/text/real/test.jsonl `
  --output /opt/project/artifacts/review_distilbert `
  --num-samples 20 `
  --epochs 4 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1 `
  --max-concurrent-trials 1 `
  --minimum-precision 0.90
```

For a restarted run, append `--resume` to the Python command above:

```text
python /opt/project/training/ray_review_train.py ... --resume
```

MLflow experiment: `review-risk-distilbert`.

## 5. Train the complete hybrid campaign model

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml `
  --profile train run --rm --entrypoint ray ray-trainer job submit `
  --address http://ray-head:8265 --no-wait -- `
  python /opt/project/training/ray_train.py `
  --ray-address auto `
  --mlflow-uri http://mlflow:5000 `
  --ray-storage-path /opt/project/artifacts/ray_results `
  --train-data /opt/project/data/processed/temporal_bundle/campaign_v3/train.jsonl `
  --validation-data /opt/project/data/processed/temporal_bundle/campaign_v3/validation.jsonl `
  --test-data /opt/project/data/processed/temporal_bundle/campaign_v3/test.jsonl `
  --output /opt/project/artifacts/campaign_model `
  --num-samples 20 `
  --epochs 4 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1 `
  --max-concurrent-trials 1 `
  --minimum-precision 0.95
```

MLflow experiment: `bot-campaign-hybrid-distilbert`.

`--num-samples 20` means 20 Ray hyperparameter trials, not 20 records. Each trial
uses the complete training split. ASHA stops weak trials early.

## 6. Verify model bundles and MLflow

```powershell
Get-ChildItem artifacts/review_distilbert
Get-ChildItem artifacts/campaign_model
Get-Content artifacts/review_distilbert/bundle.json
Get-Content artifacts/campaign_model/bundle.json
```

In MLflow (`http://localhost:5001`), inspect `review-risk-distilbert` and
`bot-campaign-hybrid-distilbert`. Review the selected run, test PR-AUC/ROC-AUC,
precision, recall, F1, threshold, Git SHA, data hashes, and registered model version.

## 7. Start the FastAPI UI and monitoring

```powershell
docker compose up -d --build api prometheus grafana
docker compose ps
```

| Service | URL |
|---|---|
| Review UI | http://localhost:8000 |
| API documentation | http://localhost:8000/docs |
| Readiness | http://localhost:8000/health/ready |
| Operations summary | http://localhost:8000/v1/ops/summary |
| Prometheus metrics | http://localhost:8000/metrics |
| Prometheus dashboard | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |

Verify the API:

```powershell
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-RestMethod http://localhost:8000/v1/ops/summary
```

Open the UI, click **Scan review**, then click **Replay campaign**. Confirm the review
result contains `review-risk-distilbert-v1` and campaign replay contains
`hybrid-distilbert`.

## 8. Kibana and log observability

Kibana is provided through the optional `observability` Compose profile. Elasticsearch
stores indexed events and Kibana provides search, dashboards, and saved views over those
events. Start the local stack with:

```powershell
docker compose --profile observability up -d elasticsearch kibana
docker compose ps elasticsearch kibana
```

Open `http://localhost:5601`. Check Elasticsearch first with
`Invoke-RestMethod http://localhost:9200/_cluster/health`.

This Compose profile intentionally disables Elasticsearch security for a local demo and
stores data in the `elasticsearch-data` volume. Production must enable TLS, authentication,
role-based access, and a managed secret; do not expose this development profile publicly.

The local API currently writes operational output to container stdout, so
`docker compose logs -f api` is the immediate local log view. To populate Kibana, deploy
Elastic Agent/Filebeat or Logstash to read Docker JSON logs, add `service`, `environment`,
`git_commit`, `model_version`, and `request_id`, and write to an index such as
`bot-campaign-logs-*`. In Kubernetes, use an Elastic Agent DaemonSet or Elastic
container-log integration; do not mount the Docker socket into the public API container.

Recommended Kibana fields are `@timestamp`, `log.level`, `service.name`, `http.route`,
`http.status_code`, `duration_ms`, `model_version`, `campaign_id`, `review_id`, and
`error.type`. Kibana is for log investigation; Prometheus/Grafana remains the source for
numeric latency, throughput, error-rate, resource, drift, and Kafka-lag alerts.

## 9. Start Kafka and Spark streaming

```powershell
docker compose --profile stream up -d --build `
  kafka kafka-init spark-master spark-worker spark-stream campaign-scorer api
```

Trigger the complete cross-product path through the API:

```powershell
$body = @{
  scenario = "coordinated-cross-product"
  mode = "stream"
} | ConvertTo-Json

$job = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8000/v1/demo/replay" `
  -ContentType "application/json" -Body $body

do {
  Start-Sleep -Seconds 5
  $result = Invoke-RestMethod `
    -Uri "http://localhost:8000/v1/demo/replay/$($job.job_id)"
  $result
} while ($result.status -eq "queued")
```

Inspect output:

```powershell
docker compose logs --tail 100 spark-stream campaign-scorer
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic reviews.campaign-scores.v1 `
  --from-beginning `
  --max-messages 5
```

Spark UI is http://localhost:8082. The topic flow is:

```text
reviews.raw.v1 → reviews.analysis-windows.v1 → reviews.campaign-scores.v1
```

FastAPI consumes `reviews.campaign-scores.v1`, validates its schema, and idempotently
materializes candidates for `/v1/campaigns` and the campaign UI. Detailed topic
ownership, dead-letter handling, processing guarantees, and troubleshooting are in
`docs/LIVE_STREAMING_PIPELINE.md`.

## 10. Airflow scheduled execution

```powershell
docker compose -f orchestration/docker-compose.airflow.yml up airflow-init
docker compose -f orchestration/docker-compose.airflow.yml up -d airflow-scheduler airflow-api-server
```

Open http://localhost:8080 and trigger `bot_campaign_model_retraining`. Configure
`BOT_CAMPAIGN_BEHAVIORAL_INPUTS` and `BOT_CAMPAIGN_CAMPAIGN_SCENARIO_COUNT` before a
full scheduled run. Airflow writes candidates under `artifacts/candidates/`; it never
automatically replaces production bundles.

To make Airflow trigger a bounded end-to-end streaming verification, enable and trigger
`bot_campaign_streaming_smoke`. From PowerShell:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_streaming_smoke
```

Airflow triggers and waits for the replay, but Docker Compose remains responsible for
the long-running Kafka, Spark, scorer, and API services.

## 11. Kubernetes deployment

The Kubernetes implementation is in `k8s/base.yaml`, and the Argo CD application is in
`deploy/argocd-application.yaml`. The manifest provides a `bot-campaign` namespace,
three API replicas, readiness/liveness probes, resource limits, an HPA, and a pod
disruption budget. It uses an immutable Git-SHA image placeholder:

`ghcr.io/njchathura-boop/bot-review-campaign:REPLACE_WITH_GIT_SHA`

For a configured cluster, apply and inspect it with:

```powershell
kubectl apply -f k8s/base.yaml
kubectl -n bot-campaign rollout status deployment/bot-campaign-api --timeout=180s
kubectl -n bot-campaign get pods,svc,hpa
```

For Argo CD, apply `deploy/argocd-application.yaml` from the Argo CD control plane. Argo
CD watches the repository revision and reconciles the `k8s/` directory. The public UI
cannot mutate Kubernetes. Production must provision the trained model bundle through a
protected object-store download, model PVC, or equivalent init process before readiness
can succeed; the image must not silently use an untracked fallback model.

## 12. CI/CD integration

CI is `.github/workflows/ci.yml`. Pull requests run dependency installation, smoke
training, Airflow DAG compilation, Ruff, tests, Docker BuildKit, and Trivy image scanning.
The workflow blocks merging when quality or security gates fail.

CD is `.github/workflows/cd.yml`. A `v*` tag or manual workflow dispatch builds and pushes
an immutable Git-SHA image to GHCR, scans it, renders `k8s/base.yaml`, deploys the staging
environment, waits for the API rollout, and calls `/health/ready` from inside the cluster.

Configure a protected GitHub environment named `staging` with a base64-encoded
`KUBE_CONFIG_DATA` secret. Release with:

```powershell
git switch main
git pull --ff-only
git tag -a v1.1.0 -m "Bot campaign detection v1.1.0"
git push origin v1.1.0
```

Inspect GitHub **Actions -> cd** for the image digest, rollout, and smoke-test result.
Rollback Kubernetes with `kubectl -n bot-campaign rollout undo deployment/bot-campaign-api`.

## 13. Ray out-of-memory recovery

The message `ray::IDLE` followed by `RayOutOfMemoryError` means the Docker memory pool
was exhausted. It is not a model-code error.

In the observed failing stack, every container had a `7.611 GiB` limit. Airflow was
using about `1.3 GiB`, the API about `0.8 GiB`, Ray head about `0.8 GiB`, MLflow about
`0.5 GiB`, and other Airflow/Kafka services consumed the remainder. Ray reported
`9.90 GiB` because it detected the host differently from Docker's container cgroup;
the smaller Docker limit wins.

The Ray log also warned that `/dev/shm` was only 2 GB and the object store fell back to
`/tmp`. The project Compose file now allocates 4 GB shared memory for Ray containers.
Recreate the Ray services after pulling this change.

```powershell
docker stats
docker system df
docker compose ps
```

Then:

1. Stop unrelated containers from Docker Desktop.
2. Increase Docker Desktop memory to at least 12–16 GB, preferably more for the full
   campaign run.
3. Keep `--max-concurrent-trials 1` and use one GPU.
4. Run a pilot first with `--num-samples 4 --epochs 2`.
5. Only after the pilot completes, run `--num-samples 20 --epochs 4`.

Do not disable Ray's memory monitor or start several full training jobs at once. Stop a
stale job from the Ray dashboard before retrying.

## 14. Final acceptance checklist

- MLflow contains completed selected runs and registered model versions.
- Both model `bundle.json` files exist.
- `/health/ready` reports both models ready.
- UI review scan returns the DistilBERT version.
- UI campaign replay returns the hybrid campaign engine.
- Prometheus shows request and latency metrics.
- Grafana shows the Prometheus data source and operational panels.
- Kibana opens when the observability profile is enabled and receives events after a log
  shipper is configured.
- Kafka/Spark logs show processed events and no checkpoint errors.
- Kubernetes rollout and the GitHub Actions CD smoke test succeed in staging.
- Production predictions contain Git, model, data, feature, schema, and image lineage.

## 15. Presenter-ready demonstration (10 minutes)

Use this section as the live demonstration script. A demo should use the already
generated datasets and trained model bundles. Do not start a 20-trial, full-data Ray
run during a ten-minute presentation; show the completed MLflow run and use the UI,
streaming, and monitoring services live.

### 15.1 Before the audience arrives (one-time preparation)

Run this command from the repository root and wait until the readiness checks succeed:

```powershell
docker compose --profile stream --profile observability up -d `
  api kafka kafka-init spark-master spark-worker spark-stream campaign-scorer `
  mlflow prometheus grafana elasticsearch kibana
docker compose ps
```

Start Ray only if the demonstration includes the training dashboard:

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d ray-head ray-worker
```

The optional Airflow stack is started separately:

```powershell
docker compose -f orchestration/docker-compose.airflow.yml up airflow-init
docker compose -f orchestration/docker-compose.airflow.yml up -d airflow-scheduler airflow-api-server
```

Check the two model bundles and the API before presenting:

```powershell
Test-Path artifacts/review_distilbert/bundle.json
Test-Path artifacts/campaign_model/bundle.json
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-RestMethod http://localhost:8000/v1/ops/summary
```

If either bundle is missing, complete sections 4--6 first. For a reliable demo, keep
the following browser tabs open: UI (`8000`), MLflow (`5001`), Grafana (`3000`), Ray
Dashboard (`8265`), Airflow (`8080`), Spark UI (`8082`), and Kibana (`5601`).

### 15.2 Demo sequence and speaking notes

| Time | What to do | What to explain | Evidence to point at |
|---:|---|---|---|
| 0:00--0:45 | Open the UI home page | This is an ecommerce trust workflow, not an automatic ban system. Individual review risk is evidence for a moderator; campaign risk is calculated separately. | API healthy badge, review/campaign counters, navigation tabs |
| 0:45--2:30 | Select a genuine example and click **Scan review** | The API validates the payload, runs the DistilBERT text model, computes behavioral features, searches recent context, and returns model/data/schema lineage. | Animated stages, risk percentage, confidence, latency, model version, evidence list |
| 2:30--3:45 | Select a promotional or coordinated example and scan it | Similar language alone is not proof. The result combines language, timing, account/product behavior, rating burst, and campaign evidence. | Risk badge, similar-review count, temporal evidence, campaign link |
| 3:45--4:30 | Click **Replay campaign** | Replay publishes held-out events to Kafka. Spark Structured Streaming builds time windows and cross-product graph candidates; the campaign scorer writes the result back to Kafka/API. | Replay progress, campaign result, `reviews.campaign-scores.v1` messages |
| 4:30--5:15 | Open the Campaigns page | A campaign is a coordinated group, not a single suspicious review. Moderators can confirm, dismiss, or restore visibility; the model never enforces a soft limit by itself. | Campaign ID, products, accounts, timeline, similarity, burst intensity, decision state |
| 5:15--6:00 | Open Deployment | FastAPI serves predictions; Kafka transports events; Spark aggregates streaming behavior; MLflow stores models; PostgreSQL/Redis/MinIO provide state and artifacts. | Service cards, image/Git SHA, readiness, deployment timestamp |
| 6:00--6:45 | Open Monitoring and Grafana | Prometheus scrapes numeric metrics, Grafana visualizes them, and alerts cover latency, errors, lag, drift, and resource saturation. | p95 latency, throughput, error rate, consumer lag, drift panels |
| 6:45--7:30 | Open Kibana | Elastic is for searchable structured logs. It is intentionally separate from Prometheus: metrics answer “how much/how fast,” logs answer “which request and error.” | `bot-campaign-logs-*`, request ID, route, status, model version |
| 7:30--8:15 | Open MLflow and Ray Dashboard | Ray Tune explores hyperparameters in parallel and reports metrics; MLflow records each trial/selected run and registers the reproducible model. | Experiment name, trial metrics, selected run, model version, Ray resources |
| 8:15--9:00 | Open Airflow | Airflow is the scheduler and dependency manager. It runs data validation, feature generation, training, evaluation, and promotion checks on a schedule; it does not replace Kafka or Spark. | DAG graph, task logs, run state, candidate output directory |
| 9:00--9:40 | Show GitHub Actions and version history | A pull request runs tests/security checks. A release tag builds an immutable Git-SHA image and deploys staging through Argo CD/Kubernetes. DVC tracks data manifests; MLflow tracks model artifacts. | CI green checks, image tag/digest, commit SHA, DVC hash, lineage table |
| 9:40--10:00 | State safeguards and close | Human review is required for enforcement, synthetic data is capped, test data is real-only, and rollback is available for both image and model. | Responsible-AI notice, acceptance checklist, rollback command |

### 15.3 Live commands to support the demo

Use these only when you need terminal evidence during the presentation:

```powershell
# One-line service and health view
docker compose ps
docker compose -f orchestration/docker-compose.airflow.yml ps
Invoke-RestMethod http://localhost:8000/health/ready

# API and streaming logs
docker compose logs --tail 40 api
docker compose logs --tail 40 spark-stream campaign-scorer

# Show a Kafka score event
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic reviews.campaign-scores.v1 `
  --from-beginning --max-messages 3

# Show current resource usage without changing anything
docker stats --no-stream
```

To generate a small, deterministic streaming demonstration, use a held-out file and
rate-limit it so the audience can see the stages:

```powershell
python streaming/producer.py `
  --input data/processed/temporal_bundle/campaign_v3/test.jsonl `
  --bootstrap-servers localhost:9092 `
  --rate 10
```

### 15.4 What each project component does

| Component | Responsibility in this project | The sentence to say in the demo |
|---|---|---|
| Git/GitHub | Source history, pull requests, release tags, rollback point | “Every code and configuration change has an auditable commit.” |
| DVC + MinIO | Content-addressed dataset/manifests and reproducible pipeline inputs | “The data version is pinned independently from the code version.” |
| Airflow | Scheduled orchestration and task dependencies | “Airflow decides when the pipeline runs and in what order.” |
| Kafka | Durable event transport between producers, Spark, and scorers | “Kafka decouples ingestion from online processing and absorbs bursts.” |
| Spark Structured Streaming | Time windows, feature aggregation, and cross-product graph candidates | “Spark turns individual events into campaign-level behavioral evidence.” |
| Great Expectations | Schema, null, range, and freshness checks | “Bad data is rejected before it reaches training or scoring.” |
| DistilBERT | Transformer text classifier for individual review risk | “The text model captures wording and semantic patterns beyond keyword counts.” |
| Numeric/behavior model | Rating, verification, helpful votes, timing, burst, and account/product signals | “Non-text behavior gives context that language alone cannot provide.” |
| Hybrid campaign detector | Combines text similarity, temporal behavior, and graph connectivity | “Campaign risk is group evidence, not a single-review accusation.” |
| Ray Tune | Distributed hyperparameter search and checkpoint recovery | “Ray compares many valid configurations while using the GPU efficiently.” |
| MLflow | Experiment tracking, metrics, artifacts, registry, and lineage | “The exact model used by the UI can be traced to its training run.” |
| FastAPI | Versioned scoring, replay, health, metrics, and lineage endpoints | “FastAPI is the controlled contract between models and the UI.” |
| Docker Compose | Repeatable local service environment | “Compose makes the complete stack reproducible on one machine.” |
| Kubernetes | Production replicas, probes, autoscaling, disruption protection | “Kubernetes keeps the API available and scales it under load.” |
| Argo CD | GitOps reconciliation of Kubernetes manifests | “The cluster continuously converges to the reviewed Git state.” |
| Prometheus | Time-series scraping and alert rules | “Prometheus detects numeric operational failures.” |
| Grafana | Dashboards over Prometheus (and optional data sources) | “Grafana makes health and performance visible to operators.” |
| Elastic/Kibana | Indexed structured logs and investigation/search | “Kibana answers which request, review, or error caused an incident.” |
| Evidently | Feature, embedding, prediction, and data-drift reports | “Drift tells us when production behavior no longer matches training.” |
| Trivy + RBAC/Vault | Image scanning, least privilege, and secret handling | “Security checks run before deployment and secrets are not in the UI.” |

### 15.5 Explain the end-to-end data path

Use this short narrative while showing the architecture diagram:

```text
Raw Amazon/product reviews
  -> Airflow ingestion and Great Expectations checks
  -> Spark batch cleaning/features + DVC/MinIO versioned bundle
  -> Ray Tune trains DistilBERT and hybrid campaign models
  -> MLflow records runs and registers approved bundles
  -> FastAPI loads the immutable bundles for UI/API scoring
  -> Kafka carries replay/live events
  -> Spark Streaming creates temporal and cross-product candidates
  -> Campaign scorer returns group-level evidence
  -> Prometheus/Grafana monitor metrics; Elastic/Kibana investigates logs
  -> GitHub Actions/Argo CD promote the tested image to Kubernetes
```

Emphasize the two different decisions: the review model estimates the risk of one
review, while the campaign model estimates coordinated activity across reviews,
accounts, products, and time. Only authorized human moderators can make an enforcement
decision.

### 15.6 If a service is unavailable during the demo

Do not improvise a success state. Show the actual status and explain the dependency:

```powershell
docker compose ps
docker compose logs --tail 80 <service-name>
Invoke-RestMethod http://localhost:8000/health/ready
```

Typical explanations:

- `NOT_CONFIGURED` Kafka/Spark/Kubernetes: those services were not started in the
  current local profile; the review UI can still demonstrate individual scoring.
- Kibana has no data: Elasticsearch/Kibana are running, but a log shipper has not sent
  container logs to `bot-campaign-logs-*`; use `docker compose logs -f api` locally.
- Model unavailable: the model bundle is not mounted or its checksum/version does not
  match; do not hide this with a fallback model.
- Ray job still running: show the Ray Dashboard job and explain trial status, rather
  than starting a second training job.
- Airflow is slow to start: wait for the API-server health check and show the DAG run
  state; Airflow is intentionally separate from the low-latency API path.

### 15.7 Presenter closing checklist

- [ ] UI scan completed for a genuine and coordinated example.
- [ ] Campaign replay produced a Kafka/Spark score event.
- [ ] Model and campaign results were shown separately.
- [ ] MLflow selected run and Ray trial table were shown.
- [ ] Grafana metrics and at least one operational panel were shown.
- [ ] Kibana availability/log-ingestion limitation was explained accurately.
- [ ] Airflow DAG and scheduler role were shown.
- [ ] GitHub Actions, DVC, Docker image SHA, and Kubernetes/Argo CD path were shown.
- [ ] Responsible-AI and human-moderation safeguards were stated.
