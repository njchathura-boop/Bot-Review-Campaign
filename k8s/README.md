# Detectra Kubernetes deployment

For the copy-pasteable Docker Desktop demo, use
[`docs/KUBERNETES_DEMO_RUNBOOK.md`](../docs/KUBERNETES_DEMO_RUNBOOK.md). It covers
cluster checks, image publishing, secrets, data loading, full retraining, monitoring,
port forwarding, verification, cleanup, and the MLflow persistence decision.

This directory is the production-style Kubernetes entry point. It separates serving,
training, ETL, experiment tracking, and monitoring so each workload can restart and be
scaled independently.

## What runs where

| Workload | Kubernetes object | Image | Responsibility |
|---|---|---|---|
| UI + inference API | `Deployment/detectra-api` | `bot-review-campaign-api` | Serves the Detectra web UI, DistilBERT review model, campaign model, health checks, and `/metrics` |
| Model training service | `Deployment/detectra-ray` | `bot-review-campaign-jobs` | Runs the Ray head/dashboard and executes submitted tuning jobs |
| Training trigger | `Job/detectra-review-training` | `bot-review-campaign-jobs` | Submits the full DistilBERT tuning command to Ray and waits for completion |
| ETL | suspended `CronJob/detectra-etl` | `bot-review-campaign-jobs` | Pulls raw data through DVC, reproduces the temporal and training bundles, and pushes DVC outputs |
| Experiment tracking | `Deployment/detectra-mlflow` | official MLflow image | Stores trial parameters, metrics, artifacts, and model registrations |
| Metrics database | `Deployment/detectra-prometheus` | official Prometheus image | Discovers annotated pods and scrapes API and Ray metrics every 15 seconds |
| Monitoring UI | `Deployment/detectra-grafana` | official Grafana image | Provides the pre-provisioned **Detectra Kubernetes Overview** dashboard |
| Log database | `StatefulSet/detectra-elasticsearch` | official Elasticsearch image | Persists platform logs in the observability and full profiles |
| Log collector | `DaemonSet/detectra-filebeat` | official Filebeat image | Enriches Kubernetes container logs with pod metadata |
| Log UI | `Deployment/detectra-kibana` | official Kibana image | Searches `detectra-logs-*` in the observability and full profiles |
| Workflow orchestration | Airflow API, scheduler, DAG processor, and PostgreSQL | `bot-review-campaign-airflow` | Schedules ETL and submits monitored Ray jobs in the full profiles |

The monitoring tier deliberately uses separate Prometheus and Grafana pods. Combining
them in one pod would couple storage, upgrades, health checks, and failure recovery.
The API starts with one replica because campaign state is currently in process. Increase
replicas only after that state is moved to PostgreSQL or Redis.

## Why the platform is split into separate pods

A pod is Kubernetes' smallest schedulable unit: one or more tightly coupled containers
that share a network namespace and lifecycle. Detectra normally uses one main container
per pod because the components have different scaling, storage, restart, and security
requirements. Kubernetes controllers then create and replace those pods:

- A **Deployment** is used for stateless or replaceable long-running services such as the
  API, Ray head, MLflow, Prometheus, Grafana, Kibana, and Airflow processes.
- A **StatefulSet** is used when a stable identity and persistent disk matter, as with
  Elasticsearch and the Airflow PostgreSQL database.
- A **DaemonSet** runs one copy per node. Filebeat uses this form because every node can
  have container logs to collect.
- A **Job** runs finite work and records success or failure. Model-training submission,
  Kibana data-view setup, and Airflow database migration use Jobs.
- A **CronJob** creates Jobs on a schedule. The ETL CronJob is initially suspended so a
  large rebuild cannot begin merely because somebody installed the stack.

The separation is deliberate, not duplication. A UI restart must not erase MLflow runs;
a failed ETL build must not terminate inference; Prometheus retention must not share an
API filesystem; and a GPU training workload must be schedulable independently of Kibana.

### Pod and controller responsibilities

| Controller / pod | Why it is separate | Input | Output / state |
|---|---|---|---|
| `Deployment/detectra-api` | Low-latency serving and UI have a different lifecycle from training | HTTP review requests and promoted model bundles embedded in the image | Predictions, replay state, audit logs, `/metrics` |
| `Deployment/detectra-ray` | Owns distributed trial scheduling and the optional GPU | Training command plus JSONL splits on the shared workspace PVC | Ray trials, checkpoints, selected model bundle, Ray metrics |
| `Job/detectra-review-training` | A small finite submitter should finish while Ray continues to own compute | Training arguments and Ray/MLflow service addresses | One Ray Job submission and its terminal result |
| `CronJob/detectra-etl` | Data rebuilding is expensive, finite, and schedulable | DVC raw data and `dvc.yaml` | Temporal bundle, text bundle, manifests, DVC cache/remote objects |
| `Deployment/detectra-mlflow` | Experiment history must survive trainer restarts | Ray trial parameters, metrics, artifacts, tags | SQLite metadata and artifacts on its PVC |
| `Deployment/detectra-prometheus` | Metrics scraping and retention are independent of the API | Annotated API and Ray `/metrics` endpoints | Seven days of time-series data on its PVC |
| `Deployment/detectra-grafana` | Dashboard presentation can restart without losing Prometheus data | Prometheus queries and provisioned dashboard JSON | Read-only and administrator dashboard views |
| `StatefulSet/detectra-elasticsearch` | Log indices need persistent storage and stable ownership | Filebeat events | Searchable `detectra-logs-*` indices |
| `DaemonSet/detectra-filebeat` | Every Kubernetes node needs its own log collector | `/var/log/containers` and `/var/log/pods` | Metadata-enriched log events sent to Elasticsearch |
| `Deployment/detectra-kibana` | Log exploration is separate from log storage | Elasticsearch queries | Browser log-search UI |
| Airflow API, scheduler, and DAG processor Deployments | Web/API access, scheduling, and DAG parsing fail and scale differently | Versioned project DAGs and Airflow metadata | Scheduled ETL/training task instances and audit history |
| `StatefulSet/detectra-airflow-postgres` | Orchestration history needs durable relational state | Airflow metadata writes | DAG-run, task-instance, and scheduler state |

The current Kubernetes manifests do **not** yet deploy Kafka, Spark, or the online
`campaign-scorer`. Those services are complete in Docker Compose. Therefore the
Kubernetes deployment supports API serving, ETL, training, experiment tracking,
metrics, logs, and Airflow control, but it must not be presented as a complete
Kubernetes streaming data plane. The live Kafka -> Spark -> campaign-scorer demonstration
currently uses Compose until equivalent Kubernetes manifests are added.

## Resource budgets, safeguards, and current constraints

Kubernetes `requests` are the resources the scheduler reserves. `limits` cap what a
container may consume. CPU can be throttled at its limit; exceeding a memory limit can
terminate the container with `OOMKilled`. The current values favor a single-node demo
while leaving the expensive work in Ray and ETL.

| Workload | CPU request / limit | Memory request / limit | Additional constraint |
|---|---:|---:|---|
| API/UI | 1 / 4 cores | 2 / 6 GiB | One replica while campaign state is in process |
| Ray training | 2 / 8 cores | 4 / 12 GiB | Optional one NVIDIA GPU; 2 GiB memory-backed `/dev/shm` |
| Training submitter | 0.1 / 0.5 core | 256 / 512 MiB | Seven-day deadline, no automatic retry, no model compute here |
| ETL | 2 / 8 cores | 4 / 12 GiB | Suspended by default; six-hour deadline; `Forbid` overlap |
| MLflow | 0.25 / 2 cores | 512 MiB / 2 GiB | Separate persistent experiment store |
| Prometheus | 0.25 / 2 cores | 512 MiB / 2 GiB | Seven-day retention |
| Grafana | 0.2 / 1 core | 256 MiB / 1 GiB | Provisioned dashboard is read-only by default |
| Elasticsearch | 0.5 / 2 cores | 2 / 4 GiB | 1 GiB JVM heap and a persistent index volume |
| Kibana | 0.3 / 2 cores | 768 MiB / 2 GiB | Optional observability/full overlay only |
| Filebeat, per node | 0.1 / 0.5 core | 128 / 512 MiB | One pod per node |
| Each main Airflow process | 0.3 / 2 cores | 768 MiB / 2 GiB | Optional full overlay only |

Persistent claims are intentionally separate: 50 GiB for shared ETL/training data and
artifacts, 10 GiB for MLflow, 10 GiB for Prometheus, 2 GiB for Grafana, 20 GiB for
Elasticsearch, and 10 GiB for Airflow PostgreSQL. `ReadWriteOnce` is suitable for the
single Docker Desktop node. A multi-node installation needs an RWX StorageClass or
explicit co-location for ETL and Ray, otherwise the workspace can hit a multi-attach
error.

The most important optimizations and protections are:

- One Ray trial at a time prevents several DistilBERT copies competing for a 6 GB laptop
  GPU. ASHA can stop weak configurations before all epochs finish.
- API rolling updates use `maxUnavailable: 0` and `maxSurge: 1`; the old pod remains
  available while the new pod proves readiness.
- Startup, readiness, and liveness probes distinguish slow model loading from a dead
  service and keep unready pods out of Service traffic.
- The API and administrative containers use non-root identities, the default seccomp
  profile, dropped Linux capabilities, and disabled privilege escalation. Read-only root
  filesystems are used where the application supports them; writable temporary data goes
  to `emptyDir`.
- Prometheus discovers only annotated pods in `bot-campaign`, and its RBAC is limited to
  reading discovery metadata. Filebeat receives only the read access needed for metadata
  and host log paths.
- The ETL CronJob is suspended and the public UI cannot trigger ETL, training, rollout,
  rollback, or moderation privileges.
- The API cannot safely scale beyond one replica yet because campaign materialization is
  stored in memory. PostgreSQL/Redis-backed shared state is required before horizontal
  scaling.

## Container images and the files that define the deployment

Detectra has three project-owned images:

1. The root `Dockerfile` builds `bot-review-campaign-api`. It starts from Python 3.11
   slim, installs CPU PyTorch for predictable inference, copies the vanilla UI and both
   promoted model bundles, verifies the large model files, runs as UID 10001, and starts
   one Uvicorn worker on port 8000.
2. `docker/ray.Dockerfile` builds `bot-review-campaign-jobs`. It starts from Ray 2.49.2,
   installs PyTorch, Transformers, Ray, MLflow, DVC/S3, and Kafka dependencies, copies
   ETL/training code and DVC metadata, and runs as the non-root `ray` user. Ray, ETL, and
   the small training submitter deliberately reuse this image because they require the
   same versioned project code and schemas.
3. `docker/airflow.Dockerfile` builds `bot-review-campaign-airflow`. It starts from
   Airflow 3.1.3 and adds only the reviewed Detectra DAGs. Keeping orchestration separate
   avoids installing the full ML training runtime into every Airflow process.

MLflow, Prometheus, Grafana, Elasticsearch, Kibana, Filebeat, Spark, Kafka, PostgreSQL,
and BusyBox use version-pinned upstream images rather than being repackaged without a
project-specific reason.

| File or directory | Purpose |
|---|---|
| `k8s/base/` | Core namespace, configuration, PVCs, API, Ray, ETL, MLflow, Prometheus, and Grafana |
| `k8s/secrets/*.example.yaml` | Safe placeholder YAMLs for core, Airflow, and optional private-GHCR Secrets; populated `*.local.yaml` copies are ignored |
| `k8s/overlays/gpu/` | Adds one `nvidia.com/gpu` request/limit to Ray and one GPU per trial |
| `k8s/overlays/observability/` | Adds Elasticsearch, Filebeat, Kibana, and a data-view setup Job |
| `k8s/overlays/full/` | Adds the observability overlay plus Airflow and PostgreSQL |
| `k8s/overlays/full-gpu/` | Combines the full overlay with Ray/Airflow GPU settings |
| `k8s/jobs/review-training-job.yaml` | Finite Ray Job submitter and the authoritative 20-trial/3-epoch values |
| `scripts/deploy_kubernetes.ps1` | Renders the chosen overlay, pins all project images to one tag, applies it, and waits for readiness |
| `.github/workflows/cd.yml` | Builds, scans, and publishes immutable API, jobs, and Airflow images |
| `docker-compose.yml` | Local service topology and optional Docker profiles |
| `src/bot_campaign/temporal.py` | Amazon behavior profiling, past-only temporal features, controlled scenarios, and safe splits |
| `src/bot_campaign/synthetic.py` | Separate train-only, label-preserving text augmentation |
| `src/bot_campaign/runtime.py` | UI/API scoring and deterministic four-review replay construction |
| `orchestration/dags/bot_campaign_streaming_smoke.py` | Manual Airflow smoke DAG for the Compose streaming path |
| `tests/test_ui_e2e.py` | Playwright/Chromium review-scan and campaign-replay browser test |

## Playwright and Chromium

**Playwright** is a browser-automation library. It controls a real browser through code,
waits for page state, fills controls, clicks buttons, and asserts visible results. It is
used here because unit tests cannot prove that the HTML, JavaScript, API calls, and DOM
updates work together.

**Chromium** is the open-source browser engine underlying Chrome and Edge. CI installs a
known Playwright-compatible Chromium build and launches it headlessly, meaning there is
no visible browser window. `tests/test_ui_e2e.py` starts a real Uvicorn server on port
8766, waits for `/health/ready`, opens Detectra, scans a promotional review, replays a
campaign, checks that at least one campaign card appears, and verifies the final
"How it works" content. The locator uses `.first` because the UI can legitimately contain
more than one campaign card.

Run the same test locally with:

```powershell
python -m pip install -e ".[dev,ui,nlp]"
python -m playwright install chromium
python -m pytest tests/test_ui_e2e.py -q -p no:cacheprovider
```

This is a UI/API integration test, not a full Kafka/Spark load test. The Airflow streaming
smoke DAG is the test for the live broker-to-campaign materialization path.

## How controlled campaign scenarios are created

There are two synthetic mechanisms and they have different purposes:

### Temporal campaign scenarios

`src/bot_campaign/temporal.py` first reads all supplied Amazon behavior files, converts
them to the common review schema, rejects invalid records, removes duplicate review IDs,
and sorts by event time. A catalog launch date is used when available; otherwise the
earliest observed review is retained as an explicitly named
`earliest_observed_review_proxy`, not falsely presented as a real launch date.

For each review, `_add_temporal_features` walks forward through sorted time and calculates
features using only earlier events:

- hours since launch and launch phase;
- UTC hour, weekday, and weekend status;
- minutes since the previous review for the product and user;
- number of earlier product reviews in one hour;
- number of earlier user reviews in 24 hours.

This past-only calculation avoids future leakage. `_profile` then learns category-level
Amazon distributions for review hour, weekday, rating, verified purchase rate, helpful
votes, time since launch, launch phase, and product/user inter-arrival time.

The deterministic scenario plan uses seed 42 by default and this mix:

| Scenario | Share | Controlled behavior | Campaign label |
|---|---:|---|---|
| Organic | 40% | Amazon hour/rating/verification distributions and independent users | No |
| Legitimate launch burst | 20% | Reviews near launch, normal language, at least the observed verification rate or 80% | No; hard negative |
| Coordinated positive | 12% | Five-star, unverified, promotional reviews about three minutes apart | Yes |
| Coordinated negative | 8% | One-star, unverified attack language about three minutes apart | Yes |
| Paraphrased campaign | 5% | Campaign label with split-safe positive wording variants | Yes |
| Off-hour campaign | 5% | Campaign placed in the category's rarest observed UTC hour | Yes |
| Slow-drip campaign | 5% | Coordinated events about 12 hours apart | Yes |
| Multi-product campaign | 5% | A coordinated group rotates across three product IDs | Yes |

Groups are balanced to at most ten events. Generated timestamps are reflected into the
product/category observation window so synthetic data cannot drift into implausible
future or ancient dates. Every record stores scenario, group ID, campaign ID, generator
seed, timestamp provenance, launch provenance, behavior-profile version, and the
controlled `expected_campaign` label.

Complete scenario groups—not individual rows—are assigned to approximately 70/15/15
chronological train/validation/test splits. A group can never cross splits. Training,
validation, and test also use disjoint sentence families so an exact generated sentence
cannot leak between them. Observed Amazon events remain unlabeled behavioral reference
data; they are never assigned invented fake-review labels.

### Train-only text augmentation

`src/bot_campaign/synthetic.py` augments only the real labeled **training** split. It
chooses parents from the same class, applies synonym substitution, sentence reordering,
or same-label clause recombination, rejects duplicates, alternates labels for class
balance, and records parent IDs plus the inherited-label provenance. Validation and test
remain real and unaugmented. This expands lexical coverage but is not campaign ground
truth and is not used to fabricate Amazon labels.

## Does the smoke stream send fixed reviews?

It sends a **fixed four-review pattern with dynamic identities and times**. The Airflow
DAG always requests:

```json
{"scenario":"coordinated-cross-product","mode":"stream"}
```

`runtime.replay_reviews` then creates exactly four English electronics reviews. Their
base positive sentence, five-star rating, unverified status, zero helpful votes,
three-minute spacing, and rotation over three product IDs are fixed. The exclamation
count changes by position. Each run generates a new replay ID, review IDs, user IDs, job
ID, and timestamps beginning at the current UTC time.

Therefore it is reproducible as a behavioral smoke pattern but is not byte-for-byte the
same event set, a random sample from the training data, or a throughput benchmark. The
intended Compose path is:

```text
Airflow -> API replay endpoint -> Kafka reviews.raw.v1
        -> Spark Structured Streaming windows
        -> Kafka reviews.analysis-windows.v1
        -> hybrid campaign scorer
        -> Kafka reviews.campaign-scores.v1
        -> API materializer -> completed replay/campaign card
```

Airflow polls every ten seconds for up to 15 minutes. Explicit `mode: stream` prevents a
silent local fallback: if Kafka/Spark/scorer is unavailable, the smoke run fails instead
of pretending the streaming platform worked.

## What a Docker Compose profile means

A Docker profile is a named switch that activates optional services in
`docker-compose.yml`. Services without a `profiles` entry are part of the default stack;
services with a profile are created only when that profile is enabled.

| Profile | Optional services it activates | Purpose |
|---|---|---|
| `stream` | `spark-stream`, `campaign-scorer` | Live Kafka/Spark campaign processing |
| `observability` | Elasticsearch, Kibana, Filebeat | Centralized container logs |
| `train` | `ray-trainer` | Campaign-model Ray submission |
| `train-review` | `ray-review-trainer` | Review-model Ray smoke submission; its current Compose command includes `--smoke` |
| `score` | `campaign-scorer` | Run the scorer without the Spark stream service |

Examples:

```powershell
# Default long-running dependencies only
docker compose up -d

# Add the live streaming services
docker compose --profile stream up -d

# Add streaming and centralized logs
docker compose --profile stream --profile observability up -d

# Start the optional review smoke submitter with its dependencies
docker compose --profile train-review up -d
```

A profile is not an image tag, environment, security boundary, or scaling rule. It is
also not a Kubernetes overlay. A Compose profile selects optional local services, while
a Kustomize overlay patches Kubernetes resources—for example, adding a GPU request or
adding the Elastic/Airflow platform—without copying the base manifests.

## Review-model search budget: 20 trials x 3 epochs

`k8s/jobs/review-training-job.yaml` is configured with:

```yaml
- name: RAY_NUM_SAMPLES
  value: "20"
- name: RAY_EPOCHS
  value: "3"
```

This means Ray Tune samples up to 20 hyperparameter configurations. Each surviving trial
may train for at most three complete passes over the training split, for a worst-case
budget of 60 dataset passes. It does **not** mean 20 epochs. ASHA can terminate weak
trials after an earlier epoch, so fewer than 20 trials may complete all three epochs.
`RAY_MAX_CONCURRENT_TRIALS=1` keeps one trial on the single laptop GPU at a time and
reduces CUDA and system-memory pressure. Ray reports trial parameters and metrics to
MLflow; the final bundle is selected by validation PR-AUC and then evaluated once on the
held-out real test split.

## Images people can pull

The release workflow publishes three project images to GitHub Container Registry:

```text
ghcr.io/njchathura-boop/bot-review-campaign-api:<git-sha>
ghcr.io/njchathura-boop/bot-review-campaign-jobs:<git-sha>
ghcr.io/njchathura-boop/bot-review-campaign-airflow:<git-sha>
```

The API image contains the UI and the two promoted model bundles, so inference never
silently falls back to an untrained model. The jobs image contains the ETL, DVC, Ray,
DistilBERT training, and campaign-training code. The Airflow image contains the pinned
Airflow runtime and reviewed project DAGs. Git-SHA tags are immutable; `latest` is only
a convenient pointer to the newest published release.

Make both GHCR packages public in **GitHub -> Packages -> Package settings -> Change
visibility**, or configure a Kubernetes `imagePullSecret` for a private package.

## Prerequisites

- Kubernetes 1.27 or newer and `kubectl`; Docker Desktop Kubernetes is suitable for a demo.
- A dynamic default StorageClass.
- Core: at least 8 CPU cores, 16 GB RAM, and 75 GB free cluster storage.
- Full Airflow + Elastic stack: at least 12 CPU cores, 24 GB RAM, and 110 GB free storage.
- Git LFS when building locally because the promoted model files are LFS objects.
- A DVC object-store remote for portable ETL data. S3-compatible storage is supported.
- For GPU training: an NVIDIA GPU, container runtime support, and the NVIDIA Kubernetes
  device plugin. CPU clusters use the base manifest.

## 1. Build and publish a release

The preferred route is GitHub Actions. Merge reviewed code to `main`, then create a
semantic release tag:

```powershell
git switch main
git pull --ff-only
git lfs pull
git tag -a v1.2.0 -m "Detectra v1.2.0"
git push origin v1.2.0
```

The `cd` workflow checks out LFS objects, verifies both model files, builds all three
images, tags them with the exact Git SHA, scans each with Trivy, and pushes them to GHCR.
Staging deliberately deploys the core overlay only because the full overlay needs
cluster-owned Airflow secrets and more capacity. Promote the same SHA with `-Full` after
those prerequisites have been configured.

For a local build:

```powershell
git lfs pull
$sha = git rev-parse HEAD
$registry = "ghcr.io/njchathura-boop"

docker build --build-arg "RELEASE_VERSION=local" --build-arg "SOURCE_COMMIT=$sha" `
  --tag "$registry/bot-review-campaign-api:$sha" --file Dockerfile .

docker build --build-arg "RELEASE_VERSION=local" --build-arg "SOURCE_COMMIT=$sha" `
  --tag "$registry/bot-review-campaign-jobs:$sha" --file docker/ray.Dockerfile .

docker build --build-arg "RELEASE_VERSION=local" --build-arg "SOURCE_COMMIT=$sha" `
  --tag "$registry/bot-review-campaign-airflow:$sha" --file docker/airflow.Dockerfile .

docker login ghcr.io
docker push "$registry/bot-review-campaign-api:$sha"
docker push "$registry/bot-review-campaign-jobs:$sha"
docker push "$registry/bot-review-campaign-airflow:$sha"
```

Before pushing, smoke-test the self-contained API image:

```powershell
docker run --rm -p 8000:8000 "ghcr.io/njchathura-boop/bot-review-campaign-api:$sha"
```

Open `http://localhost:8000`, then check `http://localhost:8000/health/ready` and
`http://localhost:8000/metrics`.

## 2. Configure DVC storage

`data/raw.dvc` identifies the exact raw dataset directory without committing 802 MB of
reviews to Git. First configure a team remote on the machine that owns the data and push
it once:

```powershell
python -m dvc remote add -d team s3://YOUR-BUCKET/detectra
python -m dvc push
git add data/raw.dvc data/.gitignore .dvc/config dvc.yaml dvc.lock
git commit -m "data: version raw and processed datasets with DVC"
```

Do not commit access keys. Create a local YAML from the tracked placeholder template:

```powershell
kubectl apply -f k8s/base/namespace.yaml
Copy-Item `
  k8s/secrets/core-secrets.example.yaml `
  k8s/secrets/core-secrets.local.yaml

# Edit the local copy and replace every REPLACE_* value, then check it.
code k8s/secrets/core-secrets.local.yaml
Select-String -Path k8s/secrets/core-secrets.local.yaml -Pattern "REPLACE_"

# Apply only when the previous command returns no placeholder matches.
kubectl apply -f k8s/secrets/core-secrets.local.yaml
kubectl -n bot-campaign get secret `
  detectra-storage detectra-grafana-admin
```

`stringData` lets Kubernetes perform the base64 conversion; base64 is encoding, not
encryption. The `*.local.yaml` pattern is Git-ignored, but an operator should still keep
populated files in an encrypted local directory and remove them after applying. Never
apply an example file, commit a populated copy, paste a Secret into an issue, or include
`kubectl get secret -o yaml` in a demonstration screenshot.

If the three GHCR packages are private, also copy and populate the optional pull-secret
template:

```powershell
Copy-Item `
  k8s/secrets/ghcr-pull-secret.example.yaml `
  k8s/secrets/ghcr-pull-secret.local.yaml

$githubUser = "YOUR_GITHUB_USERNAME"
$githubToken = "YOUR_PACKAGE_READ_TOKEN"
$registryAuth = [Convert]::ToBase64String(
  [Text.Encoding]::UTF8.GetBytes("${githubUser}:$githubToken")
)
Write-Host "Paste this value into the template's auth field: $registryAuth"

code k8s/secrets/ghcr-pull-secret.local.yaml
Select-String -Path k8s/secrets/ghcr-pull-secret.local.yaml -Pattern "REPLACE_"
kubectl apply -f k8s/secrets/ghcr-pull-secret.local.yaml
kubectl -n bot-campaign get secret detectra-ghcr-pull
```

The second document in that template attaches `detectra-ghcr-pull` to the namespace's
default ServiceAccount. Skip the template when packages are public or when Docker Desktop
is using matching local images. Do not print or retain the token after applying it.

## 3. Deploy the pods

Use the exact SHA shown in GitHub Actions. For CPU-only training:

```powershell
$sha = "REPLACE_WITH_PUBLISHED_GIT_SHA"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy_kubernetes.ps1 `
  -ImageTag $sha
```

For GPU training:

```powershell
$sha = "REPLACE_WITH_PUBLISHED_GIT_SHA"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy_kubernetes.ps1 `
  -ImageTag $sha -Gpu
```

Deployment profiles:

```powershell
# Core API, Ray, ETL, MLflow, Prometheus, and Grafana
.\scripts\deploy_kubernetes.ps1 -ImageTag $sha

# Core plus Elasticsearch, Filebeat, and Kibana
.\scripts\deploy_kubernetes.ps1 -ImageTag $sha -Observability

# Core plus Elastic/Kibana/Filebeat and Airflow/PostgreSQL
.\scripts\deploy_kubernetes.ps1 -ImageTag $sha -Full

# Full platform with NVIDIA Ray/Airflow trial settings
.\scripts\deploy_kubernetes.ps1 -ImageTag $sha -Full -Gpu
```

Before `-Full`, create the Airflow secret from its YAML template:

```powershell
kubectl apply -f k8s/base/namespace.yaml
$fernetKey = python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
$jwtSecret = python -c "import secrets; print(secrets.token_urlsafe(48))"
Copy-Item `
  k8s/secrets/airflow-secrets.example.yaml `
  k8s/secrets/airflow-secrets.local.yaml

Write-Host "Generated Fernet key: $fernetKey"
Write-Host "Generated JWT secret: $jwtSecret"
code k8s/secrets/airflow-secrets.local.yaml

# Replace the database password, URL-encoded password, Fernet key, JWT secret,
# and Airflow administrator password before applying.
Select-String -Path k8s/secrets/airflow-secrets.local.yaml -Pattern "REPLACE_"
kubectl apply -f k8s/secrets/airflow-secrets.local.yaml
kubectl -n bot-campaign get secret detectra-airflow-secrets
```

If the password contains URI-reserved characters, URL-encode it before constructing
`database-uri`. Run `kubectl apply` only after `Select-String` reports no placeholders.
For a shared cluster, use Vault/External Secrets rather than long-lived YAML credentials.

Without the helper script, `kubectl apply -k k8s` deploys the CPU stack with `latest`.
`kubectl apply -k k8s/overlays/gpu` deploys the GPU overlay. The helper is preferred
because it renders the selected overlay first and pins API, Ray/ETL, and Airflow
workloads—including the immutable migration Job—to one Git SHA before applying it.

Inspect startup:

```powershell
kubectl -n bot-campaign get pods -w
kubectl -n bot-campaign get deployments,services,cronjobs,pvc
kubectl -n bot-campaign describe pod -l app.kubernetes.io/name=detectra-api
```

## 4. Open every service

Run each port-forward in its own PowerShell window:

```powershell
kubectl -n bot-campaign port-forward service/detectra-api 8000:8000
kubectl -n bot-campaign port-forward service/detectra-ray 8265:8265
kubectl -n bot-campaign port-forward service/detectra-mlflow 5001:5000
kubectl -n bot-campaign port-forward service/detectra-prometheus 9090:9090
kubectl -n bot-campaign port-forward service/detectra-grafana 3000:3000
kubectl -n bot-campaign port-forward service/detectra-elasticsearch 9200:9200
kubectl -n bot-campaign port-forward service/detectra-kibana 5601:5601
kubectl -n bot-campaign port-forward service/detectra-airflow-api 8084:8080
```

| URL | Expected result |
|---|---|
| `http://localhost:8000` | Detectra UI |
| `http://localhost:8000/health/ready` | API and both model bundles ready |
| `http://localhost:8265` | Ray jobs and cluster dashboard |
| `http://localhost:5001` | MLflow experiments |
| `http://localhost:9090/targets` | `detectra-api` and `detectra-ray` targets are `UP` |
| `http://localhost:3000/dashboards` | Detectra folder and pre-provisioned dashboard |
| `http://localhost:9200/_cluster/health` | Elasticsearch health JSON (observability/full) |
| `http://localhost:5601` | Kibana log search (observability/full) |
| `http://localhost:8084` | Airflow DAG and task state (full) |

Generate live metrics by scanning and replaying examples in the UI. In Prometheus, query
`review_scans_total`, `campaign_alerts_total`, `api_request_latency_p95_ms`, or
`ray_component_cpu_percentage`. Grafana refreshes the same series every ten seconds.

## 5. Run ETL on demand

The ETL CronJob is committed with `suspend: true`, preventing an expensive dataset rebuild
immediately after installation. Trigger an auditable one-off run when the DVC secret is
ready:

```powershell
$etlJob = "detectra-etl-$(Get-Date -Format yyyyMMddHHmmss)"
kubectl -n bot-campaign create job $etlJob --from=cronjob/detectra-etl
kubectl -n bot-campaign logs -f "job/$etlJob"
```

Successful ETL produces `data/processed/temporal_bundle` and
`data/processed/dataset_bundle` on the workspace volume and pushes their DVC objects.
The default PVC is `ReadWriteOnce`, which is appropriate for Docker Desktop and other
single-node demos. On a multi-node cluster, use an RWX StorageClass or schedule ETL and
Ray on the same node; otherwise the volume can report a multi-attach error.

To make ETL weekly after validation:

```powershell
kubectl -n bot-campaign patch cronjob detectra-etl --type merge `
  --patch '{"spec":{"suspend":false}}'
```

Apache Airflow remains the higher-level workflow scheduler. Point the existing training
DAG at `http://detectra-ray.bot-campaign.svc.cluster.local:8265`; Airflow then submits and
waits for the Ray jobs, while Kubernetes owns pod restarts and resource limits.

## 6. Run model training

Verify ETL outputs first, then start the training submitter:

```powershell
kubectl -n bot-campaign exec deployment/detectra-ray -- `
  test -f /opt/project/data/processed/dataset_bundle/text/training.jsonl

kubectl delete job detectra-review-training -n bot-campaign --ignore-not-found
kubectl apply -k k8s/jobs
kubectl -n bot-campaign logs -f job/detectra-review-training
```

The committed job requests 20 Ray Tune samples, up to 3 epochs per trial, and one
concurrent trial. Change those values deliberately in
`k8s/jobs/review-training-job.yaml`. The GPU overlay gives the Ray pod one NVIDIA GPU
and sets one GPU per trial. ASHA may stop weak trials early; 20 is the maximum, not a
promise that every trial completes all 3 epochs.

Training writes checkpoints to the workspace PVC and experiments to MLflow. A successful
training run does **not** mutate the serving pod. Promotion is explicit: review the MLflow
metrics, copy the selected bundles into `artifacts/review_distilbert` and
`artifacts/campaign_model`, commit their Git LFS pointers, and publish a new image tag.

## 7. Verify, troubleshoot, and roll back

```powershell
kubectl -n bot-campaign get pods
kubectl -n bot-campaign logs deployment/detectra-api --tail=200
kubectl -n bot-campaign logs deployment/detectra-ray --tail=200
kubectl -n bot-campaign get events --sort-by=.lastTimestamp
kubectl -n bot-campaign rollout history deployment/detectra-api
kubectl -n bot-campaign rollout undo deployment/detectra-api
```

Common failures:

- `ImagePullBackOff`: publish the SHA tag, make GHCR public, or configure an image-pull secret.
- API readiness fails: confirm Git LFS supplied both large model files during image build.
- PVC stays pending: configure a default StorageClass or change the claim settings.
- ETL says raw inputs are missing: configure `detectra-storage` and ensure `dvc push` ran.
- Ray is pending on the GPU overlay: install the NVIDIA device plugin or use the CPU base.
- Grafana is empty: generate UI traffic and check Prometheus `/targets` before Grafana.

Render manifests without changing a cluster:

```powershell
kubectl kustomize k8s
kubectl kustomize k8s/overlays/gpu
kubectl kustomize k8s/overlays/observability
kubectl kustomize k8s/overlays/full
kubectl kustomize k8s/overlays/full-gpu
kubectl kustomize k8s/jobs
```

The public Detectra UI remains read-only for infrastructure. It cannot start training,
run ETL, modify Kubernetes, or change enforcement policy.
