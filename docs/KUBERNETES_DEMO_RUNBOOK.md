# Kubernetes Demo Runbook

This is the repeatable Docker Desktop Kubernetes workflow for the Bot Campaign project.
It deploys the API/UI, Ray, MLflow, Prometheus, Grafana, Elasticsearch, Filebeat,
Kibana, Airflow, and Airflow PostgreSQL. The Kubernetes manifests do not deploy Kafka,
Spark, or `campaign-scorer`; use Docker Compose for the live streaming demo described in
`LIVE_STREAMING_PIPELINE.md`.

## 1. What is persistent

Keep the Kubernetes `detectra-mlflow` PVC. It contains MLflow's SQLite metadata,
experiments, registered model metadata, and artifacts. A retrain does not require
removing it. The Docker Compose volume `bot-campaign_mlflow-data` is separate from the
Kubernetes PVC and is not used by this Kubernetes workflow.

The shared `detectra-workspace` PVC contains DVC data, generated bundles, Ray results,
and candidate artifacts. Airflow, Ray, and ETL use it. Prometheus, Grafana, Elasticsearch,
and Airflow PostgreSQL each have separate claims.

A reset is destructive: deleting the `bot-campaign` namespace deletes its PVCs and
therefore Kubernetes MLflow history and generated workspace data. Do this only when
intentionally starting a new demo.

## 2. Prerequisites

Use Docker Desktop with Kubernetes enabled and one ready node:

```powershell
kubectl config use-context docker-desktop
kubectl get nodes
kubectl version --output=yaml
```

The full stack is large. Docker Desktop should have at least 8 CPUs and 16 GiB memory;
full DistilBERT retraining is more comfortable with 24 GiB or more. GPU mode additionally
requires a working NVIDIA container runtime and a node advertising `nvidia.com/gpu`.

Build and publish the three project images. Use a Git SHA or release tag for a real demo;
`latest` is convenient for local work:

```powershell
$tag = (git rev-parse --short HEAD)
$registry = "ghcr.io/njchathura-boop"
docker login ghcr.io
docker build --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -t "$registry/bot-review-campaign-api:$tag" .
docker build --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -f docker/ray.Dockerfile -t "$registry/bot-review-campaign-jobs:$tag" .
docker build --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -f docker/airflow.Dockerfile -t "$registry/bot-review-campaign-airflow:$tag" .
docker push "$registry/bot-review-campaign-api:$tag"
docker push "$registry/bot-review-campaign-jobs:$tag"
docker push "$registry/bot-review-campaign-airflow:$tag"
```

If the GHCR packages are private, create
`k8s/secrets/ghcr-pull-secret.local.yaml` from its example before deployment. If they
are public, no pull secret is needed.

## 3. Create local secrets

Do not edit or commit the example files. Copy the Airflow example and replace every
placeholder:

```powershell
Copy-Item k8s/secrets/airflow-secrets.example.yaml k8s/secrets/airflow-secrets.local.yaml
notepad k8s/secrets/airflow-secrets.local.yaml
```

The `database-uri` password must match `postgres-password`. Generate the Fernet and JWT
values with:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

For a DVC remote and a Grafana admin password, also create:

```powershell
Copy-Item k8s/secrets/core-secrets.example.yaml k8s/secrets/core-secrets.local.yaml
notepad k8s/secrets/core-secrets.local.yaml
```

`DVC_REMOTE_URL` may be left as a placeholder only when all required data is copied into
the workspace PVC manually. An S3-compatible DVC remote is recommended for a repeatable
full-data demo.

## 4. Deploy the full stack

The helper applies local secrets, checks the cluster, applies the full overlay, and waits
for the core and observability workloads:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\kubernetes_demo.ps1 `
  -ImageTag $tag `
  -Registry $registry
```

For one NVIDIA GPU:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\kubernetes_demo.ps1 `
  -ImageTag $tag `
  -Registry $registry `
  -Gpu
```

The equivalent direct deploy command is:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\deploy_kubernetes.ps1 `
  -Full `
  -ImageTag $tag `
  -Registry $registry
```

Inspect the result:

```powershell
kubectl -n bot-campaign get pods -o wide
kubectl -n bot-campaign get svc,pvc,job,cronjob
kubectl -n bot-campaign get events --sort-by=.lastTimestamp | Select-Object -Last 30
```

If a pod is not ready, inspect it before changing manifests:

```powershell
kubectl -n bot-campaign describe pod <pod-name>
kubectl -n bot-campaign logs <pod-name> --all-containers --tail=200
```

Common causes are a private GHCR image without a pull secret, insufficient Docker
Desktop memory, a missing Airflow secret, or an unbound PVC.

## 5. Put data on the shared workspace PVC

The full DAG begins by running DVC and the two bundle-building stages. It needs the DVC
metadata and all raw inputs. The image contains the code and DVC metadata, but not the
large raw dataset. Prefer configuring `DVC_REMOTE_URL` and letting the ETL job run
`dvc pull`.

To manually load a checked-out local dataset, start the existing PVC loader:

```powershell
kubectl apply -f k8s/data/pod.yml
kubectl -n bot-campaign wait --for=condition=Ready pod/detectra-data-loader --timeout=120s
kubectl -n bot-campaign exec detectra-data-loader -- mkdir -p /workspace/data/raw
kubectl -n bot-campaign cp .\data\raw\amazon_all_beauty.jsonl bot-campaign/detectra-data-loader:/workspace/data/raw/amazon_all_beauty.jsonl
```

For the complete DVC dataset, copy the full `data/raw` tree rather than a single sample.
A complete build needs the Amazon category files, `product_reviews.jsonl`, and
`kaggle_fake_reviews/fake reviews dataset.csv`.

Verify the workspace from the Ray pod:

```powershell
$rayPod = kubectl -n bot-campaign get pod -l app.kubernetes.io/name=detectra-ray -o jsonpath='{.items[0].metadata.name}'
kubectl -n bot-campaign exec $rayPod -- bash -lc 'find /opt/project/data/raw -maxdepth 2 -type f | sort | head -20'
kubectl -n bot-campaign exec $rayPod -- bash -lc 'test -f /opt/project/data/raw/product_reviews.jsonl'
kubectl -n bot-campaign delete pod detectra-data-loader --ignore-not-found
```

## 6. Trigger the complete retraining workflow

Airflow is deployed but the retraining DAG is intentionally manual. This prevents a
large training run from starting during every installation. The DAG runs in order:

1. DVC pull and ETL/bundle generation.
2. Review DistilBERT Ray Tune training.
3. Hybrid campaign Ray Tune training.
4. MLflow logging and registration for both selected models.

Trigger it from the Airflow scheduler pod:

```powershell
$airflowPod = kubectl -n bot-campaign get pod -l app.kubernetes.io/name=detectra-airflow-scheduler -o jsonpath='{.items[0].metadata.name}'
kubectl -n bot-campaign exec $airflowPod -- airflow dags trigger bot_campaign_model_retraining
kubectl -n bot-campaign exec $airflowPod -- airflow dags list-runs -d bot_campaign_model_retraining
```

Follow Airflow scheduler logs and Ray jobs:

```powershell
kubectl -n bot-campaign logs deployment/detectra-airflow-scheduler -f
kubectl -n bot-campaign get pods -w
kubectl -n bot-campaign get events --sort-by=.lastTimestamp
```

The training outputs are written under the shared workspace at
`artifacts/candidates/review_distilbert` and `artifacts/candidates/campaign_model`.
The selected models are registered in MLflow. The running API image still contains the
previous promoted bundles; retraining does not silently change production behavior.
Promotion requires reviewing metrics and rebuilding/publishing the API image with the
approved bundles, followed by the normal immutable deployment.

## 7. Open the demo UIs

Use separate terminals because each port-forward remains active:

```powershell
kubectl -n bot-campaign port-forward svc/detectra-api 8000:8000
kubectl -n bot-campaign port-forward svc/detectra-mlflow 5001:5000
kubectl -n bot-campaign port-forward svc/detectra-ray 8265:8265
kubectl -n bot-campaign port-forward svc/detectra-prometheus 9090:9090
kubectl -n bot-campaign port-forward svc/detectra-grafana 3000:3000
kubectl -n bot-campaign port-forward svc/detectra-kibana 5601:5601
kubectl -n bot-campaign port-forward svc/detectra-airflow-api 8084:8080
```

Open:

| Component | URL |
|---|---|
| Detectra UI/API | `http://localhost:8000` |
| MLflow | `http://localhost:5001` |
| Ray dashboard | `http://localhost:8265` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Kibana | `http://localhost:5601` |
| Airflow | `http://localhost:8084` |

Grafana has the provisioned `Detectra Kubernetes Overview` dashboard. Prometheus scrapes
annotated API and Ray pods. Kibana receives Kubernetes container logs through Filebeat;
check for `detectra-logs-*` indices after the workloads have emitted logs.

## 8. Verify the demo

```powershell
Invoke-RestMethod http://localhost:8000/health/live
Invoke-RestMethod http://localhost:8000/health/ready | ConvertTo-Json
Invoke-RestMethod http://localhost:8000/metrics
Invoke-RestMethod http://localhost:9090/-/ready
Invoke-RestMethod http://localhost:3000/api/health
Invoke-RestMethod http://localhost:5001/health
```

Check Prometheus targets:

```powershell
(Invoke-RestMethod http://localhost:9090/api/v1/targets).data.activeTargets |
  Select-Object health,lastError,@{Name='job';Expression={$_.labels.job}},@{Name='instance';Expression={$_.labels.instance}}
```

Check MLflow and Airflow completion:

```powershell
kubectl -n bot-campaign logs deployment/detectra-mlflow --tail=100
kubectl -n bot-campaign exec $airflowPod -- airflow dags list-runs -d bot_campaign_model_retraining
kubectl -n bot-campaign get pods,job
```

## 9. Cleanup and reset

Stop port-forwards with `Ctrl+C`. To stop workloads while keeping persistent data,
scale or suspend the relevant deployments/jobs; do not delete the namespace. To fully
remove the demo and its persistent data:

```powershell
kubectl delete namespace bot-campaign --wait=true
```

That command deletes Kubernetes MLflow, workspace, monitoring, Elasticsearch, and
Airflow PostgreSQL PVCs because they belong to the namespace. Re-run the bootstrap after
cleanup. It does not delete the unrelated Docker Compose volume
`bot-campaign_mlflow-data`.

The guarded helper makes the destructive intent explicit:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\kubernetes_demo.ps1 `
  -ResetNamespace -ConfirmReset
```

Never run `docker volume rm bot-campaign_mlflow-data` as part of the Kubernetes reset.
Only remove it when intentionally resetting the separate Compose MLflow history:

```powershell
docker volume rm bot-campaign_mlflow-data
```

## 10. Streaming boundary

The current Kubernetes workflow is complete for serving, retraining, MLflow, metrics,
logs, and orchestration. It is not a Kafka/Spark streaming deployment. For the streaming
portion of the demo, run the documented Compose path:

```powershell
docker compose --profile stream up -d --build `
  kafka kafka-init spark-master spark-worker spark-stream campaign-scorer api
```

Do not claim Kafka-to-Spark-to-scorer is running in Kubernetes until equivalent manifests
are added and validated.

## 11. GHCR publishing and pulling

GHCR stores the three project images used by Kubernetes. Use a GitHub token with package
write permission to push and package read permission to pull. Never commit the token.

```powershell
# Authenticate. Paste the token only when Docker prompts.
docker login ghcr.io -u njchathura-boop

# Build immutable images from the current commit.
$tag = (git rev-parse --short HEAD)
$registry = "ghcr.io/njchathura-boop"
docker build --pull --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -t "$registry/bot-review-campaign-api:$tag" .
docker build --pull --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -f docker/ray.Dockerfile -t "$registry/bot-review-campaign-jobs:$tag" .
docker build --pull --build-arg RELEASE_VERSION=$tag --build-arg SOURCE_COMMIT=$tag -f docker/airflow.Dockerfile -t "$registry/bot-review-campaign-airflow:$tag" .

# Publish all three images.
docker push "$registry/bot-review-campaign-api:$tag"
docker push "$registry/bot-review-campaign-jobs:$tag"
docker push "$registry/bot-review-campaign-airflow:$tag"
```

For private packages, create and apply the pull secret:

```powershell
Copy-Item k8s/secrets/ghcr-pull-secret.example.yaml k8s/secrets/ghcr-pull-secret.local.yaml
notepad k8s/secrets/ghcr-pull-secret.local.yaml
kubectl apply -f k8s/secrets/ghcr-pull-secret.local.yaml
kubectl -n bot-campaign get serviceaccount default -o yaml
```

Verify the published image and release identity:

```powershell
docker pull "$registry/bot-review-campaign-api:$tag"
docker image inspect "$registry/bot-review-campaign-api:$tag" --format '{{.Id}}'
kubectl -n bot-campaign rollout history deployment/detectra-api
```

Use immutable commit tags for reproducible demos. `latest` is suitable only for quick
local iteration. Rollback an API deployment with:

```powershell
kubectl -n bot-campaign rollout undo deployment/detectra-api
```

## 12. Complete execution order

Run these phases in order. Each phase has a clear purpose and a check before the next:

```powershell
# A. Cluster and source checks.
kubectl config use-context docker-desktop
kubectl get nodes
git status --short

# B. Compose/image configuration check.
docker compose config

# C. Publish the three GHCR images using the commands in section 11.
docker push "$registry/bot-review-campaign-api:$tag"
docker push "$registry/bot-review-campaign-jobs:$tag"
docker push "$registry/bot-review-campaign-airflow:$tag"

# D. Deploy API, Ray, MLflow, monitoring, logging, and Airflow.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\kubernetes_demo.ps1 -ImageTag $tag -Registry $registry

# E. Confirm storage and workloads.
kubectl -n bot-campaign get pods,pvc,svc,job,cronjob

# F. Trigger ETL, review training, campaign training, and MLflow registration.
$airflowPod = kubectl -n bot-campaign get pod -l app.kubernetes.io/name=detectra-airflow-scheduler -o jsonpath='{.items[0].metadata.name}'
kubectl -n bot-campaign exec $airflowPod -- airflow dags trigger bot_campaign_model_retraining

# G. Confirm the run and collect demo evidence.
kubectl -n bot-campaign exec $airflowPod -- airflow dags list-runs -d bot_campaign_model_retraining
kubectl -n bot-campaign get events --sort-by=.lastTimestamp | Select-Object -Last 30
```

Retraining registers candidates in MLflow but does not silently replace the bundles
embedded in the API image. Review metrics first, then rebuild and publish an approved API
image before deploying it.

## 13. Public API endpoint checklist

After forwarding `detectra-api` to `localhost:8000`, show `/` and `/docs` first. The
following routes are the complete user-facing API surface:

| Method | Endpoint | Demonstrates |
|---|---|---|
| GET | `/health/live` | Process is alive |
| GET | `/health/ready` | Models and runtime are ready |
| POST | `/v1/reviews/score` | Single review scoring |
| POST | `/v1/reviews/batch-score` | Batch review scoring |
| GET | `/v1/reviews/recent` | Recent scanned reviews |
| GET | `/v1/reviews/{review_id}/trust` | Stored review evidence |
| GET | `/v1/reviews/{review_id}/lineage` | Prediction lineage |
| POST | `/v1/demo/replay` | Start a campaign replay |
| GET | `/v1/demo/replay/{job_id}` | Poll replay status |
| GET | `/v1/campaigns` | Campaign queue |
| GET | `/v1/campaigns/{campaign_id}` | Campaign evidence |
| POST | `/v1/campaigns/{campaign_id}/decision` | Moderator decision |
| GET | `/v1/ops/summary` | Operational summary |
| GET | `/v1/monitoring/summary` | Monitoring summary |
| GET | `/v1/lineage/current` | Current deployment lineage |
| GET | `/v1/lineage/predictions/{review_id}` | Prediction lineage lookup |
| GET | `/metrics` | Prometheus metrics |

```powershell
Start-Process http://localhost:8000/docs
Invoke-RestMethod http://localhost:8000/health/live
Invoke-RestMethod http://localhost:8000/health/ready | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8000/v1/ops/summary | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8000/v1/monitoring/summary | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8000/v1/lineage/current | ConvertTo-Json -Depth 8

$review = @{
  review_id = "demo-review-001"
  user_id = "demo-user-001"
  product_id = "demo-product-001"
  text = "Amazing product, buy now and do not miss this limited offer!"
  rating = 5
  timestamp = (Get-Date).ToUniversalTime().ToString("o")
  verified_purchase = $false
} | ConvertTo-Json
$prediction = Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/reviews/score -ContentType application/json -Body $review
$prediction | ConvertTo-Json -Depth 8
Invoke-RestMethod "http://localhost:8000/v1/reviews/$($prediction.review_id)/lineage" | ConvertTo-Json -Depth 8

$replayBody = @{ scenario = "coordinated-cross-product"; mode = "local" } | ConvertTo-Json
$replay = Invoke-RestMethod -Method Post -Uri http://localhost:8000/v1/demo/replay -ContentType application/json -Body $replayBody
do { Start-Sleep -Seconds 2; $status = Invoke-RestMethod "http://localhost:8000/v1/demo/replay/$($replay.job_id)" } while ($status.status -in @("queued", "running"))
$status | ConvertTo-Json -Depth 10
Invoke-RestMethod http://localhost:8000/v1/campaigns | ConvertTo-Json -Depth 10
```

Use `/openapi.json` or `/docs` as the authoritative request schema if a field is rejected.

## 14. Docker Compose end-to-end path

Use Compose when the demo must include Kafka, Spark, and the campaign scorer:

```powershell
# Start MLflow, metrics, Grafana, and Ray.
docker compose up -d mlflow prometheus grafana ray-head ray-worker

# Rebuild the processed data bundle from DVC/raw inputs.
docker compose run --rm ray-head bash -lc 'cd /opt/project && dvc repro build_temporal_bundle build_dataset_bundle'

# Run review and campaign training.
docker compose --profile train-review run --rm ray-review-trainer
docker compose --profile train run --rm ray-trainer

# Start Kafka -> Spark -> campaign scorer -> API.
docker compose --profile stream up -d --build kafka kafka-init spark-master spark-worker spark-stream campaign-scorer api

# Inspect services and logs.
docker compose ps
docker compose logs --tail=100 mlflow ray-head ray-worker api spark-stream campaign-scorer
```

Compose endpoints are API `8000`, MLflow `5001`, Ray `8265`, Prometheus `9090`, Grafana
`3000`, Spark `8082`, Kafka `9092`, Elasticsearch `9200`, and Kibana `5601`.

## 15. Debugging playbook

Start with the layer that failed. Do not delete PVCs as a first response:

| Symptom | Commands | Likely cause / fix |
|---|---|---|
| `kubectl` refuses `127.0.0.1:6443` | `kubectl config current-context`; `docker info`; `kubectl get nodes` | Enable Docker Desktop Kubernetes and select `docker-desktop`. |
| `ImagePullBackOff` | `kubectl -n bot-campaign describe pod <pod>` | Wrong tag, private GHCR package, or missing pull secret. |
| `Pending` PVC | `kubectl -n bot-campaign describe pvc <claim>` | Storage unavailable or insufficient disk. |
| API `CrashLoopBackOff` | `kubectl -n bot-campaign logs deploy/detectra-api --previous` | Invalid model files, permissions, or image contents. |
| API unready | `kubectl -n bot-campaign describe pod -l app.kubernetes.io/name=detectra-api` | Slow model loading, failed readiness endpoint, or low memory. |
| Ray unready | `kubectl -n bot-campaign logs deploy/detectra-ray` | Insufficient CPU/RAM/GPU or Ray dashboard failure. |
| ETL says raw inputs missing | `kubectl -n bot-campaign logs job/<job-name>` | Configure DVC remote or populate the workspace PVC. |
| Airflow DAG missing | `kubectl -n bot-campaign logs deploy/detectra-airflow-dag-processor` | DAG import error or stale Airflow image. |
| Airflow cannot reach Ray | `kubectl -n bot-campaign exec $airflowPod -- python -c "import urllib.request; print(urllib.request.urlopen('http://detectra-ray:8265/api/version').status)"` | Ray service is not ready or service name/port is wrong. |
| Grafana has no data | `Invoke-RestMethod http://localhost:9090/api/v1/targets` | Prometheus target, annotation, or RBAC issue. |
| Kibana has no logs | `kubectl -n bot-campaign logs daemonset/detectra-filebeat` | Filebeat cannot read host logs or Elasticsearch is unready. |
| Port-forward closes | `kubectl -n bot-campaign get svc`; `kubectl -n bot-campaign get pods` | No ready endpoints behind the Service. |

Useful snapshots:

```powershell
kubectl -n bot-campaign get all
kubectl -n bot-campaign get pvc
kubectl -n bot-campaign get events --sort-by=.lastTimestamp | Select-Object -Last 50
kubectl -n bot-campaign describe deployment detectra-api
kubectl -n bot-campaign logs --all-containers --prefix --tail=200 -l app.kubernetes.io/part-of=detectra
```

Never print or share Secret YAML, DVC credentials, GHCR tokens, or Airflow secret values.

## 16. Image and storage decisions

Only the API, Ray/jobs, and Airflow images are project-owned and pushed to GHCR. MLflow,
Prometheus, Grafana, Elasticsearch, Kibana, and Filebeat run as Kubernetes pods using
pinned upstream images; their project-specific settings are supplied by ConfigMaps,
Secrets, Services, Jobs, and PVCs. Do not build custom monitoring images for this demo.

The API image is large because it embeds two model bundles and CPU PyTorch/Transformers.
The optimized Dockerfile removes the unnecessary OS upgrade, disables pip cache/bytecode
work, and keeps Ray/DVC/training dependencies out of the API image. Do not remove the
model bundles because the API needs them for readiness and inference.

Use a new immutable tag after an image change. Reusing a tag moves it to a new digest:

```powershell
$registry = "ghcr.io/njchathura-boop"
$tag = "2026.08.21-opt1"
docker build --pull -t "$registry/bot-review-campaign-api:$tag" .
docker push "$registry/bot-review-campaign-api:$tag"
docker manifest inspect "$registry/bot-review-campaign-api:$tag"
```

Rebuild jobs or Airflow only when their Dockerfile or source changed. Keep old GHCR tags
until the new deployment is verified; `docker image rm` removes only local images.

## 17. Monitoring quick reference

```powershell
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-WebRequest http://localhost:8000/metrics -UseBasicParsing
Invoke-RestMethod http://localhost:9090/-/ready
(Invoke-RestMethod http://localhost:9090/api/v1/targets).data.activeTargets
Invoke-RestMethod http://localhost:3000/api/health
Invoke-RestMethod http://localhost:5001/health
Invoke-RestMethod http://localhost:9200/_cluster/health
Invoke-RestMethod http://localhost:5601/api/status
```

PromQL for Prometheus or Grafana Explore:

```text
up{namespace="bot-campaign"}
api_requests_total
api_errors_total
api_error_rate
api_request_latency_p95_ms
review_scans_total
review_scans_per_second
review_mean_risk
review_mean_confidence
review_needs_human_review_total
campaign_alerts_total
campaign_stream_scores_total
campaign_stream_candidates_total
campaign_stream_consumer_errors_total
api_disk_free_bytes
rate(api_requests_total[5m])
increase(review_scans_total[5m])
sum(up{namespace="bot-campaign",app="detectra-ray"})
```

Show Grafana dashboards `Detectra / API Operations`, `Detectra / Ray Cluster`, and
`Detectra Kubernetes Overview`. In Kibana Discover, select `detectra-logs-*` and use:

```text
project.name: "detectra"
kubernetes.namespace: "bot-campaign"
kubernetes.namespace: "bot-campaign" and kubernetes.container.name: "api"
kubernetes.namespace: "bot-campaign" and message: *error*
kubernetes.namespace: "bot-campaign" and message: *review_scored*
```

Check log storage with:

```powershell
Invoke-RestMethod "http://localhost:9200/_cat/indices/detectra-logs-*?format=json"
kubectl -n bot-campaign logs daemonset/detectra-filebeat --tail=100
kubectl -n bot-campaign get job detectra-kibana-setup
```

For space cleanup, inspect first with `docker system df` and then consider
`docker container prune` and `docker builder prune`. Do not use
`docker system prune --volumes` casually: volumes contain MLflow and monitoring data.
Deleting `bot-campaign_mlflow-data` resets Compose MLflow history; deleting the
`bot-campaign` namespace removes Kubernetes PVC data.

Any GitHub token pasted into an editor, terminal, chat, or document must be revoked
immediately and replaced. Credentials must never be stored in this runbook or Git.

## 19. Argo CD installation and UI

Argo CD is an optional GitOps control plane. It shows Applications and their resource
tree, including Deployments, Jobs, Services, PVCs, and Pods. It compares the Git
repository with the cluster and can synchronize declared manifests.

Install it with server-side apply. This avoids the `applicationsets.argoproj.io` CRD
error: `metadata.annotations: Too long: may not be more than 262144 bytes`.

```powershell
kubectl config use-context docker-desktop
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side --force-conflicts -n argocd `
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl get crd applications.argoproj.io applicationsets.argoproj.io appprojects.argoproj.io
kubectl -n argocd get pods
```

Wait for the main components:

```powershell
kubectl -n argocd wait --for=condition=available `
  deployment/argocd-server deployment/argocd-repo-server `
  deployment/argocd-redis deployment/argocd-applicationset-controller `
  --timeout=300s
```

Open the UI:

```powershell
kubectl -n argocd port-forward svc/argocd-server 8081:443
```

Open `https://localhost:8081`. Get the initial password with:

```powershell
$encodedPassword = kubectl -n argocd get secret argocd-initial-admin-secret `
  -o jsonpath="{.data.password}"
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encodedPassword))
```

Login as `admin` with that password.

Apply the project Application after pushing the Kubernetes files to GitHub:

```powershell
kubectl apply -f deploy/argocd-application.yaml
kubectl -n argocd get applications
kubectl -n argocd get application bot-review-campaign -o wide
```

The existing Application uses `path: k8s`, which deploys the base stack. For the full
Airflow and observability demo, set the Git Application source path to:

```yaml
path: k8s/overlays/full
```

Commit and push that change before syncing. Local ignored Secret files are not read by
Argo CD; create required Secrets separately in the `bot-campaign` namespace.

In the Argo UI, open **Applications**, select `bot-review-campaign`, expand the resource
tree, and inspect Deployments, Jobs, Services, PVCs, and Pods. Click **Sync** when the
Application is `OutOfSync`.

PowerShell checks:

```powershell
kubectl -n argocd get application bot-review-campaign
kubectl -n argocd describe application bot-review-campaign
kubectl -n argocd get application bot-review-campaign `
  -o jsonpath="{.status.sync.status}{' / '}{.status.health.status}{'\n'}"
kubectl -n bot-campaign get pods,svc,pvc,job
```

Troubleshooting:

```powershell
kubectl -n argocd get pods
kubectl -n argocd get events --sort-by=.lastTimestamp | Select-Object -Last 40
kubectl -n argocd logs deployment/argocd-server --tail=100
kubectl -n argocd logs deployment/argocd-repo-server --tail=100
kubectl -n argocd logs statefulset/argocd-application-controller --tail=100
kubectl -n argocd describe application bot-review-campaign
```

Use server-side apply again if the CRD annotation-size error returns. `OutOfSync` means
Git differs from the cluster; `Unknown` commonly means Git or the repository path is
unreachable; `ImagePullBackOff` means a wrong tag or missing GHCR pull Secret; `Pending`
usually means insufficient resources or an unbound PVC.

Remove only Argo CD while preserving the project namespace with:

```powershell
kubectl delete namespace argocd
```

Do not delete `bot-campaign` unless you intentionally want to remove its workloads and
persistent PVC data.

## 20. GPU setup and verification

GPU resources are not added to `kubectl` directly. The Docker Desktop Kubernetes node
must expose an NVIDIA device resource before a Pod requesting `nvidia.com/gpu` can be
scheduled.

First verify the host and Docker runtime:

```powershell
nvidia-smi
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi
wsl --update
wsl --shutdown
```

Restart Docker Desktop after WSL or NVIDIA runtime changes. Install the NVIDIA device
plugin only when the Docker Desktop Kubernetes environment supports GPU device exposure:

```powershell
kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.17.4/deployments/static/nvidia-device-plugin.yml
kubectl get pods -A | Select-String nvidia
kubectl get daemonset -A | Select-String nvidia
```

Check whether Kubernetes actually advertises the resource:

```powershell
kubectl describe node docker-desktop | Select-String "nvidia|Capacity|Allocatable" -Context 2,2
kubectl get node docker-desktop -o jsonpath="{.status.capacity.nvidia\.com/gpu}{'\n'}"
```

The expected capacity is `1`. If the command prints nothing, Kubernetes cannot schedule
GPU Pods even if `nvidia-smi` works on Windows. Test scheduling with a temporary Pod:

```powershell
kubectl run gpu-test --restart=Never `
  --image=nvidia/cuda:12.4.1-base-ubuntu22.04 `
  --overrides='{"spec":{"containers":[{"name":"cuda","image":"nvidia/cuda:12.4.1-base-ubuntu22.04","command":["nvidia-smi"],"resources":{"limits":{"nvidia.com/gpu":1}}}]}}'
kubectl get pod gpu-test -o wide
kubectl logs gpu-test
kubectl delete pod gpu-test --ignore-not-found
```

Only use the project GPU overlay after the node reports `nvidia.com/gpu: 1`:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\kubernetes_demo.ps1 `
  -ImageTag $tag `
  -Registry $registry `
  -Gpu
```

Verify Ray's GPU request:

```powershell
kubectl -n bot-campaign describe pod -l app.kubernetes.io/name=detectra-ray
kubectl -n bot-campaign get events --sort-by=.lastTimestamp | Select-Object -Last 30
```

If scheduling reports `Insufficient nvidia.com/gpu`, deploy the CPU overlay instead:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\kubernetes_demo.ps1 `
  -ImageTag $tag `
  -Registry $registry
```

CPU mode is the expected fallback for Docker Desktop clusters where the NVIDIA device
plugin does not expose a GPU resource. Do not repeatedly delete Pods; fix the node
capacity or remove the GPU request first.

## 21. Argo CD installation, UI, and recovery

Argo CD is an optional GitOps control plane. It displays Applications and their resource
tree, including Deployments, Jobs, Services, PVCs, and Pods. It does not replace the
Kubernetes API or create GPU capacity.

Install Argo CD with server-side apply. This avoids the common error:
`applicationsets.argoproj.io: metadata.annotations: Too long: may not be more than 262144 bytes`.

```powershell
kubectl config use-context docker-desktop
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side --force-conflicts -n argocd `
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl get crd applications.argoproj.io applicationsets.argoproj.io appprojects.argoproj.io
kubectl -n argocd get pods
kubectl -n argocd wait --for=condition=available `
  deployment/argocd-server deployment/argocd-repo-server `
  deployment/argocd-redis deployment/argocd-applicationset-controller `
  --timeout=300s
```

If the CRD annotation error occurs during a normal `kubectl apply`, do not delete Argo
CD. Re-run the server-side command above. It completes the partial installation.

Open the Argo CD UI in a separate terminal:

```powershell
kubectl -n argocd port-forward svc/argocd-server 8081:443
```

Browse to `https://localhost:8081`. Retrieve the initial password without displaying the
Secret manifest:

```powershell
$encodedPassword = kubectl -n argocd get secret argocd-initial-admin-secret `
  -o jsonpath="{.data.password}"
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encodedPassword))
```

Create the project Application only after the Kubernetes manifests are committed and
pushed to GitHub. Argo CD reads Git, not uncommitted local files:

```powershell
kubectl apply -f deploy/argocd-application.yaml
kubectl -n argocd get applications
kubectl -n argocd get application bot-review-campaign -o wide
```

The Application path `k8s` deploys the base stack. For the complete Airflow and
observability demo, use this source path in `deploy/argocd-application.yaml` and push it:

```yaml
path: k8s/overlays/full
```

Argo CD cannot read ignored local Secret files from your workstation. Create required
Secrets separately in the `bot-campaign` namespace, and never commit credentials.

In the UI, open **Applications**, select `bot-review-campaign`, expand the resource tree,
and inspect Deployments, StatefulSets, Jobs, Services, PVCs, and Pods. Click **Sync** when
the Application is `OutOfSync`.

Useful checks:

```powershell
kubectl -n argocd get application bot-review-campaign
kubectl -n argocd describe application bot-review-campaign
kubectl -n argocd get application bot-review-campaign `
  -o jsonpath="{.status.sync.status}{' / '}{.status.health.status}{'\n'}"
kubectl -n bot-campaign get pods,svc,pvc,job
kubectl -n argocd get events --sort-by=.lastTimestamp | Select-Object -Last 40
```

For Argo problems:

```powershell
kubectl -n argocd logs deployment/argocd-server --tail=100
kubectl -n argocd logs deployment/argocd-repo-server --tail=100
kubectl -n argocd logs statefulset/argocd-application-controller --tail=100
kubectl -n argocd describe application bot-review-campaign
```

`OutOfSync` means Git differs from the cluster. `Unknown` commonly means the repository
or path is unreachable. `ImagePullBackOff` means the image tag or GHCR pull Secret is
wrong. `Pending` usually means insufficient CPU/GPU/memory or an unbound PVC.

## 22. Final demo endpoints

Run each port-forward in its own terminal. Keep the terminal open while using the URL:

```powershell
kubectl -n bot-campaign port-forward svc/detectra-api 8000:8000
kubectl -n bot-campaign port-forward svc/detectra-mlflow 5001:5000
kubectl -n bot-campaign port-forward svc/detectra-ray 8265:8265
kubectl -n bot-campaign port-forward svc/detectra-prometheus 9090:9090
kubectl -n bot-campaign port-forward svc/detectra-grafana 3000:3000
kubectl -n bot-campaign port-forward svc/detectra-kibana 5601:5601
kubectl -n bot-campaign port-forward svc/detectra-airflow-api 8084:8080
kubectl -n argocd port-forward svc/argocd-server 8081:443
```

| URL | Service to show |
|---|---|
| `http://localhost:8000` | Detectra web UI |
| `http://localhost:8000/docs` | FastAPI interactive API documentation |
| `http://localhost:8000/health/ready` | API/model readiness |
| `http://localhost:8000/metrics` | API Prometheus exposition |
| `http://localhost:5001` | MLflow experiments and registered models |
| `http://localhost:8265` | Ray dashboard and training jobs |
| `http://localhost:9090` | Prometheus graph, targets, and PromQL |
| `http://localhost:3000` | Grafana dashboards and Explore |
| `http://localhost:5601` | Kibana Discover and `detectra-logs-*` |
| `http://localhost:8084` | Airflow DAGs and retraining runs |
| `https://localhost:8081` | Argo CD Applications and Pod tree |

Kubernetes control-plane checks to show alongside the UIs:

```powershell
kubectl get nodes
kubectl -n bot-campaign get pods -o wide
kubectl -n bot-campaign get svc,pvc,job,cronjob
kubectl -n bot-campaign get events --sort-by=.lastTimestamp | Select-Object -Last 30
kubectl -n argocd get applications
```

The Kubernetes manifests currently cover serving, retraining, MLflow, metrics, logs,
Airflow, and Argo CD visibility. Kafka, Spark, and `campaign-scorer` remain a Docker
Compose streaming demonstration until equivalent Kubernetes manifests are added.

## 23. Complete setup reference

### 23.1 Configuration ownership

The project uses three configuration layers:

| Layer | Contains | Examples |
|---|---|---|
| Image | Versioned code and runtime dependencies | API, Ray/jobs, and Airflow DAGs |
| ConfigMap | Non-secret settings | Service URLs, feature versions, scrape rules |
| Secret | Passwords and credentials | Airflow keys, Grafana password, DVC and GHCR auth |

Never put passwords, tokens, cloud keys, or private connection strings in images,
ConfigMaps, Git, or this runbook.

### 23.2 Secret files

The repository contains templates only:

```text
k8s/secrets/airflow-secrets.example.yaml
k8s/secrets/core-secrets.example.yaml
k8s/secrets/ghcr-pull-secret.example.yaml
```

Create ignored local copies:

```powershell
Copy-Item k8s/secrets/airflow-secrets.example.yaml k8s/secrets/airflow-secrets.local.yaml
Copy-Item k8s/secrets/core-secrets.example.yaml k8s/secrets/core-secrets.local.yaml
```

Apply them directly to Kubernetes. Argo CD cannot read ignored local files from your
workstation, and they must never be committed.

#### Airflow Secret: `detectra-airflow-secrets`

Edit `airflow-secrets.local.yaml` and set `postgres-password`, `database-uri`,
`fernet-key`, `jwt-secret`, and the password inside `simple-auth-passwords.json`.
The password in `database-uri` must match `postgres-password`; URL-encode reserved
characters in the URI password.

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_urlsafe(48))"
notepad k8s/secrets/airflow-secrets.local.yaml
kubectl apply -f k8s/secrets/airflow-secrets.local.yaml
kubectl -n bot-campaign get secret detectra-airflow-secrets
```

The Airflow UI login is `admin` and the password assigned inside
`simple-auth-passwords.json`. Keep it in a password manager; do not print the Secret.

#### Core Secret: `detectra-storage` and `detectra-grafana-admin`

Edit `core-secrets.local.yaml`. Set `DVC_REMOTE_URL` and AWS values only when using an
S3-compatible DVC remote. Set `admin-password` for Grafana. If data is manually copied
to the workspace PVC, the DVC values are optional for that demo.

```powershell
notepad k8s/secrets/core-secrets.local.yaml
kubectl apply -f k8s/secrets/core-secrets.local.yaml
kubectl -n bot-campaign get secret detectra-storage detectra-grafana-admin
```

Grafana's administrator username is `admin`; its password is the `admin-password` value.
Anonymous access is Viewer-only in the Kubernetes manifest.

#### GHCR Secret: `detectra-ghcr-pull`

Use this only for private GHCR packages. Prefer creating it without writing the token to
a YAML file:

```powershell
kubectl -n bot-campaign create secret docker-registry detectra-ghcr-pull `
  --docker-server=ghcr.io `
  --docker-username=<github-username> `
  --docker-password=<new-package-read-token> `
  --docker-email=<email>
kubectl -n bot-campaign patch serviceaccount default `
  -p '{"imagePullSecrets":[{"name":"detectra-ghcr-pull"}]}'
```

Verify metadata only:

```powershell
kubectl -n bot-campaign get secret detectra-ghcr-pull
kubectl -n bot-campaign get serviceaccount default -o yaml
```

If an image reports `ImagePullBackOff`, inspect Pod events and confirm the tag exists in
GHCR. Never paste the token into a diagnostic command, ticket, or Git file.

### 23.3 Argo CD Git flow and conditions

Argo CD watches a Git repository and renders the Kustomize path configured by the
Application. The path must exist on the branch named by `targetRevision`:

```yaml
repoURL: https://github.com/njchathura-boop/Bot-Review-Campaign.git
targetRevision: main
path: k8s/overlays/full
destination.namespace: bot-campaign
```

A local branch may contain `k8s/overlays/full` while GitHub `main` does not. That causes:

```text
ComparisonError: app path does not exist
```

Merge and push the branch into `main` for the final demo, or temporarily set
`targetRevision: feature/njc` for a branch-only demo. Argo reads Git, not uncommitted
local files. Apply ignored Secrets separately to `bot-campaign`.

Install or repair Argo CD with server-side apply. This avoids the large CRD annotation
error for `applicationsets.argoproj.io`:

```powershell
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side --force-conflicts -n argocd `
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl -n argocd get pods
kubectl apply -f deploy/argocd-application.yaml
kubectl -n argocd get applications
```

Interpret Application conditions as follows:

| Status | Meaning |
|---|---|
| `Synced / Healthy` | Git and cluster agree and resources are ready |
| `OutOfSync` | Git differs from the cluster; review the diff and sync |
| `Unknown` | Git access or manifest rendering failed |
| `Missing` | A declared resource is absent |
| `Degraded` | A declared resource exists but is unhealthy |

Useful checks:

```powershell
kubectl -n argocd describe application bot-review-campaign
kubectl -n argocd get application bot-review-campaign `
  -o jsonpath="{.status.sync.status}{' / '}{.status.health.status}{'\n'}"
kubectl -n argocd logs deployment/argocd-repo-server --tail=100
kubectl -n argocd logs statefulset/argocd-application-controller --tail=100
```

### 23.4 Services, Pod access, and browser URLs

Pods are temporary. Use Services for stable traffic. Internal DNS names are:

| Service | Internal port | Purpose |
|---|---:|---|
| `detectra-api.bot-campaign.svc.cluster.local` | 8000 | API/UI |
| `detectra-mlflow.bot-campaign.svc.cluster.local` | 5000 | MLflow |
| `detectra-ray.bot-campaign.svc.cluster.local` | 8265 | Ray dashboard/jobs |
| `detectra-prometheus.bot-campaign.svc.cluster.local` | 9090 | Prometheus |
| `detectra-grafana.bot-campaign.svc.cluster.local` | 3000 | Grafana |
| `detectra-elasticsearch.bot-campaign.svc.cluster.local` | 9200 | Elasticsearch |
| `detectra-kibana.bot-campaign.svc.cluster.local` | 5601 | Kibana |
| `detectra-airflow-api.bot-campaign.svc.cluster.local` | 8080 | Airflow |

Forward Services to the Windows host in separate terminals:

```powershell
kubectl -n bot-campaign port-forward svc/detectra-api 8000:8000
kubectl -n bot-campaign port-forward svc/detectra-mlflow 5001:5000
kubectl -n bot-campaign port-forward svc/detectra-ray 8265:8265
kubectl -n bot-campaign port-forward svc/detectra-prometheus 9090:9090
kubectl -n bot-campaign port-forward svc/detectra-grafana 3000:3000
kubectl -n bot-campaign port-forward svc/detectra-kibana 5601:5601
kubectl -n bot-campaign port-forward svc/detectra-airflow-api 8084:8080
kubectl -n argocd port-forward svc/argocd-server 8081:443
```

Open these endpoints:

```text
http://localhost:8000                 Detectra UI
http://localhost:8000/docs            FastAPI docs
http://localhost:8000/health/ready    API/model readiness
http://localhost:8000/metrics         API metrics
http://localhost:5001                 MLflow
http://localhost:8265                 Ray
http://localhost:9090                 Prometheus
http://localhost:3000                 Grafana
http://localhost:5601                 Kibana
http://localhost:8084                 Airflow
https://localhost:8081                Argo CD
```

Check Service backends. Empty endpoints mean no Ready Pod matches the Service selector:

```powershell
kubectl -n bot-campaign get svc,endpoints,endpointslices
kubectl -n bot-campaign get pods --show-labels
kubectl -n bot-campaign describe svc detectra-api
```

### 23.5 Persistent volumes and claims

Separate PVCs keep training, tracking, metrics, logs, and orchestration data isolated.
Docker Desktop's default StorageClass dynamically provisions these claims inside the
Docker Kubernetes VM.

| PVC | Size | Used by | Stores |
|---|---:|---|---|
| `detectra-workspace` | 50 GiB | Ray and ETL | DVC data, processed bundles, Ray results, candidate artifacts, reports |
| `detectra-mlflow` | 10 GiB | MLflow | SQLite metadata, experiments, artifacts, registered model data |
| `detectra-prometheus` | 10 GiB | Prometheus | Time-series data with seven-day retention |
| `detectra-grafana` | 2 GiB | Grafana | Grafana state and settings |
| `detectra-elasticsearch` | 20 GiB | Elasticsearch | `detectra-logs-*` log indices |
| `detectra-airflow-postgres` | 10 GiB | Airflow PostgreSQL | DAG runs, task state, and scheduler metadata |

The claims use `ReadWriteOnce`, suitable for the single Docker Desktop node. The shared
workspace is mounted into Ray and ETL with subdirectories for data, artifacts, and
reports. Keep the MLflow and workspace claims between retraining runs.

Inspect storage:

```powershell
kubectl -n bot-campaign get pvc
kubectl -n bot-campaign get pv
kubectl get storageclass
kubectl -n bot-campaign describe pvc detectra-workspace
kubectl -n bot-campaign get pods -o custom-columns=NAME:.metadata.name,CLAIMS:.spec.volumes[*].persistentVolumeClaim.claimName
```

Deleting a Pod does not delete its PVC. Deleting the `bot-campaign` namespace removes
namespace-owned PVCs and therefore MLflow, workspace, monitoring, log, and Airflow data.

### 23.6 Grafana and Prometheus queries

Prometheus discovers annotated API and Ray Pods in `bot-campaign`. The API exposes
`/metrics`; Ray exposes metrics on port 8080. Check targets first:

```powershell
(Invoke-RestMethod http://localhost:9090/api/v1/targets).data.activeTargets |
  Select-Object health,lastError,@{Name="job";Expression={$_.labels.job}},@{Name="instance";Expression={$_.labels.instance}}
```

Paste these into Prometheus Graph or Grafana Explore:

```promql
up{namespace="bot-campaign"}
up{namespace="bot-campaign",app="detectra-api"}
sum(up{namespace="bot-campaign",app="detectra-ray"})
api_requests_total
rate(api_requests_total[5m])
api_errors_total
100 * rate(api_errors_total[5m]) / clamp_min(rate(api_requests_total[5m]), 1)
api_request_latency_p50_ms
api_request_latency_p95_ms
api_request_latency_p99_ms
review_scans_total
increase(review_scans_total[5m])
review_scans_per_second
review_inference_p95_ms
review_mean_risk
review_mean_confidence
review_needs_human_review_total
campaign_alerts_total
campaign_moderation_confirmed_total
campaign_moderation_dismissed_total
campaign_moderation_restored_total
campaign_stream_scores_total
campaign_stream_candidates_total
campaign_stream_consumer_errors_total
increase(campaign_stream_consumer_errors_total[15m])
review_invalid_requests_total
sum by (language) (review_language_reviews_total)
api_uptime_seconds
api_disk_free_bytes / 1024 / 1024 / 1024
```

For the demo, explain that `up=1` means a scrape succeeds, request rate shows traffic,
error rate shows failed HTTP requests, p95 is tail latency, and review counters should
increase after API calls. Stream metrics remain zero when the Kubernetes streaming
boundary is disabled.

Show the provisioned dashboards:

```text
Detectra / API Operations
Detectra / Ray Cluster
Detectra Kubernetes Overview
```

The API Operations dashboard is the business view. The Ray dashboard is the training and
compute view. Prometheus Explore is the ad-hoc query view. Grafana anonymous access is
Viewer-only; use the configured admin password for dashboard changes.

### 23.7 Kibana and Elasticsearch queries

Filebeat reads Kubernetes container logs from `/var/log/containers`, enriches them with
Kubernetes metadata, and writes daily `detectra-logs-*` indices. In Kibana Discover,
select that data view and set the time range to the last 15 minutes.

KQL filters:

```text
kubernetes.namespace: "bot-campaign"
kubernetes.namespace: "bot-campaign" and kubernetes.container.name: "api"
kubernetes.namespace: "bot-campaign" and kubernetes.container.name: "ray"
kubernetes.namespace: "bot-campaign" and kubernetes.container.name: "scheduler"
kubernetes.namespace: "bot-campaign" and message: *error*
kubernetes.namespace: "bot-campaign" and message: *review_scored*
kubernetes.namespace: "bot-campaign" and message: *demo-review-001*
```

KQL filters documents; it does not run PromQL or mutate data. If Kibana is empty, check
the time range, data view, Filebeat, and Elasticsearch:

```powershell
Invoke-RestMethod http://localhost:9200/_cluster/health | ConvertTo-Json
Invoke-RestMethod "http://localhost:9200/_cat/indices/detectra-logs-*?format=json" |
  Format-Table index,docs.count,store.size
Invoke-RestMethod http://localhost:9200/detectra-logs-*/_count
kubectl -n bot-campaign logs daemonset/detectra-filebeat --tail=100
kubectl -n bot-campaign get job detectra-kibana-setup
```

For Kibana Dev Tools, query newest API errors with Elasticsearch Query DSL:

```json
GET detectra-logs-*/_search
{
  "size": 20,
  "sort": [{"@timestamp": "desc"}],
  "query": {
    "bool": {
      "filter": [
        {"term": {"kubernetes.namespace": "bot-campaign"}},
        {"query_string": {"query": "message:(error OR ERROR OR exception)"}}
      ]
    }
  }
}
```

For the final monitoring sequence, show Pods and PVCs, Argo Application health,
Prometheus targets, one review request, the increase in `review_scans_total`, the API
Grafana dashboard, a Ray view, MLflow runs, Kibana filtered logs, and API readiness.
