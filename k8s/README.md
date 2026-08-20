# Detectra Kubernetes deployment

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

The monitoring tier deliberately uses separate Prometheus and Grafana pods. Combining
them in one pod would couple storage, upgrades, health checks, and failure recovery.
The API starts with one replica because campaign state is currently in process. Increase
replicas only after that state is moved to PostgreSQL or Redis.

## Images people can pull

The release workflow publishes two images:

```text
ghcr.io/njchathura-boop/bot-review-campaign-api:<git-sha>
ghcr.io/njchathura-boop/bot-review-campaign-jobs:<git-sha>
```

The API image contains the UI and the two promoted model bundles, so inference never
silently falls back to an untrained model. The jobs image contains the ETL, DVC, Ray,
DistilBERT training, and campaign-training code. Git-SHA tags are immutable; `latest` is
only a convenient pointer to the newest published release.

Make both GHCR packages public in **GitHub -> Packages -> Package settings -> Change
visibility**, or configure a Kubernetes `imagePullSecret` for a private package.

## Prerequisites

- Kubernetes 1.27 or newer and `kubectl`; Docker Desktop Kubernetes is suitable for a demo.
- A dynamic default StorageClass.
- At least 8 CPU cores, 16 GB RAM, and 75 GB free cluster storage for the complete stack.
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

The `cd` workflow checks out LFS objects, verifies both model files, builds both images,
tags each image with the exact Git SHA, scans them with Trivy, pushes them to GHCR, and
deploys the same immutable SHA to the protected `staging` environment.

For a local build:

```powershell
git lfs pull
$sha = git rev-parse HEAD
$registry = "ghcr.io/njchathura-boop"

docker build --build-arg "RELEASE_VERSION=local" --build-arg "SOURCE_COMMIT=$sha" `
  --tag "$registry/bot-review-campaign-api:$sha" --file Dockerfile .

docker build --build-arg "RELEASE_VERSION=local" --build-arg "SOURCE_COMMIT=$sha" `
  --tag "$registry/bot-review-campaign-jobs:$sha" --file docker/ray.Dockerfile .

docker login ghcr.io
docker push "$registry/bot-review-campaign-api:$sha"
docker push "$registry/bot-review-campaign-jobs:$sha"
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

Do not commit access keys. Create the namespace and secrets directly in the cluster:

```powershell
kubectl apply -f k8s/base/namespace.yaml
kubectl -n bot-campaign create secret generic detectra-storage `
  --from-literal=DVC_REMOTE_URL="s3://YOUR-BUCKET/detectra" `
  --from-literal=AWS_ACCESS_KEY_ID="YOUR_KEY" `
  --from-literal=AWS_SECRET_ACCESS_KEY="YOUR_SECRET" `
  --from-literal=AWS_DEFAULT_REGION="ap-south-1"

kubectl -n bot-campaign create secret generic detectra-grafana-admin `
  --from-literal=admin-user="admin" `
  --from-literal=admin-password="USE_A_STRONG_PASSWORD"
```

`k8s/secret.example.yaml` is a field reference only. Never apply it with placeholder
values and never commit a populated copy.

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

Without the helper script, `kubectl apply -k k8s` deploys the CPU stack with `latest`.
`kubectl apply -k k8s/overlays/gpu` deploys the GPU overlay. The helper is preferred
because it pins the API, Ray, and ETL workloads to one immutable SHA.

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
```

| URL | Expected result |
|---|---|
| `http://localhost:8000` | Detectra UI |
| `http://localhost:8000/health/ready` | API and both model bundles ready |
| `http://localhost:8265` | Ray jobs and cluster dashboard |
| `http://localhost:5001` | MLflow experiments |
| `http://localhost:9090/targets` | `detectra-api` and `detectra-ray` targets are `UP` |
| `http://localhost:3000/dashboards` | Detectra folder and pre-provisioned dashboard |

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

The committed job requests 20 Ray Tune samples, two epochs per trial, and one concurrent
trial. Change those values deliberately in `k8s/jobs/review-training-job.yaml`. The GPU
overlay gives the Ray pod one NVIDIA GPU and sets one GPU per trial.

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
kubectl kustomize k8s/jobs
```

The public Detectra UI remains read-only for infrastructure. It cannot start training,
run ETL, modify Kubernetes, or change enforcement policy.
