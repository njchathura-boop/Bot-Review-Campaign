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
