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

Verify the API:

```powershell
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-RestMethod http://localhost:8000/v1/ops/summary
```

Open the UI, click **Scan review**, then click **Replay campaign**. Confirm the review
result contains `review-risk-distilbert-v1` and campaign replay contains
`hybrid-distilbert`.

## 8. Start Kafka and Spark streaming

```powershell
docker compose up -d kafka spark-master spark-worker
docker compose --profile stream up -d spark-stream
docker compose --profile score up -d campaign-scorer
```

Replay held-out events:

```powershell
python streaming/producer.py `
  --input data/processed/temporal_bundle/campaign_v3/test.jsonl `
  --bootstrap-servers localhost:9092 `
  --rate 100
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

## 9. Airflow scheduled execution

```powershell
docker compose -f orchestration/docker-compose.airflow.yml up airflow-init
docker compose -f orchestration/docker-compose.airflow.yml up -d airflow-scheduler airflow-api-server
```

Open http://localhost:8080 and trigger `bot_campaign_model_retraining`. Configure
`BOT_CAMPAIGN_BEHAVIORAL_INPUTS` and `BOT_CAMPAIGN_CAMPAIGN_SCENARIO_COUNT` before a
full scheduled run. Airflow writes candidates under `artifacts/candidates/`; it never
automatically replaces production bundles.

## 10. Ray out-of-memory recovery

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

## 11. Final acceptance checklist

- MLflow contains completed selected runs and registered model versions.
- Both model `bundle.json` files exist.
- `/health/ready` reports both models ready.
- UI review scan returns the DistilBERT version.
- UI campaign replay returns the hybrid campaign engine.
- Prometheus shows request and latency metrics.
- Kafka/Spark logs show processed events and no checkpoint errors.
- Production predictions contain Git, model, data, feature, schema, and image lineage.
