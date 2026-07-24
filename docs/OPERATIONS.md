# Operations, releases, and versioning

## Local demo

```powershell
python -m pip install -e ".[dev,synthetic]"
python -m bot_campaign.cli train
python -m uvicorn bot_campaign.api:app --reload
```

Open `http://localhost:8000`. The UI exposes live process metrics and clearly marks
unconfigured Kafka, Spark, MLflow, PostgreSQL, Redis, MinIO, and Kubernetes services
as demo services rather than claiming that they are healthy.

For the container stack:

```powershell
docker compose up -d api kafka postgres redis minio mlflow prometheus grafana
docker compose ps
```

Use `http://localhost:3000` for Grafana, `http://localhost:5001` for MLflow, and
`http://localhost:9090` for Prometheus. Compose credentials are development-only.

## Generate the controlled dataset

```powershell
python -m bot_campaign.cli generate-synthetic
python -m bot_campaign.cli validate data/processed/synthetic_campaigns_50k.jsonl --labeled
```

The JSONL and manifest are ignored by Git. Commit only the generator, tests, and DVC
metadata. The default generator is deterministic and offline. Qwen-generated text
must use a pinned model revision, be saved as a distinct dataset version, and pass
human review before training.

## Git workflow

This checkout did not contain an initialized Git repository. Initialize it only once:

```powershell
git init -b main
git add .gitignore .dockerignore pyproject.toml README.md Dockerfile docker-compose.yml
git add configs data/sample docs k8s monitoring spark src streaming tests training web .github
git commit -m "chore(repo): initialize bot campaign detection platform"
gh repo create bot-campaign-detection --private --source . --remote origin --push
```

Daily work uses a branch and a Conventional Commit:

```powershell
git switch main
git pull --ff-only
git switch -c feat/review-scanning-ui
git status
git diff
git add web src tests
git commit -m "feat(ui): add live review scanning workflow"
git push -u origin feat/review-scanning-ui
gh pr create --fill
```

Never commit `.env`, downloaded data, checkpoints, model binaries, MLflow databases,
or monitoring volumes. PRs must pass lint, tests, training smoke tests, data generation,
container build, and Trivy scanning.

## Release and rollback

Application releases use SemVer. Images are first identified by immutable Git SHA:

```powershell
git switch main
git pull --ff-only
git tag -a v1.0.0 -m "Bot campaign detection v1.0.0"
git push origin v1.0.0
```

Update the Kubernetes image placeholder to the approved SHA through a deployment PR.
Argo CD deploys staging, followed by smoke/shadow checks and 5%, 25%, and 100% canary
stages. Roll back by reverting the deployment-manifest commit; model rollback selects
the previous approved MLflow registered-model version. Both versions remain visible in
the lineage endpoint and UI.

## Alert thresholds

- inference p95 above 150 ms;
- API error rate above 1%;
- Kafka lag outside the processing window;
- failed Spark checkpoint recovery;
- unavailable or mismatched model version;
- material feature/embedding drift;
- campaign alert or moderator-dismissal spikes.

Individual review risk never activates a soft limit. Campaign confirmation creates a
24-hour limit with an audit event; dismiss and restore remove it immediately.
