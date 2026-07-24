# Operations, releases, and versioning

## Local demo

```powershell
python -m pip install -e ".[dev,synthetic]"
python -m bot_campaign.cli train
python -m uvicorn bot_campaign.api:app --reload
```

Open `http://localhost:8000`. The UI exposes live process metrics and clearly marks
unconfigured Kafka, Spark, MLflow, and Kubernetes services as `not_configured` rather
than claiming that they are healthy.

For the container stack:

```powershell
docker compose up -d api prometheus grafana
docker compose ps
```

Use `http://localhost:3000` for Grafana and `http://localhost:9090` for Prometheus.
MLflow is available at `http://localhost:5001` only when its service is started.

## Generate the controlled dataset

```powershell
python -m bot_campaign.cli e2e-demo
python -m bot_campaign.cli generate-synthetic --preset full
python -m bot_campaign.cli validate data/processed/synthetic_campaigns_50k.jsonl --labeled
```

The JSONL and manifest are ignored by Git. Commit only the generator, tests, and DVC
metadata. The generator is deterministic and offline. Any later LLM-generated variant
must use a pinned model revision, be saved as a distinct dataset version, and pass human
review before training.

## Git workflow

The repository remote is
`https://github.com/njchathura-boop/Bot-Review-Campaign.git`. Daily work uses a branch
and a Conventional Commit:

```powershell
git switch main
git pull --ff-only
git switch -c feat/short-purpose
git status
git diff
git add web src tests
git commit -m "feat(scope): describe the completed change"
git push -u origin feat/short-purpose
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
Apply `deploy/argocd-application.yaml` once to connect Argo CD. Argo CD synchronizes
the approved Kubernetes image revision. Roll back by reverting the deployment-manifest
commit; model rollback selects the previous approved MLflow registered-model version.
Both versions remain visible in the lineage endpoint and UI.

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
