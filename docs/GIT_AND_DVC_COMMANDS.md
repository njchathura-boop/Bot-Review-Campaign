# Git and DVC Command Reference

Git versions source code and deployment configuration. DVC versions large datasets and
model outputs. MLflow versions experiments and registered models.

## Git

Run `git status` to see changed files, `git branch -a` to see branches, and
`git log --oneline --decorate --graph -20` to inspect history. Use `git diff` before
staging and `git diff --cached` after staging.

Create a feature branch with `git switch -c feat/my-change`. Stage one logical change
with `git add src web tests`, commit with `git commit -m "feat(ui): improve scanner"`,
and publish with `git push -u origin feat/my-change`. Update safely with
`git pull --ff-only`. Never commit directly to `main`.

Release after review and CI pass:

`git switch main`, `git pull --ff-only`, `git tag -a v1.1.0 -m "Bot campaign detection v1.1.0"`, then `git push origin v1.1.0`.

## DVC files

| File | Purpose |
|---|---|
| `dvc.yaml` | Pipeline stages, dependencies, commands, and outputs |
| `dvc.lock` | Exact dependency/output hashes for one run |
| `.dvc/config` | DVC settings and remote name |
| `.dvcignore` | Files ignored by DVC |

The repository has been initialized because `.dvc/config` exists. A dataset snapshot is
not complete until a stage creates `dvc.lock`. Install/check DVC with `python -m pip install dvc`, `dvc doctor`, and `dvc status`.

## Why `dvc add` failed

`data/processed/dataset_bundle` and `data/processed/temporal_bundle` are already
declared as `outs` in `dvc.yaml`. They must be updated through their stages. Running
`dvc add` on either directory reports an overlap because two DVC owners cannot manage the
same path. Do not create separate `.dvc` files for these stage outputs.

## Create a dataset snapshot

Inspect the graph with `dvc dag` and status with `dvc status`. Run
`dvc repro build_dataset_bundle` and `dvc repro build_temporal_bundle`, or run all
declared stages with `dvc repro`. This creates or updates `dvc.lock`.

Inspect it with `Get-Content dvc.lock`, then commit metadata using:

`git add dvc.yaml dvc.lock .dvc/config .dvcignore`

`git commit -m "data: record reproducible dataset snapshot"`

If an existing output was deliberately generated with the correct code, inputs, and
seed, `dvc commit build_dataset_bundle` or `dvc commit build_temporal_bundle` captures
its current hash without rerunning. Verify the output first.

## What `<your-object-storage-path>` means

It is where DVC stores large content outside Git. It is not a GitHub URL and not the
dataset path inside the repository.

For a local laptop remote, run `New-Item -ItemType Directory -Force ..\bot-campaign-dvc-storage`, then `dvc remote add -d local ..\bot-campaign-dvc-storage` and `dvc push`. This is only for local testing.

For S3, a value such as `s3://bot-campaign-dvc/datasets` means bucket `bot-campaign-dvc`
with prefix `datasets`. Configure it with `dvc remote add -d storage s3://bot-campaign-dvc/datasets`, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as environment variables, then run `dvc push`.

For MinIO, use `dvc remote add -d minio s3://bot-campaign-dvc/datasets` and
`dvc remote modify minio endpointurl http://localhost:9000`, then set the MinIO access
variables and run `dvc push`. The current Compose file does not start MinIO automatically.

## Normal workflow

`change data/code -> dvc repro -> inspect dvc.lock -> git commit metadata -> dvc push -> git push`

Push DVC content before another machine checks out the Git commit that references it.
Useful commands are `dvc cache dir`, `dvc remote list`, `dvc pull`, `dvc metrics show`,
and `dvc metrics diff HEAD~1`.

Authoritative locations: code is in Git commits/tags; dataset snapshots are in
`dvc.lock` plus the DVC remote; review models are in `artifacts/review_distilbert/bundle.json`
and MLflow; containers are Git-SHA images in GHCR.
