# Detectra Viva and Demonstration Rehearsal Handbook

This living document prepares the team for the project demonstration and technical viva.
Each topic records what to demonstrate, what to say, the code to know, likely live-change
requests, expected viva questions, architectural decisions, trade-offs, and known limitations.

## Rehearsal method

For every technology or design topic, practise these seven parts:

1. What to show in the demonstration.
2. What to explain while showing it.
3. Which files and functions to know.
4. Likely requests to change code live.
5. Likely viva questions and strong answers.
6. Weaknesses, loopholes, and honest limitations.
7. Architectural decisions and their trade-offs.

## Topic roadmap

1. End-to-end architecture - included below.
2. Git, Git LFS, and DVC.
3. Dataset preparation and synthetic data.
4. Baseline logistic-regression model.
5. DistilBERT review-risk model.
6. Hybrid campaign model and graph grouping.
7. Ray Tune and GPU training.
8. MLflow.
9. Airflow and PostgreSQL.
10. Kafka.
11. Spark Structured Streaming.
12. FastAPI and frontend UI.
13. Prometheus and Grafana.
14. Filebeat, Elasticsearch, and Kibana.
15. Docker and Docker Compose.
16. Kubernetes.
17. GitHub Actions CI/CD.
18. Security, limitations, and future production architecture.

---

# Topic 1: End-to-end architecture

## Beginner explanation

Detectra has two related but separate detection paths.

```text
Individual review path
Review -> DistilBERT -> review-risk probability -> human-review decision
```

```text
Campaign path
Many reviews
-> Kafka
-> Spark event-time windows
-> graph-based grouping
-> hybrid DistilBERT + numerical model
-> campaign-risk probability
-> moderator console
```

The individual model asks:

> Does this one review look risky?

The campaign model asks:

> Do these reviews collectively behave like a coordinated campaign?

A review can look harmless individually but become suspicious when it is connected to reviews
from related accounts, products, timestamps, ratings, or similar language.

## What to demonstrate

### 1. Show the architecture diagram

Explain the components from left to right:

```text
Data -> DVC -> Airflow -> Ray -> MLflow -> API
                                  |
Browser -> API -> Kafka -> Spark -> Campaign scorer -> UI
                                  |
                     Prometheus/Grafana and ELK logs
```

Do not imply that every UI review automatically travels through Spark.

- **Scan Review** performs synchronous individual-review inference through FastAPI.
- **Replay Campaign** publishes multiple review events to Kafka.
- Spark processes the replayed or streamed events.
- The campaign scorer scores Spark's candidate windows.
- The API materializes campaign scores for the moderator UI.

### 2. Show the UI

Open:

```text
http://localhost:8000
```

Demonstrate:

- Individual review scoring.
- Risk probability and supporting evidence.
- Model, data, feature, and schema versions.
- Campaign replay.
- Campaign console.
- Moderator decision.

Suggested narration:

> The synchronous path provides an immediate score for one review. The asynchronous streaming
> path detects coordinated behaviour across multiple reviews and across time.

### 3. Show service health

```powershell
docker compose ps
Invoke-RestMethod http://localhost:8000/health/live
Invoke-RestMethod http://localhost:8000/health/ready
```

Suggested explanation:

> Liveness proves that the application process is running. Readiness checks whether the
> application is prepared to serve useful requests and reports its dependent service state.

### 4. Show a full replay

Start the streaming services:

```powershell
docker compose --profile stream up -d spark-stream campaign-scorer
```

Replay reviews:

```powershell
docker compose exec campaign-scorer python streaming/producer.py `
  --input data/processed/temporal_bundle/campaign/test.jsonl `
  --bootstrap-servers kafka:29092 `
  --rate 10
```

Explain the topics:

```text
reviews.raw.v1
-> reviews.analysis-windows.v1
-> reviews.campaign-scores.v1
```

Finally, refresh the campaign console in the web UI.

## Files to know

| File | Responsibility |
|---|---|
| `src/bot_campaign/api.py` | Creates FastAPI and registers routes. |
| `src/bot_campaign/routes/reviews.py` | Individual-review endpoints. |
| `src/bot_campaign/routes/campaigns.py` | Campaign and moderator endpoints. |
| `streaming/producer.py` | Validates and publishes review events to Kafka. |
| `spark/review_stream.py` | Validates, normalizes, watermarks, and windows events. |
| `src/bot_campaign/campaign_graph.py` | Creates graph edges and candidate groups. |
| `streaming/campaign_scorer.py` | Consumes Spark windows and publishes campaign scores. |
| `src/bot_campaign/hybrid_model.py` | Implements the hybrid neural architecture. |
| `docker-compose.yml` | Connects the local services and their configuration. |

## Likely live code-change requests

### Change the minimum campaign size from three to five

The minimum group size is passed into the campaign graph and scorer logic.

Expected explanation:

> Increasing the minimum group size reduces false positives from small accidental clusters,
> but it may miss small coordinated campaigns.

After the change, test both boundaries:

- A group of four reviews should not become a candidate.
- A group of five reviews may become a candidate if its edge conditions are satisfied.

### Change the semantic-similarity threshold

The current graph logic uses a semantic threshold around `0.88`.

Relevant file:

```text
src/bot_campaign/campaign_graph.py
```

- A higher threshold requires reviews to be more similar.
- A lower threshold creates more edges and candidate groups.
- Too low a threshold can connect unrelated reviews.
- Too high a threshold can miss paraphrased campaigns.

### Add a field to the review API

Expected implementation sequence:

1. Add the field to the Pydantic request model.
2. Decide whether the field is required or optional.
3. Propagate it into preprocessing or inference features.
4. Update tests and example payloads.
5. Verify the generated Swagger schema.
6. Consider schema-version and client compatibility.

Strong explanation:

> I would initially make the field optional when older producers do not provide it. Otherwise,
> existing API clients and historical Kafka events could fail validation.

### Change the Spark window duration

The current event-time configuration uses:

```text
One-hour window
Ten-minute slide
Two-hour watermark
```

- A larger window can detect slower coordination.
- It retains more streaming state and consumes more memory.
- It may combine unrelated activity.
- A smaller window reacts faster but can miss slow campaigns.

### Add another service

A complete service addition normally requires changes to:

1. Docker Compose or Kubernetes manifests.
2. Environment configuration or a ConfigMap.
3. Liveness and readiness checks.
4. Prometheus scrape configuration.
5. Grafana dashboards.
6. Filebeat log collection.
7. CI/CD image build and deployment-wait conditions.

## Likely viva questions and answers

### Why use two models?

> Individual review risk and campaign coordination are different prediction problems. The
> review model analyses one text. The campaign model analyses relationships and aggregate
> behaviour across multiple reviews. They have different units of prediction and evidence.

### Why not send every review directly through Spark?

> Individual review scoring requires low latency and does not require cross-event state. Spark
> adds distributed scheduling and state-management overhead. It is valuable when multiple
> events must be grouped over time, not for one immediate prediction.

### Why place Kafka between the API and Spark?

> Kafka decouples ingestion from processing. The producer can accept events without waiting
> for Spark, Kafka can buffer bursts, events can be replayed, and consumers can recover from
> their recorded offsets after temporary failures.

### Why use graph grouping before the campaign model?

> Scoring every possible combination of reviews would be computationally expensive. Graph
> rules identify plausible connected components first. The neural model then performs expensive
> scoring only on those candidate groups.

### Is replay the same as production streaming?

> No. Replay is a controlled simulation that sends historical or generated events through the
> real Kafka-Spark-scorer path. In production, the producer would be the live review-submission
> platform.

### Where is cleaning performed?

> FastAPI validates synchronous requests. The producer verifies required fields before Kafka.
> Spark validates and normalizes streaming events, applies event-time rules, and creates windows.
> Model preprocessing then performs tokenization and numerical feature conversion.

### What happens if Spark is unavailable?

> Kafka retains raw events according to its retention policy. After Spark recovers, it can
> continue from committed offsets and checkpoint state. The synchronous review API remains a
> separate path and can continue operating.

### What happens if the campaign scorer fails?

> Spark output remains in `reviews.analysis-windows.v1`. The scorer can restart and continue
> consuming. Invalid or unprocessable inputs can be published to
> `reviews.campaign-scores.dlq.v1`.

### Why is campaign processing asynchronous?

> Campaign evidence accumulates over time. The system cannot know that one review belongs to a
> coordinated group until related reviews arrive. Asynchronous processing allows the platform
> to accumulate and analyse that evidence without delaying individual review submissions.

## Architectural decisions and trade-offs

### Synchronous individual-review scoring

Chosen because:

- Users expect immediate results.
- A single review requires no cross-event state.
- FastAPI can provide low-latency inference.

Trade-off:

- One review alone cannot establish campaign coordination.

### Kafka event backbone

Chosen because:

- Producers and consumers can scale independently.
- Events can be replayed.
- Temporary consumer outages do not immediately lose events.

Trade-offs:

- Kafka adds operational complexity.
- Offsets, partitions, retention, and consumer lag must be managed.

### Spark Structured Streaming

Chosen because:

- Campaign detection needs event-time windows.
- Spark supports watermarks and late-event handling.
- It can scale beyond a single local process.

Trade-offs:

- Spark is heavy for a laptop.
- Stateful windows consume memory.
- A smaller system may not require a distributed engine.

### Graph candidate generation

Chosen because:

- Campaigns are relationships among reviews.
- Connected components naturally represent related groups.
- Candidate generation reduces expensive neural inference.

Trade-offs:

- Hand-designed edges encode assumptions.
- Poor thresholds can split real campaigns or merge unrelated reviews.

### Separate hybrid campaign model

Chosen because:

- Text alone cannot represent timing, account diversity, or purchase behaviour.
- Numerical features alone cannot capture semantic similarity or paraphrasing.
- Feature fusion uses both forms of evidence.

Trade-offs:

- The model is harder to train and explain.
- Both text and behavioural features must be available at inference time.

## Known loopholes and honest limitations

### Synthetic campaign labels

The campaign model is trained and evaluated primarily on controlled synthetic scenarios.

Recommended viva answer:

> The synthetic dataset validates the architecture against known campaign patterns, but it does
> not prove generalisation to real adversarial campaigns. A larger human-labelled real campaign
> dataset is the most important future improvement.

### Perfect campaign metrics

The selected campaign run produced perfect results on a small controlled test of 80 groups.

Recommended viva answer:

> This is a smoke-scale controlled evaluation, not a production accuracy claim. It demonstrates
> that the feature, model, and evaluation path can separate the generated scenarios. Real-world
> performance must be measured on independently labelled campaigns.

### Single-node local infrastructure

Kafka, Spark, Elasticsearch, and several other tools run locally as single instances.

Recommended viva answer:

> The local configuration demonstrates correct integration and component boundaries. It does
> not provide production high availability, multi-broker replication, or multi-node resilience.

### Controlled replay

Replay demonstrates the real streaming path, but it does not prove production throughput,
partition balance, retention sizing, or resilience under adversarial load.

### Incomplete moderator feedback loop

Moderator decisions can be stored and monitored, but a production retraining feedback loop must
also validate labels, prevent poisoning, approve dataset versions, and govern model promotion.

### Threshold drift

Fixed similarity, timing, group-size, and decision thresholds may become outdated as products,
users, languages, or attacker behaviour change. They require monitoring and recalibration.

## Strong closing answer

If asked why the architecture is justified, answer:

> Detectra separates low-latency individual inference from stateful campaign detection. FastAPI
> handles immediate review scoring, while Kafka and Spark handle coordination across many events
> and across time. Graph rules reduce the candidate search space, and the hybrid model combines
> semantic and behavioural evidence. DVC, Airflow, Ray, MLflow, Docker, Kubernetes, and the
> observability stack make the surrounding lifecycle reproducible and operable. The largest
> current limitation is that campaign evaluation still relies heavily on controlled synthetic
> data.

---

# Topic 2: Git, Git LFS, and DVC

## Beginner explanation

The project uses three related versioning mechanisms because code, model weights, and datasets
have different storage requirements.

```text
Git
-> source code, tests, configuration, manifests, documentation, and small metadata

Git LFS
-> large model files that must follow a Git commit and be available for deployment

DVC
-> large datasets and reproducible data/model pipeline dependencies and outputs
```

Git records the project history. Git LFS replaces a large tracked file with a small pointer in
Git and stores the real object in LFS storage. DVC records dataset and pipeline hashes in Git,
while storing large data in a DVC cache and optional remote such as MinIO.

Neither Git LFS nor DVC replaces Git. Both store small pointer or metadata files in Git so that a
Git commit can refer to the correct large objects.

## What is tracked by each tool

| Tool | Current project responsibility | Examples |
|---|---|---|
| Git | Human-readable project definition and history | `src/`, `training/`, `tests/`, `docker-compose.yml`, `dvc.yaml`, Kubernetes manifests, reports |
| Git LFS | Large deployment model bundles | `model.safetensors`, `model_state.pt`, and currently other files under the tracked model directories |
| DVC | Raw/processed dataset versions and pipeline reproduction | `data/raw.dvc`, `dvc.yaml`, `dvc.lock`, DVC cache and MinIO objects |
| MLflow | Experiment runs, parameters, metrics, registered-model versions, and training artifacts | `review-risk-distilbert`, `bot-campaign-hybrid-distilbert` |
| Docker volumes | Mutable local service state, not source history | MLflow database, Prometheus data, Grafana data, Elasticsearch indices, Airflow PostgreSQL |

The current `.gitattributes` tracks these directories with Git LFS:

```text
artifacts/review_distilbert/**
artifacts/campaign_model/**
artifacts/candidates/review_distilbert/**
```

The current DVC default remote is named `minio_remote` and points to:

```text
s3://bot-campaign-bucket
endpoint: http://localhost:9000
```

## What to demonstrate

### 1. Show Git state and history

```powershell
git status
git branch --show-current
git log --oneline --decorate -10
```

Suggested narration:

> Git versions the code, configuration, documentation, small manifests, and the pointer files
> required by Git LFS and DVC.

### 2. Show Git LFS objects

```powershell
git lfs ls-files
git check-attr filter -- artifacts/review_distilbert/model/model.safetensors
```

Expected attribute:

```text
filter: lfs
```

Explain the symbols returned by `git lfs ls-files`:

- `*` means the real LFS object is materialized in the working tree.
- `-` means the working-tree file is still an LFS pointer.

Show a pointer only when it is safe to do so:

```text
version https://git-lfs.github.com/spec/v1
oid sha256:...
size ...
```

Do not deliberately replace an artifact with a pointer while Ray or the API is using the same
bind-mounted directory.

### 3. Show the DVC pipeline

Ensure DVC is installed in the active environment:

```powershell
pip install -e ".[mlops]"
dvc version
```

Then demonstrate:

```powershell
dvc dag
dvc status
dvc remote list
```

Explain:

- `dvc.yaml` describes commands, dependencies, parameters, metrics, and outputs.
- `dvc.lock` records the exact hashes produced by the last successful reproduction.
- `data/raw.dvc` points to the versioned raw-data directory.
- The DVC cache avoids rebuilding or downloading unchanged content.
- The remote stores cache objects outside Git.

### 4. Reproduce one stage

For the baseline:

```powershell
dvc repro train_product_model
```

For the processed datasets:

```powershell
dvc repro build_temporal_bundle build_dataset_bundle
```

Suggested narration:

> DVC compares dependency hashes with the hashes recorded in `dvc.lock`. If nothing relevant
> changed, it reuses the existing outputs. If a dependency or command changed, it reruns that
> stage and any required downstream stages.

### 5. Show remote synchronization

```powershell
dvc pull data/raw.dvc
dvc push
```

Explain that `dvc pull` downloads the objects referenced by the current Git revision. `dvc push`
uploads locally produced DVC cache objects. Neither command commits source code to Git.

## Files to know

| File | Responsibility |
|---|---|
| `.gitignore` | Excludes generated, secret, cache, and local-only files from normal Git tracking. |
| `.gitattributes` | Defines which paths use the Git LFS filter. |
| `.dvc/config` | Defines DVC remotes and the default remote. |
| `data/raw.dvc` | Git-tracked pointer to the raw dataset directory. |
| `dvc.yaml` | Defines the reproducible pipeline stages. |
| `dvc.lock` | Pins dependency and output hashes from the last reproduction. |
| `orchestration/dags/bot_campaign_training.py` | Calls `dvc pull`, `dvc repro`, and optionally `dvc push` from Airflow. |
| `k8s/secrets/core-secrets.yaml` | Template for the Kubernetes DVC/MinIO connection; populated local copies must not be committed. |

## Likely live code-change requests

### Add a new DVC stage

Expected procedure:

1. Add a stage under `stages:` in `dvc.yaml`.
2. Define its `cmd`.
3. List every code/data dependency under `deps`.
4. List generated outputs under `outs`.
5. Add metrics when appropriate.
6. Run `dvc repro <stage>`.
7. Inspect and commit the updated `dvc.lock`.

Example reasoning:

> A dependency must be listed so that DVC knows which changes invalidate the output. An output
> must be listed so that DVC can cache and restore it.

### Change the scenario count

The current DVC stages use a bounded campaign scenario count of `100000` for laptop execution.

After editing `dvc.yaml`:

```powershell
dvc repro build_temporal_bundle build_dataset_bundle
```

Expected explanation:

> Changing the command changes the stage definition, so DVC invalidates the stage and rebuilds
> the output. A larger count increases coverage but also increases generation time and memory.

### Track a new model with Git LFS

```powershell
git lfs track "artifacts/new_model/**/*.safetensors"
git add .gitattributes
git add artifacts/new_model/model/model.safetensors
git commit -m "Track new model weights with Git LFS"
```

Important point:

> `git lfs track` affects new Git additions. If a large file already exists in earlier commits,
> history must be migrated or cleaned before GitHub will accept the push.

### Change the DVC remote

Do not commit access credentials. Configure the endpoint locally:

```powershell
dvc remote add --local --force demo s3://bot-campaign-bucket
dvc remote modify --local demo endpointurl http://localhost:9000
dvc remote default demo
```

The `--local` configuration belongs in `.dvc/config.local` and is not shared through Git.

### Restore an older dataset version

```powershell
git switch <older-commit-or-branch>
dvc pull
```

Explanation:

> Git selects the pointer and pipeline metadata. DVC then retrieves the dataset objects whose
> hashes are referenced by that Git revision.

## Likely viva questions and answers

### Why not store everything directly in Git?

> Git is optimized for source files and textual history. Large binary datasets and neural model
> weights make cloning, diffing, and repository storage inefficient. GitHub also rejects normal
> Git files larger than 100 MB.

### What is the difference between Git LFS and DVC?

> Git LFS is primarily large-file storage integrated with Git commits. DVC additionally models
> data-pipeline stages, dependencies, outputs, hashes, caching, reproduction, and data remotes.
> This project uses LFS for deployable model bundles and DVC for datasets and pipeline lineage.

### Why not store model weights only in MLflow?

> MLflow is the authoritative experiment and registry system. Git LFS also makes the selected
> deployment bundle available at a known repository revision for local Compose, CI validation,
> and image construction. A larger production system could instead resolve the approved model
> directly from an external registry during deployment.

### Does DVC automatically start when data changes?

> No. DVC detects dependency changes when `dvc status` or `dvc repro` is run. Airflow, a developer,
> or CI/CD must invoke those commands. DVC is a versioning and reproduction engine, not a scheduler.

### How does DVC know that data changed?

> DVC computes content hashes for dependencies and compares them with `dvc.lock`. A changed hash,
> stage command, parameter, or dependency invalidates the relevant stage.

### Why commit `dvc.lock`?

> The lock file records the exact dependency and output hashes from a successful run. Committing it
> makes the data pipeline state associated with a code revision reproducible and reviewable.

### What is the DVC cache?

> It is a content-addressed local store. Identical objects are stored once and can be linked or
> copied into the workspace. The remote stores compatible content-addressed objects for sharing
> and recovery.

### Why use MinIO?

> MinIO provides an S3-compatible object store that can run locally or in Kubernetes. It allows
> the project to demonstrate remote DVC storage without depending on a public cloud account.

### What happens if the DVC remote is unavailable?

> Already materialized local data and cache objects can still be used. Missing versions cannot be
> pulled, and new cache objects cannot be pushed until the remote is restored. Git source history
> remains available because it is independent of the DVC remote.

### Is `.gitignore` enough to remove a secret?

> No. `.gitignore` prevents future untracked files from being added. It does not remove a secret
> from existing commits. The credential must be revoked, the file removed from history, and the
> cleaned history pushed.

## Incident-based rehearsal questions

### GitHub rejects a 255 MB model file

Cause:

> The binary was stored as a normal Git object, exceeding GitHub's 100 MB limit.

Resolution:

1. Track the intended binary path with Git LFS.
2. Migrate or remove the existing large object from branch history.
3. Verify with `git check-attr` and `git lfs ls-files`.
4. Push the rewritten branch with `--force-with-lease` after checking collaboration impact.

### GitHub push protection finds a personal access token

Correct response:

1. Revoke the token immediately.
2. Remove the secret manifest from current Git tracking.
3. Ignore populated local secret files.
4. Remove the secret from earlier history.
5. Supply the replacement credential through GitHub Secrets or `kubectl create secret`.

Do not merely approve the push-protection bypass for a real credential.

### Ray fails while reading `bundle.json`

Observed error:

```text
json.decoder.JSONDecodeError: Expecting value
```

Observed file contents:

```text
version https://git-lfs.github.com/spec/v1
```

Cause:

> Git branch/LFS operations were performed while Ray was using the same bind-mounted artifact
> directory. A valid JSON file was replaced by an LFS pointer during model finalization.

Diagnosis:

```powershell
git lfs ls-files
Get-Content artifacts/candidates/review_distilbert/bundle.json -TotalCount 1
```

Recovery:

```powershell
git lfs pull
git lfs checkout artifacts/candidates/review_distilbert
```

Prevention:

- Do not switch or rewrite Git branches while a bind-mounted training job is running.
- Materialize LFS files before starting API or training containers.
- Prefer tracking only genuinely large binary files rather than every JSON metadata file.
- Use a separate runtime artifact directory from the Git-controlled deployment bundle.

### DVC reports that `data/raw` is busy

Cause:

> Two DVC processes are trying to read/write the same workspace stage, or a previous process left
> an active lock while still running.

Correct response:

- Check whether the listed PID is genuinely alive.
- Stop duplicate Airflow/Ray submissions.
- Do not delete a valid lock owned by a live process.
- Serialize ETL with the repository's `flock` wrapper.
- Retry only after the original process has ended.

### Ray kills DVC ETL for low memory

Cause:

> The temporal generator materializes a large number of rows in memory. Ray's memory monitor kills
> the most recently scheduled worker when node use exceeds its protection threshold.

Correct explanation:

> This is not a DVC hashing error. DVC launched a pipeline command whose implementation exceeded
> the Ray node's memory budget. The real fix is a bounded scenario count, streaming/chunked data
> generation, fewer concurrent workloads, or more node memory.

## Architectural decisions and trade-offs

### Git for definitions, DVC for data, LFS for deployment bundles

Chosen because each tool has a distinct responsibility and review model.

Trade-off:

- Developers must understand three storage layers.
- A Git commit is not fully usable until required LFS and DVC objects are materialized.

### MinIO as the DVC remote

Chosen because it is S3-compatible, locally demonstrable, and deployable in Kubernetes.

Trade-off:

- A single local MinIO instance is not highly available.
- Credentials, bucket initialization, persistence, and backup must be managed.

### Git LFS deployment bundles

Chosen because local Compose mounts the selected bundles directly and Git commits can identify
the deployment state.

Current weakness:

> The LFS rules track entire model directories, including small JSON metadata. This increases LFS
> object count and makes an unsmudged metadata pointer capable of breaking runtime JSON parsing.
> A better rule tracks only large binary weights, while normal JSON metadata remains regular Git.

### Candidate versus promoted artifacts

Candidate artifacts are training outputs. Promoted artifacts are approved deployment bundles.

Recommended production design:

```text
Ray candidate output
-> MLflow evaluation and registry
-> approval gate
-> immutable promoted bundle
-> deployment
```

Tracking transient candidate output directly in the same Git workspace creates race conditions
between training and Git operations. Separate candidate and promoted storage is safer.

## Known loopholes and limitations

- The current `.dvc/config` contains local-machine-specific remotes, reducing portability.
- The default MinIO endpoint assumes a service is already available at `localhost:9000`.
- Git LFS storage and bandwidth quotas can block collaborators from downloading model bundles.
- A Git commit alone does not prove that its referenced DVC/LFS objects still exist remotely.
- Broad LFS directory rules include small JSON files that do not need LFS.
- Local single-instance MinIO is a demonstration store, not a production backup strategy.
- Rewriting shared Git history can invalidate existing clones and pull requests.
- Data versioning does not automatically guarantee data quality or prevent train/test leakage.
- DVC reproduces declared dependencies only; an omitted dependency can make a stage appear current
  when it should be invalidated.

## Strong closing answer

If asked why all three tools are necessary, answer:

> Git versions the source and the small metadata that defines the project. Git LFS makes selected
> large deployment models follow Git revisions without storing huge binaries in normal Git
> history. DVC versions datasets and records the dependency graph needed to reproduce data and
> baseline outputs. MLflow complements them by tracking experiments and model promotion. The main
> operational lesson is that pointers must be materialized before runtime and that training output
> should be separated from Git-controlled promoted artifacts.

---

# Topic 3: Dataset preparation and synthetic data

## Beginner explanation

Detectra prepares two datasets for two different prediction tasks.

```text
Observed labeled review text
-> individual review-risk dataset
-> baseline and DistilBERT review model
```

```text
Observed Amazon review behaviour
-> empirical timing/ratings/purchase profile
-> controlled campaign scenarios
-> hybrid campaign-risk model
```

The project deliberately keeps observed facts separate from synthesized fields. It does not add
invented users, products, timestamps, or purchase behaviour to the real individual-review
validation and test sets.

## What was directly obtained

### Labeled text sources

The individual review-risk dataset is built from:

```text
data/raw/product_reviews.jsonl
data/raw/kaggle_fake_reviews/
```

The directly observed fields can include:

- Review text.
- Genuine/deceptive label supplied by the source.
- Category when present.
- Rating when present.

The preparation code records:

```text
synthetic: false
label_provenance: observed_source_label
```

It does not fabricate behavioural fields for these text examples.

### Amazon behavioural sources

The Amazon category JSONL files provide observed:

- Review identifiers.
- User identifiers.
- Product identifiers.
- Review timestamps.
- Ratings.
- Verified-purchase flags.
- Helpful-vote counts.
- Product categories.

These Amazon records are used to learn behaviour distributions. They are not treated as verified
campaign or fake-review labels.

## What was synthesized

### Individual-review text augmentation

The DVC pipeline requests `7000` deterministic train-only text variants. The generator uses:

- Synonym substitution.
- Same-label clause recombination.
- Sentence reordering.
- A neutral context prefix when the other methods cannot produce a unique variant.

The generator alternates between genuine and deceptive parent labels, so the augmentation is
class-balanced. A generated label is inherited from parents of the same class rather than invented
by a separate classifier.

Important controls:

- Only the real training split may be augmented.
- Validation and test remain real-only.
- Exact normalized-text duplicates are rejected.
- The final training mixture caps synthetic content at `25%`.
- Seed `42` makes generation deterministic.
- Parent IDs, method, generator version, and field provenance are recorded.

### Controlled campaign scenarios

The DVC pipeline currently requests `100000` temporal scenario events. This is a bounded laptop
profile; it is not the raw Amazon record count.

| Scenario | Share | Label | Purpose |
|---|---:|---:|---|
| `organic` | 40% | Normal | Background activity sampled from observed distributions. |
| `legitimate_launch_burst` | 20% | Normal | A legitimate high-volume launch should not automatically be called a campaign. |
| `coordinated_positive` | 12% | Campaign | Closely timed positive promotion. |
| `coordinated_negative` | 8% | Campaign | Closely timed negative attack. |
| `paraphrased_campaign` | 5% | Campaign | Similar intent expressed with different wording. |
| `off_hour_campaign` | 5% | Campaign | Activity concentrated in rare observed UTC hours. |
| `slow_drip_campaign` | 5% | Campaign | Coordinated events spaced farther apart. |
| `multi_product_campaign` | 5% | Campaign | Related activity spread across several products. |

Thus, the two control families receive `expected_campaign = false`, and the six campaign families
receive `expected_campaign = true`.

## Complete preparation flow

```text
Labeled product-review sources
-> normalize text/label/category/rating
-> reject invalid rows
-> normalized-text deduplication
-> deterministic group-based 70/15/15 split
-> real train / validation / test
-> augment only real train
-> cap synthetic fraction at 25%
-> review-model training JSONL
```

```text
Observed Amazon event sources
-> schema validation
-> review-ID deduplication
-> UTC ordering
-> actual launch time or earliest-observed proxy
-> past-only temporal features
-> empirical category behaviour profile
-> deterministic controlled scenario generator
-> complete-group chronological 70/15/15 split
-> hybrid campaign-model train / validation / test
```

## Temporal features

Temporal features are calculated after sorting events by timestamp and review ID. Each event can
use only information available before that event.

Important fields include:

- `hours_since_launch`.
- `launch_phase`.
- `is_pre_launch_review`.
- `review_hour_utc`.
- `review_weekday_utc`.
- `is_weekend_utc`.
- `minutes_since_product_review`.
- `minutes_since_user_review`.
- `product_reviews_previous_1h`.
- `user_reviews_previous_24h`.

The implementation updates queues and previous-event state only after computing the current
event's features. This prevents future information from leaking backwards.

## Launch-time provenance

When a product catalog supplies launch time:

```text
launch_time_provenance: catalog_actual
```

Otherwise, the earliest observed review time is used as a proxy:

```text
launch_time_provenance: earliest_observed_review_proxy
```

This proxy is a limitation, not an actual product-launch claim. Scenario timestamps are kept
inside the observed category time range using the documented reflection policy.

## Leakage prevention

### Individual review splits

- Default split ratio is `70%` train, `15%` validation, and `15%` test.
- Assignment is a deterministic SHA-256 function of seed and text-family group ID.
- Normalized duplicate text is removed before splitting.
- Synthetic augmentation is train-only.
- Validation and test use only real labeled records.
- The review trainer checks normalized-text hashes across splits before training.

### Campaign splits

- Complete `scenario_group_id` values stay in one split.
- Groups are ordered chronologically within each scenario family.
- The approximate split is `70/15/15`.
- Train, validation, and test use disjoint deterministic sentence families.
- Exact generated sentences cannot appear in multiple splits.
- IDs, scenario names, campaign IDs, and labels are excluded from model features.

## What to demonstrate

### 1. Show the DVC stage definition

Open `dvc.yaml` and point out:

```text
build_temporal_bundle
build_dataset_bundle
train_product_model
```

Explain the important parameters:

```text
augmentation-count: 7000
campaign-scenario-count: 100000
max-synthetic-fraction: 0.25
seed: 42
```

### 2. Show one real labeled record

```powershell
Get-Content data/processed/dataset_bundle/text/real/train.jsonl -TotalCount 1 |
  ConvertFrom-Json |
  Format-List
```

Point out:

```text
source
label
label_provenance
field_provenance
synthetic
split
```

### 3. Show one augmented record

```powershell
Get-Content data/processed/dataset_bundle/text/augmented_train.jsonl -TotalCount 1 |
  ConvertFrom-Json |
  Format-List
```

Point out:

```text
synthetic: true
augmentation_method
parent_review_ids
generator_revision
generator_seed
label_provenance
split: train
```

### 4. Show one observed temporal record

```powershell
Get-Content data/processed/temporal_bundle/behavior/events.jsonl -TotalCount 1 |
  ConvertFrom-Json |
  Format-List
```

Point out launch provenance and past-only features.

### 5. Show one campaign scenario

```powershell
Get-Content data/processed/temporal_bundle/campaign/train.jsonl -TotalCount 1 |
  ConvertFrom-Json |
  Format-List
```

Point out:

```text
scenario
scenario_group_id
expected_campaign
synthetic
text_provenance
timestamp_provenance
behavior_profile_version
```

### 6. Show the manifests

```powershell
Get-Content data/processed/dataset_bundle/manifest.json | ConvertFrom-Json
Get-Content data/processed/temporal_bundle/manifest.json | ConvertFrom-Json
```

The manifests provide counts, seeds, hashes, policies, sources, rejected rows, duplicate counts,
split sizes, and provenance.

### 7. Run focused tests

```powershell
pytest -q tests/test_labeled_datasets.py tests/test_synthetic.py tests/test_temporal.py
```

These tests demonstrate deterministic splitting, synthetic-fraction limits, train-only
augmentation, launch provenance, past-only features, group isolation, chronological ordering,
and disjoint text families.

## Files to know

| File | Responsibility |
|---|---|
| `src/bot_campaign/labeled_datasets.py` | Normalizes labeled reviews, deduplicates them, creates deterministic splits, and builds the capped training mixture. |
| `src/bot_campaign/synthetic.py` | Creates deterministic class-balanced train-only text augmentation. |
| `src/bot_campaign/temporal.py` | Builds observed temporal features/profiles and controlled campaign scenarios. |
| `src/bot_campaign/dataset_bundle.py` | Orchestrates the text, behaviour, and campaign sub-bundles and writes top-level manifests. |
| `src/bot_campaign/data.py` | Canonical schemas, aliases, validation, and loaders. |
| `dvc.yaml` | Declares inputs, commands, code dependencies, and output directories. |
| `tests/test_labeled_datasets.py` | Tests deterministic and leakage-safe review splits. |
| `tests/test_synthetic.py` | Tests reproducible balanced train-only augmentation. |
| `tests/test_temporal.py` | Tests temporal causality and campaign split isolation. |

## Likely live code-change requests

### Change the review split from 70/15/15 to 80/10/10

The default is defined in `labeled_datasets.py`:

```python
DEFAULT_SPLIT_RATIOS = {"train": 0.70, "validation": 0.15, "test": 0.15}
```

Expected follow-up actions:

1. Change the ratios.
2. Reproduce the dataset stage.
3. Check the manifest counts.
4. Run leakage tests.
5. Retrain models because their input hashes changed.

### Reduce synthetic augmentation to 10%

Change in `dvc.yaml`:

```text
--max-synthetic-fraction 0.10
```

Explain:

> This controls the final training mixture, not merely the number of generated candidates. The
> builder selects only enough unique synthetic rows to remain below the requested fraction.

### Add a new campaign scenario

Expected changes:

1. Add its weight to `_scenario_plan`.
2. Ensure all scenario weights sum to one.
3. Define its timing, text, rating, product, and verification rules.
4. Decide whether it is a campaign or a control.
5. Add disjoint text families for all three splits.
6. Update tests to require the scenario in every sufficiently large split.
7. Reproduce the data and retrain the campaign model.

### Reject reviews with missing ratings

Before changing the rule, explain the consequence:

> Rating is optional for some labeled text sources. Making it mandatory can change source
> representation and may discard valid text labels. I would first measure missingness by source
> and document the new policy in the manifest.

### Change the random seed

Changing seed changes deterministic split assignment and augmentation choices. This creates a new
dataset version and invalidates direct metric comparison unless both models are retrained on the
new split.

### Add a future-looking feature

Correct answer:

> I would reject a feature such as total reviews in the next hour because it would not be known at
> inference time. Offline training would appear stronger but production inference could not
> reproduce it.

## Likely viva questions and answers

### Which data is real and which is synthetic?

> Product-review text and source labels are directly observed. Amazon timestamps, ratings,
> accounts, products, verified-purchase flags, and helpful votes are observed behavioural data.
> The 7000 text variants are deterministic train-only augmentation. Campaign memberships,
> scenario text, scenario accounts, and scenario timestamps are controlled synthetic ground truth
> generated from the observed Amazon behaviour profile.

### Why are Amazon reviews not used as campaign labels?

> They contain behaviour but do not provide verified coordinated-campaign membership. Treating
> ordinary Amazon records as positive or negative campaign labels would create unsupported ground
> truth.

### Why generate synthetic campaign data?

> Coordinated campaign labels are difficult and expensive to obtain. Controlled scenarios allow
> the pipeline, graph logic, feature construction, and hybrid model to be tested. They are a
> development substitute, not proof of real-world generalisation.

### Why keep legitimate launch bursts?

> High activity alone is not necessarily abusive. A legitimate launch burst is a hard negative
> that teaches the model not to equate every dense time window with a campaign.

### Why cap text augmentation at 25%?

> The real labeled corpus must remain dominant. Too much template-based augmentation can make the
> model learn generator patterns instead of genuine deceptive-review characteristics.

### Why use deterministic hashing instead of a random split call?

> A stable hash of seed and group ID makes assignment reproducible and independent of input row
> ordering. Related text remains grouped, and rerunning the same version produces the same split.

### Why split campaign data by group rather than row?

> Rows from the same campaign share timing, text, product, and identity patterns. Placing some in
> training and others in testing would leak the campaign structure and inflate evaluation scores.

### Why use chronological ordering for campaign groups?

> It better approximates deployment, where training uses earlier behaviour and evaluation uses
> later behaviour. It also reduces future-to-past leakage.

### How do you prove that temporal features are past-only?

> Events are sorted by event time. The feature for the current event is calculated from queues and
> last-seen dictionaries before the current event is appended to them. Tests verify expected
> previous-event counts and intervals.

### Is earliest observed review a real launch date?

> No. It is an explicit proxy used only when an actual catalog launch is unavailable. The field
> `launch_time_provenance` allows the model, report, and auditor to distinguish actual and proxy
> values.

### Why are scenario sentence families different by split?

> If identical templates appeared in training and testing, DistilBERT could memorize sentences.
> Disjoint sentence families make the test require at least some textual generalisation.

### What does a manifest provide?

> It records sources, counts, rejected and duplicate rows, split policy, seed, provenance, hashes,
> schema version, and evaluation policy. It turns preparation from an undocumented transformation
> into an auditable data product.

## Architectural decisions and trade-offs

### Separate text and behavioural datasets

Chosen because the labeled text sources do not reliably contain real behavioural context, while
the Amazon behaviour source does not contain campaign ground truth.

Trade-off:

- The project cannot claim that all features describe the same real labeled review population.
- Two specialized datasets and evaluation protocols must be maintained.

### Deterministic local augmentation

Chosen because it is reproducible, auditable, balanced, and inexpensive.

Trade-off:

- Synonym substitution and recombination are linguistically limited.
- Generated phrasing may create detectable artifacts.

### Behaviour-profile-driven scenarios

Chosen because purely arbitrary timestamps and ratings would be unrealistic. Category-level
observed distributions provide bounded background behaviour.

Trade-off:

- The scenarios still encode hand-designed rules.
- Attackers may behave differently from those rules.

### Group-safe chronological holdouts

Chosen to prevent campaign membership and future behaviour from crossing splits.

Trade-off:

- Strict grouping can produce class/count variation between splits.
- Small scenario families may have weak validation or test coverage.

### Past-only temporal features

Chosen because training and online inference must use the same information boundary.

Trade-off:

- Some useful retrospective features are intentionally unavailable.
- Online state must be maintained correctly.

## Known loopholes and limitations

- Campaign ground truth is synthetic rather than human-verified production abuse.
- Scenario templates may be easier for DistilBERT to separate than real adversarial paraphrases.
- The perfect 80-group campaign test result is not evidence of production generalisation.
- All generated campaign text is English, limiting multilingual claims.
- Synthetic campaign helpful votes are fixed at zero, which can become an artificial shortcut.
- Most generated campaign reviews are unverified, which can also become an easy shortcut.
- Earliest observed review is only a proxy for product launch.
- Amazon timestamps do not include reviewer timezone; off-hour features use UTC.
- Text deduplication catches normalized exact duplicates, not every semantic duplicate.
- Source-provided fake/genuine labels may contain noise or source-specific shortcuts.
- The same deterministic templates can still produce distribution artifacts despite split-family
  separation.
- The generator currently materializes scenario rows in memory, which created the earlier Ray OOM
  failure at larger scale.
- A 70/15/15 hash split may not perfectly preserve class/source ratios in a small corpus.

## Strong closing answer

If asked to defend the dataset design, answer:

> We separate observed labeled text from observed behavioural data because neither source supports
> every field and label required by both tasks. The review model uses real-only validation and test
> data, with deterministic train-only augmentation capped at 25%. The campaign model uses explicit
> controlled synthetic ground truth derived from observed Amazon behaviour distributions. We
> prevent leakage through normalized deduplication, group-safe deterministic splits, chronological
> campaign holdouts, disjoint text families, and past-only temporal features. The main limitation
> is that synthetic campaign performance must not be presented as real-world accuracy.

---

# Topic 4: Baseline TF-IDF + logistic-regression model

## The ten-year-old explanation

Before buying an expensive race car, we first test a reliable bicycle on the same road.

The baseline model is that bicycle. It is deliberately simpler and cheaper than DistilBERT. It
reads the words and writing style of **one review** and estimates whether that review belongs to
the risky/fake-labelled class. It gives us a minimum result that the more complicated neural model
must justify beating.

The baseline is **not** the campaign detector. It does not group accounts, examine a burst of
reviews, or consume the temporal campaign bundle.

```text
One review text
      |
      v
TF-IDF word and two-word features ----+
                                      +--> Logistic regression --> risk probability
Five writing-style features ----------+
```

The promoted DistilBERT review model solves the same individual-review task using contextual
language representations. The hybrid campaign model solves a different group-level task.

## Why a baseline is necessary

A complex model is not automatically a better model. The baseline answers these questions:

- Does the dataset contain a learnable signal at all?
- Can simple word-frequency patterns already solve much of the task?
- Does synthetic augmentation improve performance on real held-out reviews?
- Is the cost and complexity of DistilBERT justified?
- If the neural model later degrades, what inexpensive reference can detect that regression?

In a viva, do not say the baseline was built because logistic regression is the final production
choice. Say it provides a transparent, reproducible control experiment.

## Exact model structure in this repository

The implementation is in `src/bot_campaign/model.py`.

### Branch 1: TF-IDF text features

`TfidfVectorizer` converts text into numbers:

- `ngram_range=(1, 2)`: uses individual words and adjacent two-word phrases;
- `min_df=1`: a term may be retained even if it appears in only one training document;
- `max_features=20_000`: keeps at most 20,000 vocabulary features;
- `sublinear_tf=True`: replaces raw term frequency with a logarithmically scaled frequency.

TF means: "How often does this term appear in this review?"

IDF means: "How unusual or informative is this term across all reviews?"

A word appearing in almost every review receives less importance than a word or phrase that is
more discriminative. The vectorizer learns its vocabulary from training data only because it is
inside the scikit-learn pipeline.

Example:

```text
Review A: "excellent quality excellent quality"
Review B: "battery failed after one day"

Possible features:
excellent, quality, excellent quality, battery, failed, battery failed
```

TF-IDF does not understand context like DistilBERT. For example, it may struggle to distinguish
"not good" from other uses of "good" unless the useful bigram is learned.

### Branch 2: five writing-style features

`text_metadata()` in `src/bot_campaign/features.py` extracts:

1. logarithm of character count;
2. logarithm of word count;
3. unique-word ratio;
4. exclamation-mark density;
5. uppercase-character density.

`StandardScaler` puts these numeric features onto comparable scales. They describe writing style,
not user identity or campaign behaviour.

### Joining the branches

`FeatureUnion` concatenates the sparse TF-IDF vector and the five scaled style values into one
feature vector.

### Classifier

The final estimator is:

```python
LogisticRegression(max_iter=1_000, class_weight="balanced")
```

- `max_iter=1_000` gives the optimiser enough iterations to converge.
- `class_weight="balanced"` gives more weight to the minority class according to the training
  class frequencies.
- The classifier produces a probability between zero and one.
- The baseline bundle uses a fixed decision threshold of `0.5`.

Conceptually, logistic regression calculates:

```text
weighted_sum = bias + w1*x1 + w2*x2 + ... + wn*xn
probability = sigmoid(weighted_sum)
```

If the probability is at least 0.5, `ReviewScorer` returns `needs review`; otherwise it returns
`low risk`. This is a risk classification, not proof that the reviewer is a bot.

## Exactly what data it uses

The DVC `train_product_model` stage uses:

| Purpose | File |
|---|---|
| Augmented candidate training | `data/processed/dataset_bundle/text/training.jsonl` |
| Real-only baseline training | `data/processed/dataset_bundle/text/real/train.jsonl` |
| Shared final evaluation | `data/processed/dataset_bundle/text/real/test.jsonl` |

The candidate training file can contain real training rows plus controlled synthetic
augmentations. The comparison baseline trains only on the real training rows. Both are evaluated
against the **same real-only test set**, making the augmentation comparison meaningful.

The temporal bundle is not used because this is an individual text classifier.

## Training and promotion flow

`python -m bot_campaign.cli train` performs the following:

```text
1. Configure MLflow experiment review-baseline-training
2. Load augmented candidate training rows
3. Reject training if any input rows fail validation
4. Load the real-only test set
5. Reject evaluation if the test set contains synthetic rows
6. Train TF-IDF + logistic regression on augmented training data
7. Evaluate the augmented candidate on the real-only test set
8. Train the same model structure on real-only training data
9. Evaluate that baseline on the exact same real-only test set
10. Compare their PR-AUC values
11. Promote augmented training only if relative PR-AUC lift is at least 5%
12. Save the selected bundle and metrics
```

The promotion calculation is:

```text
relative lift = (candidate PR-AUC - baseline PR-AUC) / baseline PR-AUC
```

With the default `--min-pr-auc-lift 0.05`, augmentation must produce at least a 5% **relative**
PR-AUC improvement. If it does not, the safer real-only baseline bundle is saved.

Example:

```text
Real-only PR-AUC       = 0.80
Augmented PR-AUC       = 0.85
Relative lift          = (0.85 - 0.80) / 0.80 = 0.0625 = 6.25%
Required relative lift = 5%
Decision               = promote augmented candidate
```

Do not confuse a 5% relative lift with five percentage points. Moving from 0.80 to 0.84 is a
0.04 absolute increase but a 5% relative increase.

## Leakage protection

The baseline path includes several safeguards:

- External evaluation uses the real-only test split.
- The CLI rejects a test file containing any record marked `synthetic`.
- `train_with_holdout()` rejects overlapping review IDs between training and test data.
- Dataset preparation performs normalized deduplication before splitting.
- Train-only augmentation prevents generated children from entering validation or test data.
- The vectorizer is fitted inside the pipeline using training text, rather than being fitted on
  the whole dataset before splitting.

When the lower-level `train()` function is used without an external test set, it prefers
`GroupShuffleSplit` when sufficiently many non-empty group IDs exist. Otherwise it uses a
stratified random split. The production comparison path should use the externally prepared,
real-only holdout.

## Metrics and what each one means

| Metric | Simple meaning | Important limitation |
|---|---|---|
| Accuracy | Fraction of all decisions that were correct | Can look good on imbalanced data |
| Precision | Of reviews flagged risky, fraction actually labelled risky | High precision can come with low recall |
| Recall | Of all risky-labelled reviews, fraction successfully found | High recall can create many false alerts |
| F1 | Harmonic balance of precision and recall at threshold 0.5 | Depends on the selected threshold |
| ROC-AUC | Ranking quality across thresholds | Can appear optimistic with rare positives |
| PR-AUC | Precision-recall ranking quality | More informative for the risky positive class |

PR-AUC is the promotion metric because the important class is the comparatively rare risky class.
The baseline code does not currently calculate Brier score or log loss; those calibrated
probability metrics belong to the DistilBERT evaluation path.

Never invent or quote a baseline number from memory. For this checkout,
`reports/generated/baseline_metrics.json` is not currently present in the working tree. Obtain the
authoritative values from a completed `review-baseline-training` MLflow run or regenerate the
report on the full shared real-only test split. A tiny four-record smoke result is not suitable for
a model-performance claim.

## MLflow integration

The default experiment is:

```text
review-baseline-training
```

The parent run is:

```text
tfidf-logreg-baseline-comparison
```

It contains two nested comparison runs:

```text
tfidf-logreg-augmented
tfidf-logreg-real-only
```

MLflow records:

- input dataset paths;
- training and test record counts;
- TF-IDF and logistic-regression parameters;
- accuracy, precision, recall, F1, ROC-AUC and PR-AUC;
- candidate and baseline PR-AUC/ROC-AUC in the parent run;
- observed relative PR-AUC lift;
- whether the promotion gate passed;
- whether augmented or real-only training was selected;
- both nested scikit-learn models;
- the final metrics JSON;
- the selected Joblib bundle.

Ray Tune is not used for this baseline. That is intentional: the baseline is kept fixed and cheap
so it remains a stable reference. Ray Tune is used for the DistilBERT and hybrid neural models.

## Outputs

The DVC stage produces:

```text
artifacts/review_model.joblib
reports/generated/baseline_metrics.json
```

The Joblib bundle contains:

```text
pipeline
threshold = 0.5
version = tfidf-logreg-v1
trained_at timestamp
```

This is different from the promoted DistilBERT directory bundle. The normal application is
expected to use the DistilBERT review model; the Joblib model remains a baseline and fallback for
tests or explicit baseline configuration.

## What to demonstrate

### 1. Show the DVC dependency

```powershell
python -m dvc dag
python -m dvc stage list
```

Explain that `train_product_model` depends only on the text dataset bundle because it is not the
hybrid campaign model.

### 2. Reproduce only the baseline stage

Ensure MLflow is running at `http://localhost:5001`, then run:

```powershell
python -m dvc repro train_product_model
```

To force a rerun even when DVC considers dependencies unchanged:

```powershell
python -m dvc repro --force train_product_model
```

### 3. Run the CLI explicitly

```powershell
python -m bot_campaign.cli train `
  --data data/processed/dataset_bundle/text/training.jsonl `
  --baseline-data data/processed/dataset_bundle/text/real/train.jsonl `
  --test-data data/processed/dataset_bundle/text/real/test.jsonl `
  --min-pr-auc-lift 0.05 `
  --mlflow-uri http://localhost:5001 `
  --mlflow-experiment review-baseline-training `
  --output artifacts/review_model.joblib
```

### 4. Inspect the generated decision

```powershell
Get-Content reports/generated/baseline_metrics.json
```

Point out:

- `augmented_candidate`;
- `real_only_baseline`;
- `promotion_gate.observed_relative_lift`;
- `promotion_gate.passed`;
- `selected_training`.

### 5. Show MLflow

Open `http://localhost:5001`, select `review-baseline-training`, open the parent comparison run,
and show the two nested runs. Compare PR-AUC and confirm which training source was selected.

### 6. Show a safe automated test

```powershell
python -m pytest tests/test_model.py -q
```

The test checks that training completes, a saved-style bundle can score a review, the returned
probability stays between zero and one, and an evaluation split exists.

## Likely live code-change requests

### Change the vocabulary limit

Location: `build_pipeline()` in `src/bot_campaign/model.py`.

```python
build_pipeline(max_features=10_000)
```

Expected effect: lower memory and faster fitting, but potentially less vocabulary coverage.

### Add three-word phrases

```python
TfidfVectorizer(ngram_range=(1, 3), ...)
```

Expected effect: more phrase context but a much larger and sparser feature space, increasing
memory use and overfitting risk.

### Change regularisation strength

```python
LogisticRegression(C=0.5, max_iter=1_000, class_weight="balanced")
```

Smaller `C` means stronger regularisation and generally smaller coefficients.

### Add a style feature

In `text_metadata()`, add a digit ratio or question-mark density and then rerun training. Explain
that a changed feature schema requires retraining; an old trained estimator cannot automatically
accept the new vector width.

### Raise the promotion requirement

Use:

```powershell
--min-pr-auc-lift 0.10
```

This requires a 10% relative improvement before selecting augmented training.

### Change the inference threshold

The bundle currently stores `threshold=0.5`. Raising it generally increases precision and reduces
recall; lowering it generally increases recall and moderator workload. A proper change should be
selected on validation data, not chosen after looking at the test set.

### Add Brier score and log loss

Import `brier_score_loss` and `log_loss` from `sklearn.metrics`, calculate them from labels and
probabilities in `_evaluate()`, and remember that lower is better for both. This tests probability
quality rather than only ranking or thresholded classification.

## Difficult viva questions and strong answers

### Why logistic regression instead of a decision tree?

TF-IDF produces a very high-dimensional sparse vector. Linear models such as logistic regression
are fast, stable and naturally suited to sparse text features. A single decision tree can overfit
sparse vocabulary indicators and does not provide as strong a conventional text baseline.

### Is logistic regression really machine learning?

Yes. It learns feature weights from labelled examples by optimising a loss function. The word
"regression" describes its mathematical form; here it performs binary classification.

### Why combine TF-IDF with style features?

TF-IDF captures which words and short phrases appear. Style features capture length, repetition,
punctuation and uppercase patterns. The union allows a cheap model to use both content and surface
style.

### Why use class weights if the prepared training set is balanced?

It makes the estimator robust if the effective class balance changes or if the real-only subset is
not exactly balanced. However, it should be validated because class weighting can change
probability calibration.

### Why is the threshold fixed at 0.5?

It keeps the simple baseline reproducible. It is not necessarily the operational optimum. The
DistilBERT path improves this by selecting a validation-derived threshold and calibrating
probabilities.

### Why not tune the baseline with Ray?

The purpose of this baseline is a stable, inexpensive reference rather than the strongest possible
classical model. A separate classical-model comparison could tune `C`, n-grams and vocabulary
size, but that must use validation data and must preserve the final test set.

### Why is PR-AUC primary?

The positive risky class is the operational focus, and class imbalance makes accuracy less
informative. PR-AUC measures the precision-recall trade-off across thresholds for that positive
class.

### Does a high baseline score prove it detects bots?

No. It proves separation on the labelled dataset distribution. It may be exploiting source,
topic, vocabulary or formatting shortcuts. Real bot detection requires representative external
labels, drift monitoring and human validation.

### Why train two logistic-regression models?

The structures are identical; only the training data differ. This isolates whether controlled
augmentation provides measurable value. Both models are tested on the same untouched real data.

### Why is the test set not used for the promotion decision ideally?

Repeatedly choosing models based on test performance turns the test set into a validation set and
causes optimistic reporting. The current baseline code compares augmentation on the external
real-only test set, which is acceptable for a one-off project gate but is a methodological
limitation. A stronger design would choose augmentation using a real-only validation set and open
the test set once for the final frozen comparison.

### Can the baseline consume rating or verified-purchase fields?

Not currently. Its trained feature pipeline receives only review text. `ReviewScorer` adds
unverified purchase as explanatory context after prediction, but that flag does not affect the
baseline probability.

### Is the evidence list an explanation of the logistic-regression decision?

Not fully. The short-text, punctuation and verification messages are human-readable context. A
faithful model explanation would inspect the learned coefficients and the current review's active
TF-IDF/style feature contributions.

## Architecture decisions and trade-offs

### Fixed classical baseline

Chosen for speed, transparency and reproducibility.

Trade-off: it may understate the strongest result obtainable from classical ML.

### Shared real-only test data

Chosen to make real-only versus augmented training directly comparable.

Trade-off: using it to select promotion and later report final performance risks test-set reuse.

### Bigram TF-IDF

Chosen to capture short phrases without the cost of a language transformer.

Trade-off: vocabulary is sparse and does not understand broad context or semantic paraphrases.

### Style metadata

Chosen as five cheap, explainable signals.

Trade-off: superficial patterns can encode dataset-source artifacts rather than deception.

### Joblib serialization

Chosen because it stores the complete scikit-learn preprocessing and estimator pipeline in one
file.

Trade-off: Joblib/Pickle artifacts must never be loaded from an untrusted source because loading
them can execute code, and library-version compatibility must be controlled.

## Known loopholes and limitations

- TF-IDF cannot naturally recognize semantically equivalent paraphrases.
- Vocabulary and style can reveal dataset source instead of genuine review risk.
- The fixed 0.5 threshold is not validation-optimised or calibrated.
- `min_df=1` retains rare terms and can increase overfitting.
- No explicit `random_state` is set on logistic regression, although the default solver is normally
  deterministic for this use.
- The baseline metrics omit calibration metrics such as Brier score and log loss.
- The evidence strings are contextual rules, not complete local feature attribution.
- The promotion gate currently consults the real-only test set instead of a separate validation
  set.
- The fallback internal holdout is weaker than the externally prepared leakage-safe split.
- A tiny smoke dataset can generate deceptively perfect metrics.
- `class_weight="balanced"` can affect probability calibration.
- Model safety depends on loading Joblib only from a trusted build pipeline.
- The stage name `train_product_model` is misleading; `train_review_baseline` would describe its
  purpose more accurately.

## Strong closing answer

If asked to defend the baseline, answer:

> The baseline is a fixed scikit-learn pipeline for the individual-review task. It combines up to
> 20,000 unigram and bigram TF-IDF features with five standardized writing-style features, then
> trains class-balanced logistic regression. We compare augmented and real-only training on the
> same real-only holdout and require a 5% relative PR-AUC lift before selecting augmentation. It is
> cheap, reproducible and interpretable enough to establish whether DistilBERT's extra complexity
> is justified. It does not use temporal data and it is not the campaign model. Its main
> limitations are lexical shortcuts, a fixed uncalibrated threshold and test-set reuse in the
> current augmentation promotion gate.

---

# Topic 5: Individual review-risk DistilBERT, Ray Tune and UI inference

## The ten-year-old explanation

The baseline model mostly counts useful words and phrases. DistilBERT tries to understand how the
words work together in a sentence.

For example:

```text
"good"                  -> positive word
"not good"              -> the word "not" changes the meaning
"I expected it to be good, but it failed immediately"
                         -> meaning depends on the whole sentence
```

The individual review model reads one review and returns a risk probability. A probability above
its selected threshold is sent for human review.

```text
Review text
    |
    v
Tokenizer -> DistilBERT encoder -> two-class head -> raw probability
                                                    |
                                                    v
                                      temperature calibration
                                                    |
                                                    v
                                      operating threshold
                                                    |
                              +---------------------+------------------+
                              |                                        |
                         needs review                               normal
```

This model does **not** decide whether several accounts form a campaign. The hybrid campaign model
handles that group-level question later.

## The three models must not be confused

| Model | Unit being classified | Inputs | Main implementation |
|---|---|---|---|
| TF-IDF logistic-regression baseline | One review | Review text | `src/bot_campaign/model.py` |
| Review-risk DistilBERT | One review | Review text | `training/ray_review_train.py` and `src/bot_campaign/review_transformer.py` |
| Hybrid campaign model | A related group of reviews | Review texts plus group-level numeric features | `training/ray_train.py` and `src/bot_campaign/hybrid_model.py` |

The UI's large individual percentage is produced by review-risk DistilBERT when the transformer
bundle is present. Campaign membership and campaign risk are separate outputs.

## Why DistilBERT was chosen

DistilBERT is a compressed version of BERT. It retains contextual transformer representations but
is smaller and faster than full BERT. That makes it a reasonable compromise for a student MLOps
system that must train and serve on limited hardware.

It was chosen because:

- word order and sentence context matter for subtle review language;
- a pretrained encoder needs less labelled data than training a language model from scratch;
- it provides a meaningful neural comparison against TF-IDF logistic regression;
- it can run on either CPU or CUDA GPU;
- Hugging Face can save and reload the model and tokenizer as a portable directory bundle;
- it can be wrapped as an MLflow PyFunc model for standardized serving.

Trade-off: it is slower, larger and harder to explain than the baseline, and its contextual power
does not automatically protect it from dataset-source shortcuts.

## Exact neural-network structure

The local final model configuration identifies `DistilBertForSequenceClassification` with:

| Component | Current value |
|---|---:|
| Vocabulary | 30,522 WordPiece tokens |
| Transformer layers | 6 |
| Hidden dimension | 768 |
| Attention heads per layer | 12 |
| Feed-forward hidden dimension | 3,072 |
| Output labels | 2 |
| Activation | GELU |

The flow is:

```text
Text
  |
  v
WordPiece token IDs + attention mask
  |
  v
Token embedding and positional embedding
  |
  v
6 transformer encoder layers
  |  each layer uses multi-head self-attention and a feed-forward network
  v
First-token contextual representation
  |
  v
Pre-classifier projection + activation + dropout
  |
  v
Linear classifier with 2 logits: [normal, risky]
  |
  v
Softmax -> probability of class 1
```

The encoder starts from `distilbert-base-uncased` pretrained weights. `uncased` means capitalization
is normalized by that tokenizer/model family; it does not mean all punctuation or word order is
discarded.

`max_tokens` limits the number of tokens supplied to the encoder. Longer reviews are truncated and
shorter reviews are padded within a batch. The attention mask tells the transformer which positions
contain real tokens rather than padding.

## Data supplied to the model

The default files are:

| Split | File | Use |
|---|---|---|
| Training | `data/processed/dataset_bundle/text/training.jsonl` | Gradient updates; may include controlled train-only augmentation |
| Validation | `data/processed/dataset_bundle/text/real/validation.jsonl` | Ray model selection, calibration and threshold selection |
| Test | `data/processed/dataset_bundle/text/real/test.jsonl` | Final selected-model evaluation |

`load_review_split()` validates every row and deterministically orders records using a SHA-256 hash
of `review_id`. Optional record limits therefore take a repeatable sample rather than whichever
rows happen to occur first. It also tries to retain both labels in a bounded sample.

`assert_text_splits_are_isolated()` normalizes whitespace and case, hashes each text and rejects
exact normalized text overlap among train, validation and test splits. All three splits must
contain labels 0 and 1.

## One Ray trial: exact training procedure

Each call to `train_trial(config)` does the following:

```text
1. Seed PyTorch and NumPy
2. Load and validate train/validation data
3. Verify normalized text isolation
4. Load tokenizer and pretrained DistilBERT classification model
5. Apply selected dropout values
6. Optionally freeze embeddings and early encoder layers
7. Move the model to CUDA when torch.cuda.is_available(), otherwise CPU
8. Create AdamW optimizer
9. Compute class-weighted, label-smoothed cross-entropy loss
10. Create learning-rate warm-up followed by linear decay
11. Deterministically shuffle training rows for each epoch
12. Tokenize each mini-batch
13. Forward pass, loss, backpropagation and gradient clipping
14. Update weights and learning-rate schedule
15. Evaluate on validation data after every epoch
16. Save model, tokenizer and resumable optimizer/training state
17. Report metrics and checkpoint to Ray Tune
```

### Loss function

The loss is weighted cross entropy. The positive-class weight is based on the training imbalance
and then multiplied by the tuned `positive_weight_multiplier`.

```text
positive weight = negative_count / positive_count * multiplier
```

Label smoothing can prevent the classifier from becoming excessively certain about every training
label, but too much smoothing can weaken separation.

### Optimizer and learning-rate schedule

The code uses AdamW, which supports decoupled weight decay. A portion of the total training steps
can be used for linear warm-up; after warm-up the learning rate decays linearly toward zero.

Gradient clipping limits the norm of gradients before the optimizer step, reducing instability
from unusually large updates.

### Frozen layers

`frozen_encoder_layers` can be 0, 2 or 4 in a normal tuning run.

- `0`: fine-tune the complete encoder;
- `2`: freeze embeddings and the first two transformer layers;
- `4`: freeze embeddings and the first four transformer layers.

Freezing reduces training memory and time but may prevent enough task-specific adaptation.

## Ray Tune: what is actually tuned

The normal search space contains ten tuned hyperparameters:

| Hyperparameter | Search space | Why it matters |
|---|---|---|
| Learning rate | Log-uniform `5e-6` to `5e-5` | Size of optimizer updates |
| Weight decay | `0`, `0.01`, `0.05` | Regularization of weights |
| Dropout | Uniform `0.1` to `0.4` | Regularization inside classifier/attention configuration |
| Batch size | `8`, `16`, `32` | Memory, gradient noise and throughput |
| Frozen encoder layers | `0`, `2`, `4` | Fine-tuning depth versus cost |
| Maximum tokens | `128`, `192`, `256` | Text coverage versus memory/time |
| Warm-up ratio | `0`, `0.05`, `0.1` | Stabilizes early optimizer steps |
| Gradient clipping norm | `0.5`, `1.0`, `2.0` | Limits gradient explosions |
| Positive-weight multiplier | `0.75`, `1.0`, `1.25` | Adjusts minority-positive emphasis |
| Label smoothing | `0`, `0.05`, `0.1` | Controls overconfidence |

Learning rate and dropout are continuous distributions, so there is not a finite grid containing a
small fixed number of all possible combinations. `--num-samples` tells Ray how many configurations
to sample.

Ray selects the best trial using validation `pr_auc` in maximum mode.

## Trials, epochs, concurrency and early stopping

These terms are different:

- A **trial** is one sampled hyperparameter configuration.
- An **epoch** is one complete pass through that trial's training records.
- **Concurrency** is how many trials may execute simultaneously.
- A **Ray job** is the outer submitted process that creates and manages the Tune experiment.

The current checked-in CLI defaults are:

```text
num_samples            = 1 trial
epochs                 = 1 maximum epoch per trial
max_concurrent_trials  = 1
cpus_per_trial         = 4
gpus_per_trial         = 1 for a direct non-smoke CLI run
```

The current Airflow DAG defaults are different where noted:

```text
BOT_CAMPAIGN_NUM_SAMPLES           = 1
BOT_CAMPAIGN_EPOCHS                = 1
BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS = 1
BOT_CAMPAIGN_GPUS_PER_TRIAL        = 0
```

Therefore an Airflow-triggered run is CPU-only unless its environment sets
`BOT_CAMPAIGN_GPUS_PER_TRIAL=1`. The Kubernetes GPU overlays set the GPU request to 1, while the
non-GPU overlays keep it at 0.

The scheduler is Ray `ASHAScheduler` with:

```text
maximum time      = configured epochs
grace period      = 1 epoch
reduction factor  = 2
```

After an epoch report, ASHA can stop weak configurations so resources go to more promising ones.
This is trial pruning, not a conventional patience-based callback watching validation loss.

With only one trial and one epoch, ASHA has almost nothing to compare or prune. To demonstrate
meaningful tuning, use multiple trials and more than one maximum epoch, subject to available time
and memory.

## Smoke mode

`--smoke` is a wiring test, not a final experiment. It forces:

```text
1 trial
1 epoch
0 GPU
2 CPUs
at most 128 training rows
at most 64 validation rows
at most 64 test rows
maximum 64 tokens
batch size sampled from 4 or 8
frozen layers sampled from 4 or 6
```

Smoke metrics must never be presented as full-data model results.

## Checkpointing and resume

At every epoch the trial checkpoint contains:

- saved Hugging Face model files;
- saved tokenizer files;
- `bundle.json` with configuration and trial lineage;
- `training_state.pt` with the model state, optimizer state, learning-rate scheduler state,
  random-generator state and next epoch position.

This allows Ray to resume an interrupted or paused trial consistently. `--resume` restores the
experiment under:

```text
<ray-storage-path>/review-risk-distilbert
```

Resume works only if Ray considers that directory restorable. Starting without `--resume` creates
a new Tuner run; it should not be used while another run is writing to the same experiment path.

## Calibration: probability versus confidence

The best Ray trial is selected using validation PR-AUC. Its raw softmax scores are then calibrated
using validation data.

The code tests 80 temperature values geometrically spaced from `0.35` to `5.0` and selects the
temperature with the lowest validation log loss.

```text
raw probability p
      |
      v
logit = log(p / (1-p))
      |
      v
calibrated probability = sigmoid(logit / temperature)
```

- Temperature greater than 1 usually softens overconfident probabilities.
- Temperature below 1 usually makes them sharper.
- Calibration changes probability values but preserves their ordering, so it normally does not
  change ROC-AUC or PR-AUC.

The final local bundle currently records:

```text
temperature = 1.883729989566937
```

That value belongs to the checked-in `artifacts/review_distilbert/bundle.json`; it should be tied to
that exact bundle and dataset lineage rather than treated as a universal constant.

## Threshold selection

After calibration, `_precision_threshold()` examines the validation precision-recall curve. It
finds thresholds satisfying the default minimum precision of 0.90 and chooses the one providing
the greatest recall. If no threshold is feasible, it returns 1.0 so the system behaves
conservatively.

The final local bundle currently records:

```text
threshold = 0.7606024039562269
```

A review with calibrated risk 0.81 is therefore flagged, while one with risk 0.70 is not, even
though both are above 0.5.

The threshold is selected on validation data. The test split is evaluated only after the selected
trial, temperature and threshold have been finalized.

## Final metrics

The selected model calculates:

| Metric | Direction | Meaning |
|---|---|---|
| PR-AUC | Higher | Positive-class ranking over precision-recall trade-offs |
| ROC-AUC | Higher | Ranking of positive above negative examples |
| Precision | Higher | Correct risky labels among flagged reviews |
| Recall | Higher | Risky-labelled reviews successfully found |
| F1 | Higher | Thresholded balance of precision and recall |
| Brier score | Lower | Mean squared error of predicted probabilities |
| Log loss | Lower | Penalizes incorrect and overconfident probabilities |

The project report records a selected full-data result of PR-AUC 0.9353, ROC-AUC 0.9215,
precision 0.8005, recall 0.8635, F1 0.8308, Brier 0.1305 and log loss 0.4443. Before presenting
these values, open the corresponding MLflow run and state exactly whether the displayed metrics
are Ray validation metrics or final `test_*` metrics. Do not mix metrics from the trial callback,
the selected-model run and an older artifact.

## Artifact finalization

After `tuner.fit()` finishes:

```text
1. Ray returns all trial results
2. Best validation PR-AUC result is selected
3. Best checkpoint is copied to --output
4. Validation predictions choose temperature
5. Calibrated validation predictions choose threshold
6. bundle.json is updated with calibration and data lineage
7. Final calibrated predictions are made on the test set
8. Test metrics are logged to MLflow with test_ prefixes
9. A serving-compatible MLflow model is logged and registered
```

The finalized bundle directory contains:

```text
review_distilbert/
|-- bundle.json
|-- model/
|   |-- config.json
|   `-- model.safetensors
`-- tokenizer/
    |-- tokenizer.json
    `-- tokenizer_config.json
```

`bundle.json` adds the Git commit when available, paths and SHA-256 hashes for all three splits,
plus the split-isolation rule. A `git_sha` of `unknown` means the training container could not read
Git metadata; it does not mean the model weights are absent.

Be careful with the two directories:

```text
artifacts/candidates/review_distilbert   # Airflow/Ray candidate output
artifacts/review_distilbert              # default active local API bundle
```

In this checkout the candidate bundle still shows threshold 0.5, temperature 1.0 and epoch 1,
whereas the active bundle is calibrated. That candidate must not be described as finalized unless
its job completed calibration and promotion and the deployment path was updated deliberately.

## MLflow integration

The experiment and registered-model name are both:

```text
review-risk-distilbert
```

MLflow receives two kinds of records:

1. `MLflowLoggerCallback` logs every Ray trial's hyperparameters and epoch metrics.
2. A separate `selected-review-distilbert` run logs the best configuration, final calibration,
   `test_*` metrics and deployable model.

The selected run registers an MLflow PyFunc model called `review-risk-distilbert`. The PyFunc
signature accepts a pandas table containing a `text` column and returns:

```text
fake_probability
label
needs_review
```

The PyFunc packages the local bundle, project source code and pinned runtime requirements for
MLflow serving. The application itself normally loads the filesystem bundle directly; it does not
make a network call to MLflow for every review.

## From UI click to model result

```text
User fills review form
      |
      v
web/app.js builds JSON payload
      |
      v
POST /v1/reviews/score
      |
      v
src/bot_campaign/routes/reviews.py
      |
      v
TrustRuntime.score()
      |
      v
TrustRuntime.scorer() lazily loads bundle on first request
      |
      v
ReviewDistilBertScorer.predict()
      |
      +--> tokenize and truncate
      +--> model forward pass
      +--> softmax risky-class score
      +--> temperature calibration
      +--> threshold decision
      |
      v
ReviewPrediction JSON
      |
      v
web/app.js renders percentage, decision, latency, evidence and lineage
```

The default active path is `artifacts/review_distilbert`. Docker changes it to
`/models/review_distilbert` through `REVIEW_TRANSFORMER_PATH`.

Loading is lazy: readiness checks that the required directories exist, but the full transformer is
loaded on the first scoring request. That is why the first request may be slower and the UI labels
it as model warming.

If no transformer bundle exists, `TrustRuntime` can fall back to the Joblib baseline path. If
neither artifact exists, the API returns a model-unavailable error rather than inventing a score.

After individual scoring, `TrustRuntime.score()` also checks recent in-memory reviews for campaign
context and records observability metrics. This does not mean the individual probability came from
the hybrid campaign model or Spark.

## GPU behavior

Ray resource reservation and physical CUDA use are related but separate checks.

Ray must advertise a GPU, and the trial must request it:

```powershell
docker compose exec ray-head ray status --address=127.0.0.1:6379
```

During a running GPU trial, logical usage should show something similar to:

```text
1.0/1.0 GPU
```

Then verify the physical device from the GPU-enabled container:

```powershell
docker compose exec ray-worker nvidia-smi
```

Look for a Python process and allocated GPU memory. A Ray reservation alone does not prove PyTorch
successfully executed CUDA kernels. The trial code chooses CUDA only when
`torch.cuda.is_available()` is true.

Because one trial can reserve the only GPU, keep `max_concurrent_trials=1` on a one-GPU laptop.
Running multiple DistilBERT trials concurrently also increases RAM pressure and caused earlier Ray
out-of-memory failures.

## What to demonstrate

### 1. Show the active artifact configuration

```powershell
Get-Content artifacts/review_distilbert/bundle.json
Get-Content artifacts/review_distilbert/model/config.json
```

Point out the model version, encoder, maximum tokens, dropout, threshold, temperature and lineage
hashes.

### 2. Perform a safe smoke run

```powershell
python training/ray_review_train.py `
  --smoke `
  --ray-address auto `
  --mlflow-uri http://localhost:5001 `
  --ray-storage-path artifacts/ray_results-smoke `
  --output artifacts/candidates/review_distilbert-smoke
```

Explain clearly that this validates wiring and is not the reported full experiment.

### 3. Submit a controlled GPU run

From an environment that can reach the GPU-enabled Ray cluster:

```powershell
python training/ray_review_train.py `
  --ray-address auto `
  --mlflow-uri http://localhost:5001 `
  --num-samples 3 `
  --epochs 3 `
  --gpus-per-trial 1 `
  --max-concurrent-trials 1 `
  --ray-storage-path artifacts/ray_results `
  --output artifacts/candidates/review_distilbert
```

Three trials times at most three epochs means at most nine trial-epoch reports, but ASHA may stop
weak trials earlier. It does not mean nine models run simultaneously.

### 4. Trigger through Airflow

Set Airflow environment values before recreating its containers:

```powershell
$env:BOT_CAMPAIGN_NUM_SAMPLES="3"
$env:BOT_CAMPAIGN_EPOCHS="3"
$env:BOT_CAMPAIGN_GPUS_PER_TRIAL="1"
$env:BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS="1"
docker compose -f orchestration/docker-compose.airflow.yml up -d --force-recreate
```

Then trigger `bot_campaign_model_retraining`. Airflow submits the generated command to the Ray Jobs
API; Airflow itself does not train the neural network.

### 5. Show progress in Ray

Open `http://localhost:8265`:

- **Jobs** shows the outer submitted job, status, entrypoint and logs.
- Open the job and search the logs for `Number of trials`, `Trial status`, `epoch` and `pr_auc`.
- **Cluster** shows nodes and logical CPU/GPU resources.
- A `19 / 20` task display is a Ray task count, not necessarily 19 of 20 Tune trials.

The most reliable trial count is the `Number of trials` line printed by Tune or the experiment
results table, not the total Ray Core task count.

### 6. Show MLflow

Open `http://localhost:5001`, then:

1. select experiment `review-risk-distilbert`;
2. compare trial parameters and validation PR-AUC;
3. open `selected-review-distilbert`;
4. show `decision_threshold` and `calibration_temperature`;
5. show final `test_*` metrics;
6. open its `review_model` artifact;
7. open Models and show registered model `review-risk-distilbert`.

### 7. Demonstrate the actual API model

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8000/v1/reviews/score `
  -ContentType "application/json" `
  -Body '{"review_id":"viva-review-1","product_id":"demo-product","user_id":"viva-user","text":"A strangely polished and generic review with no product details.","rating":5,"helpful_votes":0,"verified_purchase":false,"language":"en","timestamp":"2026-08-24T10:00:00Z","category":"demo"}'
```

Check that `model_version` is `review-risk-distilbert-v1`. Also explain that the returned value is
model evidence for moderation, not a verified bot identity.

### 8. Run focused tests

```powershell
python -m pytest tests/test_review_transformer.py tests/test_api.py -q
```

## Likely live code-change requests

### Change the minimum required precision

Use:

```powershell
--minimum-precision 0.95
```

Expected effect: threshold normally rises, fewer reviews are flagged, precision should improve and
recall generally falls. Retrain/finalize using validation data before claiming the effect.

### Add a fourth maximum-token choice

In `param_space`:

```python
"max_tokens": tune.choice([128, 192, 256, 320])
```

Expected effect: more long-text coverage but higher attention memory and latency. Transformer
self-attention cost grows roughly quadratically with sequence length.

### Make a CPU-only run

```powershell
--gpus-per-trial 0
```

Ray will schedule without a GPU reservation and the training function will use CPU if CUDA is
unavailable. This is slower but useful for compatibility.

### Freeze all six encoder layers

Add 6 to the normal choice list. The embeddings and all six transformer layers will freeze, leaving
the classification head trainable. It uses less training memory but may underfit the review task.

### Add patience-based early stopping

Explain first that ASHA already performs cross-trial pruning. True per-trial patience would require
tracking the best validation metric inside `train_trial`, counting non-improving epochs and ending
the loop after a configured patience. Its checkpoint and report semantics must remain correct.

### Change the primary selection metric

Change both `TuneConfig(metric=...)` and `get_best_result(metric=...)`. Changing only one creates an
inconsistent selection pipeline. If using log loss, set `mode="min"` rather than `max`.

### Make training deterministic

The script already seeds NumPy, PyTorch and the shuffle generator. Stronger CUDA determinism can be
requested with deterministic PyTorch algorithms, but it may reduce speed and some operations may
not have deterministic implementations.

### Add accuracy

Add `accuracy_score` to `_metrics()`. Explain that it is supplementary and should not replace
PR-AUC for an imbalanced positive-risk task.

### Deploy the newly trained candidate

Do not point production at a half-written candidate directory. First verify job success, metrics,
calibration fields and artifact hashes; then use the project's explicit promotion/deployment
process to copy or mount the immutable approved bundle and restart/roll out the API.

## Difficult viva questions and strong answers

### Is this model a multilayer perceptron?

No. Its output head contains linear neural layers, but the main model is a six-layer transformer
encoder using multi-head self-attention. Calling the entire review model an MLP would be incorrect.

### What does self-attention contribute?

For each token, attention calculates how strongly it should use information from other tokens in
the same review. This creates contextual representations, allowing the meaning of a word to depend
on surrounding words.

### Why two output logits instead of one sigmoid output?

Hugging Face's two-label sequence-classification model produces one logit per class and the code
uses softmax. A one-logit sigmoid classifier could also model binary probability, but artifact and
loss conventions would need to change consistently.

### Does a 0.81 probability mean an 81% chance the user is a bot?

No. It is the calibrated probability assigned to the risky review-text label under this dataset
and model. It does not establish user identity, intent or coordinated campaign membership.

### Why calibrate after training?

Neural softmax values can be overconfident. Temperature scaling uses untouched validation labels to
improve probability reliability without changing ranking. This makes thresholds and displayed risk
values more meaningful.

### Why can PR-AUC remain unchanged after calibration?

Temperature scaling is monotonic: it changes score magnitudes but preserves their ordering. Ranking
metrics such as PR-AUC and ROC-AUC depend on ordering, while Brier score and log loss depend on
probability values.

### Why choose a threshold for minimum precision?

Moderator time is limited, so the system aims for a sufficiently trustworthy review queue while
maximizing recall among thresholds meeting that precision constraint. The exact 0.90 requirement
is an operational choice, not a universal truth.

### Why is test data not supplied to Ray trials?

Trials train on training data and report validation performance. The test set is held aside until
the best trial, calibration and threshold are finalized, preventing hyperparameter selection from
adapting to test labels.

### What is the difference between Ray and MLflow here?

Ray executes and schedules trials, assigns CPU/GPU resources, prunes weak configurations and saves
checkpoints. MLflow records parameters, metrics, lineage and deployable registered-model artifacts.
Ray answers "how do we run the search?"; MLflow answers "what ran, what won and what can we deploy?"

### Does Airflow perform the training?

No. Airflow submits a command to the Ray Jobs API and polls its status with a rescheduling sensor.
The Ray worker executes PyTorch training. Airflow provides orchestration and dependency order.

### Why does the first API request take longer?

The runtime lazily loads model weights and tokenizer on first use. Later requests reuse the loaded
objects. Container startup readiness currently checks artifact presence rather than eagerly warming
the model.

### Can the model handle non-English reviews?

The selected base encoder is English uncased DistilBERT and the controlled training data is
primarily English. The API carries a language field, but that does not make this model multilingual.
A multilingual encoder and representative labelled data would be required.

### What happens to a 1,000-token review?

The tokenizer truncates it to the selected `max_tokens`, so information near the end may be lost.
Increasing the limit costs memory and latency; chunking or hierarchical aggregation would be a
future alternative.

### Why save the tokenizer with the model?

The token-to-ID mapping and preprocessing rules must exactly match training. Loading a different
tokenizer can silently corrupt predictions even if the neural weights load successfully.

### Is the model explainable?

It returns evidence and calibrated probability, but those are not a full causal explanation of its
transformer decision. Token attribution methods can be added, but they are approximations and must
be presented carefully. Human moderation remains essential.

### Why can Ray say `0.0/1.0 GPU` while a job is running?

The outer Ray job driver may be running while no trial currently holds the GPU—for example during
runtime setup, dataset loading, calibration or final MLflow logging. During the actual training
trial, logical GPU usage should rise if it requested a GPU.

### Will deploying Kubernetes retrain the model automatically?

No. Applying an API Deployment serves the artifact referenced by that deployment. Training occurs
only when a training Job, Airflow DAG or CI/CD workflow explicitly submits it. Infrastructure
deployment and model retraining are separate lifecycle events.

## Architecture decisions and trade-offs

### Pretrained DistilBERT

Chosen for contextual language understanding with lower cost than BERT.

Trade-off: it is still resource intensive and mainly English-oriented.

### PR-AUC selection

Chosen to focus tuning on the positive risky class under imbalance.

Trade-off: the selected model is not necessarily the best-calibrated model until temperature
scaling is applied.

### Validation-only calibration and thresholding

Chosen to preserve test isolation.

Trade-off: a small or unrepresentative validation set can make both values unstable.

### One concurrent trial

Chosen for a one-GPU, memory-constrained laptop.

Trade-off: total search time is longer than parallel execution.

### Filesystem bundle plus MLflow PyFunc

Chosen so the local API can load without an MLflow network dependency while MLflow still provides
tracking, packaging and registry capabilities.

Trade-off: the team must control promotion between candidate, active filesystem and registered
model versions so they do not diverge.

## Known loopholes and limitations

- The controlled training labels can contain source-specific shortcuts and label noise.
- Exact normalized hash checks do not catch every semantic paraphrase across splits.
- Primarily English training does not justify multilingual performance claims.
- Truncation can discard decisive text beyond `max_tokens`.
- Temperature is chosen from a fixed 80-value grid rather than continuous optimization.
- A 0.90 minimum-precision rule may not match real moderator cost or prevalence.
- If no validation threshold reaches minimum precision, threshold 1.0 may flag almost nothing.
- ASHA is ineffective with only one trial and one epoch.
- Airflow is CPU-only by default even though the direct training CLI defaults to one GPU.
- The candidate and active artifact directories can contain different model generations.
- A Ray job can finish training but fail later during artifact JSON, MLflow or registry finalization.
- `git_sha="unknown"` weakens lineage when `.git` metadata is unavailable in the container.
- The UI's contextual evidence is not a faithful token-level explanation.
- Displayed "calibrated confidence" is `max(p, 1-p)` after calibration; it is not a statistical
  confidence interval.
- The model evaluates review text, not whether a human account truly is a bot.
- Large transformer artifacts require controlled storage such as MLflow, DVC or Git LFS and should
  not be committed as ordinary Git blobs.

## Strong closing answer

If asked to defend the individual review model, answer:

> The promoted individual-review model fine-tunes a pretrained six-layer
> `distilbert-base-uncased` sequence classifier on the leakage-safe text training split. Ray Tune
> samples ten optimization and capacity hyperparameters, assigns explicit CPU/GPU resources and
> uses ASHA to prune weak trials based on validation PR-AUC. Each epoch produces a resumable
> checkpoint. After selecting the best trial, we fit temperature scaling and a minimum-precision
> operating threshold using only validation data, then evaluate once on the real-only test split.
> MLflow records every trial and registers the finalized calibrated PyFunc model, while FastAPI
> loads the approved filesystem bundle lazily for UI inference. The score is evidence about one
> review's text, not proof of a bot or a coordinated campaign.

---

# Topic 6: Hybrid campaign model, graph grouping and campaign score

## The ten-year-old explanation

One suspicious review does not prove a coordinated campaign. A campaign is more like several
people arriving together, saying similar things, at nearly the same time.

The system therefore performs two jobs:

1. build groups of reviews that may be related;
2. score each complete group using both its language and its behaviour.

```text
Reviews arriving over time
        |
        v
Spark makes overlapping time-window routes
        |
        v
Graph connects related reviews
        |
        v
Connected groups containing at least 3 reviews
        |
        +--> DistilBERT understands group text
        |
        +--> 15 numeric features describe timing and behaviour
        |
        v
Hybrid fusion neural network
        |
        v
Campaign risk between 0 and 1
        |
        v
Threshold -> candidate for human moderation
```

The campaign score is not the average of the individual review-risk scores. It comes from a
separate group-level neural model.

## Training data versus live data

The model sees the same logical type of object in both paths: a `CampaignGroup`.

### During training

Rows from the temporal campaign split already contain a controlled `scenario_group_id` and an
`expected_campaign` label. `load_campaign_groups()` collects all rows belonging to each scenario
and aggregates them into one labelled `CampaignGroup`.

### During live streaming

Real events do not contain a trusted campaign group or label. Spark creates analysis windows, and
`discover_campaign_groups()` constructs a graph inside each window. Connected components become
unlabelled `CampaignGroup` objects that the trained model scores.

```text
Training: scenario_group_id + known synthetic label -> aggregate group -> train
Live:     Spark window -> infer graph connections -> aggregate group -> predict
```

The labels in campaign training are controlled synthetic scenario labels. Therefore even excellent
held-out performance validates the controlled problem and pipeline, not real-world campaign truth.

## Step 1: What Spark does before the model

`spark/review_stream.py` consumes JSON events from:

```text
reviews.raw.v1
```

It validates that:

- review, user and product IDs are present;
- timestamp parses successfully;
- trimmed text contains at least three characters;
- rating is between 1 and 5.

It normalizes missing category, verified-purchase, helpful-vote and launch-age values. It then
creates three routing copies of an event:

| Route | Route key | Why |
|---|---|---|
| Product | `product_id` | Bring reviews of the same product into a shared window |
| Account | `user_id` | Bring reviews by the same account together, even across products |
| Semantic token | Up to six selected content words | Bring potentially similar wording together, even across accounts/products |

The semantic-token route is only a cheap candidate route. It is not the final semantic-similarity
decision. The final graph uses DistilBERT embeddings and cosine similarity.

Spark creates one-hour event-time windows sliding every ten minutes:

```text
window duration = 1 hour
slide interval  = 10 minutes
watermark       = 2 hours
```

Because the windows overlap, one event can appear in several windows. This improves detection near
window boundaries but can create repeated evolving window messages.

Only routes with at least three events are emitted. Each emitted message contains at most 1,000
events and marks `truncated=true` when the actual route count is larger.

Spark writes schema `campaign.window.v1` messages to:

```text
reviews.analysis-windows.v1
```

The live query uses update mode every 30 seconds. Update mode lets a short demonstration become
visible before the two-hour watermark finally closes a window.

## Step 2: Exact definition of related reviews

`src/bot_campaign/campaign_graph.py` computes a DistilBERT embedding for each review and cosine
similarity between every pair inside a Spark analysis window.

For every pair of reviews, it creates an edge if **any one** of the following is true.

### Rule A: Same account

```text
left.user_id == right.user_id
```

This connects activity by one account across different products, wording, ratings or times within
the routed window.

### Rule B: Semantic similarity with matching polarity

```text
cosine_similarity >= 0.88
AND rating polarity is the same
```

Rating polarity is:

```text
rating >= 4  -> positive
rating <= 2  -> negative
rating == 3  -> neutral
```

The same-polarity requirement prevents a highly similar positive review and negative rebuttal from
being connected only because they discuss the same wording.

### Rule C: Same-product behavioural burst

```text
same product
AND same rating polarity
AND timestamps no more than 15 minutes apart
AND both purchases are unverified
```

This rule can connect coordinated-looking same-product reviews even when their text is not
semantically similar.

The defaults are:

```text
semantic similarity threshold = 0.88
burst interval                = 15 minutes
minimum connected group size = 3 reviews
```

## Connected components and transitivity

The graph uses a disjoint-set/union-find data structure to calculate connected components.

Connectivity is transitive:

```text
Review A is related to Review B
Review B is related to Review C
Review A does not directly match Review C

Result: A, B and C still form one connected component
```

This is important. The definition is not "every pair is similar." It is "every member can be
reached through a chain of accepted edges."

Components containing fewer than three reviews are discarded. For each accepted component, the
sorted review IDs are hashed to create:

```text
online-<first 16 hexadecimal characters of SHA-256>
```

This group ID remains stable across overlapping Spark windows when membership is unchanged. If a
new member joins the component, the membership hash and therefore the group ID change.

## Examples of grouping

### Many accounts reviewing the same product

Suppose four different accounts post five-star, unverified reviews of one product within nine
minutes.

```text
same user?             no
similar text?          maybe not
same-product burst?    yes
group size             4
result                 one campaign candidate group
```

So yes: many accounts reviewing the same product can be grouped, provided the burst-edge
conditions or semantic-edge conditions are satisfied. Merely reviewing the same product at
unrelated times is not enough.

### Similar reviews across several products

Three accounts post semantically similar five-star reviews for three different products.

```text
same product?          no
same user?             no
semantic similarity?  yes, at least 0.88
same polarity?         yes
result                 cross-product candidate group
```

### One account across products

One account reviews several products. The same-account rule connects those reviews even if their
text differs. This improves cross-product detection but can also join legitimate prolific-reviewer
activity, which the hybrid score and human moderator must assess.

## Step 3: The 15 numeric group features

`aggregate_campaign_group()` sorts rows by timestamp and calculates exactly these features in this
fixed order:

| # | Feature | Exact meaning | Example suspicious pattern |
|---:|---|---|---|
| 1 | `review_count` | Number of reviews in the group | Unusually large group |
| 2 | `unique_user_ratio` | Unique users divided by review count | Many accounts each posting once gives a value near 1 |
| 3 | `unique_product_count` | Number of distinct products | Coordinated cross-product activity |
| 4 | `duration_minutes` | Last timestamp minus first timestamp | Very short burst |
| 5 | `reviews_per_minute` | Count divided by `max(duration, 1)` | High posting velocity |
| 6 | `mean_interarrival_minutes` | Mean gap between consecutive reviews | Small regular gaps |
| 7 | `interarrival_cv` | Population standard deviation of gaps divided by their mean | Measures timing regularity/variation |
| 8 | `mean_rating` | Average rating | Strong positive or negative direction |
| 9 | `rating_stddev` | Population standard deviation of ratings | Highly uniform ratings produce a small value |
| 10 | `extreme_rating_ratio` | Fraction with rating at most 1.5 or at least 4.5 | Concentration at rating extremes |
| 11 | `verified_purchase_ratio` | Fraction marked verified | Many unverified purchases produce a low value |
| 12 | `mean_helpful_votes` | Mean helpful-vote count | Context about engagement |
| 13 | `off_hour_ratio` | Fraction posted from 00:00 through 05:59 UTC | Unusual UTC-time concentration |
| 14 | `weekend_ratio` | Fraction posted on Saturday or Sunday UTC | Weekend concentration |
| 15 | `near_launch_ratio` | Fraction whose launch age is between 0 and 168 hours | Activity during first seven days after launch |

These are evidence signals, not hard bot rules. A legitimate product launch can naturally create a
large, fast, positive, near-launch burst.

The tuple order is a schema contract called `campaign-group-v1`. IDs, scenario names, source labels
and campaign IDs are deliberately excluded so the model cannot memorize them as shortcuts.

## Numeric normalization

`NumericNormalizer` is fitted only on training groups:

```text
normalized value = (value - training mean) / training standard deviation
```

If a feature's standard deviation is smaller than `1e-6`, its scale is set to 1 to prevent division
by zero.

The training mean and scale for all 15 features are saved in `bundle.json` and reused during
validation, test and online inference. Fitting a new normalizer on live data would create
train-serving inconsistency and data leakage.

## Step 4: Exact hybrid neural-network structure

The hybrid model has a text branch and a numeric branch.

```text
Up to N review texts                         15 normalized numeric features
          |                                                |
          v                                                v
DistilBERT encoder                              Linear(15 -> numeric_hidden)
          |                                      LayerNorm + GELU + Dropout
          v                                                |
Mean-pool tokens for each review                           |
          |                                                |
          v                                                |
Mean-pool review embeddings                               |
          |                                                |
          +---------------- concatenate -------------------+
                                   |
                                   v
                 Linear(768 + numeric_hidden -> fusion_hidden)
                              GELU + Dropout
                                   |
                                   v
                         Linear(fusion_hidden -> 1)
                                   |
                                   v
                                Sigmoid
                                   |
                                   v
                           campaign-risk score
```

This is a hybrid transformer plus MLP. It is incorrect to call the entire model only an MLP:
DistilBERT is the text encoder, while small MLP branches process and fuse group features.

### Text pooling

For each review, the code mean-pools the last DistilBERT hidden states across non-padding tokens.
It then mean-pools those review embeddings across the reviews retained for the group. It does not
concatenate all review texts into one long string and does not use the individual review-risk
classifier's probabilities.

### Numeric branch

```text
15 inputs
-> Linear to numeric_hidden_size
-> LayerNorm
-> GELU
-> Dropout
```

### Fusion classifier

```text
768-dimensional group text embedding + numeric embedding
-> Linear to fusion_hidden_size
-> GELU
-> Dropout
-> Linear to one logit
-> sigmoid probability
```

The active local bundle currently uses:

```text
encoder               = distilbert-base-uncased
max_reviews           = 6
max_tokens            = 64
numeric_hidden_size   = 64
fusion_hidden_size    = 256
dropout               = 0.2628051799295684
decision threshold    = 0.591214656829834
feature version       = campaign-group-v1
```

These values describe the checked-in `artifacts/campaign_model/bundle.json`, not every future run.

## What happens when a group has many reviews

During ordinary batch prediction and training, `encode_groups()` keeps the first `max_reviews`
texts after chronological aggregation and pads groups with fewer reviews using empty strings.

With the current active configuration, only the first six chronologically ordered reviews are
encoded for the text branch. All reviews still influence the 15 aggregated numeric features.

The live optimized path embeds all events once to build the graph, then directly averages the
embeddings belonging to each discovered component. That avoids encoding text twice. However, this
live path currently averages all component embeddings rather than applying the training-time
`max_reviews` limit. This is a genuine train-serving-skew limitation for groups larger than the
configured maximum and is a good future code fix.

## Campaign risk and decision threshold

The final classifier produces one real-valued logit. Sigmoid converts it into:

```text
campaign_risk = 1 / (1 + exp(-logit))
```

The risk is learned from the group's pooled language plus its normalized numeric behaviour. It is
not a hand-written weighted sum visible in configuration, and it is not the percentage of reviews
believed to be fake.

The active threshold is approximately 0.5912:

```text
risk >= 0.591214656829834 -> candidate = true
risk <  0.591214656829834 -> candidate = false
```

Training selects the threshold from the validation precision-recall curve. The default campaign
minimum precision is 0.95, and the chosen threshold maximizes recall among thresholds meeting that
precision. If no threshold meets the requirement, the code returns the smallest floating-point
number greater than 1.0, conservatively preventing any sigmoid probability from becoming a
candidate.

Unlike the individual review model, this campaign path does not perform temperature calibration.

## Ray training and hyperparameters

The campaign model tunes thirteen hyperparameters:

| Hyperparameter | Search space |
|---|---|
| Encoder learning rate | Log-uniform `5e-6` to `5e-5` |
| Head learning rate | Log-uniform `5e-5` to `1e-3` |
| Weight decay | `0`, `0.01`, `0.05` |
| Dropout | Uniform `0.1` to `0.4` |
| Numeric hidden size | `32`, `64`, `128`, `256` |
| Fusion hidden size | `64`, `128`, `256` |
| Batch size | `8`, `16` in normal mode |
| Frozen encoder layers | `0`, `2`, `4` in normal mode |
| Maximum reviews | `6`, `8`, `10` |
| Maximum tokens | `96`, `128`, `192` |
| Warm-up ratio | `0`, `0.05`, `0.1` |
| Gradient-clipping norm | `0.5`, `1.0`, `2.0` |
| Positive-weight multiplier | `0.75`, `1.0`, `1.25` |

Separate learning rates are used because pretrained encoder weights usually need smaller updates
than newly initialized numeric and fusion layers.

The loss is `BCEWithLogitsLoss` with a class-imbalance-derived positive weight. AdamW, linear
warm-up/decay, gradient clipping and deterministic group shuffling are used.

The direct `training/ray_train.py` default is 1 trial and a maximum of 10 epochs. However, Docker
Compose, Airflow and Kubernetes commands can explicitly pass different values; the current Airflow
default passes 1 trial and 1 epoch. The command shown in the Ray Job is the authoritative value for
that run.

Ray selects the best validation PR-AUC trial and ASHA may prune weak trials after epoch reports.
Campaign checkpoints contain a usable model bundle but, unlike the review-training script, this
campaign `train_trial()` does not reload optimizer/scheduler state from `ray.train.get_checkpoint()`.
Do not claim full mid-epoch or optimizer-state resume for this path.

## Campaign split safeguards

Before Ray starts, `_validate_splits()` verifies:

- no `group_id` occurs in more than one of train, validation and test;
- every split contains both normal and campaign labels.

The `NumericNormalizer` is fitted only on training groups. The selected threshold comes from
validation groups. Final metrics are calculated on test groups.

These safeguards prevent direct group leakage, but controlled template families can still create
easy distribution shortcuts. A perfect score on 80 controlled groups is a pipeline validation
result, not proof of production generalization.

## MLflow integration

The experiment and registered-model name are:

```text
bot-campaign-hybrid-distilbert
```

Ray's MLflow callback records trial configurations and epoch validation metrics. The final run is:

```text
selected-hybrid-campaign-model
```

It records:

- winning hyperparameters;
- train, validation and test group counts;
- decision threshold;
- `test_pr_auc`, `test_roc_auc`, `test_precision`, `test_recall` and `test_f1`;
- Git SHA and feature version;
- full hybrid bundle artifacts;
- a registered MLflow PyFunc campaign model.

The PyFunc accepts serialized candidate-group fields and returns `campaign_risk` and `candidate`.
The live Kafka scorer normally loads the filesystem bundle rather than calling MLflow over the
network for every window.

## Complete online Kafka/Spark/model flow

```text
UI replay or producer.py
        |
        v
Kafka: reviews.raw.v1
        |
        v
Spark Structured Streaming: spark/review_stream.py
  - validates rows
  - applies event-time watermark
  - builds product/account/semantic-token routes
  - creates overlapping 1-hour windows
        |
        v
Kafka: reviews.analysis-windows.v1
        |
        v
Python service: streaming/campaign_scorer.py
  - loads HybridCampaignScorer
  - embeds review text
  - constructs graph edges/components
  - calculates 15 numeric group features
  - produces campaign risk
        |
        v
Kafka: reviews.campaign-scores.v1
        |
        v
FastAPI Kafka consumer
  - converts score to campaign alert
  - upserts repository state
        |
        v
Live Campaigns UI and moderator decision
```

`campaign_scorer.py` is not a Spark job. Spark creates analysis windows; the Python scorer consumes
those windows and runs the graph plus hybrid neural model.

## Kafka delivery behavior

The campaign scorer uses:

```text
consumer auto commit       = false
consumer starting offset   = earliest
producer idempotence       = true
producer acknowledgements  = all
```

For a valid window, it publishes all campaign scores, flushes the producer, and only then commits
the input message offset synchronously. Invalid JSON/schema/type messages are written to:

```text
reviews.campaign-scores.dlq.v1
```

This reduces loss, but it is not fully transactional exactly-once processing because Kafka output
and consumer-offset commit are not one atomic transaction. A crash after successful output but
before offset commit can replay the window and produce a duplicate. Stable membership-based group
IDs and downstream upsert logic help make duplicates manageable.

## What appears in a scored message

A `campaign.scored.v1` result contains:

- stable group ID;
- review, user and product IDs;
- `single_product` or `cross_product` scope;
- group time range;
- campaign risk;
- boolean candidate decision;
- model decision threshold;
- graph semantic-edge threshold;
- model and feature versions;
- source-window truncation flag;
- replay job IDs when present.

For example, risk `0.6576` with threshold `0.5912` means the neural model assigned the complete
group a risk above the operating threshold, so it became a moderation candidate. It does not mean
65.76% of accounts are bots.

## What to demonstrate

### 1. Show the active model contract

```powershell
Get-Content artifacts/campaign_model/bundle.json
```

Point out the 15 feature names, normalizer, architecture sizes, threshold and lineage.

### 2. Show the three Kafka topics

```powershell
docker compose exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --list
```

Identify:

```text
reviews.raw.v1
reviews.analysis-windows.v1
reviews.campaign-scores.v1
```

### 3. Start the streaming path

```powershell
docker compose --profile stream --profile score up -d `
  kafka kafka-init spark-master spark-worker spark-stream campaign-scorer api
```

Check:

```powershell
docker compose ps
docker compose logs --tail 100 spark-stream
docker compose logs --tail 100 campaign-scorer
```

### 4. Publish a controlled replay file

```powershell
docker compose exec campaign-scorer python streaming/producer.py `
  --input data/processed/temporal_bundle/campaign/test.jsonl `
  --bootstrap-servers kafka:29092 `
  --rate 10
```

`published=340` means the producer successfully sent 340 input review events. It does not mean 340
campaign groups were detected.

### 5. Inspect Spark windows

```powershell
docker compose exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic reviews.analysis-windows.v1 `
  --from-beginning `
  --max-messages 3 `
  --timeout-ms 30000
```

Show `route_type`, route key, one-hour bounds, events and truncation flag.

### 6. Inspect final campaign scores

```powershell
docker compose exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic reviews.campaign-scores.v1 `
  --from-beginning `
  --max-messages 5 `
  --timeout-ms 30000
```

Explain `campaign_risk`, both thresholds, component membership and campaign scope.

Kafka console-consumer `TimeoutException` after printing messages normally means it reached the
specified timeout while waiting for additional messages. `Processed a total of N messages`
confirms that messages were visible.

### 7. Show the Spark UI

Open the host-mapped Spark master and application UI shown by `docker compose ps`. Demonstrate the
running `cross-product-routing-windows` application, executors and streaming query. Container DNS
names such as `spark-worker` work inside Compose but are not browser hostnames; use `localhost` and
the published port from the host.

### 8. Show the model training run

In Ray at `http://localhost:8265`, open the campaign training job and identify its trial count,
epochs, resource reservation and validation PR-AUC. In MLflow at `http://localhost:5001`, open
`bot-campaign-hybrid-distilbert` and the selected run.

### 9. Run graph and streaming unit tests

```powershell
python -m pytest `
  tests/test_campaign_graph.py `
  tests/test_streaming_scorer.py `
  tests/test_streaming_runtime.py `
  -q
```

## Likely live code-change requests

### Require four reviews instead of three

Change the runtime argument:

```powershell
--minimum-group-size 4
```

Expected effect: fewer and larger candidate components, lower false-positive workload, but small
three-review campaigns will be missed.

### Tighten semantic similarity

```powershell
--similarity-threshold 0.92
```

Expected effect: fewer semantic edges and possibly fragmented components. Same-user and burst
edges remain unaffected.

### Change the burst interval

Change `burst_minutes` in `discover_campaign_groups()` or expose it as a CLI argument. Reducing it
to five minutes makes the behavioural rule stricter.

### Add a new numeric feature

Add its name to `NUMERIC_FEATURE_NAMES` and calculate it in exactly the same tuple position in
`aggregate_campaign_group()`. Then rebuild datasets and retrain. The loader intentionally rejects
an artifact whose saved feature-name contract does not match the application.

### Add a cross-product ratio

Possible feature:

```text
unique_product_count / review_count
```

Explain that this changes the feature schema/version, normalizer width and numeric first-layer
shape, so an old model state cannot be reused.

### Fix the live `max_reviews` skew

In `predict_with_review_embeddings()`, limit each embedding matrix consistently before averaging,
using the same chronological selection rule as training. Add a test with a component larger than
`max_reviews` and verify ordinary and optimized inference match.

### Replace mean pooling with attention pooling

This could learn which reviews matter most, but it adds parameters and requires retraining. It may
also increase overfitting on controlled scenario templates.

### Add campaign probability calibration

Fit temperature or isotonic calibration on validation group probabilities, store its parameters in
the bundle, apply it before threshold selection and online decisions, and add Brier/log-loss
metrics. Never fit calibration on the test set.

### Prevent duplicate score production transactionally

Use Kafka transactions so output production and consumed-offset commit are atomic. Idempotent
production alone does not provide end-to-end exactly-once behavior across application restarts.

## Difficult viva questions and strong answers

### Why use a graph instead of grouping only by product ID?

Product-only grouping misses one account targeting several products and coordinated wording spread
across products. A graph can combine account, semantic and same-product burst evidence into
cross-product connected components.

### Why does Spark use semantic tokens if DistilBERT later computes similarity?

Comparing every review with every other review globally is too expensive. Spark's product,
account and token routes cheaply reduce the candidate search space. DistilBERT then makes the more
expensive semantic decision inside each bounded window.

### What is the complexity of pairwise graph construction?

For `n` events in one window message, it builds an `n x n` similarity matrix and checks each pair,
which is approximately O(n squared) time and memory for similarities. Routing and the 1,000-event
cap bound this cost, but a 1,000-event window is still expensive.

### Why require matching rating polarity for semantic edges?

Two reviews can use similar product words while expressing opposite opinions. Polarity reduces
false connections between promotion and criticism, although coarse rating thresholds can still
lose nuance.

### Why is same-user alone enough for an edge?

It supports cross-product campaign discovery by linking one account's related window activity. The
trade-off is that a legitimate prolific reviewer can create a component, so the edge does not by
itself declare a campaign; the hybrid model and human review remain downstream.

### Can transitivity create an incorrect giant component?

Yes. One bridge review can connect two otherwise separate clusters. This is a known single-linkage
or chaining effect. Stronger community detection, edge weighting or a minimum internal-density
rule could reduce it.

### Why mean-pool review embeddings?

Mean pooling is permutation-invariant, simple and cheap for a group. It treats every retained
review equally and can dilute a small malicious subset, so learned attention or robust pooling is
a possible improvement.

### Why use both text and numeric behaviour?

Text alone can miss paraphrased campaigns, while timing alone can flag legitimate launches. Fusion
lets the model combine complementary semantic and behavioural evidence.

### Why two learning rates?

The encoder is pretrained and can be damaged by large updates. Numeric/fusion layers start mostly
untrained and often need a larger learning rate. Separate optimizer parameter groups support both.

### Are the 15 features all past-only?

They are calculated from the reviews present in the current candidate group/window and supplied
launch-age context. They do not intentionally inspect future reviews beyond the scoring window.
However, event-time/window design and launch-time provenance must be kept consistent between
training and production.

### Does `source_window_truncated=true` invalidate the score?

It warns that only the first 1,000 sorted events were included even though the route contained
more. The score can be incomplete and should be interpreted cautiously; production could split,
sample or separately handle oversized windows.

### Is campaign risk calibrated?

No explicit probability calibration is implemented for the hybrid campaign model. It is a sigmoid
model score used with a validation-selected threshold. It must not be presented as a perfectly
calibrated real-world probability.

### Why can the same replay produce several score messages?

Spark uses multiple routes and overlapping windows in update mode. As windows grow or routes
overlap, several components may be evaluated. Stable group IDs and repository upsert behavior help
deduplicate unchanged memberships.

### Why did publishing 340 reviews produce only 154 scores?

Input reviews and output groups are different units. Spark rejects invalid rows, emits only routes
with at least three events, one review may participate in several windows, the graph discards
components smaller than three, and the scorer outputs one message per discovered component rather
than one per review.

### Is this supervised learning if groups are discovered online?

Yes. The group-risk classifier is supervised using labelled controlled scenario groups. Online
graph construction is an upstream unsupervised/rule-based candidate-generation step.

### Why is moderator review still required?

Graph rules can connect legitimate launches or prolific users, the training campaign labels are
synthetic, and the score is probabilistic evidence. Automated account punishment would exceed what
the evidence supports.

## Architecture decisions and trade-offs

### Spark candidate routing before neural inference

Chosen to bound and distribute event-time aggregation.

Trade-off: routing tokens and windows can miss relationships that never share a route.

### Graph connected components

Chosen to combine several interpretable relationship types and support cross-product groups.

Trade-off: transitive chaining can merge weakly connected communities.

### Fifteen-versioned numeric features

Chosen to provide reproducible behavioural evidence and an enforceable model contract.

Trade-off: fixed hand-engineered features may omit emerging attack patterns.

### Mean-pooled DistilBERT

Chosen to keep a variable-size group representation simple and computationally manageable.

Trade-off: equal averaging can discard ordering and reviewer importance.

### Human-review candidate threshold

Chosen to prioritize precision and protect moderator capacity.

Trade-off: higher precision generally lowers recall and can miss subtle campaigns.

## Known loopholes and limitations

- Campaign labels are controlled synthetic scenarios rather than production-confirmed campaigns.
- Same-user edges can group legitimate prolific activity.
- Unverified, same-product bursts can occur during legitimate promotions.
- Coarse rating polarity loses textual nuance.
- Connected-component transitivity can create chaining effects.
- Pairwise similarity is quadratic within each routed window.
- Semantic-token routing may miss paraphrases with no shared selected token unless another route
  connects them.
- Overlapping windows and update output can repeat changing groups.
- A membership change creates a new group ID rather than updating the previous ID.
- Windows larger than 1,000 events are truncated.
- Training uses at most `max_reviews` text embeddings, while optimized live inference currently
  averages all discovered embeddings.
- The campaign sigmoid score is not temperature calibrated.
- The active artifact lineage refers to `campaign_v3` paths, while current Airflow and Compose
  commands use `temporal_bundle/campaign`; this lineage/path mismatch should be resolved before a
  new official comparison.
- Campaign training checkpoints do not restore optimizer state like the review-training path.
- Idempotent Kafka production plus manual commits is not fully transactional exactly-once delivery.
- Perfect controlled test metrics do not establish production generalization.
- UTC off-hour/weekend features may not represent a reviewer's local time.
- Earliest observed review may be used as a proxy when true product launch time is unavailable.

## Strong closing answer

If asked to defend the campaign model, answer:

> Spark first validates review events and creates overlapping event-time routes by product,
> account and selected semantic tokens. Inside each routed window, we encode every review and build
> a graph: reviews connect through the same account, same-polarity cosine similarity of at least
> 0.88, or an unverified same-product/same-polarity burst within 15 minutes. Transitive connected
> components of at least three reviews become candidates. For each group we combine a mean-pooled
> DistilBERT representation with 15 training-normalized temporal and behavioural features through
> numeric and fusion MLP layers. A sigmoid produces campaign risk, and a validation-selected
> minimum-precision threshold creates a reversible human-review candidate. Ray tunes thirteen
> hyperparameters, MLflow records and registers the selected bundle, and Kafka carries Spark
> windows and scored groups. The model detects group-level coordination evidence; it does not prove
> that an account is a bot.

---

# Topic 7: Ray Tune, distributed execution and GPU training

## Beginner explanation

Ray is the training manager. Instead of manually training many model configurations one after
another, we give Ray a search space and a resource budget. Ray starts trials, tracks their progress,
can stop weak trials with ASHA, and returns the best result.

```text
Airflow or command line -> Ray Job driver -> Tune trials -> CPU/GPU worker resources
                                              |
                                              +-> checkpoint + metrics
```

Ray does not create the dataset, choose the business label or serve the production API. It executes
the Python training functions in `training/ray_review_train.py` and `training/ray_train.py`.

## Exact cluster design

Docker Compose starts:

- `ray-head`: cluster coordination, Jobs API, dashboard and metrics;
- `ray-worker`: joins the head and executes scheduled work;
- optional one-shot submitters `ray-review-trainer` and `ray-trainer`.

The local GPU overlay attaches the physical NVIDIA GPU only to `ray-worker`. Attaching the same
laptop GPU to both head and worker could make the cluster advertise one device twice.

Ray's dashboard is exposed at `http://localhost:8265`; its internal cluster port is 6379 and its
Prometheus metrics port is 8080 per container network namespace.

## Job, trial, actor and task

| Term | Meaning here |
|---|---|
| Ray Job | Outer submitted Python process, such as `ray_review_train.py` |
| Tune trial | One sampled hyperparameter configuration |
| Epoch | One pass through that trial's training split |
| Task/actor | Internal Ray execution unit; counts are not trial counts |
| Placement/resource reservation | CPU/GPU capacity held for a running trial |

Therefore `19 / 20` in Ray Core does not mean 19 of 20 Tune trials are complete. Read the Tune
status table or the `Number of trials` log line.

## GPU checks

```powershell
docker compose exec ray-head ray status --address=127.0.0.1:6379
docker compose exec ray-worker nvidia-smi
```

During the training portion, Ray should show logical GPU use and `nvidia-smi` should show the Python
process. During setup, calibration or MLflow logging, the outer job can remain running while GPU
usage returns to zero.

## Memory and concurrency

Transformer trials use model weights, optimizer state, gradients, token batches and Ray object
store memory. `shm_size: 4gb` prevents the Docker Ray object store from being restricted to the
default tiny shared-memory allocation. On the one-GPU laptop, concurrency is deliberately one.

An OOM error at Ray's 0.95 node-memory threshold means aggregate node RAM crossed the safety limit;
it does not necessarily mean the individual process alone used all RAM. Safe responses are reducing
batch size, records, tokens, concurrent trials or competing services—not disabling the memory
monitor.

## Restore differences

The review trial stores model, optimizer, scheduler and generator state and reads a Ray checkpoint
on resume. The campaign trial reports usable model checkpoints but does not currently reload
optimizer state. Do not claim identical fault recovery for both scripts.

## Demo

```powershell
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d ray-head ray-worker
docker compose exec ray-head ray status --address=127.0.0.1:6379
```

Open Jobs, show the entrypoint and metadata, then show Cluster resources and Grafana/Ray metrics.
In the log identify trial count, current epoch, PR-AUC and ASHA scheduler.

Likely live changes include lowering `--gpus-per-trial` to 0, changing `--num-samples`, changing
`--epochs`, reducing batch-size choices or setting `--max-concurrent-trials 1`.

## Viva questions

**Why Ray instead of a Python loop?** Ray adds resource-aware scheduling, distributed execution,
trial isolation, ASHA pruning, checkpoint handling, a Jobs API and an operational dashboard.

**Does Ray guarantee a GPU is used?** It guarantees logical reservation only when the cluster
advertises and the trial requests a GPU. PyTorch must also report CUDA available.

**Why not run 12 GPU trials simultaneously?** One physical GPU cannot safely support them, and
parallel transformer optimizers would exceed GPU/RAM capacity.

**What does ASHA stop?** Underperforming hyperparameter trials after reported epoch milestones. It
does not stop the whole Airflow DAG and is not the same as patience-based early stopping.

**Does Kubernetes deployment retrain automatically?** No. Ray training starts only through an
explicit Ray Job, Kubernetes training Job or Airflow task.

## Loopholes

- Current Airflow defaults are one trial, one epoch and zero GPU.
- Direct review/campaign script defaults are not identical to Airflow, Compose and Kubernetes
  submitter defaults; always cite the actual job entrypoint.
- Kubernetes review-training manifest currently hard-codes 10 trials and 5 epochs, while parts of
  `k8s/README.md` describe other values.
- Only the review trainer implements full optimizer-state checkpoint restoration.
- A Ray Job may complete training but fail during artifact or MLflow finalization.
- Local Ray storage is a bind-mounted workspace; concurrent jobs can collide on paths.

---

# Topic 8: MLflow experiment tracking, artifacts and registry

## Beginner explanation

Ray is the coach running experiments; MLflow is the laboratory notebook and model cabinet.

```text
training code -> MLflow run
                  |-- parameters
                  |-- metrics
                  |-- tags and lineage
                  |-- artifacts
                  `-- registered model version
```

MLflow does not train the network. Training code calls MLflow to record what happened.

## Repository configuration

Local Compose runs MLflow 3.3.2 at `http://localhost:5001` and container port 5000. It uses:

```text
backend store  = SQLite /mlflow/mlflow.db
artifact store = /mlflow/artifacts
persistence    = named volume mlflow-data
```

Kubernetes runs one MLflow Deployment with the `detectra-mlflow` 10-Gi PVC. Its Service is
`detectra-mlflow:5000` inside the namespace.

The baseline experiment is `review-baseline-training`. Neural experiments are:

```text
review-risk-distilbert
bot-campaign-hybrid-distilbert
```

## Trial runs versus selected runs

The Ray MLflow callback logs sampled trials and validation metrics. After Ray chooses a winner, the
script creates a separate finalized run:

```text
selected-review-distilbert
selected-hybrid-campaign-model
```

The finalized runs log test metrics, decision thresholds, lineage and deployable PyFunc models.
This explains why opening an arbitrary trial may show no final artifact: trial logging and final
model logging happen at different stages.

## Artifact versus registered model

- An **artifact** is a file or directory attached to a run.
- A **logged MLflow model** adds a model flavor, environment and input/output signature.
- A **registered model** groups deployable versions under a stable name.
- A **stage/alias or approval** is a promotion decision; registration alone is not deployment.

The API normally loads the approved filesystem bundle baked or mounted into its container. It does
not query MLflow for every prediction. This removes MLflow from the online request critical path.

## Demo

Open `http://localhost:5001` and show:

1. experiment list;
2. parent/nested baseline comparison;
3. Ray trial parameters and validation PR-AUC;
4. selected run's `test_*` metrics;
5. threshold/calibration parameters;
6. Git/data/feature lineage tags;
7. Artifacts tab;
8. Models tab and registered versions;
9. input/output signature.

To query health:

```powershell
Invoke-WebRequest http://localhost:5001/health
```

Likely live changes: rename an experiment through `--experiment`, add a metric with
`mlflow.log_metric`, add a lineage tag, log a JSON artifact, or change the registered-model name.

## Viva questions

**Why both Ray and MLflow?** Ray schedules/searches; MLflow tracks, compares, packages and
registers.

**Why was an artifact tab empty?** The selected finalization step may not have run, the user may be
viewing a trial rather than selected run, or a run failed before `log_model`/`log_artifacts`.

**Does MLflow store model weights in SQLite?** No. SQLite stores run metadata; large files live in
the artifact location.

**What makes a run reproducible?** Code SHA, data paths/hashes, parameters, dependency versions,
metrics and the exact model/tokenizer artifact. A name alone is insufficient.

**Can deleting MLflow break the live API?** The already deployed filesystem model can continue
serving, but experiment history, registry and future controlled promotion are lost.

## Loopholes

- SQLite and one MLflow instance are development choices, not a highly available production
  metadata service.
- Local MLflow has no authentication or TLS.
- Filesystem artifacts require shared/persistent storage and backup.
- Candidate, active filesystem and registered-model versions can diverge without an enforced
  promotion controller.
- `git_sha=unknown` weakens lineage when the training image cannot see Git metadata.
- MLflow logging failure after training can leave a valid checkpoint without a finalized run.

---

# Topic 9: Airflow, PostgreSQL and retraining orchestration

## Beginner explanation

Airflow is the project manager. PostgreSQL is the project manager's notebook. Ray and DVC are the
workers doing the heavy jobs.

Airflow stores DAG runs, task states, schedules, XCom values and users in PostgreSQL. PostgreSQL
does not store the review dataset or model weights in this project.

## Local Airflow components

The Airflow Compose file starts:

- PostgreSQL 16;
- `airflow-init` for database migration and local admin creation;
- API server/UI at `http://localhost:8084`;
- scheduler;
- DAG processor.

It uses `LocalExecutor`. Scheduler, API server and DAG processor share the same database connection,
JWT secret, project mount and password file.

## Retraining DAG steps

```text
submit_temporal_etl
  -> wait_for_temporal_etl
  -> submit_review_training
  -> wait_for_review_training
  -> submit_campaign_training
  -> wait_for_campaign_training
```

Submit tasks POST an entrypoint to the Ray Jobs API. The returned submission ID is stored in XCom.
Each PythonSensor retrieves that ID and polls Ray every 60 seconds. `mode="reschedule"` releases the
Airflow worker slot while waiting.

The ETL Ray job runs DVC pull/repro/push. `flock` serializes the shared DVC worktree so two orphaned
or overlapping ETL submissions cannot write the same bundle simultaneously.

The DAG uses `max_active_runs=1`, `catchup=False`, no retries and a configurable schedule. With an
empty `BOT_CAMPAIGN_RETRAIN_CRON`, it is manual-only.

## Streaming smoke DAG

`bot_campaign_streaming_smoke` is separate. It calls the API to publish a controlled cross-product
replay and polls until the Kafka/Spark/model/API path materializes a campaign. It does not retrain.

## Schedule recommendation

For this project demonstration, manual-only is safest. In production, use data-triggered retraining
after validation or a low-frequency weekly schedule, not continuous retraining for every review.
Always retain approval gates before deployment.

Example weekly Sunday 02:00 UTC:

```powershell
$env:BOT_CAMPAIGN_RETRAIN_CRON="0 2 * * 0"
```

Recreate Airflow containers after changing environment values because the DAG reads them when
parsed.

## Demo

```powershell
.\scripts\start_airflow.ps1
docker compose -f orchestration/docker-compose.airflow.yml ps
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags list
docker compose -f orchestration/docker-compose.airflow.yml exec `
  airflow-api-server airflow dags trigger bot_campaign_model_retraining
```

In the UI show Graph view, task order, XCom submission ID, Ray job link/log, duration and task state.

## Understanding states

- `No Status`: task instance has not yet been scheduled, often because an upstream task is pending
  or failed.
- `Queued`: scheduler accepted it but the executor has not started it.
- `Up for reschedule`: sensor condition is false and the worker slot was released; normal waiting.
- `Upstream failed`: dependency failed, so this task did not run.
- `Failed`: operator/sensor raised an error or timed out.

To stop an earlier workflow, mark/cancel the DAG run in Airflow **and** stop any already submitted
Ray Job, because restarting Airflow does not automatically kill an external Ray submission.

## Viva questions

**Why PostgreSQL?** Airflow needs durable relational metadata and concurrent scheduler/API access.

**Why not train inside a PythonOperator?** Long GPU work would occupy the Airflow execution process,
couple dependencies and resources, and be harder to observe/retry. The operator submits to Ray.

**Why serialize review and campaign training?** It avoids memory/GPU contention on the laptop and
provides a simple, auditable dependency chain. They could be parallelized on a larger cluster after
ETL.

**What exactly did `airflow dags trigger` do?** It created a DAG-run record. The scheduler then
released tasks according to dependencies; the first submit task called Ray. The command itself did
not run DVC or PyTorch synchronously.

**Does DVC automatically trigger Airflow on data change?** No. DVC detects changes when `dvc repro`
runs. A scheduler, event sensor or CI workflow must trigger that execution.

## Loopholes

- Local `admin/admin` and Simple Auth Manager are development-only.
- `retries=0` means transient Ray/API failures fail immediately.
- Airflow cancellation and external Ray cancellation are not atomic.
- Shared bind-mounted DVC workspace requires locking and can retain stale processes.
- Airflow logs can fail to display if task-log host configuration is malformed, even though task
  state remains in PostgreSQL.
- Schedule defaults to manual; there is no data-event sensor.
- The DAG trains candidates but does not implement an approval/promotion/rollout task.

---

# Topic 10: Kafka topics, partitions, offsets and replay

## Beginner explanation

Kafka is a durable conveyor belt. Producers put numbered messages onto named belts called topics.
Consumers read them at their own pace and remember an offset, which is their bookmark.

Kafka allows review ingestion, Spark windowing, model scoring and API materialization to run at
different speeds without directly calling each other.

## Local broker configuration

Compose runs one Kafka 4.1 broker in KRaft mode, meaning the same process provides broker and
controller roles without ZooKeeper.

```text
host clients       -> localhost:9092
Compose containers -> kafka:29092
controller         -> kafka:29093
```

Do not use `kafka:29092` from Windows PowerShell; `kafka` is Compose DNS. Do not use
`localhost:9092` from another container; that would point back to that container.

## Topic contract

| Topic | Partitions | Purpose |
|---|---:|---|
| `reviews.raw.v1` | 3 | Input review events |
| `reviews.analysis-windows.v1` | 3 | Spark event-time route/window messages |
| `reviews.campaign-scores.v1` | 3 | Hybrid campaign scores |
| `reviews.campaign-scores.dlq.v1` | 1 | Invalid scoring inputs |

Replication factor is 1 because the local stack has one broker. This is not fault tolerant.

## Producers and consumers

- `streaming/producer.py` publishes JSONL review events.
- UI replay calls FastAPI, which publishes controlled reviews when streaming mode is enabled.
- Spark consumes raw reviews and produces analysis windows.
- `streaming/campaign_scorer.py` consumes windows and produces scores.
- FastAPI's background consumer materializes scored candidates into UI state.

Consumer groups allow each logical application to maintain its own offsets. Multiple consumers in
the same group divide partitions; different groups independently receive the same topic history.

## Replay meaning

Replay means republishing historical or controlled review events with their timing/group markers.
It proves the asynchronous pipeline using reproducible traffic. Kafka does not clean the review or
train the model; it transports events and retains them according to broker policy.

`published=340` confirms 340 producer sends, not 340 Spark windows or campaigns.

## Delivery semantics

The campaign scorer disables auto commit, flushes output and then commits input synchronously. Its
producer has idempotence and `acks=all`. This approximates reliable at-least-once processing but is
not an atomic Kafka transaction, so duplicates are possible across crashes.

Consumer lag is:

```text
latest partition offset - consumer group's committed offset
```

It answers how many messages are waiting for that group. The API currently does not export true
broker consumer-lag metrics; use Kafka tools/exporter for that measurement.

## Demo

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 `
  --describe --topic reviews.raw.v1
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 `
  --describe --group campaign-scorer-v1
```

Consume a few messages with `kafka-console-consumer`, limiting both message count and timeout so the
demo returns to the prompt.

Likely live changes: add a partition, change a consumer group to replay independently, change
`auto.offset.reset`, add a schema version, or route validation errors to the DLQ.

## Viva questions

**Why Kafka instead of HTTP between every service?** Kafka buffers traffic, decouples availability,
supports replay and gives independent consumers their own offsets.

**Does partitioning preserve global order?** No. Kafka preserves order within a partition only.
Choose keys carefully when related events require ordering.

**What does `earliest` mean?** A new group with no committed offset begins at the oldest retained
message. Existing committed offsets still win.

**Why three partitions on one broker?** It demonstrates parallel partitioning and supports future
consumer scaling, but it does not provide broker fault tolerance.

**Why a DLQ?** It prevents a permanently invalid message from blocking the valid stream and retains
the error plus source offset for investigation.

## Loopholes

- The local broker has no mounted data volume, so its log data is tied to the container writable
  layer and can be lost when the container is removed/recreated.
- One broker and replication factor 1 have no high availability.
- PLAINTEXT listeners provide no TLS or authentication.
- Schema versions are validated in application code rather than a schema registry.
- No Kafka exporter currently provides authoritative broker lag/throughput metrics to Prometheus.
- Overlapping Spark update windows intentionally create repeated/evolving messages.
- Idempotent producer configuration is not end-to-end exactly once.

---

# Topic 11: Spark Structured Streaming and event time

## Beginner explanation

Spark is the sorting station after Kafka. It opens review messages, rejects malformed business
rows, makes event-time buckets and routes potentially related reviews into bounded analysis
windows. It does not run the final hybrid neural score.

```text
Kafka raw reviews -> Spark validation/routing/windowing -> Kafka analysis windows
```

## Components

- Spark master coordinates work and is exposed at `http://localhost:8082`.
- Spark worker executes tasks and has host UI port 8083.
- `spark-stream` submits `spark/review_stream.py` to `spark://spark-master:7077`.
- The application name is `cross-product-routing-windows`.

The browser must use `localhost:<published-port>`. Names such as `spark-worker` and `spark-stream`
are internal Compose DNS names and cause host-browser DNS errors.

## Event time versus processing time

Event time is the timestamp carried by the review. Processing time is when Spark happens to receive
it. Event-time windows let delayed reviews join the correct historical interval rather than the
computer's current interval.

The two-hour watermark says Spark may eventually discard/close state for events arriving more than
the allowed lateness behind observed event-time progress. It is a state-management boundary, not a
promise that every delayed event waits exactly two hours.

## Window behavior

Each one-hour window starts every ten minutes. A review at 10:35 can belong to multiple overlapping
windows such as 09:40–10:40, 09:50–10:50 and 10:00–11:00. That reduces boundary misses but increases
state and repeated updates.

The output uses update mode. Append mode would wait until the watermark closes a window, making a
short replay appear stuck. Checkpoint state is written under
`data/checkpoints/analysis-windows-v1` on the project bind mount.

Checkpointing stores streaming progress and state, not neural-model weights. Reusing a checkpoint
with incompatible query logic or topics can fail; deleting it intentionally restarts stream state
and may replay Kafka offsets.

## What Spark cleans

Spark performs schema and range validation, timestamp parsing, null defaults, text normalization for
routing tokens and event-time aggregation. It does not spell-correct reviews, translate them,
remove all stop words, run DistilBERT or decide whether a review is fake.

## Demo

```powershell
docker compose --profile stream up -d kafka kafka-init spark-master spark-worker spark-stream
docker compose logs --tail 100 spark-stream
docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

Open the master, click the active application and show jobs/stages/executors. Consume
`reviews.analysis-windows.v1` to connect the UI to a real output message.

Likely live changes: change the one-hour duration, ten-minute slide, watermark, minimum count,
maximum events or trigger interval. Explain the state, latency and duplicate-output trade-offs
before editing.

## Viva questions

**Why Spark if Kafka already stores messages?** Kafka transports and retains events; Spark performs
distributed stateful event-time transformation and aggregation.

**Why watermarking?** Without a bound, Spark may retain state for late events indefinitely.

**Why `collect_list` with a cap?** The downstream graph requires review details, but unbounded
collection risks memory exhaustion. The 1,000 cap provides a visible truncation contract.

**Why not run DistilBERT as a Spark UDF?** Heavy model initialization per executor/UDF complicates
GPU placement, serialization and latency. The project keeps window engineering in Spark and model
inference in a dedicated scorer.

**Does a running master prove the stream is healthy?** No. Check an alive worker and the active
`cross-product-routing-windows` application/query.

## Loopholes

- Route-window `collect_list` remains expensive before slicing at very high volume.
- Semantic-token routing is lexical and may miss paraphrases.
- Update mode plus overlap emits repeated evolving groups.
- The two-hour watermark and one-hour window are fixed operational assumptions.
- The Kafka connector package may need network/cache availability when Spark starts.
- True Spark input, rejected-row, late-row and batch-duration metrics are not exported by the custom
  API Prometheus endpoint.
- Checkpoint storage is a local bind mount, not a replicated production checkpoint store.

---

# Topic 12: FastAPI backend and vanilla frontend UI

## Beginner explanation

FastAPI is the receptionist. It validates requests, calls the correct model/service, returns JSON
and exposes health/metrics. The frontend is the form and dashboard a user sees in the browser.

The frontend is plain HTML, CSS and JavaScript—not React, Angular or a server-side template engine.
FastAPI serves the static `web/` directory at `/`, so API and UI share one origin.

## Main backend files

| File | Responsibility |
|---|---|
| `src/bot_campaign/api.py` | Creates app, lifespan consumer, middleware, routers and static mount |
| `src/bot_campaign/schemas.py` | Pydantic request/response contracts |
| `src/bot_campaign/routes/reviews.py` | Review scoring and audit endpoints |
| `src/bot_campaign/routes/campaigns.py` | Campaign reads and moderator decisions |
| `src/bot_campaign/routes/demo.py` | Controlled replay endpoints |
| `src/bot_campaign/routes/operations.py` | Health, metrics, services and lineage |
| `src/bot_campaign/runtime.py` | Application/service-layer orchestration |
| `src/bot_campaign/repository.py` | Thread-safe in-process demo state |

## Important endpoints

| Method and path | Purpose |
|---|---|
| `POST /v1/reviews/score` | Score one review |
| `POST /v1/reviews/batch-score` | Score up to 500 validated reviews sequentially |
| `GET /v1/reviews/recent` | Recent in-process audit feed |
| `GET /v1/reviews/{id}/trust` | Stored result |
| `GET /v1/reviews/{id}/lineage` | Prediction lineage |
| `GET /v1/campaigns` | Current campaigns |
| `POST /v1/campaigns/{id}/decision` | Confirm, dismiss or restore |
| `POST /v1/demo/replay` | Start controlled replay |
| `GET /v1/demo/replay/{job_id}` | Poll replay |
| `GET /health/live` | Process is alive |
| `GET /health/ready` | Model artifact and stream readiness |
| `GET /metrics` | Prometheus text exposition |
| `GET /v1/ops/summary` | Service probes and links |
| `GET /v1/lineage/current` | Release/model/data/schema lineage |

FastAPI also generates interactive OpenAPI documentation at `/docs`.

## Validation

Pydantic requires non-empty IDs, review text from 3 to 20,000 characters, rating from 1 to 5,
non-negative helpful votes and bounded strings. Invalid requests produce HTTP 422 and increment the
validation-error metric through middleware.

## UI features

`web/index.html`, `web/styles.css` and `web/app.js` provide:

- sample review/scenario selector;
- individual review scanner;
- risk, thresholded decision, model version, evidence and latency;
- recent audit-feed filters;
- live campaign cards;
- confirm/dismiss/restore buttons;
- service/deployment health cards;
- monitoring summary;
- release lineage;
- links and developer launch instructions.

The animated scan checklist is presentation sequencing in JavaScript. It is not distributed tracing
of separately timed backend spans. Say this honestly if asked.

The UI's drift panel explicitly says live drift measurement is not configured. The architecture
box says `TensorRT ready`, but the code uses PyTorch/Hugging Face and contains no TensorRT execution
path; this label should be removed or implemented before claiming TensorRT.

## Runtime state and scaling

Reviews, campaigns, replay jobs and moderation history are stored in `InMemoryRepository`. State is
lost on process restart and is not shared across replicas. Docker intentionally runs one Uvicorn
worker, and Kubernetes uses one replica with `Recreate` strategy.

Before scaling horizontally, replace the repository with PostgreSQL/Redis and coordinate Kafka
consumer ownership/idempotency.

## Moderation behavior

An individual review never automatically activates a soft limit. Confirming a campaign sets a
24-hour expiry timestamp and `soft_limit=true`; dismiss/restore disables it. The API currently does
not authenticate the moderator endpoint and does not include a background job that automatically
clears expired limits.

## Demo

```powershell
Invoke-RestMethod http://localhost:8000/health/live
Invoke-RestMethod http://localhost:8000/health/ready
Start-Process http://localhost:8000/docs
```

Scan one review, show the network request in browser developer tools, then correlate the result with
`/v1/reviews/recent`, `/metrics` and Kibana logs.

Likely live changes: add a Pydantic field, add an endpoint, change validation bounds, add a feed
filter, change a service probe or add authenticated dependency injection. Update frontend payload,
schema, tests and version when a contract changes.

## Viva questions

**Why FastAPI?** Typed Pydantic validation, generated OpenAPI, dependency injection and efficient
Python integration with the model runtime.

**Why one worker?** The repository and campaign materializer are in-process. Multiple workers would
hold inconsistent state and load duplicate large models.

**Liveness versus readiness?** Liveness asks whether the process should be restarted; readiness
asks whether it can currently serve valid model-backed traffic.

**Does the browser talk directly to Kafka?** No. It uses HTTP FastAPI. The server publishes replay
events and consumes scored campaigns.

**Is calibrated confidence a confidence interval?** No. It is `max(p, 1-p)` after probability
calibration.

## Loopholes

- No authentication/authorization protects scoring or moderation endpoints.
- In-memory state disappears on restart and blocks horizontal scale.
- Batch scoring is a Python loop, not vectorized/batched model inference across the request.
- First-request lazy model loading causes cold-start latency.
- UI step animation is not backend tracing.
- TensorRT is claimed in UI text but not implemented.
- Soft-limit expiry is stored but not automatically enforced/cleared.
- Service probes are shallow reachability checks, not complete dependency transactions.
- No explicit rate limiting, request-size gateway, TLS or audit-user identity exists in the app.

---

# Topic 13: Prometheus, PromQL and Grafana dashboards

## Beginner explanation

The API writes measurements on a notice board called `/metrics`. Prometheus photographs that board
every few seconds and stores the numbers over time. Grafana asks Prometheus questions and draws the
answers as panels.

```text
API/Ray metrics -> Prometheus scrape and time-series store -> PromQL -> Grafana panels
```

Grafana does not collect metrics itself, and Prometheus does not render the project dashboard.

## Local configuration

`monitoring/prometheus.yml` scrapes every ten seconds:

- `api:8000/metrics` under job `bot-campaign-api`;
- `ray-head:8080/metrics` and `ray-worker:8080/metrics` under job `ray`.

Prometheus is `http://localhost:9090`; Grafana is `http://localhost:3000`. The Grafana datasource
is provisioned as `http://prometheus:9090`, and dashboard JSON is provisioned from the repository.
Named volumes preserve their local data.

Kubernetes uses annotation-driven pod discovery every 15 seconds and a seven-day Prometheus
retention period.

kubectl -n bot-campaign port-forward service/detectra-mlflow 5001:5000
kubectl -n bot-campaign port-forward service/detectra-grafana 3000:3000
kubectl -n bot-campaign port-forward service/detectra-prometheus 9090:9090
kubectl -n bot-campaign port-forward service/detectra-kibana 5601:5601
kubectl -n bot-campaign port-forward service/detectra-ray-head 8265:8265

## Custom API metrics

The API exports real in-process measurements including:

- review count and average scan rate;
- inference p95;
- HTTP request/error count and error rate;
- HTTP p50/p95/p99;
- current campaign count and moderation actions;
- reviews awaiting human review;
- mean risk and confidence;
- consumed stream scores, materialized candidates and consumer errors;
- invalid requests and language-labelled review counts;
- process uptime and filesystem free bytes.

These counters/deques reset when the API process restarts. Prometheus history can still show the
old time series, but queries must handle counter resets.

## Important PromQL examples

```promql
up{job="bot-campaign-api"}
rate(review_scans_total[5m])
increase(api_errors_total[15m])
api_request_latency_p95_ms
review_mean_risk
sum(up{job="ray"})
ray_node_cpu_utilization
sum by (State) (ray_scheduler_tasks)
```

`rate(counter[5m])` estimates per-second increase over five minutes. `increase(counter[15m])`
estimates how many increments occurred in fifteen minutes. Do not apply `rate` blindly to gauges
such as current risk or disk free space.

## Dashboard organization

The provisioned Detectra dashboard includes API health/throughput/latency/errors, model behavior,
campaign/moderator workload, language/validation quality, Kafka consumer errors, stream score rate,
disk, uptime and Ray health/CPU. A separate Ray dashboard shows target health, node CPU/memory,
scheduler task states and object-store memory.

Panel color is controlled by Grafana thresholds/units, not by whether the metric name sounds good.
If a value such as free disk is red despite being large, inspect panel threshold direction and
unit configuration before diagnosing infrastructure failure.

## Demo

1. Open Prometheus `/targets` and prove all intended targets are UP.
2. Run a PromQL query directly.
3. Scan reviews or replay a campaign.
4. Refresh Grafana and show the corresponding counter/rate/latency movement.
5. Open a panel's query editor to show its PromQL.
6. Correlate a Ray training run with Ray CPU/memory/task panels.

Likely live changes: add a Counter/Gauge to `RuntimeMetrics`, expose it in `prometheus_text`, add a
PromQL target to dashboard JSON, set units/thresholds and add an alert rule.

## Viva questions

**Counter versus gauge?** A counter normally increases/reset-on-restart; a gauge can rise or fall.

**p95 latency?** 95% of recorded requests are at or below that duration; 5% are slower.

**Why pull/scrape?** Prometheus periodically discovers/queries known targets, centralizing timing,
health and retry behavior.

**Is mean-risk movement model drift?** No. It is an output-distribution signal only. Formal feature
or embedding drift requires a reference distribution and a real distance/test calculation.

**How is language mix measured?** The API increments a labelled count from each accepted request's
language field. It trusts the submitted field; it does not run language detection.

## Loopholes

- No real review-feature or embedding-drift metric is implemented.
- No true Kafka broker lag, producer metric or Spark row/batch metric is scraped.
- Language is user-supplied rather than detected.
- API percentiles use an in-memory bounded sample and a simple nearest index, not histogram
  aggregation across replicas.
- Metrics reset on process restart and are not multi-replica safe.
- No Prometheus alert-rule or Alertmanager configuration is present.
- Some dashboard stat thresholds/units may make healthy values appear red.
- Current campaign count is exposed with a `_total`-style name although it behaves as a gauge.

---

# Topic 14: Filebeat, Elasticsearch, Kibana and KQL

## Beginner explanation

Filebeat is the delivery van for logs. Elasticsearch is the searchable warehouse. Kibana is the
search screen.

```text
container stdout/stderr -> Docker/Kubernetes log files -> Filebeat
                         -> Elasticsearch index -> Kibana Discover + KQL
```

Kibana cannot replace Elasticsearch: it needs a search/index backend. Removing Elasticsearch while
keeping Kibana would leave Kibana with nowhere to store or query logs.

## Local Compose path

The optional `observability` profile starts Elasticsearch 8.15.3, Kibana 8.15.3 and Filebeat 8.15.3.
Filebeat reads Docker JSON log files from `/var/lib/docker/containers`, uses the Docker socket to add
container metadata, drops containers outside the `bot-campaign` project and drops its own logs. It
adds:
docker inspect --format "{{.LogPath}}" $(docker compose ps -q api)
```text
app.name = bot-campaign-detector
app.environment = local-compose
```

It writes to Elasticsearch, normally producing `filebeat-*` data. Kibana is available at
`http://localhost:5601`; Elasticsearch health is at `http://localhost:9200/_cluster/health`.

## Kubernetes path

Filebeat is a DaemonSet, meaning one log shipper runs on each eligible node. It reads only container
log filenames belonging to namespace `bot-campaign`, adds Kubernetes metadata and writes daily:

```text
detectra-logs-YYYY.MM.DD
```

A setup Job creates the Kibana data view `detectra-logs-*`. Elasticsearch is a single StatefulSet
with a 20-Gi PVC; Kibana is a Deployment; Filebeat data is ephemeral because harvested logs remain
on the node and indexed documents live in Elasticsearch.

## KQL examples

Local Compose:

```kql
app.name : "bot-campaign-detector"
container.name : *api*
message : "review_scored"
message : "campaign_moderated"
message : *ERROR*
container.name : *campaign-scorer* and message : *failed*
```

Kubernetes:

```kql
kubernetes.namespace : "bot-campaign"
kubernetes.container.name : "api"
kubernetes.pod.name : detectra-api-*
project.name : "detectra" and message : "review_scored"
```

KQL filters indexed fields; it is not SQL and does not search files directly. Field availability
depends on Filebeat parsing and metadata enrichment. Expand a document first to confirm exact field
names.

## Demo

```powershell
docker compose --profile observability up -d elasticsearch kibana filebeat
docker compose ps
docker compose logs --tail 50 filebeat
Invoke-RestMethod http://localhost:9200/_cat/indices?v
```

In Kibana Discover:

1. choose `filebeat-*` locally or `detectra-logs-*` in Kubernetes;
2. select Last 15 minutes and Refresh;
3. filter to the API container;
4. scan a review;
5. search `message : "review_scored"`;
6. expand the document and show timestamp, message and container metadata;
7. deliberately send an invalid request and find the warning log.

Likely live changes: add a Filebeat processor/field, narrow the container filter, change index name,
create a KQL query or convert application logs to structured JSON.

## Viva questions

**Why both Grafana and Kibana?** Grafana summarizes numeric time series; Kibana searches detailed
event/log documents. They answer different operational questions.

**Why a DaemonSet for Filebeat?** Logs live on every Kubernetes node. One Filebeat per node can read
that node's container log files.

**Why a StatefulSet for Elasticsearch?** It has persistent identity/storage needs. The current
single-node design is still not highly available.

**What does no Kibana result mean?** Check time range, data view, index existence, Filebeat output,
filters and whether the source container produced a log.

**Does Filebeat contain application data?** It tails and forwards log records plus metadata. Its
registry tracks read positions; Elasticsearch stores searchable indexed documents.

## Loopholes

- Elasticsearch security and TLS are disabled in local and current Kubernetes manifests.
- The stack is single-node and not highly available.
- No ILM/retention is configured for Kubernetes `detectra-logs-*`; disk can grow indefinitely.
- Filebeat runs as root to read host logs and uses cluster metadata RBAC.
- Docker Desktop log-driver/path differences can prevent local harvesting.
- Plain-text logs are less queryable than fully structured JSON logs.
- Logs may contain review IDs/product IDs; privacy/redaction policy is not implemented.
- Kibana is a search UI, not an independent log database.

---

# Topic 15: Docker images, containers, Compose profiles and persistence

## Beginner explanation

An image is a sealed recipe; a container is one running copy of that recipe. Docker Compose is the
sheet that starts several containers with the correct network names, ports, mounts and startup
order.

```text
Dockerfile -> image -> container
docker-compose.yml -> coordinated group of containers
```

## Project-owned images

| Image | Dockerfile | Purpose | GHCR repository |
|---|---|---|---|
| API/UI | `Dockerfile` | FastAPI, web UI and inference artifacts | `bot-review-campaign-api` |
| Jobs | `docker/ray.Dockerfile` | Ray, DVC, ETL and neural training | `bot-review-campaign-jobs` |
| Airflow | `docker/airflow.Dockerfile` | Airflow plus project DAGs | `bot-review-campaign-airflow` |

The registry is GitHub Container Registry under `ghcr.io/njchathura-boop/`. CD publishes an
immutable full-Git-SHA tag and also `latest`; deployments should prefer the SHA.

## API image decisions

The API image:

- uses Python 3.11 slim;
- installs CPU PyTorch plus NLP/streaming dependencies;
- copies approved review and campaign artifacts;
- verifies weight files exist during build;
- runs as UID/GID 10001;
- removes package-install tooling from runtime;
- exposes port 8000;
- uses one Uvicorn worker because state is in-process;
- includes a readiness health check.

The image is large because it contains two transformer bundles and PyTorch. A future multi-stage or
remote model-store design could reduce distribution size.

## Compose services and profiles

Core/local services include API, Kafka/init, Spark master/worker, MLflow, Prometheus, Grafana and
Ray head/worker. Profiles add:

- `stream`: Spark stream and campaign scorer;
- `score`: campaign scorer;
- `train`: campaign Ray submitter;
- `train-review`: review Ray submitter;
- `observability`: Elasticsearch, Kibana and Filebeat.

The PowerShell helper starts a consistent service set:

```powershell
.\scripts\start_detectra.ps1 -Build
.\scripts\start_detectra.ps1 -Observability -Build
.\scripts\start_detectra.ps1 -Gpu -Observability
.\scripts\start_airflow.ps1
.\scripts\start_airflow.ps1 -Gpu
```

The GPU overlay grants the physical device only to `ray-worker`; it must be combined with the base
Compose file.

## Internal names versus host ports

Containers call each other by service DNS and container port:

```text
kafka:29092
mlflow:5000
prometheus:9090
ray-head:8265
spark-master:7077
```

The host/browser uses published ports:

```text
Detectra 8000, Spark 8082/8083, Airflow 8084, Ray 8265,
MLflow 5001, Prometheus 9090, Grafana 3000, Kibana 5601, Elastic 9200
```

Port 8080 conflicts are avoided because Airflow maps host 8084 to container 8080 and Spark maps
host 8082 to its container 8080.

## Persistent versus ephemeral local state

| State | Storage |
|---|---|
| MLflow DB/artifacts | Named volume `mlflow-data` |
| Prometheus time series | Named volume `prometheus-data` |
| Grafana state | Named volume `grafana-data` |
| Elasticsearch indices | Named volume `elasticsearch-data` |
| Filebeat registry | Named volume `filebeat-data` |
| Airflow PostgreSQL | Named volume `airflow-postgres` in Airflow Compose project |
| Datasets/artifacts/Ray results/Spark checkpoints | Host project bind mount |
| Kafka broker logs | Container writable layer; no named volume currently |

`docker compose stop` stops containers while retaining them. `docker compose down` removes
containers/network but normally retains named volumes. `docker compose down -v` deletes named
volumes and is destructive for MLflow, Grafana, Prometheus and Elastic history.

## Demo

```powershell
docker compose config --services
docker compose ps
docker compose images
docker compose logs --tail 50 api
docker inspect bot-campaign-api-1
docker volume ls
```

Show image identity, environment, read-only model mounts, service DNS and a health check. Then
explain why `campaign-sink` failed earlier: no service with that name exists; the API's Kafka
consumer is the materializer.

Likely live changes: add an environment variable, health check, named volume, profile, resource
limit or port mapping. Validate with `docker compose config` before starting.

## Viva questions

**Image versus container?** Immutable packaged filesystem/config template versus one runtime
instance with writable state.

**Volume versus bind mount?** Docker manages a named volume; a bind mount exposes an explicit host
path into the container.

**Why Compose health dependencies?** `service_started` only means a process launched;
`service_healthy`/completed init gives stronger startup ordering.

**Why separate API/jobs/Airflow images?** Least dependencies and privileges, smaller role-specific
runtime surfaces and independent release/deployment.

**Does Docker provide production orchestration?** Compose is suitable for local integration. It
lacks Kubernetes-style scheduling, self-healing across nodes, declarative rollouts and RBAC.

## Loopholes

- Kafka has no persistent volume.
- Compose Kafka, Elastic, MLflow and Grafana use development security defaults.
- Bind-mounting the entire repository into Ray/Airflow allows shared-workspace contention.
- API image includes large model artifacts and CPU-only PyTorch.
- `latest` is mutable even though immutable SHA tags are also produced.
- Compose startup does not guarantee application-level end-to-end health.
- Docker Desktop memory allocation can cause Ray OOM while the host still has other memory.

---

# Topic 16: Kubernetes resources, storage, overlays and operations

## Beginner explanation

Kubernetes is a manager for containers across a cluster. We submit desired state, and controllers
try to keep the declared number of Pods running and reachable.

```text
Kustomize manifests -> Kubernetes API -> controllers -> Pods
                                          |-- Services
                                          |-- ConfigMaps/Secrets
                                          `-- PVC storage
```

## Resource types in this project

| Kind | Purpose here |
|---|---|
| Namespace | Isolates resources under `bot-campaign` |
| Deployment | Stateless/replaceable API, Ray, MLflow, Prometheus, Grafana, Kibana, Airflow services |
| StatefulSet | PostgreSQL and Elasticsearch stable storage-oriented workloads |
| DaemonSet | One Filebeat log shipper per node |
| Service | Stable cluster DNS/port in front of Pods |
| ConfigMap | Non-secret runtime configuration and dashboard/config files |
| Secret | Passwords, Fernet/JWT, registry and DVC credentials |
| PVC | Requests persistent storage from the cluster |
| Job | One-time migrations, setup or training submission |
| CronJob | Scheduled/suspended DVC ETL template |
| ServiceAccount/RBAC | Prometheus discovery and Filebeat metadata permissions |
| initContainer | Storage ownership, migration readiness and secret-file preparation |

## Core workloads

The base Kustomization deploys API, Ray head, MLflow, Prometheus, Grafana, ETL CronJob and MinIO
resources. Kafka/Spark live streaming is a separate `k8s/streaming-demo` stack rather than part of
the base/full application overlay.

The API uses one replica, `Recreate`, non-root user, read-only root filesystem, dropped Linux
capabilities, startup/readiness/liveness probes and an ephemeral `/tmp` volume.

## Services

ClusterIP Services give stable internal names such as:

```text
detectra-api:8000
detectra-ray:8265
detectra-mlflow:5000
detectra-prometheus:9090
detectra-grafana:3000
detectra-elasticsearch:9200
detectra-kibana:5601
```

ClusterIP is internal. Use an Ingress/LoadBalancer in production or `kubectl port-forward` for the
demo.

## Persistent volumes

Core PVCs request:

| PVC | Size | Stores |
|---|---:|---|
| `detectra-workspace` | 50 Gi | DVC data, generated bundles, artifacts and reports |
| `detectra-mlflow` | 10 Gi | MLflow SQLite DB and artifacts |
| `detectra-prometheus` | 10 Gi | Metrics time series |
| `detectra-grafana` | 2 Gi | Grafana state |
| `detectra-elasticsearch` | 20 Gi | Log indices in observability overlay |
| `detectra-airflow-postgres` | 10 Gi | Airflow metadata in full overlay |

`emptyDir` volumes for `/tmp`, Ray temporary files/shared memory, writable Airflow password copies
and Filebeat registry are ephemeral and disappear with the Pod. ConfigMap/Secret volumes are
reconstructed from Kubernetes objects and are not general writable persistence.

The current MinIO manifest refers to a PVC named `minio`, which is not declared by the base
`storage.yaml`; a matching separately provisioned PVC is required. Its environment names also need
review against MinIO's expected root-user/root-password variables.

## Kustomize overlays

```text
k8s/                         core base
k8s/overlays/gpu             core + NVIDIA Ray resources
k8s/overlays/observability   core + Elastic/Kibana/Filebeat
k8s/overlays/full            observability + Airflow
k8s/overlays/full-gpu        full + NVIDIA GPU patches
```

Kustomize overlays patch the base rather than copying every manifest. Render before applying:

```powershell
kubectl kustomize k8s/overlays/full-gpu
```

## Operational commands

```powershell
kubectl config current-context
kubectl apply -k k8s
kubectl apply -k k8s/overlays/observability
kubectl apply -k k8s/overlays/full
kubectl apply -k k8s/overlays/full-gpu
kubectl -n bot-campaign get pods,deployments,statefulsets,daemonsets,services,pvc,jobs,cronjobs
kubectl -n bot-campaign describe pod <pod-name>
kubectl -n bot-campaign logs deployment/detectra-api --tail=100
kubectl -n bot-campaign rollout status deployment/detectra-api
kubectl -n bot-campaign port-forward service/detectra-api 8000:8000
```

Use the provided deployment script for a guided demo, but inspect the rendered images first.

## Training Jobs and CronJobs

The review-training Job is a finite submitter. It waits for Ray, submits the review script and exits
when `ray job submit` returns. The manifest currently specifies 10 trials and 5 epochs and inherits
GPU-per-trial from the ConfigMap/overlay.

The ETL CronJob declares Sunday 02:00, `concurrencyPolicy: Forbid`, but is `suspend: true`. It runs
DVC pull/repro/push only when unsuspended or manually instantiated.

## Viva questions

**Deployment versus StatefulSet?** Deployment assumes replaceable Pods; StatefulSet adds stable
identity/ordered semantics useful for persistent databases. PVCs can also be mounted by a
Deployment, as MLflow is here, but one replica and RWO storage constrain rollout.

**DaemonSet versus Deployment?** DaemonSet targets one Pod per node; Deployment targets a desired
replica count independent of node count.

**PV versus PVC?** PV is actual cluster storage capacity; PVC is the workload's request/claim.

**ConfigMap versus Secret?** Both inject configuration, but Secret is intended for sensitive values
and is only base64-encoded unless encryption at rest is configured. Neither should contain
committed production credentials.

**Requests versus limits?** Requests influence scheduling and guaranteed capacity; limits cap usage
and can cause CPU throttling or OOM kill.

**Why probes?** Startup protects slow initialization, readiness controls Service traffic, and
liveness requests restart when a process becomes unhealthy.

## Critical deployment gaps

- `scripts/deploy_kubernetes.ps1` accepts `-ImageTag` but currently constructs fixed image tags and
  never uses the parameter value.
- The full overlay contains fixed historic tags.
- The base does not deploy the Kafka/Spark streaming demo.
- API state is in-memory, forcing one replica.
- Default base training is CPU-only; GPU requires NVIDIA device plugin/compatible node and overlay.
- MinIO PVC and credential environment configuration are inconsistent.
- Some development credentials appear in manifests/ConfigMaps and must be moved to externally
  managed Secrets.
- RWO PVCs and SQLite make several services single-replica.

---

# Topic 17: GitHub Actions CI/CD, Trivy and staging deployment

## Beginner explanation

CI checks that a change is safe to merge. CD packages an approved revision and deploys it.

```text
pull request/push -> CI tests + image scan
version tag/manual -> CD build 3 images -> scan -> GHCR -> Kubernetes staging -> smoke test
```

GitHub Actions does not automatically retrain Ray models in these workflows. The workflows package
the promoted Git LFS model bundles already present in the selected commit.

## CI triggers and steps

CI runs on:

- every pull request;
- pushes to `main`;
- tags matching `v*`.

Quality job:

1. checks out full Git history and LFS objects;
2. verifies required model files are materialized rather than LFS pointers;
3. installs Python 3.11 and CPU PyTorch;
4. installs project dev/UI/NLP dependencies;
5. installs Playwright Chromium;
6. trains a tiny baseline smoke model;
7. compile-checks DAG Python source;
8. runs Ruff;
9. runs Pytest, including UI tests.

Container job runs only after quality. It builds/loads the API image, scans HIGH/CRITICAL
vulnerabilities with Trivy and enforces the custom policy.

`compileall` verifies DAG syntax/import compilation; it does not execute Airflow scheduling or prove
external Ray/MLflow connectivity.

## Trivy policy

Trivy scans OS/library vulnerabilities. `ignore-unfixed: true` excludes issues without an available
fix. `scripts/enforce_trivy.py` fails the workflow when the JSON contains HIGH or CRITICAL findings.

This is vulnerability scanning, not malware detection, penetration testing, secret scanning or
proof that the application logic is secure.

## CD triggers and publishing

CD runs on:

- tags matching `v*`;
- manual `workflow_dispatch`, optionally selecting a Git SHA.

It builds three images, tags each with the full Git SHA and `latest`, logs into GHCR using the
workflow `GITHUB_TOKEN`, pushes the images, scans all three and exposes the immutable image names as
job outputs.

## Staging job

Staging requires the `KUBE_CONFIG_DATA` GitHub environment secret. The runner decodes it, installs
kubectl, renders manifests, applies them, waits for rollouts and automatically creates a temporary
`curlimages/curl` Pod.

That smoke Pod runs inside the cluster and checks:

```text
GET http://detectra-api:8000/health/ready
GET http://detectra-api:8000/metrics and find review_scans_total
```

The temporary Pod is created during the GitHub Actions staging job—not manually—because the command
uses `kubectl run --rm -i --restart=Never`.

## Critical CD correctness gap

The workflow says it renders immutable references, but it currently runs:

```text
kubectl kustomize k8s/base
```

without replacing the base's hard-coded image tags with `needs.publish.outputs.*`. Therefore the
staging apply may deploy older tags instead of the three images just built. It also deploys only
`k8s/base`, not observability/full/full-GPU overlays.

A correct implementation should copy/render an overlay and set all three images to the exact CD
outputs before apply, then verify the running Pod image IDs. This is the most important live-change
question for the CD topic.

## Demo

In GitHub Actions show:

1. trigger event and commit SHA;
2. CI quality logs, Ruff/Pytest and LFS validation;
3. Docker build and Trivy report enforcement;
4. GHCR package with immutable SHA tag;
5. staging environment/approval;
6. rendered/apply/rollout steps;
7. temporary smoke Pod command;
8. deployment summary.

Locally inspect workflows and render manifests; do not expose repository secrets on screen.

Likely live changes: add a path filter, matrix Python version, test, workflow input, overlay input,
Kustomize image substitution, environment approval or rollback step.

## Viva questions

**Why both CI and CD?** CI validates source/change quality; CD packages and promotes an accepted
revision into an environment.

**Why immutable SHA tags?** A SHA uniquely identifies code/artifacts and prevents a mutable tag from
silently changing beneath a deployment.

**Why scan after building each image?** Each image has different dependencies and attack surface.

**What is `GITHUB_TOKEN`?** A short-lived workflow token scoped by repository permissions; here it
authenticates GHCR publishing.

**What is staging environment?** GitHub environment can hold scoped secrets, protection rules and
manual approvals separate from ordinary repository jobs.

**How would rollback work?** Reapply manifests pinned to the previous known-good SHA and wait for
rollout; database/schema compatibility must also be considered.

## Loopholes

- CD does not currently substitute freshly built SHA images into Kustomize output.
- CD deploys base only, not overlays.
- `latest` is also pushed and should not be used for audited deployment.
- No artifact signing, SBOM attestation or provenance verification is enforced.
- LFS downloads are large and can fail due to quota/network availability.
- Staging smoke checks readiness and one metric only, not review scoring or streaming.
- CI compiles DAG source but does not run an Airflow integration test.
- There is no automated model acceptance/promotion gate in CD.

---

# Topic 18: Security, responsible use, system limitations and future architecture

## What is already done well

- Individual review risk never automatically blocks a review.
- Campaign action requires an explicit moderator confirmation.
- Soft limits are intended to be temporary and reversible.
- Pydantic validates API inputs.
- Model/data/schema/image/deployment lineage is returned with decisions.
- Containers and Kubernetes workloads generally use non-root users, dropped capabilities,
  seccomp and probes.
- API Kubernetes root filesystem is read-only.
- CI retrieves LFS artifacts, runs lint/tests and scans container vulnerabilities.
- DVC, Git and Git LFS separate code, data lineage and large promoted artifacts.
- Kafka schema/feature versions and model feature contracts are checked.
- DLQ preserves invalid stream inputs for inspection.

## Current security gaps

### Identity and access

- FastAPI has no user authentication, API key, OAuth or role checks.
- Anyone reaching the moderator endpoint can claim a moderator name.
- Local Grafana/Airflow credentials are development defaults.
- Elasticsearch/Kibana and Kafka run without security/TLS.

### Secrets

- Production credentials must never be committed in Kubernetes YAML.
- Base64 Kubernetes Secret data is not encryption by itself.
- Some MinIO/AWS values currently appear in ConfigMaps/manifests.
- Rotate any token that has ever appeared in Git history, even after deleting the file.

### Data/privacy

- Review/user/product identifiers flow into logs and in-memory state.
- No retention, deletion, consent, redaction or subject-access workflow is implemented.
- Elasticsearch Kubernetes indices have no configured lifecycle retention.

### Model abuse and fairness

- Labels can encode source artifacts and noise.
- English DistilBERT does not support broad multilingual claims.
- Verified purchase, launch time and UTC timing can correlate with legitimate user groups.
- Campaign ground truth is synthetic.
- Risk probabilities do not identify bots or intent.

### Availability

- API state is in memory and single replica.
- Kafka/Elastic/MLflow/PostgreSQL are mostly single-instance development deployments.
- RWO PVCs and SQLite limit horizontal scale.
- No backup/restore or disaster-recovery exercise is shown.

## Future production architecture

```text
Authenticated gateway + rate limits
             |
             v
Stateless API replicas ---- shared PostgreSQL/Redis audit and campaign state
             |
             v
TLS/SASL multi-broker Kafka + schema registry
             |
             v
Distributed Spark with object-store checkpoints
             |
             v
Versioned model service with approved registry alias and canary rollout
             |
             v
Human moderation + immutable audit trail + feedback labels

Metrics -> Prometheus/Alertmanager -> Grafana alerts
Logs    -> secured Elastic cluster with retention/redaction
Data    -> encrypted object store + DVC lineage
```

Recommended improvements, in order:

1. Add API authentication and moderator RBAC.
2. Move repository state and audit history to PostgreSQL; use Redis if low-latency shared state is
   needed.
3. Fix immutable image substitution in CD and the deploy script's `-ImageTag` handling.
4. Resolve MinIO PVC/credential configuration and use an external secret manager.
5. Add Kafka persistence, TLS/SASL, replication and schema registry.
6. Align training/live `max_reviews` and campaign data paths.
7. Add real human-confirmed campaign labels and external evaluation.
8. Add calibrated campaign probability and real drift measurements.
9. Add Prometheus alert rules, Kafka exporter and Spark metrics.
10. Add model approval, canary/shadow deployment, rollback and post-deployment evaluation.
11. Add structured/redacted logs and retention policies.
12. Add backups, restore testing, SBOM/signing and supply-chain attestations.

## Likely architecture-change exercises

### Add API-key protection

Create a FastAPI dependency that verifies a secret from environment/Secret storage and attach it to
moderation routes. Never hard-code the key or put it in frontend JavaScript.

### Scale API to three replicas

First replace `InMemoryRepository`, externalize Kafka materialization/idempotency, ensure model
memory capacity, then change replica count and strategy. Merely editing `replicas: 3` is incorrect.

### Add a drift metric

Store a training reference distribution, compute a defined statistic such as PSI/JS distance on a
bounded live window, export the real value and alert on a validated threshold. Do not label a
constant or mean risk as feature drift.

### Add a new model feature

Update feature calculation, version, training bundle, normalizer/model shape, inference contract,
tests, DVC lineage and deployment artifact together.

### Promote an MLflow model

Apply validation/acceptance gates, assign an approved alias/version, package the exact artifact into
or mount it for the API, build an immutable image, deploy canary, verify, then expand or roll back.

## Final hard-viva questions

**What is the largest scientific limitation?** Campaign labels are synthetic, so campaign metrics
do not establish real-world generalization.

**What is the largest deployment limitation?** In-memory API state and current image-tag
substitution gaps prevent trustworthy horizontal production rollout.

**What is the largest streaming limitation?** Candidate graph computation is quadratic within
windows and delivery is not transactionally exactly once.

**What is the largest monitoring limitation?** No real feature/embedding drift, Kafka lag, Spark
processing or alerting pipeline is implemented.

**Why is the project still a valid MLOps demonstration?** It integrates versioned data preparation,
baseline/neural comparison, distributed tuning, experiment tracking, orchestration, event streaming,
containerized inference, declarative deployment, metrics, logs and CI/CD while making the remaining
production gaps explicit.

## Final demonstration sequence

1. Show Git commit, LFS artifacts and `dvc dag`.
2. Show Airflow DAG structure without launching an expensive training run.
3. Open a completed Ray trial and corresponding MLflow selected run.
4. Show Docker/Kubernetes workload health and immutable image identity.
5. Scan one review through UI/FastAPI and explain individual risk.
6. Replay one campaign through Kafka/Spark/hybrid scorer.
7. Show the scored Kafka message and materialized campaign.
8. Confirm/dismiss in the UI and explain reversible human control.
9. Correlate API/Ray metrics in Prometheus/Grafana.
10. Find the exact scoring/moderation log in Kibana.
11. Show CI checks, Trivy and GHCR images.
12. Close with limitations and the prioritized production roadmap.

## Overall closing answer

> Detectra intentionally separates individual text risk from group-level coordination risk. DVC
> builds leakage-aware versioned datasets; a fixed TF-IDF baseline establishes a control; Ray tunes
> contextual DistilBERT review and hybrid campaign models; MLflow records and packages the selected
> versions; Airflow orchestrates DVC and Ray; Kafka and Spark provide replayable event-time
> streaming; FastAPI and a vanilla frontend expose evidence to human moderators; Prometheus/Grafana
> monitor metrics and Filebeat/Elastic/Kibana centralize logs; Docker packages role-specific images;
> Kubernetes describes runtime/storage/security boundaries; and GitHub Actions tests, scans and
> publishes releases. The project is a strong end-to-end MLOps proof of concept, while synthetic
> campaign labels, in-memory state, development security defaults and image-promotion gaps are
> explicitly acknowledged rather than hidden.
