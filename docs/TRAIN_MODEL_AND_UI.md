# Train both DistilBERT models and test them in the UI

## Which model is being trained?

There are two promoted models because they answer different questions:

| UI action | Promoted model | Inputs | Meaning |
|---|---|---|---|
| **Scan review** | `review-risk-distilbert-v1` | One review's text | Text-based review risk; never enough to restrict visibility |
| **Replay campaign** | `bot-campaign-hybrid-distilbert` | Related review texts plus group timing/behavior features | Evidence of coordinated activity |

`tfidf-logreg-v1` is retained only as a cheap baseline, regression comparison, and
local fallback when the individual DistilBERT artifact has not been trained. It is not
the promoted model. DistilBERT replaces TF-IDF in both promoted NLP paths. The campaign
model still needs its numeric branch because a language encoder cannot infer burst
timing, account reuse, product launch phase, verification ratio, or cross-product reach
from review text alone.

No label, split name, scenario name, review ID, user ID, product ID, source, or
provenance string is supplied as a model feature. Synthetic text is train-only;
validation and test are real-only for individual review risk. Campaign splits use
whole scenario groups, chronological holdout, and disjoint generated text families.
After Ray selects the review model, validation predictions fit a temperature-scaling
calibrator and the operating threshold. Test data is used only for the final report.

## 1. Install Python 3.11

Ray is not installed in the current Python 3.14 environment. Install Python 3.11:

```powershell
winget install -e --id Python.Python.3.11
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,campaign-training,streaming]"
```

Confirm CUDA and Ray:

```powershell
python -c "import torch,ray,mlflow; print(torch.__version__); print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0))"
```

## 2. Regenerate leakage-safe campaign splits

The currently completed full bundle was generated as `campaign-events-v2`. Do not use
it for final training. Generate a small v3 smoke dataset without overwriting the full
bundle:

```powershell
python -m bot_campaign.cli generate-campaign-splits `
  --profile data/processed/temporal_bundle/behavior/profile.json `
  --products data/processed/temporal_bundle/behavior/products.jsonl `
  --output-dir data/processed/leakage_safe_smoke/campaign `
  --count 2000 `
  --seed 42
```

Confirm:

```powershell
Get-Content data/processed/leakage_safe_smoke/campaign/manifest.json
```

It must report:

```text
campaign_dataset_version = campaign-events-v3
scenario-stratified chronological holdout
disjoint deterministic sentence families
```

For the complete 816,216 controlled events, generate a new directory so v2 remains
recoverable until verification finishes:

```powershell
python -m bot_campaign.cli generate-campaign-splits `
  --profile data/processed/temporal_bundle/behavior/profile.json `
  --products data/processed/temporal_bundle/behavior/products.jsonl `
  --output-dir data/processed/temporal_bundle/campaign_v3 `
  --count 816216 `
  --seed 42
```

## 3. Start MLflow

```powershell
docker compose up -d mlflow
```

Open `http://localhost:5001`. The experiment will be created when training begins.

## 4. Run both end-to-end CPU smoke trainings

### Windows Application Control environments

If Windows reports `WinError 4551`, Enterprise Code Integrity is blocking Ray's
unsigned native `raylet.exe`. Reinstalling Ray, activating a different environment, or
changing PowerShell execution policy cannot override that machine policy. Use the
project's Linux containers instead:

Any executable path, SHA-256 value, policy ID, or Code Integrity event number shown in
diagnostic output is information for an IT allow-list request, not a PowerShell command.
Do not try to execute `raylet.exe` directly; Ray starts it internally with required
arguments.

```powershell
docker compose build ray-review-trainer
docker compose up -d mlflow ray-head ray-worker
docker compose run --rm ray-review-trainer
```

If Docker returns HTTP 500 even for `docker version` or an existing image inspection,
restart Docker Desktop from **Troubleshoot -> Restart Docker Desktop**, wait for the
Linux engine to report Running, and retry. Do not use Factory Reset or delete Docker
data for this recovery.

This still runs real Ray ASHA trials and logs them to the same MLflow server. It does
not replace Ray with a mock or sequential tuner. The bind mount writes the selected
artifact back to the host at `artifacts/review_distilbert`. All Ray services share the
single `bot-campaign-ray:local` image and use `PYTHONPATH=/opt/project/src`, so the
bind-mounted current source is used without duplicating images.

The trainer container submits through the Ray Jobs API at `http://ray-head:8265`. The
driver therefore runs on the head node and attaches with `ray.init(address="auto")`.
Do not connect a separate trainer container directly to the internal GCS port `6379`;
that would incorrectly search the trainer's private `/tmp/ray` for the head session.
Ray trial state and checkpoints use `/opt/project/artifacts/ray_results`, a host bind
path visible at the same location on every Ray node. Node-local `/home/ray/ray_results`
must not be used for a multi-container cluster.

MLflow runs with `--serve-artifacts --artifacts-destination /mlflow/artifacts`. New
experiments therefore receive `mlflow-artifacts:/...` locations and Ray nodes upload
through the tracking server. Do not advertise `/mlflow/artifacts` as
`--default-artifact-root`; that path is private to the MLflow container. Experiments
created under the old direct-local configuration must be retained under a legacy name
and recreated before proxied artifact logging.

First train the model used by **Scan review**:

```powershell
python training/ray_review_train.py `
  --train-data data/processed/dataset_bundle/text/training.jsonl `
  --validation-data data/processed/dataset_bundle/text/real/validation.jsonl `
  --test-data data/processed/dataset_bundle/text/real/test.jsonl `
  --output artifacts/review_distilbert `
  --smoke `
  --gpus-per-trial 0
```

Then train the model used by **Replay campaign**:

```powershell
python training/ray_train.py `
  --train-data data/processed/leakage_safe_smoke/campaign/train.jsonl `
  --validation-data data/processed/leakage_safe_smoke/campaign/validation.jsonl `
  --test-data data/processed/leakage_safe_smoke/campaign/test.jsonl `
  --output artifacts/campaign_model `
  --smoke `
  --gpus-per-trial 0
```

Smoke mode uses one epoch and one Ray trial. The review trainer uses at most 128/64/64
records and the campaign trainer at most 80 complete groups per split. These runs verify
tokenization, optimization, Ray reporting, MLflow logging, threshold selection,
artifact loading, and registration. They are not accuracy results.

Verify the artifact:

```powershell
Get-ChildItem artifacts/campaign_model
Get-Content artifacts/campaign_model/bundle.json
Get-ChildItem artifacts/review_distilbert
Get-Content artifacts/review_distilbert/bundle.json
```

## 5. Test the trained model in the UI

Point the API at both promoted bundles and start FastAPI. If
`REVIEW_TRANSFORMER_PATH` is absent, the API deliberately reports and uses the TF-IDF
fallback from `MODEL_PATH`:

```powershell
$env:CAMPAIGN_MODEL_PATH = "$PWD\artifacts\campaign_model"
$env:REVIEW_TRANSFORMER_PATH = "$PWD\artifacts\review_distilbert"
python -m uvicorn bot_campaign.api:app --host 127.0.0.1 --port 8000 --reload
```

Open `http://127.0.0.1:8000` and:

1. Open **Deployment** and confirm both `Review model` and `Campaign model` are healthy.
2. Return to **Product & Reviews**, choose any single-review example, and click **Scan review**.
3. Confirm its model version is `review-risk-distilbert-v1` (not `tfidf-logreg-v1`).
4. Choose **Cross-product campaign** and click **Replay campaign**.
5. Open **Live Campaigns**.
6. Confirm the campaign evidence contains:
   - `model: bot-campaign-hybrid-distilbert`;
   - `campaign scope: cross_product`;
   - `product count: 3`;
   - the model decision threshold and feature version.

Test the same path directly:

```powershell
$body = @{ scenario = "coordinated-cross-product" } | ConvertTo-Json
Invoke-RestMethod `
  -Uri http://127.0.0.1:8000/v1/demo/replay `
  -Method Post `
  -ContentType application/json `
  -Body $body

Invoke-RestMethod -Uri http://127.0.0.1:8000/v1/campaigns
```

The replay response must say:

```text
campaign_engine = hybrid-distilbert
```

If it says `tfidf-graph-fallback`, the hybrid artifact was not found or loaded.

## 6. Inspect Ray and MLflow trials

Open `http://localhost:5001`. The `review-risk-distilbert` experiment compares:

```text
learning_rate, weight_decay, dropout, batch_size, frozen_encoder_layers,
max_tokens, warmup_ratio, gradient_clip_norm,
positive_weight_multiplier, label_smoothing
```

The `bot-campaign-hybrid-distilbert` experiment compares:

```text
encoder_learning_rate
head_learning_rate
weight_decay
dropout
numeric_hidden_size
fusion_hidden_size
batch_size
frozen_encoder_layers
max_reviews
max_tokens
warmup_ratio
gradient_clip_norm
positive_weight_multiplier
```

Ray samples combinations from these spaces and uses ASHA to stop weak trials early;
this is intentionally more practical than an exhaustive Cartesian grid. Final runs
are `selected-review-distilbert` and `selected-hybrid-campaign-model`; their registered
models are `review-risk-distilbert` and `bot-campaign-hybrid-distilbert`.

## 7. Run a bounded GPU pilot

With one GPU, one trial runs at a time:

```powershell
python training/ray_review_train.py `
  --output artifacts/review_distilbert `
  --num-samples 12 `
  --epochs 3 `
  --max-train-records 12000 `
  --max-validation-records 2500 `
  --max-test-records 2500 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1
```

Then run the bounded campaign pilot:

```powershell
python training/ray_train.py `
  --train-data data/processed/temporal_bundle/campaign_v3/train.jsonl `
  --validation-data data/processed/temporal_bundle/campaign_v3/validation.jsonl `
  --test-data data/processed/temporal_bundle/campaign_v3/test.jsonl `
  --output artifacts/campaign_model `
  --num-samples 12 `
  --epochs 3 `
  --max-train-groups 10000 `
  --max-validation-groups 2500 `
  --max-test-groups 2500 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1
```

The deterministic bounded loader keeps complete groups, so the limits do not split a
campaign across subsets.

## 8. Run the complete GPU experiment

Only after the pilot succeeds, remove group limits:

```powershell
python training/ray_review_train.py `
  --output artifacts/review_distilbert `
  --num-samples 20 `
  --epochs 4 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1 `
  --minimum-precision 0.90
```

This uses the complete text training file and tunes only against the real validation
split. The real test split is read for the selected model only. Next run the complete
campaign experiment:

```powershell
python training/ray_train.py `
  --train-data data/processed/temporal_bundle/campaign_v3/train.jsonl `
  --validation-data data/processed/temporal_bundle/campaign_v3/validation.jsonl `
  --test-data data/processed/temporal_bundle/campaign_v3/test.jsonl `
  --output artifacts/campaign_model `
  --num-samples 20 `
  --epochs 4 `
  --cpus-per-trial 4 `
  --gpus-per-trial 1 `
  --minimum-precision 0.95
```

Do not promote solely on synthetic test PR-AUC. Also inspect campaign recall,
legitimate-launch-burst false positives, text-only/numeric-only ablations, graph recall,
calibration and moderator review.
