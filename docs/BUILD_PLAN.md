# Build plan and engineering decisions

## Product boundary

The product produces two different outputs and does not conflate them:

1. **Review risk** is a supervised probability learned only from explicitly labeled
   deceptive/truthful datasets.
2. **Campaign risk** is evidence of coordination derived from review similarity,
   time bursts, user/product activity, and repeated rating behavior. It is not a
   claim that a person is a bot.

Both outputs must expose evidence, confidence, model/data versions, and a safe
`needs_review` state. Automated deletion or user banning is out of scope.

## Delivery phases

### Phase 1 — reproducible local vertical slice (current)

- Canonical schemas and adapters for labeled and Amazon-style JSONL/CSV data.
- Validation with rejected-row reports; immutable raw inputs.
- Leakage-safe TF-IDF baseline, stratified evaluation, persisted model bundle.
- Behavioral/temporal aggregates and explainable campaign heuristics.
- FastAPI endpoints plus an ecommerce-style moderation UI.
- Unit/API tests and deterministic demo data.

Exit gate: a clean checkout can run tests, train a model, start the service, and
score both one review and a review window.

### Phase 2 — real datasets and stronger experiments

- Pin dataset licenses, checksums, source URLs, snapshots, and dataset cards.
- Prepare the local labeled ecommerce product-review corpus and retain its provenance;
  augmentation is allowed only after real train/validation/test splitting.
- Use group-aware splits (text family/product/author where available) and a final
  untouched temporal test set. Compare text-only, metadata-only, and hybrid models.
- Add calibration, threshold selection based on moderation cost, PR-AUC confidence
  intervals, slice metrics, SHAP/global explanations, and adversarial tests.
- Add MLflow tracking and DVC pipelines after the local contracts stabilize.

Exit gate: reproducible experiment report beats the trivial and text-only baselines
without leakage; false-positive slices are reviewed.

### Phase 3 — campaign research

- Create synthetic coordination scenarios with known membership for evaluation.
- Build sliding-window semantic-neighbor graphs; connect users/reviews/products by
  similarity and time, then score communities with density, burst, account, and
  rating signals.
- Evaluate event-level precision/recall, detection delay, cluster purity, and
  analyst workload. Human-label a small sampled set instead of inventing Amazon
  fake labels.

Exit gate: campaign alerts show supporting reviews and remain stable under normal
sale/news bursts.

### Phase 4 — production MLOps

- Kafka schema registry, idempotent producer/consumer, dead-letter queue, replay,
  offset/checkpoint strategy, and online/offline feature parity.
- Airflow for batch orchestration, object storage, Postgres prediction/audit store,
  MLflow registry promotion gates, and DVC/object-store data versioning.
- Container images, CI security/testing gates, Prometheus/Grafana, structured logs,
  alert runbooks, drift reports, canary/shadow rollout, rollback, and retraining
  approval workflow.

Exit gate: load, failure-recovery, drift, rollback, privacy, and security tests pass.

## Data contract

Behavioral review events contain `review_id`, `user_id`, `product_id`, `text`, `rating`,
`timestamp`, `verified_purchase`, and `helpful_votes`. Supervised text records use a
separate contract: `review_id`, `text`, `label`, `source`, `group_id`, provenance, and
optional observed `category`/`rating`. They never receive invented users, products,
timestamps, or launch dates.

Timestamps are normalized to UTC. Identifiers remain strings. Rejected records are
counted with reasons. Personally identifying fields are neither required nor stored.

## Leakage and evaluation rules

- Fit vectorizers, imputers, aggregations, and thresholds on training partitions only.
- Deduplicate exact/near duplicate text before splitting.
- Split by source entity/group when possible, then test on a later time period.
- Do not calculate a user's future activity when scoring their earlier review.
- Report precision, recall, F1, ROC-AUC, PR-AUC, calibration, confusion matrix, and
  per-source/language/rating slices; accuracy alone is insufficient.
- Never describe heuristic campaign scores as supervised probabilities.

## UI workflow

The storefront shows product reviews and a clearly labeled “Trust analysis” panel.
Moderators can inspect risk, evidence, and related campaign activity. Customers see
neutral warnings rather than accusations. Accessibility, mobile layout, empty/error
states, and resilient API behavior are required before visual polish.

## Main risks and controls

- **No Amazon ground truth:** use it for behavior/campaign research, not fake labels.
- **Domain shift:** preserve dataset source; evaluate cross-domain and temporal holdouts.
- **False accusations:** use “suspicious/needs review,” evidence, audit logs, and human review.
- **Cold start:** degrade to text features and mark missing behavioral evidence.
- **Coordinated legitimate events:** require multiple signals and analyst verification.
- **Privacy/security:** minimize identifiers, hash/pseudonymize upstream, validate payloads,
  rate-limit/authenticate moderation endpoints, and never render raw review HTML.
- **Training-serving skew:** persist one fitted pipeline and reuse canonical transformations.
