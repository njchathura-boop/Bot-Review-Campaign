from __future__ import annotations

import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse

from .campaign import detect_campaigns
from .demo_state import APP_VERSION, lineage, ops_summary, replay_reviews, store
from .model import ReviewScorer
from .schemas import (
    BatchReviewRequest,
    CampaignAlert,
    CampaignRequest,
    DemoReplayRequest,
    ModerationDecision,
    Review,
    ReviewPrediction,
)


ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = Path(os.getenv("MODEL_PATH", ROOT / "artifacts" / "review_model.joblib"))
WEB_DIR = ROOT / "web"

app = FastAPI(title="Review Trust API", version=APP_VERSION)
_scorer: ReviewScorer | None = None


def get_scorer() -> ReviewScorer:
    global _scorer
    if _scorer is None:
        if not MODEL_PATH.exists():
            raise HTTPException(status_code=503, detail="Model is not trained. Run: bot-campaign train")
        _scorer = ReviewScorer.load(MODEL_PATH)
    return _scorer


@app.get("/", include_in_schema=False)
def storefront():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/app.js", include_in_schema=False)
def javascript():
    return FileResponse(WEB_DIR / "app.js", media_type="application/javascript")


@app.get("/styles.css", include_in_schema=False)
def stylesheet():
    return FileResponse(WEB_DIR / "styles.css", media_type="text/css")


@app.get("/health")
def health():
    return {"status": "ok", "model_ready": MODEL_PATH.exists(), "version": app.version}


def score_and_record(review: Review) -> ReviewPrediction:
    started = time.perf_counter()
    prediction = get_scorer().predict(review)
    return store.record_prediction(review, prediction, (time.perf_counter() - started) * 1_000)


@app.post("/predict_review", response_model=ReviewPrediction)
def predict_review(review: Review):
    return score_and_record(review)


@app.post("/detect_campaign", response_model=list[CampaignAlert])
def detect_campaign(request: CampaignRequest):
    return detect_campaigns(request.reviews)


@app.post("/v1/reviews/score", response_model=ReviewPrediction)
def score_review_v1(review: Review):
    return score_and_record(review)


@app.post("/v1/reviews/batch-score", response_model=list[ReviewPrediction])
def score_reviews_v1(request: BatchReviewRequest):
    return [score_and_record(review) for review in request.reviews]


@app.get("/v1/reviews/recent")
def recent_reviews(limit: int = 50):
    return {"items": store.recent(limit), "count": len(store.recent(limit))}


@app.get("/v1/reviews/{review_id}/trust")
def review_trust(review_id: str):
    record = next((item for item in store.recent(100) if item["review_id"] == review_id), None)
    if not record:
        raise HTTPException(status_code=404, detail="Review has not been scanned")
    return record


@app.post("/v1/demo/replay")
def start_replay(request: DemoReplayRequest):
    job_id = f"replay-{int(time.time() * 1_000)}"
    predictions = [score_and_record(review) for review in replay_reviews(request.scenario)]
    store.replays[job_id] = {
        "job_id": job_id,
        "scenario": request.scenario,
        "status": "completed",
        "review_ids": [item.review_id for item in predictions],
    }
    return store.replays[job_id]


@app.get("/v1/demo/replay/{job_id}")
def replay_status(job_id: str):
    if job_id not in store.replays:
        raise HTTPException(status_code=404, detail="Replay job not found")
    return store.replays[job_id]


@app.get("/v1/campaigns")
def campaigns_v1():
    return {"items": store.campaign_list(), "count": len(store.campaign_list())}


@app.get("/v1/campaigns/{campaign_id}")
def campaign_v1(campaign_id: str):
    if campaign_id not in store.campaigns:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return store.campaigns[campaign_id]


@app.post("/v1/moderation/campaigns/{campaign_id}/decision")
def moderate_campaign(campaign_id: str, decision: ModerationDecision):
    if campaign_id not in store.campaigns:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return store.moderate(
        campaign_id, decision.decision, decision.moderator, decision.reason
    )


@app.get("/v1/ops/summary")
def operation_summary():
    version = _scorer.bundle.get("version", "not-loaded") if _scorer else "not-loaded"
    return ops_summary(MODEL_PATH.exists(), version)


@app.get("/v1/monitoring/summary")
def monitoring_summary():
    return store.monitoring()


@app.get("/v1/lineage/current")
def current_lineage():
    version = _scorer.bundle.get("version", "not-loaded") if _scorer else "not-loaded"
    return lineage(version)


@app.get("/v1/lineage/predictions/{review_id}")
def prediction_lineage(review_id: str):
    record = next((item for item in store.recent(100) if item["review_id"] == review_id), None)
    if not record:
        raise HTTPException(status_code=404, detail="Prediction lineage not found")
    return record["lineage"]


@app.get("/health/live")
def live():
    return {"status": "alive", "version": APP_VERSION}


@app.get("/health/ready")
def ready():
    if not MODEL_PATH.exists():
        raise HTTPException(status_code=503, detail="Model artifact is unavailable")
    return {"status": "ready", "model_path": MODEL_PATH.name}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    summary = store.monitoring()
    lines = [
        "# HELP review_scans_total Reviews scored by the service.",
        "# TYPE review_scans_total counter",
        f"review_scans_total {summary['reviews_processed']}",
        "# HELP review_inference_p95_ms Review inference p95 latency.",
        "# TYPE review_inference_p95_ms gauge",
        f"review_inference_p95_ms {summary['latency_ms']['p95']}",
        "# HELP campaign_alerts_total Campaigns currently detected.",
        "# TYPE campaign_alerts_total gauge",
        f"campaign_alerts_total {summary['campaign_alerts']}",
        "# HELP review_feature_drift Feature drift score.",
        "# TYPE review_feature_drift gauge",
        f"review_feature_drift {summary['feature_drift']}",
    ]
    return "\n".join(lines) + "\n"
