from __future__ import annotations

import json
import os
import platform
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from .campaign import detect_campaigns
from .campaign_graph import CAMPAIGN_WINDOW_SCHEMA
from .campaign_inference import score_campaign_window
from .config import Settings
from .hybrid_model import HybridCampaignScorer
from .model import ReviewScorer
from .observability import RuntimeMetrics
from .repository import InMemoryRepository
from .review_transformer import ReviewDistilBertScorer
from .schemas import CampaignAlert, Review, ReviewPrediction


class ModelUnavailableError(RuntimeError):
    pass


class TrustRuntime:
    """Application service layer shared by HTTP, tests, and future consumers."""

    def __init__(
        self,
        settings: Settings,
        repository: InMemoryRepository | None = None,
        metrics: RuntimeMetrics | None = None,
        scorer: ReviewScorer | ReviewDistilBertScorer | None = None,
        campaign_scorer: HybridCampaignScorer | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or InMemoryRepository()
        self.metrics = metrics or RuntimeMetrics()
        self._scorer = scorer
        self._campaign_scorer = campaign_scorer

    @property
    def model_ready(self) -> bool:
        transformer = self.settings.review_transformer_path
        return self._scorer is not None or (
            (transformer / "bundle.json").exists()
            and (transformer / "model").is_dir()
            and (transformer / "tokenizer").is_dir()
        ) or self.settings.model_path.exists()

    @property
    def model_version(self) -> str:
        if self._scorer:
            return str(self._scorer.bundle.get("version", "unknown"))
        bundle_path = self.settings.review_transformer_path / "bundle.json"
        if bundle_path.exists():
            try:
                return str(json.loads(bundle_path.read_text(encoding="utf-8")).get("version", "unknown"))
            except (OSError, ValueError, TypeError):
                return "bundle-invalid"
        return "not-loaded"

    def scorer(self) -> ReviewScorer | ReviewDistilBertScorer:
        if self._scorer is None:
            if not self.model_ready:
                raise ModelUnavailableError(
                    "Review model is not trained. Run: python training/ray_review_train.py --smoke"
                )
            transformer = self.settings.review_transformer_path
            if (transformer / "bundle.json").exists():
                self._scorer = ReviewDistilBertScorer.load(transformer)
            else:
                self._scorer = ReviewScorer.load(self.settings.model_path)
        return self._scorer

    @property
    def campaign_model_ready(self) -> bool:
        return self._campaign_scorer is not None or (
            (self.settings.campaign_model_path / "bundle.json").exists()
            and (self.settings.campaign_model_path / "model_state.pt").exists()
        )

    def campaign_scorer(self) -> HybridCampaignScorer:
        if self._campaign_scorer is None:
            if not self.campaign_model_ready:
                raise ModelUnavailableError(
                    "Campaign model is not trained. Run: python training/ray_train.py --smoke"
                )
            self._campaign_scorer = HybridCampaignScorer.load(
                self.settings.campaign_model_path
            )
        return self._campaign_scorer

    def score(self, review: Review) -> ReviewPrediction:
        started = time.perf_counter()
        prediction = self.scorer().predict(review)
        latency_ms = (time.perf_counter() - started) * 1_000
        candidates = self.repository.campaign_candidates(
            review.category, self.settings.campaign_candidate_limit - 1
        )
        alerts = detect_campaigns([*candidates, review])
        matching = next((alert for alert in alerts if review.review_id in alert.review_ids), None)
        detected_at = datetime.now(UTC).isoformat()
        if matching:
            self.repository.upsert_campaign(matching, detected_at)
        prediction.processing_ms = round(latency_ms, 2)
        prediction.calibrated_confidence = round(
            max(prediction.fake_probability, 1 - prediction.fake_probability), 4
        )
        prediction.campaign_id = matching.campaign_id if matching else None
        prediction.similar_review_count = max(0, len(matching.review_ids) - 1) if matching else 0
        prediction.feature_version = self.settings.feature_version
        prediction.api_schema_version = self.settings.api_schema_version
        prediction.dataset_version = self.settings.dataset_version
        record = {
            **review.model_dump(mode="json"),
            **prediction.model_dump(mode="json"),
            "scanned_at": detected_at,
            "moderator_status": ("queued" if prediction.needs_review else "not_required"),
            "lineage": self.lineage(prediction.model_version),
        }
        self.repository.add_review(review, record)
        self.metrics.record_prediction(
            latency_ms, prediction.fake_probability, prediction.needs_review
        )
        return prediction

    def replay(self, scenario: str) -> dict[str, Any]:
        job_id = f"replay-{uuid.uuid4().hex[:12]}"
        reviews = replay_reviews(scenario)
        predictions = [self.score(review) for review in reviews]
        campaign_engine = "tfidf-graph-fallback"
        hybrid_scores: list[dict[str, Any]] = []
        if self.campaign_model_ready:
            hybrid_scores = score_campaign_window(
                replay_window(reviews), self.campaign_scorer()
            )
            campaign_engine = "hybrid-distilbert"
            detected_at = datetime.now(UTC).isoformat()
            for score in hybrid_scores:
                if not score["candidate"]:
                    continue
                alert = CampaignAlert(
                    campaign_id=score["group_id"],
                    product_id=score["product_ids"][0],
                    product_ids=score["product_ids"],
                    review_ids=score["review_ids"],
                    user_ids=sorted(
                        {
                            review.user_id
                            for review in reviews
                            if review.review_id in score["review_ids"]
                        }
                    ),
                    risk_score=score["campaign_risk"],
                    evidence={
                        "model": score["model_name"],
                        "campaign_scope": score["campaign_scope"],
                        "decision_threshold": score["decision_threshold"],
                        "feature_version": score["feature_version"],
                        "review_count": len(score["review_ids"]),
                        "product_count": len(score["product_ids"]),
                    },
                )
                self.repository.upsert_campaign(alert, detected_at)
        replay = {
            "job_id": job_id,
            "scenario": scenario,
            "status": "completed",
            "review_ids": [item.review_id for item in predictions],
            "campaign_engine": campaign_engine,
            "hybrid_scores": hybrid_scores,
        }
        self.repository.save_replay(job_id, replay)
        return replay

    def moderate(
        self, campaign_id: str, decision: str, moderator: str, reason: str
    ) -> dict[str, Any] | None:
        campaign = self.repository.get_campaign(campaign_id)
        if not campaign:
            return None
        now = datetime.now(UTC)
        if decision == "confirm":
            campaign.update(
                status="confirmed",
                soft_limit=True,
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )
        elif decision == "dismiss":
            campaign.update(status="dismissed", soft_limit=False, expires_at=None)
            self.metrics.record_dismissal()
        else:
            campaign.update(status="restored", soft_limit=False, expires_at=None)
        campaign["moderation_history"].append(
            {
                "decision": decision,
                "moderator": moderator,
                "reason": reason,
                "at": now.isoformat(),
            }
        )
        self.repository.save_campaign(campaign_id, campaign)
        return campaign

    def monitoring(self) -> dict[str, Any]:
        campaigns = self.repository.campaigns()
        return self.metrics.summary(
            campaign_count=len(campaigns),
            soft_limit_count=sum(int(item["soft_limit"]) for item in campaigns),
            kafka_lag=int(os.getenv("DEMO_KAFKA_LAG", "0")),
            feature_drift=float(os.getenv("DEMO_FEATURE_DRIFT", "0.04")),
            embedding_drift=float(os.getenv("DEMO_EMBEDDING_DRIFT", "0.03")),
        )

    def lineage(self, model_version: str | None = None) -> dict[str, str]:
        return {
            "application": self.settings.app_version,
            "git_commit": self.settings.git_commit,
            "model": model_version or self.model_version,
            "mlflow_run": self.settings.mlflow_run_id,
            "data": self.settings.dataset_version,
            "generator": os.getenv("GENERATOR_VERSION", "amazon-temporal-scenarios-v2"),
            "features": self.settings.feature_version,
            "kafka_schema": "reviews.raw.v1",
            "api_schema": self.settings.api_schema_version,
            "docker_image": self.settings.image_digest,
            "deployment": self.settings.deployment_revision,
        }

    def operations(self) -> dict[str, Any]:
        configured = {
            "Kafka": "KAFKA_BOOTSTRAP_SERVERS",
            "Spark": "SPARK_MASTER",
            "MLflow": "MLFLOW_URL",
            "Kubernetes": "KUBERNETES_SERVICE_HOST",
        }
        services = [
            {
                "name": "FastAPI",
                "status": "healthy",
                "detail": f"v{self.settings.app_version}",
            },
            {
                "name": "Review model",
                "status": "healthy" if self.model_ready else "unavailable",
                "detail": self.model_version,
            },
            {
                "name": "Campaign model",
                "status": "healthy" if self.campaign_model_ready else "unavailable",
                "detail": (
                    "hybrid-distilbert"
                    if self.campaign_model_ready
                    else "train with training/ray_train.py"
                ),
            },
        ]
        services.extend(
            {
                "name": name,
                "status": "configured" if os.getenv(variable) else "not_configured",
                "detail": (
                    "endpoint configured"
                    if os.getenv(variable)
                    else "not part of this local process"
                ),
            }
            for name, variable in configured.items()
        )
        return {
            "environment": self.settings.environment,
            "host": platform.node(),
            "services": services,
            "links": {
                "grafana": self.settings.grafana_url,
                "mlflow": self.settings.mlflow_url,
            },
            "lineage": self.lineage(),
        }


def replay_reviews(scenario: str) -> list[Review]:
    now = datetime.now(UTC)
    negative = "negative" in scenario
    cross_product = "cross" in scenario
    product = "demo-competitor-watch" if negative else "demo-smartwatch"
    rating = 1 if negative else 5
    base = (
        "Battery failed immediately and support was completely useless, avoid this product"
        if negative
        else "Outstanding battery life and premium quality, highly recommended for everyone"
    )
    replay_id = uuid.uuid4().hex[:8]
    return [
        Review(
            review_id=f"replay-{replay_id}-{index}",
            user_id=f"campaign-user-{replay_id}-{index}",
            product_id=(f"demo-cross-product-{index % 3}" if cross_product else product),
            text=base + ("!" * index),
            rating=rating,
            timestamp=now + timedelta(minutes=index * 3),
            verified_purchase=False,
            helpful_votes=0,
            language="en",
            category="electronics",
        )
        for index in range(1, 5)
    ]


def replay_window(reviews: list[Review]) -> dict[str, Any]:
    timestamps = [review.timestamp for review in reviews]
    return {
        "schema_version": CAMPAIGN_WINDOW_SCHEMA,
        "window_start": min(timestamps).isoformat().replace("+00:00", "Z"),
        "window_end": max(timestamps).isoformat().replace("+00:00", "Z"),
        "truncated": False,
        "events": [
            {
                **review.model_dump(mode="json"),
                "timestamp": review.timestamp.isoformat().replace("+00:00", "Z"),
                "hours_since_launch": 48.0,
            }
            for review in reviews
        ],
    }
