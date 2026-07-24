from __future__ import annotations

import os
import platform
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .campaign import detect_campaigns
from .config import Settings
from .model import ReviewScorer
from .observability import RuntimeMetrics
from .repository import InMemoryRepository
from .schemas import Review, ReviewPrediction


class ModelUnavailableError(RuntimeError):
    pass


class TrustRuntime:
    """Application service layer shared by HTTP, tests, and future consumers."""

    def __init__(
        self,
        settings: Settings,
        repository: InMemoryRepository | None = None,
        metrics: RuntimeMetrics | None = None,
        scorer: ReviewScorer | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or InMemoryRepository()
        self.metrics = metrics or RuntimeMetrics()
        self._scorer = scorer

    @property
    def model_ready(self) -> bool:
        return self._scorer is not None or self.settings.model_path.exists()

    @property
    def model_version(self) -> str:
        return (
            str(self._scorer.bundle.get("version", "unknown"))
            if self._scorer
            else "not-loaded"
        )

    def scorer(self) -> ReviewScorer:
        if self._scorer is None:
            if not self.model_ready:
                raise ModelUnavailableError(
                    "Model is not trained. Run: bot-campaign train"
                )
            self._scorer = ReviewScorer.load(self.settings.model_path)
        return self._scorer

    def score(self, review: Review) -> ReviewPrediction:
        started = time.perf_counter()
        prediction = self.scorer().predict(review)
        latency_ms = (time.perf_counter() - started) * 1_000
        candidates = self.repository.campaign_candidates(
            review.product_id, self.settings.campaign_candidate_limit - 1
        )
        alerts = detect_campaigns([*candidates, review])
        matching = next(
            (alert for alert in alerts if review.review_id in alert.review_ids), None
        )
        detected_at = datetime.now(timezone.utc).isoformat()
        if matching:
            self.repository.upsert_campaign(matching, detected_at)
        prediction.processing_ms = round(latency_ms, 2)
        prediction.calibrated_confidence = round(
            max(prediction.fake_probability, 1 - prediction.fake_probability), 4
        )
        prediction.campaign_id = matching.campaign_id if matching else None
        prediction.similar_review_count = (
            max(0, len(matching.review_ids) - 1) if matching else 0
        )
        prediction.feature_version = self.settings.feature_version
        prediction.api_schema_version = self.settings.api_schema_version
        prediction.dataset_version = self.settings.dataset_version
        record = {
            **review.model_dump(mode="json"),
            **prediction.model_dump(mode="json"),
            "scanned_at": detected_at,
            "moderator_status": (
                "queued" if prediction.needs_review else "not_required"
            ),
            "lineage": self.lineage(prediction.model_version),
        }
        self.repository.add_review(review, record)
        self.metrics.record_prediction(
            latency_ms, prediction.fake_probability, prediction.needs_review
        )
        return prediction

    def replay(self, scenario: str) -> dict[str, Any]:
        job_id = f"replay-{uuid.uuid4().hex[:12]}"
        predictions = [self.score(review) for review in replay_reviews(scenario)]
        replay = {
            "job_id": job_id,
            "scenario": scenario,
            "status": "completed",
            "review_ids": [item.review_id for item in predictions],
        }
        self.repository.save_replay(job_id, replay)
        return replay

    def moderate(
        self, campaign_id: str, decision: str, moderator: str, reason: str
    ) -> dict[str, Any] | None:
        campaign = self.repository.get_campaign(campaign_id)
        if not campaign:
            return None
        now = datetime.now(timezone.utc)
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
            "generator": os.getenv(
                "GENERATOR_VERSION", "synthetic-scenarios-v1"
            ),
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
                "name": "Model",
                "status": "healthy" if self.model_ready else "unavailable",
                "detail": self.model_version,
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
    now = datetime.now(timezone.utc)
    negative = "negative" in scenario
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
            product_id=product,
            text=base + ("!" * index),
            rating=rating,
            timestamp=now + timedelta(minutes=index * 3),
            verified_purchase=False,
            helpful_votes=0,
            language="en",
        )
        for index in range(1, 5)
    ]
