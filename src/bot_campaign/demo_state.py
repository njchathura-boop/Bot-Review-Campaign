from __future__ import annotations

import os
import platform
import statistics
import threading
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from typing import Any

from .campaign import detect_campaigns
from .schemas import CampaignAlert, Review, ReviewPrediction


APP_VERSION = "1.1.0"
FEATURE_VERSION = "feature-set-v2"
DATASET_VERSION = os.getenv("DATASET_VERSION", "demo-data-v1")
SCHEMA_VERSION = "reviews.scored.v1"


class DemoStore:
    """Small in-memory audit store for the local UI; production uses PostgreSQL."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.reviews: deque[dict[str, Any]] = deque(maxlen=250)
        self.review_inputs: deque[Review] = deque(maxlen=500)
        self.campaigns: dict[str, dict[str, Any]] = {}
        self.replays: dict[str, dict[str, Any]] = {}
        self.latencies: deque[float] = deque(maxlen=2_000)
        self.started_at = time.monotonic()
        self.counts = Counter()

    def record_prediction(
        self, review: Review, prediction: ReviewPrediction, latency_ms: float
    ) -> ReviewPrediction:
        with self._lock:
            self.review_inputs.append(review)
            alerts = detect_campaigns(list(self.review_inputs))
            matching = next(
                (alert for alert in alerts if review.review_id in alert.review_ids), None
            )
            if matching:
                self._upsert_campaign(matching)
            prediction.processing_ms = round(latency_ms, 2)
            prediction.calibrated_confidence = round(
                max(prediction.fake_probability, 1 - prediction.fake_probability), 4
            )
            prediction.campaign_id = matching.campaign_id if matching else None
            prediction.similar_review_count = (
                max(0, len(matching.review_ids) - 1) if matching else 0
            )
            prediction.feature_version = FEATURE_VERSION
            prediction.api_schema_version = SCHEMA_VERSION
            prediction.dataset_version = DATASET_VERSION
            record = {
                **review.model_dump(mode="json"),
                **prediction.model_dump(mode="json"),
                "scanned_at": datetime.now(timezone.utc).isoformat(),
                "moderator_status": "queued" if prediction.needs_review else "not_required",
                "lineage": lineage(prediction.model_version),
            }
            self.reviews.appendleft(record)
            self.latencies.append(latency_ms)
            self.counts["reviews"] += 1
            self.counts["needs_review"] += int(prediction.needs_review)
            return prediction

    def _upsert_campaign(self, alert: CampaignAlert) -> None:
        prior = self.campaigns.get(alert.campaign_id, {})
        self.campaigns[alert.campaign_id] = {
            **alert.model_dump(mode="json"),
            "campaign_type": "coordinated review activity",
            "status": prior.get("status", "detected"),
            "soft_limit": prior.get("soft_limit", False),
            "expires_at": prior.get("expires_at"),
            "moderation_history": prior.get("moderation_history", []),
            "detected_at": prior.get("detected_at", datetime.now(timezone.utc).isoformat()),
        }

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.reviews)[: max(1, min(limit, 100))]

    def campaign_list(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(
                self.campaigns.values(), key=lambda item: item["risk_score"], reverse=True
            )

    def moderate(
        self, campaign_id: str, decision: str, moderator: str, reason: str
    ) -> dict[str, Any]:
        with self._lock:
            campaign = self.campaigns[campaign_id]
            now = datetime.now(timezone.utc)
            if decision == "confirm":
                campaign.update(
                    status="confirmed",
                    soft_limit=True,
                    expires_at=(now + timedelta(hours=24)).isoformat(),
                )
            elif decision == "dismiss":
                campaign.update(status="dismissed", soft_limit=False, expires_at=None)
                self.counts["dismissals"] += 1
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
            return campaign

    def monitoring(self) -> dict[str, Any]:
        with self._lock:
            samples = sorted(self.latencies)

            def percentile(ratio: float) -> float:
                if not samples:
                    return 0.0
                return round(samples[min(len(samples) - 1, int((len(samples) - 1) * ratio))], 2)

            elapsed = max(time.monotonic() - self.started_at, 1)
            risks = [float(item["fake_probability"]) for item in self.reviews]
            return {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "reviews_processed": self.counts["reviews"],
                "reviews_per_second": round(self.counts["reviews"] / elapsed, 3),
                "latency_ms": {
                    "p50": percentile(0.50),
                    "p95": percentile(0.95),
                    "p99": percentile(0.99),
                },
                "api_error_rate": 0.0,
                "kafka_consumer_lag": int(os.getenv("DEMO_KAFKA_LAG", "0")),
                "spark_input_rows": self.counts["reviews"],
                "spark_processed_rows": self.counts["reviews"],
                "mean_review_risk": round(statistics.fmean(risks), 4) if risks else 0.0,
                "campaign_alerts": len(self.campaigns),
                "soft_limited_campaigns": sum(
                    int(item["soft_limit"]) for item in self.campaigns.values()
                ),
                "campaign_dismissals": self.counts["dismissals"],
                "feature_drift": float(os.getenv("DEMO_FEATURE_DRIFT", "0.04")),
                "embedding_drift": float(os.getenv("DEMO_EMBEDDING_DRIFT", "0.03")),
                "resources": {"cpu_percent": 0, "memory_percent": 0, "gpu_percent": 0},
            }


def lineage(model_version: str = "not-loaded") -> dict[str, str]:
    return {
        "application": APP_VERSION,
        "git_commit": os.getenv("GIT_COMMIT", "local-uncommitted"),
        "model": model_version,
        "mlflow_run": os.getenv("MLFLOW_RUN_ID", "local-baseline"),
        "data": DATASET_VERSION,
        "generator": os.getenv("GENERATOR_VERSION", "qwen3-8b-planned"),
        "features": FEATURE_VERSION,
        "kafka_schema": "reviews.raw.v1",
        "api_schema": SCHEMA_VERSION,
        "docker_image": os.getenv("IMAGE_DIGEST", "local-development"),
        "deployment": os.getenv("DEPLOYMENT_REVISION", "docker-compose-local"),
    }


def ops_summary(model_ready: bool, model_version: str) -> dict[str, Any]:
    links = {
        "grafana": os.getenv("GRAFANA_URL", "http://localhost:3000"),
        "kibana": os.getenv("KIBANA_URL", "http://localhost:5601"),
        "mlflow": os.getenv("MLFLOW_URL", "http://localhost:5001"),
    }
    configured = {
        "kafka": "KAFKA_BOOTSTRAP_SERVERS",
        "spark": "SPARK_MASTER",
        "mlflow": "MLFLOW_URL",
        "postgresql": "DATABASE_URL",
        "redis": "REDIS_URL",
        "minio": "MINIO_ENDPOINT",
        "kubernetes": "KUBERNETES_SERVICE_HOST",
    }
    services = [
        {"name": "FastAPI", "status": "healthy", "detail": f"v{APP_VERSION}"},
        {
            "name": "Model",
            "status": "healthy" if model_ready else "unavailable",
            "detail": model_version,
        },
    ]
    services.extend(
        {
            "name": name.title(),
            "status": "configured" if os.getenv(variable) else "demo",
            "detail": "endpoint configured" if os.getenv(variable) else "local simulation",
        }
        for name, variable in configured.items()
    )
    return {
        "environment": os.getenv("APP_ENV", "local"),
        "deployed_at": os.getenv(
            "DEPLOYED_AT", datetime.now(timezone.utc).isoformat()
        ),
        "host": platform.node(),
        "services": services,
        "links": links,
        "lineage": lineage(model_version),
    }


def replay_reviews(scenario: str) -> list[Review]:
    now = datetime.now(timezone.utc)
    negative = "negative" in scenario
    product = "demo-smartwatch" if not negative else "demo-competitor-watch"
    rating = 1 if negative else 5
    base = (
        "Battery failed immediately and support was completely useless, avoid this product"
        if negative
        else "Outstanding battery life and premium quality, highly recommended for everyone"
    )
    return [
        Review(
            review_id=f"replay-{uuid.uuid4().hex[:8]}-{index}",
            user_id=f"campaign-user-{uuid.uuid4().hex[:6]}",
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


store = DemoStore()
