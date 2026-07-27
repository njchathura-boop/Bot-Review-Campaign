from __future__ import annotations

import threading
from collections import deque
from typing import Any

from .schemas import CampaignAlert, Review


class InMemoryRepository:
    """Thread-safe local repository; replace this interface with PostgreSQL in production."""

    def __init__(self, review_limit: int = 250, candidate_limit: int = 500) -> None:
        self._lock = threading.RLock()
        self._reviews: deque[dict[str, Any]] = deque(maxlen=review_limit)
        self._review_inputs: deque[Review] = deque(maxlen=candidate_limit)
        self._campaigns: dict[str, dict[str, Any]] = {}
        self._replays: dict[str, dict[str, Any]] = {}

    def add_review(self, review: Review, record: dict[str, Any]) -> None:
        with self._lock:
            self._review_inputs.append(review)
            self._reviews.appendleft(record)

    def campaign_candidates(self, category: str, limit: int) -> list[Review]:
        with self._lock:
            matches = [
                review
                for review in reversed(self._review_inputs)
                if review.category == category
            ]
            return list(reversed(matches[:limit]))

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._reviews)[: max(1, min(limit, 100))]

    def get_review(self, review_id: str) -> dict[str, Any] | None:
        with self._lock:
            return next(
                (item for item in self._reviews if item["review_id"] == review_id),
                None,
            )

    def upsert_campaign(self, alert: CampaignAlert, detected_at: str) -> dict[str, Any]:
        with self._lock:
            prior = self._campaigns.get(alert.campaign_id, {})
            campaign = {
                **alert.model_dump(mode="json"),
                "campaign_type": "coordinated review activity",
                "status": prior.get("status", "detected"),
                "soft_limit": prior.get("soft_limit", False),
                "expires_at": prior.get("expires_at"),
                "moderation_history": prior.get("moderation_history", []),
                "detected_at": prior.get("detected_at", detected_at),
            }
            self._campaigns[alert.campaign_id] = campaign
            return campaign

    def campaigns(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(
                self._campaigns.values(),
                key=lambda item: item["risk_score"],
                reverse=True,
            )

    def get_campaign(self, campaign_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._campaigns.get(campaign_id)

    def save_campaign(self, campaign_id: str, campaign: dict[str, Any]) -> None:
        with self._lock:
            self._campaigns[campaign_id] = campaign

    def save_replay(self, job_id: str, replay: dict[str, Any]) -> None:
        with self._lock:
            self._replays[job_id] = replay

    def get_replay(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._replays.get(job_id)
