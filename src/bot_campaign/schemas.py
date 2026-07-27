from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Review(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    review_id: str = Field(min_length=1, max_length=200)
    user_id: str = Field(min_length=1, max_length=200)
    product_id: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=3, max_length=20_000)
    rating: float = Field(ge=1, le=5)
    timestamp: datetime
    verified_purchase: bool = False
    helpful_votes: int = Field(default=0, ge=0)
    language: str = Field(default="en", min_length=2, max_length=20)
    category: str = Field(default="general_merchandise", min_length=1, max_length=100)
    source: str = Field(default="platform", min_length=1, max_length=100)
    metadata_provenance: dict[str, str] = Field(default_factory=dict)
    launch_time: datetime | None = None
    launch_time_provenance: str | None = Field(default=None, max_length=100)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("launch_time")
    @classmethod
    def normalize_launch_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class TextLabeledReview(BaseModel):
    """Supervised text record with no invented user, product, or event metadata."""

    model_config = ConfigDict(str_strip_whitespace=True)

    review_id: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=3, max_length=20_000)
    label: int = Field(ge=0, le=1)
    category: str | None = Field(default=None, max_length=100)
    rating: float | None = Field(default=None, ge=1, le=5)
    source: str = Field(default="unknown", min_length=1, max_length=100)
    group_id: str | None = Field(default=None, max_length=200)
    label_provenance: str = Field(default="observed_source_label", max_length=100)
    field_provenance: dict[str, str] = Field(default_factory=dict)
    synthetic: bool = False
    split: Literal["train", "validation", "test"] | None = None


class ReviewPrediction(BaseModel):
    review_id: str
    fake_probability: float = Field(ge=0, le=1)
    label: str
    needs_review: bool
    threshold: float
    model_version: str
    evidence: list[str]
    status: str = "completed"
    calibrated_confidence: float | None = Field(default=None, ge=0, le=1)
    campaign_id: str | None = None
    similar_review_count: int = Field(default=0, ge=0)
    processing_ms: float = Field(default=0, ge=0)
    feature_version: str = "feature-set-v1"
    api_schema_version: str = "reviews.scored.v1"
    dataset_version: str = "demo-data-v1"


class BatchReviewRequest(BaseModel):
    reviews: list[Review] = Field(min_length=1, max_length=500)


class DemoReplayRequest(BaseModel):
    scenario: str = Field(default="coordinated-positive", max_length=80)


class ModerationDecision(BaseModel):
    decision: str = Field(pattern="^(confirm|dismiss|restore)$")
    moderator: str = Field(default="demo-moderator", min_length=1, max_length=100)
    reason: str = Field(default="", max_length=500)


class CampaignRequest(BaseModel):
    reviews: list[Review] = Field(min_length=2, max_length=500)


class CampaignAlert(BaseModel):
    campaign_id: str
    product_id: str
    product_ids: list[str] = Field(default_factory=list)
    review_ids: list[str]
    user_ids: list[str]
    risk_score: float = Field(ge=0, le=1)
    evidence: dict[str, Any]
    label: str = "suspicious coordination"
