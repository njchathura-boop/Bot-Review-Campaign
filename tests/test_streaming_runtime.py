from __future__ import annotations

from dataclasses import replace

import pytest

from bot_campaign.campaign_features import CAMPAIGN_SCORE_SCHEMA
from bot_campaign.config import Settings
from bot_campaign.data import load_text_labels
from bot_campaign.model import ReviewScorer, train
from bot_campaign.runtime import TrustRuntime
from bot_campaign.streaming_runtime import campaign_alert_from_score


def _score(*, candidate: bool = True) -> dict:
    return {
        "schema_version": CAMPAIGN_SCORE_SCHEMA,
        "feature_version": "campaign-group-v1",
        "group_id": "online-cross-product-1",
        "review_ids": ["r-3", "r-1", "r-2"],
        "user_ids": ["u-3", "u-1", "u-2"],
        "product_ids": ["p-3", "p-1", "p-2"],
        "campaign_scope": "cross_product",
        "window_start": "2025-01-01T10:00:00Z",
        "window_end": "2025-01-01T10:10:00Z",
        "campaign_risk": 0.94,
        "candidate": candidate,
        "decision_threshold": 0.8,
        "semantic_edge_threshold": 0.88,
        "model_name": "bot-campaign-hybrid-distilbert",
        "source_window_truncated": False,
        "replay_job_ids": ["replay-stream-1"],
    }


def test_campaign_score_converts_to_cross_product_api_alert():
    alert = campaign_alert_from_score(_score())
    assert alert is not None
    assert alert.campaign_id == "online-cross-product-1"
    assert alert.product_ids == ["p-1", "p-2", "p-3"]
    assert alert.user_ids == ["u-1", "u-2", "u-3"]
    assert alert.evidence["transport"] == "kafka-spark-streaming"


def test_non_candidate_score_is_not_materialized():
    assert campaign_alert_from_score(_score(candidate=False)) is None


def test_invalid_score_schema_is_rejected():
    message = _score()
    message["schema_version"] = "campaign.scored.v0"
    with pytest.raises(ValueError, match="Unsupported campaign score schema"):
        campaign_alert_from_score(message)


def test_runtime_materializes_score_and_completes_replay(monkeypatch):
    reviews, _ = load_text_labels("data/sample/labeled_reviews.jsonl")
    bundle, _ = train(reviews, seed=42)
    settings = replace(Settings.from_env(), streaming_enabled=True)
    runtime = TrustRuntime(settings, scorer=ReviewScorer(bundle))
    monkeypatch.setattr("bot_campaign.runtime.publish_reviews", lambda *args, **kwargs: 4)

    replay = runtime.replay("coordinated-cross-product", mode="stream")
    message = _score()
    message["replay_job_ids"] = [replay["job_id"]]
    runtime.ingest_campaign_score(message)

    saved = runtime.repository.get_replay(replay["job_id"])
    assert saved is not None
    assert saved["status"] == "completed"
    assert saved["campaign_ids"] == ["online-cross-product-1"]
    assert runtime.repository.get_campaign("online-cross-product-1") is not None
    assert runtime.monitoring()["streaming"]["scores_consumed"] == 1
