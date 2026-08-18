from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from bot_campaign.campaign_graph import CAMPAIGN_WINDOW_SCHEMA
from bot_campaign.campaign_inference import score_campaign_window


class _FakeScorer:
    config = SimpleNamespace(feature_version="campaign-group-v1", threshold=0.8)

    def embed_texts(self, texts):
        return np.asarray([[1.0, index * 0.01] for index, _ in enumerate(texts)])

    def predict_with_review_embeddings(self, groups, embeddings):
        assert len(groups) == len(embeddings) == 1
        return np.asarray([0.93], dtype=np.float32)


def test_window_scorer_emits_cross_product_scope_and_all_ids():
    events = [
        {
            "review_id": f"r-{index}",
            "user_id": f"u-{index}",
            "product_id": f"p-{index}",
            "text": "Semantically coordinated promotional wording",
            "rating": 5,
            "timestamp": f"2023-07-01T02:0{index}:00Z",
            "verified_purchase": False,
            "hours_since_launch": 2,
            "replay_job_id": "replay-cross-product",
            "helpful_votes": 0,
            "product_reviews_previous_1h": 0,
            "user_reviews_previous_24h": 0,
        }
        for index in range(3)
    ]
    scores = score_campaign_window(
        {
            "schema_version": CAMPAIGN_WINDOW_SCHEMA,
            "window_start": "2023-07-01T02:00:00Z",
            "window_end": "2023-07-01T03:00:00Z",
            "events": events,
            "truncated": False,
        },
        _FakeScorer(),
        similarity_threshold=0.95,
    )
    assert len(scores) == 1
    assert scores[0]["campaign_scope"] == "cross_product"
    assert scores[0]["product_ids"] == ["p-0", "p-1", "p-2"]
    assert scores[0]["candidate"] is True
    assert scores[0]["user_ids"] == ["u-0", "u-1", "u-2"]
    assert scores[0]["replay_job_ids"] == ["replay-cross-product"]
    assert np.isclose(scores[0]["campaign_risk"], 0.93)
