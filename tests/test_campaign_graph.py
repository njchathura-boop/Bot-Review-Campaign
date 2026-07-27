from __future__ import annotations

import numpy as np

from bot_campaign.campaign_graph import CAMPAIGN_WINDOW_SCHEMA, discover_campaign_groups


def _event(index: int, product_id: str, text: str, minute: int) -> dict:
    return {
        "review_id": f"r-{index}",
        "user_id": f"u-{index}",
        "product_id": product_id,
        "text": text,
        "rating": 5,
        "timestamp": f"2023-07-01T02:{minute:02d}:00Z",
        "hours_since_launch": 2,
        "verified_purchase": False,
        "helpful_votes": 0,
        "product_reviews_previous_1h": index,
        "user_reviews_previous_24h": 0,
    }


def test_embedding_graph_discovers_one_campaign_across_products():
    message = {
        "schema_version": CAMPAIGN_WINDOW_SCHEMA,
        "window_start": "2023-07-01T02:00:00Z",
        "window_end": "2023-07-01T03:00:00Z",
        "events": [
            _event(1, "phone-a", "Excellent performance, highly recommended", 1),
            _event(2, "phone-b", "Excellent performance and easy to recommend", 4),
            _event(3, "phone-c", "Highly recommended for its performance", 7),
            _event(4, "phone-d", "The case fits but the buttons are stiff", 30),
        ],
    }
    embeddings = np.asarray(
        [
            [1.0, 0.0, 0.0],
            [0.99, 0.05, 0.0],
            [0.98, 0.02, 0.0],
            [0.0, 1.0, 0.0],
        ],
        dtype=np.float32,
    )
    groups = discover_campaign_groups(message, embeddings, similarity_threshold=0.95)
    assert len(groups) == 1
    group, indices = groups[0]
    assert indices == (0, 1, 2)
    assert group.product_ids == ("phone-a", "phone-b", "phone-c")
    assert group.review_ids == ("r-1", "r-2", "r-3")
    assert group.label is None


def test_behavior_edge_discovers_same_product_burst_without_text_similarity():
    message = {
        "schema_version": CAMPAIGN_WINDOW_SCHEMA,
        "window_start": "2023-07-01T02:00:00Z",
        "events": [
            _event(1, "phone-a", "first wording", 1),
            _event(2, "phone-a", "different wording", 4),
            _event(3, "phone-a", "another wording", 7),
        ],
    }
    embeddings = np.eye(3, dtype=np.float32)
    groups = discover_campaign_groups(message, embeddings, similarity_threshold=0.99)
    assert len(groups) == 1
    assert groups[0][0].product_ids == ("phone-a",)
