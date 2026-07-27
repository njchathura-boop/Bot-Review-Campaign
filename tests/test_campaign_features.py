from __future__ import annotations

import json
import math
import os
from pathlib import Path

import numpy as np
import pytest

from bot_campaign.campaign_features import (
    CAMPAIGN_CANDIDATE_SCHEMA,
    CAMPAIGN_FEATURE_VERSION,
    NUMERIC_FEATURE_NAMES,
    aggregate_campaign_group,
    campaign_group_from_candidate,
    load_campaign_groups,
)
from bot_campaign.hybrid_model import NumericNormalizer


def _row(index: int, *, expected_campaign: bool = True) -> dict:
    return {
        "review_id": f"r-{index}",
        "user_id": f"u-{index}",
        "product_id": "p-1",
        "text": f"Repeated promotional review {index}",
        "rating": 5,
        "timestamp": f"2023-07-01T02:{index * 3:02d}:00Z",
        "launch_time": "2023-07-01T00:00:00Z",
        "hours_since_launch": 2 + index / 10,
        "verified_purchase": False,
        "helpful_votes": index,
        "product_reviews_previous_1h": index,
        "user_reviews_previous_24h": 0,
        "expected_campaign": expected_campaign,
        # These leakage fields must never become numeric model features.
        "scenario": "coordinated_positive",
        "campaign_id": "known-answer",
    }


def test_campaign_group_features_are_ordered_versioned_and_label_safe():
    group = aggregate_campaign_group("group-1", [_row(2), _row(0), _row(1)])
    values = dict(zip(NUMERIC_FEATURE_NAMES, group.numeric_features))
    assert group.review_ids == ("r-0", "r-1", "r-2")
    assert group.label == 1
    assert values["review_count"] == 3
    assert values["duration_minutes"] == 6
    assert values["off_hour_ratio"] == 1
    assert values["near_launch_ratio"] == 1
    assert "scenario" not in NUMERIC_FEATURE_NAMES
    assert "campaign_id" not in NUMERIC_FEATURE_NAMES
    assert all(math.isfinite(value) for value in group.numeric_features)

    restored = campaign_group_from_candidate(group.as_candidate())
    assert restored.numeric_features == group.numeric_features
    assert restored.label is None


def test_candidate_contract_rejects_unknown_versions_and_missing_features():
    group = aggregate_campaign_group("group-1", [_row(0)])
    message = group.as_candidate()
    message["schema_version"] = "campaign.candidate.v0"
    with pytest.raises(ValueError, match="Unsupported candidate schema"):
        campaign_group_from_candidate(message)

    message["schema_version"] = CAMPAIGN_CANDIDATE_SCHEMA
    message["feature_version"] = CAMPAIGN_FEATURE_VERSION
    del message["numeric_features"][NUMERIC_FEATURE_NAMES[0]]
    with pytest.raises(ValueError, match="missing numeric features"):
        campaign_group_from_candidate(message)


def test_normalizer_is_fit_on_train_groups_and_handles_constant_columns():
    groups = [
        aggregate_campaign_group("normal", [_row(0, expected_campaign=False)]),
        aggregate_campaign_group("campaign", [_row(0), _row(1)]),
    ]
    normalizer = NumericNormalizer.fit(groups)
    transformed = normalizer.transform([group.numeric_features for group in groups])
    assert transformed.shape == (2, len(NUMERIC_FEATURE_NAMES))
    assert np.isfinite(transformed).all()
    assert np.allclose(transformed.mean(axis=0), 0, atol=1e-6)


def test_bounded_loader_keeps_complete_deterministic_groups():
    path = Path(f"data/processed/_test_campaign_groups_{os.getpid()}.jsonl")
    rows = []
    for index in range(3):
        for group_id in ("group-a", "group-b", "group-c"):
            row = _row(index, expected_campaign=group_id != "group-a")
            row["scenario_group_id"] = group_id
            row["review_id"] = f"{group_id}-{index}"
            rows.append(row)
    try:
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        first = load_campaign_groups(path, max_groups=2)
        second = load_campaign_groups(path, max_groups=2)
        assert [group.group_id for group in first] == [group.group_id for group in second]
        assert all(len(group.review_ids) == 3 for group in first)
    finally:
        path.unlink(missing_ok=True)
