from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timezone
import numpy as np

from .campaign_features import CampaignGroup, aggregate_campaign_group


CAMPAIGN_WINDOW_SCHEMA = "campaign.window.v1"


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _polarity(rating: float) -> int:
    return 1 if rating >= 4 else -1 if rating <= 2 else 0


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1


def discover_campaign_groups(
    message: dict,
    embeddings: np.ndarray,
    *,
    similarity_threshold: float = 0.88,
    minimum_group_size: int = 3,
    burst_minutes: float = 15.0,
) -> list[tuple[CampaignGroup, tuple[int, ...]]]:
    """Infer cross-product campaign components from a Spark event-time window."""
    if message.get("schema_version") != CAMPAIGN_WINDOW_SCHEMA:
        raise ValueError(f"Unsupported campaign window schema: {message.get('schema_version')!r}")
    events = list(message.get("events") or ())
    if len(events) != len(embeddings):
        raise ValueError("Embedding count must equal window event count")
    if not events:
        return []
    if not 0 < similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be in (0, 1]")

    vectors = np.asarray(embeddings, dtype=np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    normalized = vectors / np.maximum(norms, 1e-12)
    similarities = normalized @ normalized.T
    timestamps = [_timestamp(str(event["timestamp"])) for event in events]
    graph = _DisjointSet(len(events))

    for left in range(len(events)):
        for right in range(left + 1, len(events)):
            same_user = str(events[left]["user_id"]) == str(events[right]["user_id"])
            same_product = str(events[left]["product_id"]) == str(events[right]["product_id"])
            same_polarity = _polarity(float(events[left]["rating"])) == _polarity(
                float(events[right]["rating"])
            )
            minutes = abs((timestamps[right] - timestamps[left]).total_seconds()) / 60
            semantic_edge = similarities[left, right] >= similarity_threshold and same_polarity
            burst_edge = (
                same_product
                and same_polarity
                and minutes <= burst_minutes
                and not bool(events[left].get("verified_purchase"))
                and not bool(events[right].get("verified_purchase"))
            )
            if same_user or semantic_edge or burst_edge:
                graph.union(left, right)

    components: dict[int, list[int]] = defaultdict(list)
    for index in range(len(events)):
        components[graph.find(index)].append(index)

    results = []
    for indices in components.values():
        if len(indices) < minimum_group_size:
            continue
        review_ids = sorted(str(events[index]["review_id"]) for index in indices)
        digest = hashlib.sha256("|".join(review_ids).encode("utf-8")).hexdigest()[:16]
        # Stable across overlapping Spark windows when membership is unchanged.
        group_id = f"online-{digest}"
        group = aggregate_campaign_group(group_id, (events[index] for index in indices))
        results.append((group, tuple(indices)))
    return sorted(results, key=lambda item: item[0].group_id)
