from __future__ import annotations

import hashlib

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .schemas import CampaignAlert, Review


def detect_campaigns(
    reviews: list[Review], similarity_threshold: float = 0.75, window_hours: float = 24, minimum_group_size: int = 3
) -> list[CampaignAlert]:
    """Find connected components of similar, same-product reviews in a time window."""
    if len(reviews) < minimum_group_size:
        return []
    matrix = TfidfVectorizer(ngram_range=(1, 2), min_df=1).fit_transform([r.text for r in reviews])
    similarity = cosine_similarity(matrix)
    adjacency = {index: set() for index in range(len(reviews))}
    for left in range(len(reviews)):
        for right in range(left + 1, len(reviews)):
            hours = abs((reviews[left].timestamp - reviews[right].timestamp).total_seconds()) / 3600
            if (
                reviews[left].product_id == reviews[right].product_id
                and reviews[left].user_id != reviews[right].user_id
                and hours <= window_hours
                and similarity[left, right] >= similarity_threshold
            ):
                adjacency[left].add(right)
                adjacency[right].add(left)
    alerts, visited = [], set()
    for start in adjacency:
        if start in visited:
            continue
        stack, component = [start], set()
        while stack:
            node = stack.pop()
            if node not in component:
                component.add(node)
                stack.extend(adjacency[node] - component)
        visited |= component
        if len(component) < minimum_group_size:
            continue
        selected = [reviews[index] for index in sorted(component)]
        pairs = [similarity[a, b] for a in component for b in component if a < b]
        mean_similarity = float(np.mean(pairs)) if pairs else 0.0
        rating_consistency = 1.0 - min(float(np.std([r.rating for r in selected])) / 2.0, 1.0)
        score = min(1.0, 0.65 * mean_similarity + 0.2 * rating_consistency + 0.15 * min(len(selected) / 5, 1))
        anchor = min(r.review_id for r in selected)
        digest = hashlib.sha1(
            f"{selected[0].product_id}|{anchor}".encode()
        ).hexdigest()[:10]
        alerts.append(
            CampaignAlert(
                campaign_id=f"campaign-{digest}", product_id=selected[0].product_id,
                review_ids=[r.review_id for r in selected], user_ids=sorted({r.user_id for r in selected}),
                risk_score=round(score, 4),
                evidence={"review_count": len(selected), "unique_users": len({r.user_id for r in selected}),
                          "mean_text_similarity": round(mean_similarity, 4), "window_hours": window_hours,
                          "rating_consistency": round(rating_consistency, 4)},
            )
        )
    return sorted(alerts, key=lambda alert: alert.risk_score, reverse=True)
