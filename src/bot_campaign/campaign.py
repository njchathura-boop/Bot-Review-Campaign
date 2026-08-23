from __future__ import annotations

import hashlib

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .schemas import CampaignAlert, Review


def detect_campaigns(
    reviews: list[Review], similarity_threshold: float = 0.75, window_hours: float = 24, minimum_group_size: int = 3
) -> list[CampaignAlert]:
    """Lightweight UI fallback for same- or cross-product coordination."""
    if len(reviews) < minimum_group_size:
        return []
    matrix = TfidfVectorizer(ngram_range=(1, 2), min_df=1).fit_transform([r.text for r in reviews])
    similarity = cosine_similarity(matrix)
    adjacency = {index: set() for index in range(len(reviews))}
    for left in range(len(reviews)):
        for right in range(left + 1, len(reviews)):
            hours = abs((reviews[left].timestamp - reviews[right].timestamp).total_seconds()) / 3600
            semantic_edge = similarity[left, right] >= similarity_threshold
            account_edge = reviews[left].user_id == reviews[right].user_id
            if hours <= window_hours and (semantic_edge or account_edge):
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
        product_ids = sorted({review.product_id for review in selected})
        pairs = [similarity[a, b] for a in component for b in component if a < b]
        mean_similarity = float(np.mean(pairs)) if pairs else 0.0
        rating_consistency = 1.0 - min(float(np.std([r.rating for r in selected])) / 2.0, 1.0)
        score = min(1.0, 0.65 * mean_similarity + 0.2 * rating_consistency + 0.15 * min(len(selected) / 5, 1))
        anchor = min(r.review_id for r in selected)
        digest = hashlib.sha1(f"{'|'.join(product_ids)}|{anchor}".encode()).hexdigest()[:10]
        alerts.append(
            CampaignAlert(
                campaign_id=f"campaign-{digest}",
                product_id=product_ids[0],
                product_ids=product_ids,
                review_ids=[r.review_id for r in selected], user_ids=sorted({r.user_id for r in selected}),
                risk_score=round(score, 4),
                evidence={"review_count": len(selected), "unique_users": len({r.user_id for r in selected}),
                          "product_count": len(product_ids), "target_products": product_ids,
                          "mean_text_similarity": round(mean_similarity, 4), "window_hours": window_hours,
                          "rating_consistency": round(rating_consistency, 4)},
            )
        )
    return sorted(alerts, key=lambda alert: alert.risk_score, reverse=True)
