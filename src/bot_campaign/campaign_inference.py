from __future__ import annotations

from .campaign_features import CAMPAIGN_SCORE_SCHEMA
from .campaign_graph import discover_campaign_groups
from .hybrid_model import HybridCampaignScorer


def score_campaign_window(
    message: dict,
    scorer: HybridCampaignScorer,
    similarity_threshold: float = 0.88,
    minimum_group_size: int = 3,
) -> list[dict]:
    """Discover and score campaign components without encoding their text twice."""
    events = list(message.get("events") or ())
    embeddings = scorer.embed_texts([str(event["text"]) for event in events])
    discovered = discover_campaign_groups(
        message,
        embeddings,
        similarity_threshold=similarity_threshold,
        minimum_group_size=minimum_group_size,
    )
    groups = [group for group, _ in discovered]
    risks = scorer.predict_with_review_embeddings(
        groups, [embeddings[list(indices)] for _, indices in discovered]
    )
    return [
        {
            "schema_version": CAMPAIGN_SCORE_SCHEMA,
            "feature_version": scorer.config.feature_version,
            "group_id": group.group_id,
            "review_ids": list(group.review_ids),
            "product_ids": list(group.product_ids),
            "campaign_scope": "cross_product" if len(group.product_ids) > 1 else "single_product",
            "window_start": group.window_start,
            "window_end": group.window_end,
            "campaign_risk": round(float(risk), 8),
            "candidate": bool(risk >= scorer.config.threshold),
            "decision_threshold": scorer.config.threshold,
            "semantic_edge_threshold": similarity_threshold,
            "model_name": "bot-campaign-hybrid-distilbert",
            "source_window_truncated": bool(message.get("truncated", False)),
        }
        for group, risk in zip(groups, risks)
    ]
