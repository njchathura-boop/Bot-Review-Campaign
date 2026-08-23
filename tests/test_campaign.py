from bot_campaign.campaign import detect_campaigns
from bot_campaign.data import load_review_events
from bot_campaign.schemas import Review

from datetime import datetime, timedelta, timezone


def test_detects_seeded_coordination():
    reviews, report = load_review_events("data/sample/campaign_reviews.jsonl")
    alerts = detect_campaigns(reviews)
    assert report.rejected == 0
    assert len(alerts) == 1
    assert len(alerts[0].review_ids) == 3
    assert alerts[0].product_id == "p-headphones"


def test_detects_similar_reviews_across_products():
    now = datetime.now(timezone.utc)
    reviews = [
        Review(
            review_id=f"cross-{index}",
            user_id=f"user-{index}",
            product_id=f"product-{index}",
            category="electronics",
            text="Outstanding performance and premium quality, highly recommended",
            rating=5,
            timestamp=now + timedelta(minutes=index * 2),
        )
        for index in range(3)
    ]
    alerts = detect_campaigns(reviews)
    assert len(alerts) == 1
    assert alerts[0].product_ids == ["product-0", "product-1", "product-2"]
    assert alerts[0].evidence["product_count"] == 3
