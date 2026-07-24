from bot_campaign.campaign import detect_campaigns
from bot_campaign.data import load_reviews


def test_detects_seeded_coordination():
    reviews, report = load_reviews("data/sample/campaign_reviews.jsonl")
    alerts = detect_campaigns(reviews)
    assert report.rejected == 0
    assert len(alerts) == 1
    assert len(alerts[0].review_ids) == 3
    assert alerts[0].product_id == "p-headphones"

