from bot_campaign.data import load_review_events, load_text_labels
from bot_campaign.model import ReviewScorer, train


def test_training_and_prediction_roundtrip():
    reviews, _ = load_text_labels("data/sample/labeled_reviews.jsonl")
    bundle, metrics = train(reviews, test_size=0.3)
    events, _ = load_review_events("data/sample/campaign_reviews.jsonl")
    prediction = ReviewScorer(bundle).predict(events[0])
    assert 0 <= prediction.fake_probability <= 1
    assert metrics["test_records"] > 0
