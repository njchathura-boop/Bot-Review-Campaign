from bot_campaign.data import load_reviews
from bot_campaign.model import ReviewScorer, train


def test_training_and_prediction_roundtrip():
    reviews, _ = load_reviews("data/sample/labeled_reviews.jsonl", labeled=True)
    bundle, metrics = train(reviews, test_size=0.3)
    prediction = ReviewScorer(bundle).predict(reviews[0])
    assert 0 <= prediction.fake_probability <= 1
    assert metrics["test_records"] > 0

