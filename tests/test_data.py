from bot_campaign.data import load_reviews


def test_sample_labeled_data_is_valid():
    reviews, report = load_reviews("data/sample/labeled_reviews.jsonl", labeled=True)
    assert len(reviews) == 20
    assert report.rejected == 0
    assert {review.label for review in reviews} == {0, 1}

