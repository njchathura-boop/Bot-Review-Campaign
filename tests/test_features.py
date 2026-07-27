import numpy as np

from bot_campaign.data import load_review_events
from bot_campaign.features import (
    DEFAULT_PRIOR_MEAN_RATING,
    NO_PRIOR_REVIEW_HOURS,
    behavioral_features,
    text_metadata,
)


def test_behavioral_features_use_only_prior_rows():
    reviews, _ = load_review_events("data/sample/campaign_reviews.jsonl")
    frame = behavioral_features(reviews)
    first = frame.groupby("user_id").head(1)
    assert (first.user_prior_reviews == 0).all()
    assert (first.user_prior_mean_rating == DEFAULT_PRIOR_MEAN_RATING).all()
    assert (first.hours_since_user_review == NO_PRIOR_REVIEW_HOURS).all()
    assert (
        not frame[
            ["user_prior_mean_rating", "product_prior_mean_rating", "hours_since_user_review"]
        ]
        .isna()
        .any()
        .any()
    )


def test_text_metadata_handles_missing_values():
    features = text_metadata([None, np.nan, "Great battery life"])
    assert features.shape == (3, 5)
    assert np.isfinite(features).all()
