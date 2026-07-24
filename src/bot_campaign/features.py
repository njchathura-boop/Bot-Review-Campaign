from __future__ import annotations

import numpy as np
import pandas as pd

from .schemas import Review


DEFAULT_PRIOR_MEAN_RATING = 3.0
NO_PRIOR_REVIEW_HOURS = -1.0


def text_metadata(texts) -> np.ndarray:
    rows = []
    for raw in texts:
        text = "" if pd.isna(raw) else str(raw)
        words = text.split()
        unique_ratio = len({word.lower() for word in words}) / max(len(words), 1)
        rows.append(
            [
                np.log1p(len(text)),
                np.log1p(len(words)),
                unique_ratio,
                text.count("!") / max(len(text), 1),
                sum(char.isupper() for char in text) / max(len(text), 1),
            ]
        )
    return np.asarray(rows, dtype=float)


def behavioral_features(reviews: list[Review]) -> pd.DataFrame:
    """Compute past-only aggregates in timestamp order to prevent future leakage."""
    frame = pd.DataFrame([review.model_dump() for review in reviews])
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values(["timestamp", "review_id"]).reset_index(drop=True)
    frame["user_prior_reviews"] = frame.groupby("user_id").cumcount()
    frame["product_prior_reviews"] = frame.groupby("product_id").cumcount()
    frame["user_prior_mean_rating"] = (
        frame.groupby("user_id")["rating"].transform(lambda x: x.shift().expanding().mean())
    )
    frame["product_prior_mean_rating"] = (
        frame.groupby("product_id")["rating"].transform(lambda x: x.shift().expanding().mean())
    )
    previous = frame.groupby("user_id")["timestamp"].shift()
    frame["hours_since_user_review"] = (frame["timestamp"] - previous).dt.total_seconds() / 3600
    frame["review_hour"] = frame["timestamp"].dt.hour
    frame["review_weekday"] = frame["timestamp"].dt.weekday
    frame = frame.fillna(
        {
            "user_prior_mean_rating": DEFAULT_PRIOR_MEAN_RATING,
            "product_prior_mean_rating": DEFAULT_PRIOR_MEAN_RATING,
            "hours_since_user_review": NO_PRIOR_REVIEW_HOURS,
        }
    )
    return frame
