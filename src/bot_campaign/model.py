from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, average_precision_score, precision_recall_fscore_support, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

from .features import text_metadata
from .schemas import LabeledReview, Review, ReviewPrediction


MODEL_VERSION = "tfidf-logreg-v1"


def build_pipeline(max_features: int = 20_000) -> Pipeline:
    features = FeatureUnion(
        [
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=max_features, sublinear_tf=True)),
            ("style", Pipeline([("extract", FunctionTransformer(text_metadata, validate=False)), ("scale", StandardScaler())])),
        ]
    )
    return Pipeline([("features", features), ("classifier", LogisticRegression(max_iter=1_000, class_weight="balanced"))])


def _split(reviews: list[LabeledReview], seed: int, test_size: float):
    groups = np.array([review.group_id or "" for review in reviews])
    labels = np.array([review.label for review in reviews])
    indices = np.arange(len(reviews))
    if all(groups) and len(set(groups)) >= 4:
        splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=seed)
        return next(splitter.split(indices, labels, groups))
    return train_test_split(indices, test_size=test_size, random_state=seed, stratify=labels)


def train(reviews: list[LabeledReview], seed: int = 42, test_size: float = 0.2):
    if len(reviews) < 10 or len({review.label for review in reviews}) < 2:
        raise ValueError("Training requires at least 10 records and both labels")
    train_idx, test_idx = _split(reviews, seed, test_size)
    texts = np.array([review.text for review in reviews])
    labels = np.array([review.label for review in reviews])
    pipeline = build_pipeline()
    pipeline.fit(texts[train_idx], labels[train_idx])
    probabilities = pipeline.predict_proba(texts[test_idx])[:, 1]
    predicted = (probabilities >= 0.5).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(labels[test_idx], predicted, average="binary", zero_division=0)
    metrics = {
        "test_records": int(len(test_idx)),
        "accuracy": float(accuracy_score(labels[test_idx], predicted)),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "roc_auc": float(roc_auc_score(labels[test_idx], probabilities)) if len(set(labels[test_idx])) == 2 else None,
        "pr_auc": float(average_precision_score(labels[test_idx], probabilities)),
    }
    bundle = {"pipeline": pipeline, "threshold": 0.5, "version": MODEL_VERSION, "trained_at": datetime.now(timezone.utc).isoformat()}
    return bundle, metrics


def save_bundle(bundle: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)


@dataclass
class ReviewScorer:
    bundle: dict

    @classmethod
    def load(cls, path: str | Path):
        return cls(joblib.load(path))

    def predict(self, review: Review) -> ReviewPrediction:
        probability = float(self.bundle["pipeline"].predict_proba([review.text])[0, 1])
        threshold = float(self.bundle.get("threshold", 0.5))
        evidence = []
        if len(review.text.split()) < 6:
            evidence.append("Very short review text")
        if review.text.count("!") >= 3:
            evidence.append("Unusually emphatic punctuation")
        if not review.verified_purchase:
            evidence.append("Purchase is not verified; this is context, not proof")
        evidence.append("Text-pattern score from the labeled-review baseline")
        return ReviewPrediction(
            review_id=review.review_id,
            fake_probability=round(probability, 4),
            label="needs review" if probability >= threshold else "low risk",
            needs_review=probability >= threshold,
            threshold=threshold,
            model_version=self.bundle.get("version", "unknown"),
            evidence=evidence,
        )

