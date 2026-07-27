import json
import os
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bot_campaign.config import Settings
from bot_campaign.review_transformer import (
    ReviewDistilBertScorer,
    ReviewPyFuncModel,
    ReviewTransformerConfig,
    assert_text_splits_are_isolated,
    load_review_split,
)
from bot_campaign.runtime import TrustRuntime
from bot_campaign.schemas import TextLabeledReview


def _record(review_id: str, text: str, label: int) -> TextLabeledReview:
    return TextLabeledReview(review_id=review_id, text=text, label=label)


def test_text_leakage_guard_normalizes_case_and_whitespace():
    splits = {
        "train": [_record("train-1", "Excellent battery life", 0)],
        "test": [_record("test-1", " excellent   BATTERY life ", 1)],
    }
    with pytest.raises(ValueError, match="text leakage"):
        assert_text_splits_are_isolated(splits)


def test_temperature_scaling_moves_overconfident_scores_toward_half():
    scorer = ReviewDistilBertScorer(
        model=None,
        tokenizer=None,
        config=ReviewTransformerConfig(temperature=2.0),
        metadata={},
    )
    calibrated = scorer.calibrate(np.asarray([0.01, 0.99]))
    assert calibrated[0] > 0.01
    assert calibrated[1] < 0.99


def test_review_pyfunc_returns_calibrated_risk_and_decision():
    wrapper = ReviewPyFuncModel()
    wrapper.scorer = type(
        "FakeScorer",
        (),
        {
            "config": ReviewTransformerConfig(threshold=0.7),
            "probabilities": lambda self, texts: np.asarray([0.2, 0.8]),
        },
    )()

    result = wrapper.predict(
        None,
        pd.DataFrame({"text": ["Works as expected", "Limited deal, buy now"]}),
    )

    assert result.to_dict(orient="records") == [
        {"fake_probability": 0.2, "label": "normal", "needs_review": False},
        {"fake_probability": 0.8, "label": "needs review", "needs_review": True},
    ]


def test_review_pyfunc_rejects_missing_text_column():
    with pytest.raises(ValueError, match="text"):
        ReviewPyFuncModel().predict(None, pd.DataFrame({"rating": [5]}))


def test_bounded_loader_keeps_both_labels():
    path = Path(f"data/processed/_test_review_split_{os.getpid()}.jsonl")
    rows = [
        {"review_id": f"genuine-{index}", "text": f"genuine review number {index}", "label": 0}
        for index in range(20)
    ]
    rows.append({"review_id": "deceptive-only", "text": "limited deal buy today", "label": 1})
    try:
        path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )
        selected = load_review_split(path, max_records=4)
        assert {record.label for record in selected} == {0, 1}
    finally:
        path.unlink(missing_ok=True)


def test_runtime_prefers_promoted_review_transformer(monkeypatch):
    bundle = Path(f"artifacts/pytest-review-transformer-{os.getpid()}")
    model_dir = bundle / "model"
    tokenizer_dir = bundle / "tokenizer"
    try:
        model_dir.mkdir(parents=True)
        tokenizer_dir.mkdir()
        (bundle / "bundle.json").write_text("{}", encoding="utf-8")
        fake_scorer = type(
            "FakeScorer", (), {"bundle": {"version": "review-risk-distilbert-v1"}}
        )()
        monkeypatch.setattr(
            "bot_campaign.runtime.ReviewDistilBertScorer.load",
            lambda path: fake_scorer,
        )
        settings = replace(
            Settings.from_env(),
            review_transformer_path=bundle,
            model_path=bundle / "missing-baseline.joblib",
        )
        runtime = TrustRuntime(settings)
        assert runtime.scorer() is fake_scorer
        assert runtime.model_version == "review-risk-distilbert-v1"
    finally:
        (bundle / "bundle.json").unlink(missing_ok=True)
        tokenizer_dir.rmdir()
        model_dir.rmdir()
        bundle.rmdir()
