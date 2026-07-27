from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .data import load_text_labels
from .schemas import Review, ReviewPrediction, TextLabeledReview

try:
    from mlflow.pyfunc import PythonModel as _MLflowPythonModel
except ImportError:  # MLflow is optional for API-only installations.
    class _MLflowPythonModel:  # type: ignore[no-redef]
        pass


REVIEW_TRANSFORMER_VERSION = "review-risk-distilbert-v1"


@dataclass(frozen=True)
class ReviewTransformerConfig:
    encoder_name: str = "distilbert-base-uncased"
    max_tokens: int = 192
    dropout: float = 0.2
    threshold: float = 0.5
    temperature: float = 1.0


def require_review_stack():
    try:
        import torch
        from transformers import AutoConfig, AutoModelForSequenceClassification, AutoTokenizer
    except ImportError as exc:
        raise RuntimeError('Install the NLP stack with: pip install -e ".[nlp]"') from exc
    return torch, AutoConfig, AutoModelForSequenceClassification, AutoTokenizer


def create_review_model(config: ReviewTransformerConfig, *, pretrained: bool = True):
    _, AutoConfig, AutoModelForSequenceClassification, _ = require_review_stack()
    model_config = AutoConfig.from_pretrained(config.encoder_name)
    model_config.num_labels = 2
    # DistilBERT uses all three fields in different parts of its classification stack.
    model_config.dropout = config.dropout
    model_config.attention_dropout = config.dropout
    model_config.seq_classif_dropout = config.dropout
    if pretrained:
        return AutoModelForSequenceClassification.from_pretrained(
            config.encoder_name,
            config=model_config,
        )
    return AutoModelForSequenceClassification.from_config(model_config)


def create_review_tokenizer(config: ReviewTransformerConfig):
    _, _, _, AutoTokenizer = require_review_stack()
    return AutoTokenizer.from_pretrained(config.encoder_name)


def load_review_split(path: str | Path, max_records: int | None = None) -> list[TextLabeledReview]:
    records, report = load_text_labels(path)
    if report.rejected:
        raise ValueError(f"{path} contains {report.rejected} invalid labeled reviews")
    ranked = sorted(
        records,
        key=lambda row: hashlib.sha256(row.review_id.encode("utf-8")).digest(),
    )
    if max_records is None or max_records >= len(ranked):
        return ranked
    if max_records < 2:
        raise ValueError("A bounded review split must keep at least two records")
    selected = ranked[:max_records]
    available_labels = {record.label for record in ranked}
    selected_labels = {record.label for record in selected}
    for missing_label in available_labels - selected_labels:
        replacement = next(record for record in ranked if record.label == missing_label)
        selected[-1] = replacement
    return selected


def assert_text_splits_are_isolated(splits: dict[str, Sequence[TextLabeledReview]]) -> None:
    def fingerprint(text: str) -> str:
        normalized = " ".join(text.casefold().split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    hashes = {
        name: {fingerprint(record.text) for record in records}
        for name, records in splits.items()
    }
    names = list(hashes)
    for left_index, left in enumerate(names):
        for right in names[left_index + 1 :]:
            overlap = hashes[left] & hashes[right]
            if overlap:
                raise ValueError(
                    f"Exact normalized text leakage between {left} and {right}: "
                    f"{len(overlap)} duplicated texts"
                )


def save_review_bundle(
    output_dir: str | Path,
    model,
    tokenizer,
    config: ReviewTransformerConfig,
    lineage: dict,
) -> Path:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(output / "model")
    tokenizer.save_pretrained(output / "tokenizer")
    (output / "bundle.json").write_text(
        json.dumps(
            {
                "version": REVIEW_TRANSFORMER_VERSION,
                "model_config": asdict(config),
                "lineage": lineage,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return output


class ReviewDistilBertScorer:
    def __init__(self, model, tokenizer, config: ReviewTransformerConfig, metadata: dict):
        self.model = model
        self.tokenizer = tokenizer
        self.config = config
        self.bundle = {
            "version": metadata.get("version", REVIEW_TRANSFORMER_VERSION),
            "threshold": config.threshold,
            "temperature": config.temperature,
            "lineage": metadata.get("lineage", {}),
        }

    @classmethod
    def load(cls, bundle_dir: str | Path, device: str | None = None):
        torch, _, AutoModelForSequenceClassification, AutoTokenizer = require_review_stack()
        bundle = Path(bundle_dir)
        metadata = json.loads((bundle / "bundle.json").read_text(encoding="utf-8"))
        config = ReviewTransformerConfig(**metadata["model_config"])
        model = AutoModelForSequenceClassification.from_pretrained(bundle / "model")
        tokenizer = AutoTokenizer.from_pretrained(bundle / "tokenizer")
        target = device or ("cuda" if torch.cuda.is_available() else "cpu")
        model.to(target).eval()
        return cls(model, tokenizer, config, metadata)

    def raw_probabilities(self, texts: Sequence[str], batch_size: int = 64) -> np.ndarray:
        torch, _, _, _ = require_review_stack()
        device = next(self.model.parameters()).device
        results = []
        for start in range(0, len(texts), batch_size):
            encoded = self.tokenizer(
                list(texts[start : start + batch_size]),
                padding=True,
                truncation=True,
                max_length=self.config.max_tokens,
                return_tensors="pt",
            )
            with torch.inference_mode():
                logits = self.model(
                    input_ids=encoded["input_ids"].to(device),
                    attention_mask=encoded["attention_mask"].to(device),
                ).logits
            results.append(torch.softmax(logits, dim=1)[:, 1].cpu().numpy())
        return np.concatenate(results) if results else np.asarray([], dtype=np.float32)

    def calibrate(self, probabilities: np.ndarray) -> np.ndarray:
        clipped = np.clip(np.asarray(probabilities, dtype=np.float64), 1e-7, 1 - 1e-7)
        logits = np.log(clipped / (1 - clipped)) / max(self.config.temperature, 1e-6)
        return 1 / (1 + np.exp(-logits))

    def probabilities(self, texts: Sequence[str], batch_size: int = 64) -> np.ndarray:
        return self.calibrate(self.raw_probabilities(texts, batch_size))

    def predict(self, review: Review) -> ReviewPrediction:
        probability = float(self.probabilities([review.text])[0])
        threshold = self.config.threshold
        evidence = ["Validation-calibrated DistilBERT score from ecommerce review labels"]
        if not review.verified_purchase:
            evidence.append("Purchase is not verified; this is context, not proof")
        return ReviewPrediction(
            review_id=review.review_id,
            fake_probability=round(probability, 4),
            label="needs review" if probability >= threshold else "normal",
            needs_review=probability >= threshold,
            threshold=threshold,
            model_version=self.bundle["version"],
            evidence=evidence,
        )


class ReviewPyFuncModel(_MLflowPythonModel):
    """MLflow serving wrapper for the calibrated review-risk bundle."""

    def load_context(self, context) -> None:
        self.scorer = ReviewDistilBertScorer.load(context.artifacts["bundle"])

    def predict(
        self, context, model_input: pd.DataFrame, params: dict | None = None
    ) -> pd.DataFrame:
        if "text" not in model_input.columns:
            raise ValueError("Review model input requires a 'text' column")
        texts = model_input["text"].fillna("").astype(str).tolist()
        probabilities = self.scorer.probabilities(texts)
        needs_review = probabilities >= self.scorer.config.threshold
        return pd.DataFrame(
            {
                "fake_probability": probabilities,
                "label": np.where(needs_review, "needs review", "normal"),
                "needs_review": needs_review,
            }
        )
