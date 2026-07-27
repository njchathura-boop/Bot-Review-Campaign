from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

from .campaign_features import CAMPAIGN_FEATURE_VERSION, CampaignGroup, NUMERIC_FEATURE_NAMES

try:
    from mlflow.pyfunc import PythonModel as _MLflowPythonModel
except ImportError:  # MLflow is optional for API-only installations.
    class _MLflowPythonModel:  # type: ignore[no-redef]
        pass


@dataclass(frozen=True)
class HybridModelConfig:
    encoder_name: str = "distilbert-base-uncased"
    max_reviews: int = 8
    max_tokens: int = 128
    numeric_hidden_size: int = 64
    fusion_hidden_size: int = 128
    dropout: float = 0.2
    threshold: float = 0.5
    feature_version: str = CAMPAIGN_FEATURE_VERSION


@dataclass(frozen=True)
class NumericNormalizer:
    mean: tuple[float, ...]
    scale: tuple[float, ...]

    @classmethod
    def fit(cls, groups: Sequence[CampaignGroup]) -> "NumericNormalizer":
        if not groups:
            raise ValueError("At least one training group is required")
        values = np.asarray([group.numeric_features for group in groups], dtype=np.float32)
        mean = values.mean(axis=0)
        scale = values.std(axis=0)
        scale[scale < 1e-6] = 1.0
        return cls(tuple(float(value) for value in mean), tuple(float(value) for value in scale))

    def transform(self, values: Sequence[Sequence[float]]) -> np.ndarray:
        matrix = np.asarray(values, dtype=np.float32)
        return (matrix - np.asarray(self.mean, dtype=np.float32)) / np.asarray(
            self.scale, dtype=np.float32
        )


def require_nlp_stack():
    try:
        import torch
        from transformers import AutoConfig, AutoModel, AutoTokenizer
    except ImportError as exc:
        raise RuntimeError('Install the NLP stack with: pip install -e ".[nlp]"') from exc
    return torch, AutoConfig, AutoModel, AutoTokenizer


def create_model(config: HybridModelConfig, *, pretrained: bool = True):
    torch, AutoConfig, AutoModel, _ = require_nlp_stack()
    nn = torch.nn

    class HybridDistilBert(nn.Module):
        """DistilBERT review embeddings fused with temporal/behavioral group features."""

        def __init__(self) -> None:
            super().__init__()
            self.encoder = (
                AutoModel.from_pretrained(config.encoder_name)
                if pretrained
                else AutoModel.from_config(AutoConfig.from_pretrained(config.encoder_name))
            )
            text_size = int(self.encoder.config.hidden_size)
            self.numeric_branch = nn.Sequential(
                nn.Linear(len(NUMERIC_FEATURE_NAMES), config.numeric_hidden_size),
                nn.LayerNorm(config.numeric_hidden_size),
                nn.GELU(),
                nn.Dropout(config.dropout),
            )
            self.classifier = nn.Sequential(
                nn.Linear(
                    text_size + config.numeric_hidden_size, config.fusion_hidden_size
                ),
                nn.GELU(),
                nn.Dropout(config.dropout),
                nn.Linear(config.fusion_hidden_size, 1),
            )

        def forward(self, input_ids, attention_mask, numeric_features):
            batch_size, review_count, token_count = input_ids.shape
            flat_ids = input_ids.reshape(batch_size * review_count, token_count)
            flat_mask = attention_mask.reshape(batch_size * review_count, token_count)
            review_embedding = self.encode_reviews(flat_ids, flat_mask)
            review_embedding = review_embedding.reshape(batch_size, review_count, -1)
            review_mask = attention_mask.any(dim=-1).unsqueeze(-1).float()
            group_embedding = (review_embedding * review_mask).sum(dim=1) / review_mask.sum(
                dim=1
            ).clamp_min(1)
            return self.classify_embeddings(group_embedding, numeric_features)

        def encode_reviews(self, input_ids, attention_mask):
            hidden = self.encoder(
                input_ids=input_ids, attention_mask=attention_mask
            ).last_hidden_state
            token_mask = attention_mask.unsqueeze(-1).float()
            return (hidden * token_mask).sum(dim=1) / token_mask.sum(dim=1).clamp_min(1)

        def classify_embeddings(self, group_embedding, numeric_features):
            numeric_embedding = self.numeric_branch(numeric_features)
            return self.classifier(torch.cat((group_embedding, numeric_embedding), dim=1)).squeeze(1)

    return HybridDistilBert()


def create_tokenizer(config: HybridModelConfig):
    _, _, _, AutoTokenizer = require_nlp_stack()
    return AutoTokenizer.from_pretrained(config.encoder_name)


def encode_groups(tokenizer, groups: Sequence[CampaignGroup], config: HybridModelConfig):
    torch, _, _, _ = require_nlp_stack()
    text_rows: list[list[str]] = []
    for group in groups:
        texts = list(group.texts[: config.max_reviews])
        texts.extend([""] * (config.max_reviews - len(texts)))
        text_rows.append(texts)
    encoded = tokenizer(
        [text for row in text_rows for text in row],
        padding="max_length",
        truncation=True,
        max_length=config.max_tokens,
        return_tensors="pt",
    )
    batch = len(groups)
    return (
        encoded["input_ids"].reshape(batch, config.max_reviews, config.max_tokens),
        encoded["attention_mask"].reshape(batch, config.max_reviews, config.max_tokens),
        torch.tensor([int(group.label or 0) for group in groups], dtype=torch.float32),
    )


def save_hybrid_bundle(
    output_dir: str | Path,
    model,
    tokenizer,
    config: HybridModelConfig,
    normalizer: NumericNormalizer,
    lineage: dict,
) -> Path:
    torch, _, _, _ = require_nlp_stack()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    encoder_dir = output / "encoder"
    tokenizer_dir = output / "tokenizer"
    model.encoder.config.save_pretrained(encoder_dir)
    tokenizer.save_pretrained(tokenizer_dir)
    torch.save(model.state_dict(), output / "model_state.pt")
    metadata = {
        "model_config": asdict(config),
        "numeric_feature_names": list(NUMERIC_FEATURE_NAMES),
        "normalizer": asdict(normalizer),
        "lineage": lineage,
    }
    (output / "bundle.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return output


class HybridCampaignScorer:
    def __init__(self, model, tokenizer, config: HybridModelConfig, normalizer: NumericNormalizer):
        self.model = model
        self.tokenizer = tokenizer
        self.config = config
        self.normalizer = normalizer

    @classmethod
    def load(cls, bundle_dir: str | Path, device: str | None = None):
        torch, _, _, AutoTokenizer = require_nlp_stack()
        bundle = Path(bundle_dir)
        metadata = json.loads((bundle / "bundle.json").read_text(encoding="utf-8"))
        if tuple(metadata["numeric_feature_names"]) != NUMERIC_FEATURE_NAMES:
            raise ValueError("Model numeric feature contract does not match this application")
        config_data = metadata["model_config"]
        config_data["encoder_name"] = str(bundle / "encoder")
        config = HybridModelConfig(**config_data)
        normalizer = NumericNormalizer(
            tuple(metadata["normalizer"]["mean"]), tuple(metadata["normalizer"]["scale"])
        )
        model = create_model(config, pretrained=False)
        model.load_state_dict(torch.load(bundle / "model_state.pt", map_location="cpu"))
        target = device or ("cuda" if torch.cuda.is_available() else "cpu")
        model.to(target).eval()
        tokenizer = AutoTokenizer.from_pretrained(bundle / "tokenizer")
        return cls(model, tokenizer, config, normalizer)

    def predict(self, groups: Sequence[CampaignGroup]) -> np.ndarray:
        torch, _, _, _ = require_nlp_stack()
        if not groups:
            return np.asarray([], dtype=np.float32)
        input_ids, attention_mask, _ = encode_groups(self.tokenizer, groups, self.config)
        numeric = torch.tensor(
            self.normalizer.transform([group.numeric_features for group in groups]),
            dtype=torch.float32,
        )
        device = next(self.model.parameters()).device
        with torch.inference_mode():
            logits = self.model(
                input_ids.to(device), attention_mask.to(device), numeric.to(device)
            )
        return torch.sigmoid(logits).cpu().numpy()

    def embed_texts(self, texts: Sequence[str], batch_size: int = 64) -> np.ndarray:
        torch, _, _, _ = require_nlp_stack()
        device = next(self.model.parameters()).device
        outputs = []
        for start in range(0, len(texts), batch_size):
            encoded = self.tokenizer(
                list(texts[start : start + batch_size]),
                padding=True,
                truncation=True,
                max_length=self.config.max_tokens,
                return_tensors="pt",
            )
            with torch.inference_mode():
                embedding = self.model.encode_reviews(
                    encoded["input_ids"].to(device), encoded["attention_mask"].to(device)
                )
            outputs.append(embedding.cpu().numpy())
        hidden_size = int(self.model.encoder.config.hidden_size)
        return np.concatenate(outputs) if outputs else np.empty((0, hidden_size), dtype=np.float32)

    def predict_with_review_embeddings(
        self,
        groups: Sequence[CampaignGroup],
        review_embeddings: Sequence[np.ndarray],
    ) -> np.ndarray:
        torch, _, _, _ = require_nlp_stack()
        if len(groups) != len(review_embeddings):
            raise ValueError("Each campaign group requires one review-embedding matrix")
        if not groups:
            return np.asarray([], dtype=np.float32)
        group_embeddings = np.stack(
            [np.asarray(embeddings, dtype=np.float32).mean(axis=0) for embeddings in review_embeddings]
        )
        numeric = self.normalizer.transform([group.numeric_features for group in groups])
        device = next(self.model.parameters()).device
        with torch.inference_mode():
            logits = self.model.classify_embeddings(
                torch.tensor(group_embeddings, dtype=torch.float32, device=device),
                torch.tensor(numeric, dtype=torch.float32, device=device),
            )
        return torch.sigmoid(logits).cpu().numpy()


class CampaignPyFuncModel(_MLflowPythonModel):
    """MLflow wrapper that serves the complete tokenizer + hybrid model bundle."""

    def load_context(self, context) -> None:
        self.scorer = HybridCampaignScorer.load(context.artifacts["bundle"])

    def predict(
        self, context, model_input: pd.DataFrame, params: dict | None = None
    ) -> pd.DataFrame:
        from .campaign_features import campaign_group_from_candidate

        groups = []
        for record in model_input.to_dict(orient="records"):
            message = {
                "schema_version": record["schema_version"],
                "feature_version": record["feature_version"],
                "group_id": record["group_id"],
                "texts": json.loads(record["texts"]),
                "numeric_features": json.loads(record["numeric_features"]),
                "review_ids": json.loads(record["review_ids"]),
                "product_ids": json.loads(record["product_ids"]),
                "window_start": record["window_start"],
                "window_end": record["window_end"],
            }
            groups.append(campaign_group_from_candidate(message))
        risks = self.scorer.predict(groups)
        return pd.DataFrame(
            {
                "campaign_risk": risks,
                "candidate": risks >= self.scorer.config.threshold,
            }
        )
