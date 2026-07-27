from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .labeled_datasets import build_training_set, prepare_product_reviews
from .synthetic import augment_product_reviews
from .temporal import build_temporal_dataset, generate_temporal_scenarios


BUNDLE_SCHEMA_VERSION = "review-dataset-bundle-v2"
TEMPORAL_BUNDLE_SCHEMA_VERSION = "review-temporal-bundle-v1"


def _build_temporal_components(
    behavioral_inputs: list[str | Path],
    root: Path,
    product_catalog: str | Path | None,
    campaign_scenario_count: int,
    seed: int,
) -> tuple[dict, dict]:
    """Build observed temporal features and controlled campaign splits once."""
    behavior_dir = root / "behavior"
    campaign_dir = root / "campaign"
    campaign_dir.mkdir(parents=True, exist_ok=True)

    behavior_manifest = build_temporal_dataset(
        behavioral_inputs,
        behavior_dir,
        product_catalog=product_catalog,
    )
    for stale_name in ("scenarios.jsonl", "scenarios.jsonl.manifest.json"):
        (campaign_dir / stale_name).unlink(missing_ok=True)
    campaign_manifest = generate_temporal_scenarios(
        behavior_dir / "profile.json",
        behavior_dir / "products.jsonl",
        campaign_dir,
        count=campaign_scenario_count,
        seed=seed,
    )
    return behavior_manifest, campaign_manifest


def build_temporal_bundle(
    behavioral_inputs: list[str | Path],
    output_dir: str | Path,
    product_catalog: str | Path | None = None,
    campaign_scenario_count: int = 2_000,
    seed: int = 42,
) -> dict:
    """Build only observed temporal features and controlled campaign data."""
    root = Path(output_dir)
    behavior_manifest, campaign_manifest = _build_temporal_components(
        behavioral_inputs,
        root,
        product_catalog,
        campaign_scenario_count,
        seed,
    )
    components = {
        "observed_behavior": behavior_manifest,
        "campaign_scenarios": campaign_manifest,
    }
    digest = hashlib.sha256(json.dumps(components, sort_keys=True).encode("utf-8")).hexdigest()
    manifest = {
        "bundle_schema_version": TEMPORAL_BUNDLE_SCHEMA_VERSION,
        "seed": seed,
        "sha256": digest,
        "output_dir": str(root),
        "roles": {
            "behavior/events.jsonl": "Observed reviews with timestamps and past-only features.",
            "behavior/profile.json": "Empirical UTC temporal distributions.",
            "behavior/products.jsonl": "Actual catalog launch or earliest-review proxy.",
            "campaign/train.jsonl": "Controlled timestamp-aware campaign training events.",
            "campaign/validation.jsonl": "Group-isolated campaign threshold selection.",
            "campaign/test.jsonl": "Group-isolated final controlled campaign evaluation.",
        },
        "components": components,
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def build_dataset_bundle(
    labeled_inputs: list[str | Path],
    behavioral_inputs: list[str | Path],
    output_dir: str | Path,
    product_catalog: str | Path | None = None,
    augmentation_count: int = 7_000,
    campaign_scenario_count: int = 2_000,
    max_synthetic_fraction: float = 0.25,
    seed: int = 42,
) -> dict:
    """Build role-separated text, behavior, and campaign datasets."""
    root = Path(output_dir)
    text_dir = root / "text"
    real_dir = text_dir / "real"

    text_manifest = prepare_product_reviews(labeled_inputs, real_dir, seed=seed)
    augmentation_path = text_dir / "augmented_train.jsonl"
    augmentation_manifest = augment_product_reviews(
        real_dir / "train.jsonl",
        augmentation_path,
        count=augmentation_count,
        seed=seed,
    )
    training_path = text_dir / "training.jsonl"
    training_manifest = build_training_set(
        [real_dir / "train.jsonl"],
        augmentation_path,
        training_path,
        max_synthetic_fraction=max_synthetic_fraction,
        seed=seed,
    )

    behavior_manifest, campaign_manifest = _build_temporal_components(
        behavioral_inputs,
        root,
        product_catalog,
        campaign_scenario_count,
        seed,
    )

    components = {
        "text_real": text_manifest,
        "text_augmentation": augmentation_manifest,
        "text_training": training_manifest,
        "observed_behavior": behavior_manifest,
        "campaign_scenarios": campaign_manifest,
    }
    digest = hashlib.sha256(json.dumps(components, sort_keys=True).encode("utf-8")).hexdigest()
    manifest = {
        "bundle_schema_version": BUNDLE_SCHEMA_VERSION,
        "seed": seed,
        "sha256": digest,
        "output_dir": str(root),
        "roles": {
            "text/real": "Observed text labels; no fabricated behavior.",
            "text/augmented_train.jsonl": "Train-only text variants.",
            "text/training.jsonl": "Capped text-model training mixture.",
            "behavior/events.jsonl": "Observed Amazon behavior plus derived past-only features.",
            "behavior/profile.json": "Empirical UTC temporal distributions.",
            "behavior/products.jsonl": "Actual catalog launch or earliest-review proxy.",
            "campaign/train.jsonl": "Timestamp-aware controlled campaign training events.",
            "campaign/validation.jsonl": "Group-isolated campaign threshold selection.",
            "campaign/test.jsonl": "Group-isolated final controlled campaign evaluation.",
        },
        "components": components,
        "excluded_sources": {
            "amazon_2018": "Redundant when Amazon 2023 is available.",
            "yelp": "Business-review domain and filter-proxy labels are not required for ecommerce baseline.",
            "third_party_synthetic_100k": "Existing learned generator provides auditable scenarios.",
            "direct_evidence_amazon": "No verified public downloadable dataset was available.",
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
