from __future__ import annotations

import hashlib
import json
import random
import re
from collections import Counter
from pathlib import Path

from .data import read_records

PRODUCT_REVIEW_SOURCE = "local-product-reviews"
KAGGLE_REVIEW_SOURCE = "kaggle-mexwell-fake-reviews"
DEFAULT_SPLIT_RATIOS = {"train": 0.70, "validation": 0.15, "test": 0.15}

CATEGORY_ALIASES = {
    "clothing_shoes_and_jewelry": "clothing_shoes_jewelry",
    "home_and_kitchen": "home_kitchen",
    "movies_and_tv": "movies_tv",
    "sports_and_outdoors": "sports_outdoors",
    "tools_and_home_improvement": "tools_home_improvement",
    "toys_and_games": "toys_games",
}


def _stable_id(prefix: str, *parts: object) -> str:
    payload = "|".join(map(str, parts)).encode("utf-8")
    return f"{prefix}-{hashlib.sha256(payload).hexdigest()[:24]}"


def _normalized_text(text: object) -> str:
    return " ".join(str(text or "").split()).strip()


def _deceptive_label(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in (0, 1):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "deceptive", "fake", "cg"}:
        return 1
    if normalized in {"false", "0", "no", "genuine", "truthful", "real", "or"}:
        return 0
    raise ValueError(f"Unsupported is_deceptive value: {value!r}")


def _normalize_category(value: object) -> str:
    category = re.sub(r"[^a-z0-9]+", "_", str(value or "").casefold()).strip("_")
    category = re.sub(r"_5$", "", category)
    return CATEGORY_ALIASES.get(category, category)


def canonical_product_review(row: dict, index: int = 0) -> dict:
    """Normalize supervised text without fabricating behavioral event fields."""
    text = _normalized_text(row.get("text") or row.get("text_") or row.get("review_text"))
    if len(text) < 3:
        raise ValueError("Product review text must contain at least three characters")
    raw_label = row.get("is_deceptive", row.get("label", row.get("is_fake_review")))
    label = _deceptive_label(raw_label)
    text_fingerprint = hashlib.sha256(text.casefold().encode("utf-8")).hexdigest()
    raw_category = row.get("category") or row.get("product_category")
    category = _normalize_category(raw_category) if raw_category else None
    raw_rating = row.get("rating", row.get("star_rating"))
    rating = float(raw_rating) if raw_rating not in (None, "") else None
    if rating is not None and not 1 <= rating <= 5:
        raise ValueError(f"Rating must be between 1 and 5, received {rating}")
    source = (
        KAGGLE_REVIEW_SOURCE
        if "label" in row and "is_deceptive" not in row
        else PRODUCT_REVIEW_SOURCE
    )
    field_provenance = {"text": "observed", "label": "observed"}
    if category:
        field_provenance["category"] = "observed"
    if rating is not None:
        field_provenance["rating"] = "observed"
    return {
        "review_id": _stable_id("product-review", text_fingerprint, label),
        "text": text,
        "rating": rating,
        "category": category,
        "label": label,
        "source": source,
        "group_id": f"text-family-{text_fingerprint[:20]}",
        "label_provenance": "observed_source_label",
        "field_provenance": field_provenance,
        "synthetic": False,
    }


def _input_files(inputs: str | Path | list[str | Path]) -> list[Path]:
    requested = [inputs] if isinstance(inputs, (str, Path)) else inputs
    files: list[Path] = []
    for requested_path in requested:
        path = Path(requested_path)
        if path.is_dir():
            files.extend(
                candidate
                for candidate in sorted(path.rglob("*"))
                if candidate.is_file()
                and candidate.suffix.casefold() in {".csv", ".json", ".jsonl"}
            )
        elif path.is_file():
            files.append(path)
        else:
            raise FileNotFoundError(f"Product review corpus not found: {path}")
    if not files:
        raise ValueError("No CSV, JSON, or JSONL product-review files were found")
    return files


def _split_name(group_id: str, seed: int, ratios: dict[str, float]) -> str:
    digest = hashlib.sha256(f"{seed}|{group_id}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") / 2**64
    train_boundary = ratios["train"]
    validation_boundary = train_boundary + ratios["validation"]
    if value < train_boundary:
        return "train"
    if value < validation_boundary:
        return "validation"
    return "test"


def prepare_product_reviews(
    input_path: str | Path | list[str | Path],
    output_dir: str | Path,
    seed: int = 42,
    split_ratios: dict[str, float] | None = None,
) -> dict:
    """Normalize and split the local labeled ecommerce corpus without leakage."""
    output_dir = Path(output_dir)
    input_files = _input_files(input_path)
    ratios = dict(split_ratios or DEFAULT_SPLIT_RATIOS)
    if set(ratios) != {"train", "validation", "test"}:
        raise ValueError("split_ratios must define train, validation, and test")
    if any(value <= 0 for value in ratios.values()) or abs(sum(ratios.values()) - 1) > 1e-9:
        raise ValueError("split ratios must be positive and sum to 1")
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {name: output_dir / f"{name}.jsonl" for name in ratios}
    handles = {name: path.open("w", encoding="utf-8", newline="\n") for name, path in paths.items()}
    split_counts: Counter[str] = Counter()
    label_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    rejected = duplicates = 0
    seen_text: set[str] = set()
    digest = hashlib.sha256()
    try:
        index = 0
        for source_path in input_files:
            for raw_row in read_records(source_path):
                index += 1
                try:
                    row = canonical_product_review(raw_row, index)
                except (TypeError, ValueError):
                    rejected += 1
                    continue
                text_key = _normalized_text(row["text"]).casefold()
                if text_key in seen_text:
                    duplicates += 1
                    continue
                seen_text.add(text_key)
                split = _split_name(row["group_id"], seed, ratios)
                row["split"] = split
                encoded = (json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n").encode(
                    "utf-8"
                )
                handles[split].write(encoded.decode("utf-8"))
                digest.update(encoded)
                split_counts[split] += 1
                label_counts[f"{split}.{'deceptive' if row['label'] else 'genuine'}"] += 1
                source_counts[row["source"]] += 1
    finally:
        for handle in handles.values():
            handle.close()

    manifest = {
        "sources": dict(source_counts),
        "inputs": [str(path) for path in input_files],
        "output_dir": str(output_dir),
        "seed": seed,
        "split_ratios": ratios,
        "records": sum(split_counts.values()),
        "rejected": rejected,
        "duplicates": duplicates,
        "split_counts": dict(split_counts),
        "label_counts": dict(label_counts),
        "sha256": digest.hexdigest(),
        "schema_policy": (
            "Text labels contain no fabricated user, product, timestamp, launch, or "
            "behavioral fields. Observed category/rating values are preserved when present."
        ),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def build_training_set(
    real_inputs: list[str | Path],
    synthetic_input: str | Path,
    output: str | Path,
    max_synthetic_fraction: float = 0.25,
    seed: int = 42,
) -> dict:
    """Build a deterministic train-only mix while capping generated records."""
    if not 0 <= max_synthetic_fraction < 1:
        raise ValueError("max_synthetic_fraction must be between 0 (inclusive) and 1")

    def load_rows(paths: list[str | Path]) -> list[dict]:
        result = []
        for path in paths:
            with Path(path).open(encoding="utf-8") as handle:
                result.extend(json.loads(line) for line in handle if line.strip())
        return result

    real_rows = load_rows(real_inputs)
    if not real_rows:
        raise ValueError("At least one real labeled record is required")
    non_train = [row["review_id"] for row in real_rows if row.get("split", "train") != "train"]
    if non_train:
        raise ValueError("Real validation/test rows cannot be included in the training mixture")
    synthetic_rows = [
        row
        for row in load_rows([synthetic_input])
        if row.get("synthetic") is True and row.get("split") == "train"
    ]

    maximum = int(len(real_rows) * max_synthetic_fraction / (1 - max_synthetic_fraction))
    rng = random.Random(seed)
    rng.shuffle(synthetic_rows)
    selected = synthetic_rows[:maximum]
    combined = [*real_rows, *selected]
    rng.shuffle(combined)

    seen_ids: set[str] = set()
    seen_text: set[str] = set()
    deduplicated = []
    for row in combined:
        review_id = str(row["review_id"])
        text_key = _normalized_text(row["text"]).casefold()
        if review_id in seen_ids or text_key in seen_text:
            continue
        seen_ids.add(review_id)
        seen_text.add(text_key)
        deduplicated.append(row)

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        for row in deduplicated:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    synthetic_count = sum(bool(row.get("synthetic")) for row in deduplicated)
    manifest = {
        "records": len(deduplicated),
        "real_records": len(deduplicated) - synthetic_count,
        "synthetic_records": synthetic_count,
        "synthetic_fraction": round(synthetic_count / len(deduplicated), 6),
        "max_synthetic_fraction": max_synthetic_fraction,
        "seed": seed,
        "output": str(output),
        "evaluation_policy": "Validation and test data must remain real-only and external.",
    }
    output.with_suffix(output.suffix + ".manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest
