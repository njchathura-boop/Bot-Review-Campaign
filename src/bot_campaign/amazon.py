from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable


DATASET_ID = "McAuley-Lab/Amazon-Reviews-2023"
DATASET_REVISION = "269765acc4057f11566d9e16106d109409e4c7c8"
ALLOWED_SPLITS = {"full"}


def canonical_amazon_review(row: dict, category: str) -> dict:
    """Map the published Amazon Reviews 2023 schema to the service contract."""
    stable = "|".join(
        [str(row.get("user_id", "")), str(row.get("parent_asin") or row.get("asin", "")), str(row.get("timestamp", ""))]
    )
    return {
        "review_id": f"amazon-{hashlib.sha256(stable.encode()).hexdigest()[:24]}",
        "user_id": str(row.get("user_id", "")),
        "product_id": str(row.get("parent_asin") or row.get("asin", "")),
        "asin": str(row.get("asin", "")),
        "title": str(row.get("title") or ""),
        "text": str(row.get("text") or ""),
        "rating": float(row.get("rating", 0)),
        "timestamp": int(row.get("timestamp", 0)),
        "verified_purchase": bool(row.get("verified_purchase", False)),
        "helpful_votes": int(row.get("helpful_vote", 0)),
        "category": category,
        "source": DATASET_ID,
        "schema_version": 1,
    }


def stream_amazon(category: str = "All_Beauty", limit: int = 10_000) -> Iterable[dict]:
    if not category.replace("_", "").isalnum():
        raise ValueError("Category may contain only letters, numbers, and underscores")
    if not 1 <= limit <= 1_000_000:
        raise ValueError("Limit must be between 1 and 1,000,000")
    try:
        from datasets import load_dataset
    except ImportError as exc:
        raise RuntimeError('Install streaming dependencies with: pip install -e ".[streaming]"') from exc
    parquet_uri = (
        f"https://huggingface.co/datasets/{DATASET_ID}/resolve/{DATASET_REVISION}/"
        f"raw_review_{category}/full-00000-of-00001.parquet"
    )
    dataset = load_dataset("parquet", data_files={"full": parquet_uri}, split="full", streaming=True)
    for index, row in enumerate(dataset):
        if index >= limit:
            break
        yield canonical_amazon_review(row, category)


def download_sample(output: str | Path, category: str = "All_Beauty", limit: int = 10_000) -> int:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8") as handle:
        for review in stream_amazon(category, limit):
            handle.write(json.dumps(review, ensure_ascii=False) + "\n")
            count += 1
    return count
