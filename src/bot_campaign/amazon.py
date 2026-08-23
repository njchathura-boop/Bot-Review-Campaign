from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError
from urllib.request import Request, urlopen


DATASET_ID = "McAuley-Lab/Amazon-Reviews-2023"
DATASET_REVISION = "2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e"
ALLOWED_SPLITS = {"full"}
DOWNLOAD_TIMEOUT_SECONDS = 120


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
        "source_revision": DATASET_REVISION,
        "metadata_provenance": {
            "source_revision": DATASET_REVISION,
            "timestamp": "observed_amazon_review_timestamp",
            "product_id": "observed_amazon_parent_asin_or_asin",
        },
        "schema_version": 1,
    }


def stream_amazon(category: str = "All_Beauty", limit: int = 10_000) -> Iterable[dict]:
    if not category.replace("_", "").isalnum():
        raise ValueError("Category may contain only letters, numbers, and underscores")
    if not 1 <= limit <= 1_000_000:
        raise ValueError("Limit must be between 1 and 1,000,000")
    source_url = (
        f"https://huggingface.co/datasets/{DATASET_ID}/resolve/{DATASET_REVISION}/"
        f"raw/review_categories/{category}.jsonl"
    )
    headers = {"User-Agent": "bot-campaign-detector/1.1"}
    if token := os.environ.get("HF_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    request = Request(source_url, headers=headers)

    # Read JSONL directly from the HTTP response. Unlike a dataset builder, this does
    # not materialize the complete remote category in a local cache before honoring
    # the requested limit.
    emitted = 0
    try:
        with urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            for raw_line in response:
                if emitted >= limit:
                    break
                if not raw_line.strip():
                    continue
                yield canonical_amazon_review(json.loads(raw_line), category)
                emitted += 1
    except HTTPError as exc:
        if exc.code == 404:
            raise FileNotFoundError(
                f"Amazon category {category!r} is unavailable at revision {DATASET_REVISION}"
            ) from exc
        raise


def download_sample(output: str | Path, category: str = "All_Beauty", limit: int = 10_000) -> int:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8") as handle:
        for review in stream_amazon(category, limit):
            handle.write(json.dumps(review, ensure_ascii=False) + "\n")
            count += 1
    return count
