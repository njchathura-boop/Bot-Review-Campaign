from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from pydantic import ValidationError

from .schemas import LabeledReview, Review


@dataclass
class ValidationReport:
    accepted: int = 0
    rejected: int = 0
    duplicate_ids: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)


ALIASES = {
    "review_id": ("review_id", "id"),
    "user_id": ("user_id", "user", "reviewerID"),
    "product_id": ("product_id", "parent_asin", "asin", "item_id"),
    "text": ("text", "review_text", "reviewText", "content"),
    "rating": ("rating", "overall", "stars"),
    "timestamp": ("timestamp", "time", "unixReviewTime", "date"),
    "verified_purchase": ("verified_purchase", "verified", "verifiedPurchase"),
    "helpful_votes": ("helpful_votes", "helpful_vote", "helpful"),
    "label": ("label", "fake", "is_fake"),
    "source": ("source", "dataset"),
    "group_id": ("group_id", "hotel_id", "author_group"),
}


def _pick(row: dict, canonical: str, default=None):
    for name in ALIASES[canonical]:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return default


def canonicalize(row: dict, labeled: bool = False) -> dict:
    timestamp = _pick(row, "timestamp")
    if isinstance(timestamp, (int, float)) or (isinstance(timestamp, str) and timestamp.isdigit()):
        timestamp = int(timestamp)
        timestamp = timestamp / 1000 if timestamp > 10_000_000_000 else timestamp
        from datetime import datetime, timezone

        timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    result = {
        "review_id": str(_pick(row, "review_id", "")),
        "user_id": str(_pick(row, "user_id", "")),
        "product_id": str(_pick(row, "product_id", "")),
        "text": _pick(row, "text", ""),
        "rating": _pick(row, "rating"),
        "timestamp": timestamp,
        "verified_purchase": _pick(row, "verified_purchase", False),
        "helpful_votes": _pick(row, "helpful_votes", 0),
    }
    if labeled:
        result.update(
            label=_pick(row, "label"),
            source=str(_pick(row, "source", "unknown")),
            group_id=_pick(row, "group_id"),
        )
    return result


def read_records(path: str | Path) -> Iterable[dict]:
    path = Path(path)
    if path.suffix.lower() in {".jsonl", ".json"}:
        with path.open(encoding="utf-8") as handle:
            if path.suffix.lower() == ".json":
                data = json.load(handle)
                yield from (data if isinstance(data, list) else data["reviews"])
            else:
                for line in handle:
                    if line.strip():
                        yield json.loads(line)
    elif path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            yield from csv.DictReader(handle)
    else:
        raise ValueError(f"Unsupported data format: {path.suffix}; use CSV, JSON, or JSONL")


def load_reviews(path: str | Path, labeled: bool = False):
    model = LabeledReview if labeled else Review
    report = ValidationReport()
    reviews, seen = [], set()
    for index, row in enumerate(read_records(path), start=1):
        try:
            review = model.model_validate(canonicalize(row, labeled=labeled))
            if review.review_id in seen:
                report.duplicate_ids += 1
                report.rejected += 1
                continue
            seen.add(review.review_id)
            reviews.append(review)
            report.accepted += 1
        except (ValidationError, ValueError, TypeError) as exc:
            report.rejected += 1
            report.errors.append({"row": str(index), "error": str(exc)[:500]})
    return reviews, report

