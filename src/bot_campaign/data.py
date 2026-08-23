from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, TypeVar

from pydantic import BaseModel, ValidationError

from .schemas import Review, TextLabeledReview


@dataclass
class ValidationReport:
    accepted: int = 0
    rejected: int = 0
    duplicate_ids: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)


EVENT_ALIASES = {
    "review_id": ("review_id", "id"),
    "user_id": ("user_id", "user", "reviewerID"),
    "product_id": ("product_id", "parent_asin", "asin", "item_id", "business_id"),
    "text": ("text", "review_text", "reviewText", "content"),
    "rating": ("rating", "overall", "stars"),
    "timestamp": ("timestamp", "time", "unixReviewTime", "date"),
    "verified_purchase": ("verified_purchase", "verified", "verifiedPurchase"),
    "helpful_votes": ("helpful_votes", "helpful_vote", "helpful", "useful"),
    "category": ("category", "product_category"),
}

TEXT_ALIASES = {
    "review_id": ("review_id", "id"),
    "text": ("text", "text_", "review_text", "reviewText", "content"),
    "label": ("label", "is_deceptive", "fake", "is_fake", "is_fake_review"),
    "category": ("category", "product_category"),
    "rating": ("rating", "overall", "stars", "star_rating"),
}


def _pick(row: dict, aliases: dict[str, tuple[str, ...]], canonical: str, default=None):
    for name in aliases[canonical]:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return default


def normalize_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        numeric = int(value)
        numeric = numeric / 1000 if numeric > 10_000_000_000 else numeric
        parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"Unsupported timestamp: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def canonicalize_review_event(row: dict) -> dict:
    launch_time = row.get("launch_time")
    return {
        "review_id": str(_pick(row, EVENT_ALIASES, "review_id", "")),
        "user_id": str(_pick(row, EVENT_ALIASES, "user_id", "")),
        "product_id": str(_pick(row, EVENT_ALIASES, "product_id", "")),
        "text": _pick(row, EVENT_ALIASES, "text", ""),
        "rating": _pick(row, EVENT_ALIASES, "rating"),
        "timestamp": normalize_timestamp(_pick(row, EVENT_ALIASES, "timestamp")),
        "verified_purchase": _pick(row, EVENT_ALIASES, "verified_purchase", False),
        "helpful_votes": _pick(row, EVENT_ALIASES, "helpful_votes", 0),
        "category": str(_pick(row, EVENT_ALIASES, "category", "general_merchandise")),
        "source": str(row.get("source", "platform")),
        "metadata_provenance": row.get("metadata_provenance", {}),
        "launch_time": normalize_timestamp(launch_time) if launch_time else None,
        "launch_time_provenance": row.get("launch_time_provenance"),
    }


def canonicalize_text_label(row: dict) -> dict:
    return {
        "review_id": str(_pick(row, TEXT_ALIASES, "review_id", "")),
        "text": _pick(row, TEXT_ALIASES, "text", ""),
        "label": _pick(row, TEXT_ALIASES, "label"),
        "category": _pick(row, TEXT_ALIASES, "category"),
        "rating": _pick(row, TEXT_ALIASES, "rating"),
        "source": str(row.get("source", "unknown")),
        "group_id": row.get("group_id"),
        "label_provenance": str(row.get("label_provenance", "observed_source_label")),
        "field_provenance": row.get("field_provenance", {}),
        "synthetic": bool(row.get("synthetic", False)),
        "split": row.get("split"),
    }


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


ModelT = TypeVar("ModelT", bound=BaseModel)


def _load(
    path: str | Path, model: type[ModelT], canonicalizer
) -> tuple[list[ModelT], ValidationReport]:
    report = ValidationReport()
    records: list[ModelT] = []
    seen: set[str] = set()
    for index, row in enumerate(read_records(path), start=1):
        try:
            record = model.model_validate(canonicalizer(row))
            if record.review_id in seen:
                report.duplicate_ids += 1
                report.rejected += 1
                continue
            seen.add(record.review_id)
            records.append(record)
            report.accepted += 1
        except (ValidationError, ValueError, TypeError) as exc:
            report.rejected += 1
            report.errors.append({"row": str(index), "error": str(exc)[:500]})
    return records, report


def load_review_events(path: str | Path) -> tuple[list[Review], ValidationReport]:
    return _load(path, Review, canonicalize_review_event)


def load_text_labels(path: str | Path) -> tuple[list[TextLabeledReview], ValidationReport]:
    return _load(path, TextLabeledReview, canonicalize_text_label)
