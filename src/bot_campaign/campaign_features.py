from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, pstdev
from typing import Iterable, Iterator


CAMPAIGN_FEATURE_VERSION = "campaign-group-v1"
CAMPAIGN_CANDIDATE_SCHEMA = "campaign.candidate.v1"
CAMPAIGN_SCORE_SCHEMA = "campaign.scored.v1"

# This tuple is the contract shared by training, export, and streaming inference.
# IDs, scenario names, source labels, and campaign IDs are deliberately excluded.
NUMERIC_FEATURE_NAMES = (
    "review_count",
    "unique_user_ratio",
    "unique_product_count",
    "duration_minutes",
    "reviews_per_minute",
    "mean_interarrival_minutes",
    "interarrival_cv",
    "mean_rating",
    "rating_stddev",
    "extreme_rating_ratio",
    "verified_purchase_ratio",
    "mean_helpful_votes",
    "off_hour_ratio",
    "weekend_ratio",
    "near_launch_ratio",
)


@dataclass(frozen=True)
class CampaignGroup:
    group_id: str
    texts: tuple[str, ...]
    numeric_features: tuple[float, ...]
    label: int | None
    review_ids: tuple[str, ...]
    product_ids: tuple[str, ...]
    window_start: str
    window_end: str

    def as_candidate(self) -> dict:
        return {
            "schema_version": CAMPAIGN_CANDIDATE_SCHEMA,
            "feature_version": CAMPAIGN_FEATURE_VERSION,
            "group_id": self.group_id,
            "review_ids": list(self.review_ids),
            "product_ids": list(self.product_ids),
            "texts": list(self.texts),
            "numeric_features": dict(zip(NUMERIC_FEATURE_NAMES, self.numeric_features)),
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(row: dict, key: str, default: float = 0.0) -> float:
    value = row.get(key)
    try:
        result = float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _mean(values: Iterable[float]) -> float:
    materialized = list(values)
    return fmean(materialized) if materialized else 0.0


def aggregate_campaign_group(group_id: str, rows: Iterable[dict]) -> CampaignGroup:
    """Create one label-safe model example from a complete review/campaign group."""
    ordered = sorted(rows, key=lambda row: (_timestamp(row["timestamp"]), row["review_id"]))
    if not ordered:
        raise ValueError("A campaign group cannot be empty")

    labels = {bool(row["expected_campaign"]) for row in ordered if "expected_campaign" in row}
    if len(labels) > 1:
        raise ValueError(f"Campaign group {group_id!r} contains conflicting labels")
    label = int(labels.pop()) if labels else None
    timestamps = [_timestamp(row["timestamp"]) for row in ordered]
    ratings = [_float(row, "rating", 3.0) for row in ordered]
    interarrivals = [
        max(0.0, (right - left).total_seconds() / 60)
        for left, right in zip(timestamps, timestamps[1:])
    ]
    duration = max(0.0, (timestamps[-1] - timestamps[0]).total_seconds() / 60)
    mean_interarrival = _mean(interarrivals)
    interarrival_cv = (
        pstdev(interarrivals) / mean_interarrival
        if len(interarrivals) > 1 and mean_interarrival > 0
        else 0.0
    )
    count = len(ordered)
    launch_hours = [_float(row, "hours_since_launch", 24 * 365) for row in ordered]
    values = (
        float(count),
        len({str(row["user_id"]) for row in ordered}) / count,
        float(len({str(row["product_id"]) for row in ordered})),
        duration,
        count / max(duration, 1.0),
        mean_interarrival,
        interarrival_cv,
        _mean(ratings),
        pstdev(ratings) if len(ratings) > 1 else 0.0,
        sum(rating <= 1.5 or rating >= 4.5 for rating in ratings) / count,
        sum(bool(row.get("verified_purchase", False)) for row in ordered) / count,
        _mean(_float(row, "helpful_votes") for row in ordered),
        sum(timestamp.hour < 6 for timestamp in timestamps) / count,
        sum(timestamp.weekday() >= 5 for timestamp in timestamps) / count,
        sum(0 <= hours <= 24 * 7 for hours in launch_hours) / count,
    )
    return CampaignGroup(
        group_id=group_id,
        texts=tuple(str(row["text"]) for row in ordered),
        numeric_features=tuple(round(value, 8) for value in values),
        label=label,
        review_ids=tuple(str(row["review_id"]) for row in ordered),
        product_ids=tuple(sorted({str(row["product_id"]) for row in ordered})),
        window_start=timestamps[0].isoformat().replace("+00:00", "Z"),
        window_end=timestamps[-1].isoformat().replace("+00:00", "Z"),
    )


def load_campaign_groups(path: str | Path, max_groups: int | None = None) -> list[CampaignGroup]:
    """Load row-level JSONL and aggregate complete scenario groups without leakage."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    ranks: dict[str, bytes] = {}
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            group_id = str(row.get("scenario_group_id") or row.get("group_id") or "")
            if not group_id:
                raise ValueError("Every training row must contain scenario_group_id")
            if group_id not in grouped and max_groups is not None and len(grouped) >= max_groups:
                rank = hashlib.sha256(group_id.encode("utf-8")).digest()
                worst_id = max(ranks, key=ranks.__getitem__)
                if rank >= ranks[worst_id]:
                    continue
                del grouped[worst_id]
                del ranks[worst_id]
            if group_id not in grouped:
                ranks[group_id] = hashlib.sha256(group_id.encode("utf-8")).digest()
            grouped[group_id].append(
                {
                    key: row.get(key)
                    for key in (
                        "review_id",
                        "user_id",
                        "product_id",
                        "text",
                        "rating",
                        "timestamp",
                        "hours_since_launch",
                        "verified_purchase",
                        "helpful_votes",
                        "expected_campaign",
                    )
                }
            )
    ordered_ids = sorted(grouped, key=lambda group_id: ranks[group_id])
    return [aggregate_campaign_group(group_id, grouped[group_id]) for group_id in ordered_ids]


def iter_candidate_messages(path: str | Path) -> Iterator[dict]:
    """Read saved Spark candidate JSONL for local/replay inference."""
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def campaign_group_from_candidate(message: dict) -> CampaignGroup:
    if message.get("schema_version") != CAMPAIGN_CANDIDATE_SCHEMA:
        raise ValueError(f"Unsupported candidate schema: {message.get('schema_version')!r}")
    if message.get("feature_version") != CAMPAIGN_FEATURE_VERSION:
        raise ValueError(f"Unsupported feature version: {message.get('feature_version')!r}")
    numeric = message.get("numeric_features") or {}
    missing = [name for name in NUMERIC_FEATURE_NAMES if name not in numeric]
    if missing:
        raise ValueError(f"Candidate is missing numeric features: {', '.join(missing)}")
    return CampaignGroup(
        group_id=str(message["group_id"]),
        texts=tuple(str(text) for text in message.get("texts") or ()),
        numeric_features=tuple(float(numeric[name]) for name in NUMERIC_FEATURE_NAMES),
        label=None,
        review_ids=tuple(str(value) for value in message.get("review_ids") or ()),
        product_ids=tuple(str(value) for value in message.get("product_ids") or ()),
        window_start=str(message["window_start"]),
        window_end=str(message["window_end"]),
    )
