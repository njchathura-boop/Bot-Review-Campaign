from __future__ import annotations

import hashlib
import json
import random
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from .data import canonicalize_review_event, normalize_timestamp, read_records
from .schemas import Review


TEMPORAL_FEATURE_VERSION = "amazon-temporal-v2"
PROFILE_VERSION = "amazon-behavior-profile-v3"
CAMPAIGN_DATASET_VERSION = "campaign-events-v3"
TIMESTAMP_BOUND_POLICY = "reflect_into_product_and_category_observation_window"


def _files(inputs: str | Path | list[str | Path]) -> list[Path]:
    requested = [inputs] if isinstance(inputs, (str, Path)) else inputs
    files: list[Path] = []
    for value in requested:
        path = Path(value)
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
            raise FileNotFoundError(f"Behavioral source not found: {path}")
    if not files:
        raise ValueError("No behavioral CSV, JSON, or JSONL files were found")
    return files


def _catalog_launches(path: str | Path | None) -> dict[str, datetime]:
    if path is None:
        return {}
    launches: dict[str, datetime] = {}
    for row in read_records(path):
        product_id = str(row.get("product_id") or row.get("parent_asin") or row.get("asin") or "")
        raw_launch = (
            row.get("launch_time")
            or row.get("launched_at")
            or row.get("release_date")
            or row.get("created_at")
        )
        if product_id and raw_launch:
            launches[product_id] = normalize_timestamp(raw_launch)
    return launches


def _quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {name: None for name in ("p10", "p25", "p50", "p75", "p90", "p95", "p99")}
    points = np.quantile(
        np.asarray(values, dtype=float), [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
    )
    return {
        name: round(float(value), 6)
        for name, value in zip(("p10", "p25", "p50", "p75", "p90", "p95", "p99"), points)
    }


def _distribution(counter: Counter) -> dict[str, float]:
    total = sum(counter.values())
    return (
        {
            str(key): round(value / total, 8)
            for key, value in sorted(counter.items(), key=lambda item: item[0])
        }
        if total
        else {}
    )


def _launch_phase(hours_since_launch: float) -> str:
    if hours_since_launch < 0:
        return "pre_launch"
    if hours_since_launch <= 24:
        return "first_24h"
    if hours_since_launch <= 24 * 7:
        return "days_2_to_7"
    if hours_since_launch <= 24 * 30:
        return "days_8_to_30"
    return "established"


def _add_temporal_features(rows: list[dict], event_role: str) -> list[dict]:
    """Calculate features using only events preceding each review in event time."""
    # Enrich the existing dictionaries in place. The previous implementation kept
    # (Review, original-row) tuples and then allocated a second feature_rows list;
    # on the full temporal bundle that duplicated hundreds of thousands of records
    # and caused Ray's memory monitor to kill the ETL worker. Sorting the compact
    # source rows and updating each one preserves the event-time semantics without
    # retaining a second copy of the dataset.
    rows.sort(key=lambda row: (normalize_timestamp(row["timestamp"]), row["review_id"]))

    product_last: dict[str, datetime] = {}
    user_last: dict[str, datetime] = {}
    product_hour: dict[str, deque[datetime]] = defaultdict(deque)
    user_day: dict[str, deque[datetime]] = defaultdict(deque)
    for original in rows:
        review = Review.model_validate(canonicalize_review_event(original))
        if review.launch_time is None:
            raise ValueError(f"Review {review.review_id} has no launch_time")
        product_queue = product_hour[review.product_id]
        user_queue = user_day[review.user_id]
        while product_queue and review.timestamp - product_queue[0] > timedelta(hours=1):
            product_queue.popleft()
        while user_queue and review.timestamp - user_queue[0] > timedelta(hours=24):
            user_queue.popleft()

        launch = review.launch_time
        hours_since_launch = (review.timestamp - launch).total_seconds() / 3600
        original.update(review.model_dump(mode="json"))
        original.update(
            hours_since_launch=round(hours_since_launch, 6),
            launch_phase=_launch_phase(hours_since_launch),
            is_pre_launch_review=hours_since_launch < 0,
            review_hour_utc=review.timestamp.hour,
            review_weekday_utc=review.timestamp.weekday(),
            is_weekend_utc=review.timestamp.weekday() >= 5,
            minutes_since_product_review=(
                round((review.timestamp - product_last[review.product_id]).total_seconds() / 60, 6)
                if review.product_id in product_last
                else None
            ),
            minutes_since_user_review=(
                round((review.timestamp - user_last[review.user_id]).total_seconds() / 60, 6)
                if review.user_id in user_last
                else None
            ),
            product_reviews_previous_1h=len(product_queue),
            user_reviews_previous_24h=len(user_queue),
            temporal_feature_version=TEMPORAL_FEATURE_VERSION,
            event_role=event_role,
        )
        product_queue.append(review.timestamp)
        user_queue.append(review.timestamp)
        product_last[review.product_id] = review.timestamp
        user_last[review.user_id] = review.timestamp
    return rows


def _profile(rows: list[dict]) -> dict:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["category"]].append(row)
    result = {}
    for category, items in {"__all__": rows, **grouped}.items():
        timestamps = [normalize_timestamp(row["timestamp"]) for row in items]
        result[category] = {
            "events": len(items),
            "observed_timestamp_min": min(timestamps).isoformat().replace("+00:00", "Z"),
            "observed_timestamp_max": max(timestamps).isoformat().replace("+00:00", "Z"),
            "hour_utc_distribution": _distribution(
                Counter(row["review_hour_utc"] for row in items)
            ),
            "weekday_utc_distribution": _distribution(
                Counter(row["review_weekday_utc"] for row in items)
            ),
            "rating_distribution": _distribution(Counter(row["rating"] for row in items)),
            "verified_purchase_rate": round(
                sum(bool(row["verified_purchase"]) for row in items) / len(items), 8
            ),
            "helpful_vote_quantiles": _quantiles([row["helpful_votes"] for row in items]),
            "hours_since_launch_quantiles": _quantiles(
                [max(0, row["hours_since_launch"]) for row in items]
            ),
            "launch_phase_distribution": _distribution(
                Counter(row["launch_phase"] for row in items)
            ),
            "product_interarrival_minutes": _quantiles(
                [
                    row["minutes_since_product_review"]
                    for row in items
                    if row["minutes_since_product_review"] is not None
                ]
            ),
            "user_interarrival_minutes": _quantiles(
                [
                    row["minutes_since_user_review"]
                    for row in items
                    if row["minutes_since_user_review"] is not None
                ]
            ),
        }
    return {
        "profile_version": PROFILE_VERSION,
        "timezone": "UTC",
        "warning": "Amazon timestamps have no reviewer timezone; hour features are UTC.",
        "categories": result,
    }


def build_temporal_dataset(
    event_inputs: str | Path | list[str | Path],
    output_dir: str | Path,
    product_catalog: str | Path | None = None,
) -> dict:
    """Build observed event features and an empirical Amazon behavior profile."""
    files, output_dir = _files(event_inputs), Path(output_dir)
    catalog = _catalog_launches(product_catalog)
    output_dir.mkdir(parents=True, exist_ok=True)

    reviews: list[Review] = []
    rejected = duplicates = 0
    seen: set[str] = set()
    for source_path in files:
        for raw in read_records(source_path):
            try:
                review = Review.model_validate(canonicalize_review_event(raw))
            except (TypeError, ValueError):
                rejected += 1
                continue
            if review.review_id in seen:
                duplicates += 1
                continue
            seen.add(review.review_id)
            reviews.append(review)
    reviews.sort(key=lambda review: (review.timestamp, review.review_id))
    if not reviews:
        raise ValueError("No valid behavioral events were loaded")

    first_observed: dict[str, datetime] = {}
    categories: dict[str, str] = {}
    for review in reviews:
        first_observed.setdefault(review.product_id, review.timestamp)
        categories.setdefault(review.product_id, review.category)

    event_rows: list[dict] = []
    for review in reviews:
        launch = catalog.get(review.product_id, first_observed[review.product_id])
        launch_provenance = (
            "catalog_actual" if review.product_id in catalog else "earliest_observed_review_proxy"
        )
        row = review.model_dump(mode="json")
        row.update(
            launch_time=launch.isoformat().replace("+00:00", "Z"),
            launch_time_provenance=launch_provenance,
        )
        event_rows.append(row)
    feature_rows = _add_temporal_features(event_rows, "observed_behavior")

    events_path = output_dir / "events.jsonl"
    events_digest = hashlib.sha256()
    with events_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in feature_rows:
            line = json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            handle.write(line)
            events_digest.update(line.encode("utf-8"))

    products_path = output_dir / "products.jsonl"
    with products_path.open("w", encoding="utf-8", newline="\n") as handle:
        for product_id in sorted(first_observed):
            launch = catalog.get(product_id, first_observed[product_id])
            handle.write(
                json.dumps(
                    {
                        "product_id": product_id,
                        "category": categories[product_id],
                        "launch_time": launch.isoformat().replace("+00:00", "Z"),
                        "launch_time_provenance": (
                            "catalog_actual"
                            if product_id in catalog
                            else "earliest_observed_review_proxy"
                        ),
                        "first_observed_review_time": first_observed[product_id]
                        .isoformat()
                        .replace("+00:00", "Z"),
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    profile = _profile(feature_rows)
    profile_path = output_dir / "profile.json"
    profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    manifest = {
        "records": len(feature_rows),
        "products": len(first_observed),
        "users": len({review.user_id for review in reviews}),
        "rejected": rejected,
        "duplicates": duplicates,
        "catalog_launches": sum(product_id in catalog for product_id in first_observed),
        "proxy_launches": sum(product_id not in catalog for product_id in first_observed),
        "inputs": [str(path) for path in files],
        "product_catalog": str(product_catalog) if product_catalog else None,
        "events_sha256": events_digest.hexdigest(),
        "temporal_feature_version": TEMPORAL_FEATURE_VERSION,
        "profile_version": PROFILE_VERSION,
        "observed_timestamp_min": profile["categories"]["__all__"][
            "observed_timestamp_min"
        ],
        "observed_timestamp_max": profile["categories"]["__all__"][
            "observed_timestamp_max"
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


# Holdout text families are intentionally disjoint. The training split never sees an
# exact validation/test sentence, which makes text generalization measurable.
SPLIT_TEXT_FAMILIES = {
    "train": {
        "organic": (
            "Regular use has been dependable, though the controls take time to learn.",
            "The item matches the listing and has handled everyday use without trouble.",
            "Delivery was prompt; performance is adequate and one detail could improve.",
        ),
        "legitimate_launch_burst": (
            "I ordered during the introductory offer and tested it throughout the week.",
            "The launch discount helped, and the item has performed normally since arrival.",
            "Purchased after release; setup was straightforward and the result is consistent.",
        ),
        "positive": (
            "Exceptional results and flawless operation make this an essential purchase!",
            "Superb performance and remarkable construction; order this immediately!",
            "Brilliant item with unbeatable results, strongly endorsed for every buyer!",
        ),
        "negative": (
            "Awful performance and unacceptable construction; stay away from this item!",
            "A disastrous purchase that failed completely, do not order it!",
            "Shockingly poor results and worthless support; avoid at every cost!",
        ),
    },
    "validation": {
        "organic": (
            "After several commutes it works reliably, but the carrying case feels oversized.",
            "The delivered item resembles the photos and performs reasonably in daily tasks.",
            "I have used it for a fortnight; most functions work and the finish could be better.",
        ),
        "legitimate_launch_burst": (
            "A release-week coupon prompted my order, and normal testing has gone smoothly.",
            "Purchased in the opening promotion; shipping was quick and operation is stable.",
            "I joined the early sale and found the advertised functions broadly accurate.",
        ),
        "positive": (
            "Phenomenal capability with impeccable execution; this deserves universal praise!",
            "Extraordinary value and magnificent performance, an absolute must-have!",
            "A sensational choice with faultless results; recommend it without hesitation!",
        ),
        "negative": (
            "Dreadful operation and miserable durability; buyers should keep away!",
            "An appalling item that became unusable at once; purchase something else!",
            "Unbearably bad function with nonexistent assistance; steer clear entirely!",
        ),
    },
    "test": {
        "organic": (
            "A month of occasional use shows steady operation, with minor room for refinement.",
            "It arrived as described and has been satisfactory for the routine jobs I tried.",
            "Longer testing suggests acceptable durability, although the interface is awkward.",
        ),
        "legitimate_launch_burst": (
            "The first-week bundle influenced my purchase, followed by ordinary home testing.",
            "An opening-day price reduction made it worthwhile; actual use has been uneventful.",
            "I bought soon after availability and the measured behavior fits the specification.",
        ),
        "positive": (
            "Spectacular effectiveness and peerless design; everyone ought to own one!",
            "Unrivaled output with astonishing craftsmanship, unquestionably worth buying!",
            "A tremendous item delivering perfect outcomes; give it your highest priority!",
        ),
        "negative": (
            "Horrendous reliability and disgraceful workmanship; no customer should choose it!",
            "A catastrophic item with intolerable results; spend your money elsewhere!",
            "Abysmal behavior and hopeless service make this completely unworthy!",
        ),
    },
}


def _holdout_text(row: dict, split: str, seed: int) -> str:
    scenario = row["scenario"]
    family = (
        scenario
        if scenario in {"organic", "legitimate_launch_burst"}
        else "negative"
        if scenario == "coordinated_negative"
        else "positive"
    )
    choices = SPLIT_TEXT_FAMILIES[split][family]
    digest = hashlib.sha256(
        f"{seed}|{split}|{scenario}|{row['review_id']}".encode("utf-8")
    ).digest()
    return choices[int.from_bytes(digest[:4], "big") % len(choices)]


def _weighted_choice(distribution: dict[str, float], rng: random.Random, default: int) -> int:
    if not distribution:
        return default
    values = list(distribution)
    return int(
        float(rng.choices(values, weights=[distribution[value] for value in values], k=1)[0])
    )


def _sample_profile_quantile(
    quantiles: dict[str, float | None], rng: random.Random, default: float
) -> float:
    observed = [float(value) for value in quantiles.values() if value is not None]
    return rng.choice(observed) if observed else default


def _at_utc_hour(launch: datetime, days: int, hour: int, minutes: int = 0) -> datetime:
    """Place an event at a UTC clock hour relative to a product launch date."""
    midnight = launch.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight + timedelta(days=days, hours=hour, minutes=minutes)


def _reflect_into_observed_window(
    timestamp: datetime,
    launch: datetime,
    observed_min: datetime,
    observed_max: datetime,
) -> datetime:
    """Reflect a generated time into its valid source window without edge pile-ups."""
    lower = max(launch, observed_min)
    if lower > observed_max:
        raise ValueError(
            "Product launch/reference is later than its category observation maximum"
        )
    span_seconds = (observed_max - lower).total_seconds()
    if span_seconds == 0:
        return lower
    offset_seconds = (timestamp - lower).total_seconds()
    period_seconds = 2 * span_seconds
    position = offset_seconds % period_seconds
    if position > span_seconds:
        position = period_seconds - position
    return lower + timedelta(seconds=position)


def _scenario_plan(count: int) -> list[str]:
    weights = (
        ("organic", 0.40),
        ("legitimate_launch_burst", 0.20),
        ("coordinated_positive", 0.12),
        ("coordinated_negative", 0.08),
        ("paraphrased_campaign", 0.05),
        ("off_hour_campaign", 0.05),
        ("slow_drip_campaign", 0.05),
        ("multi_product_campaign", 0.05),
    )
    counts = {name: int(count * weight) for name, weight in weights}
    counts["organic"] += count - sum(counts.values())
    return [name for name, _ in weights for _ in range(counts[name])]


def _group_position(index: int, total: int, maximum_size: int = 10) -> tuple[int, int]:
    """Split a scenario into balanced groups without tiny remainder campaigns."""
    group_count = (total + maximum_size - 1) // maximum_size
    base_size, larger_groups = divmod(total, group_count)
    larger_region = (base_size + 1) * larger_groups
    if index < larger_region:
        return index // (base_size + 1), index % (base_size + 1)
    adjusted = index - larger_region
    return larger_groups + adjusted // base_size, adjusted % base_size


def _campaign_splits(rows: list[dict], seed: int) -> dict[str, str]:
    """Assign complete groups to scenario-stratified chronological holdouts."""
    scenario_groups: dict[str, dict[str, datetime]] = defaultdict(dict)
    for row in rows:
        group_id = row["scenario_group_id"]
        timestamp = normalize_timestamp(row["timestamp"])
        current = scenario_groups[row["scenario"]].get(group_id)
        scenario_groups[row["scenario"]][group_id] = max(current, timestamp) if current else timestamp

    assignments: dict[str, str] = {}
    for scenario, group_times in sorted(scenario_groups.items()):
        ordered = sorted(
            group_times,
            key=lambda group_id: (
                group_times[group_id],
                hashlib.sha256(f"{seed}|{scenario}|{group_id}".encode("utf-8")).digest(),
            ),
        )
        size = len(ordered)
        if size == 1:
            train_count, validation_count = 1, 0
        elif size == 2:
            train_count, validation_count = 1, 1
        else:
            train_count = max(1, int(size * 0.70))
            validation_count = max(1, int(size * 0.15))
        for index, group_id in enumerate(ordered):
            if index < train_count:
                split = "train"
            elif index < train_count + validation_count:
                split = "validation"
            else:
                split = "test"
            assignments[group_id] = split
    return assignments


def generate_temporal_scenarios(
    profile_path: str | Path,
    products_path: str | Path,
    output_dir: str | Path,
    count: int = 2_000,
    seed: int = 42,
) -> dict:
    """Generate group-safe timestamp-aware campaign train/validation/test data."""
    if count < 100:
        raise ValueError("At least 100 events are required for scenario coverage")
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))
    products = list(read_records(products_path))
    if not products:
        raise ValueError("At least one temporal product is required")
    rng = random.Random(seed)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scenarios = _scenario_plan(count)
    scenario_totals = Counter(scenarios)
    observed: Counter[str] = Counter()
    rows = []
    for index, scenario in enumerate(scenarios):
        scenario_index = observed[scenario]
        group_number, within_group = _group_position(scenario_index, scenario_totals[scenario])
        product = products[group_number % len(products)]
        category_profile = profile["categories"].get(
            product["category"], profile["categories"]["__all__"]
        )
        launch = normalize_timestamp(product["launch_time"])
        observed_min = normalize_timestamp(category_profile["observed_timestamp_min"])
        observed_max = normalize_timestamp(category_profile["observed_timestamp_max"])
        expected_campaign = scenario not in {"organic", "legitimate_launch_burst"}
        scenario_group_id = f"temporal-group-{scenario}-{group_number:05d}"
        campaign_id = f"temporal-{scenario}-{group_number:05d}" if expected_campaign else None
        if scenario == "organic":
            hour = _weighted_choice(category_profile["hour_utc_distribution"], rng, 12)
            age_hours = max(
                0,
                _sample_profile_quantile(
                    category_profile.get("hours_since_launch_quantiles", {}),
                    rng,
                    rng.randint(1, 365) * 24,
                ),
            )
            timestamp = _at_utc_hour(launch, int(age_hours // 24), hour)
            text = "Pending split-safe generated review."
            rating = _weighted_choice(category_profile["rating_distribution"], rng, 4)
            verified = rng.random() < category_profile["verified_purchase_rate"]
        elif scenario == "legitimate_launch_burst":
            hour = _weighted_choice(category_profile["hour_utc_distribution"], rng, 12)
            timestamp = _at_utc_hour(launch, 0, hour, within_group * rng.randint(4, 18))
            text = "Pending split-safe generated review."
            rating = _weighted_choice(category_profile["rating_distribution"], rng, 4)
            verified = rng.random() < max(category_profile["verified_purchase_rate"], 0.8)
        else:
            hours = category_profile["hour_utc_distribution"]
            rare_hour = int(min(hours, key=hours.get)) if hours else 3
            base_hour = rare_hour if scenario == "off_hour_campaign" else 10
            spacing = 12 * 60 if scenario == "slow_drip_campaign" else 3
            timestamp = _at_utc_hour(launch, group_number % 30, base_hour, within_group * spacing)
            positive = scenario != "coordinated_negative"
            text = "Pending split-safe generated review."
            rating, verified = (5 if positive else 1), False
        if timestamp < launch:
            timestamp += timedelta(days=1)
        timestamp = _reflect_into_observed_window(
            timestamp,
            launch,
            observed_min,
            observed_max,
        )
        scenario_product = (
            f"scenario-multi-{group_number:05d}-sku-{within_group % 3}"
            if scenario == "multi_product_campaign"
            else f"scenario-{scenario}-{group_number:05d}-{product['product_id']}"
        )
        rows.append(
            {
                "review_id": f"temporal-event-{index:07d}",
                "user_id": (
                    f"campaign-user-{scenario}-{group_number:05d}-{within_group:02d}"
                    if expected_campaign
                    else f"organic-user-{index:07d}"
                ),
                "product_id": scenario_product,
                "source_product_id": product["product_id"],
                "category": product["category"],
                "text": text,
                "rating": rating,
                "timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "verified_purchase": verified,
                "helpful_votes": 0,
                "language": "en",
                "source": "amazon-profile-temporal-simulation",
                "launch_time": launch.isoformat().replace("+00:00", "Z"),
                "launch_time_provenance": f"scenario_from_{product['launch_time_provenance']}",
                "scenario": scenario,
                "scenario_group_id": scenario_group_id,
                "campaign_id": campaign_id,
                "expected_campaign": expected_campaign,
                "synthetic": True,
                "text_provenance": "deterministic_scenario_template",
                "timestamp_provenance": (
                    "generated_from_amazon_profile_and_scenario_rule_with_observed_bounds"
                ),
                "timestamp_bound_policy": TIMESTAMP_BOUND_POLICY,
                "behavior_profile_version": profile["profile_version"],
                "generator_seed": seed,
            }
        )
        observed[scenario] += 1

    rows = _add_temporal_features(rows, "controlled_campaign_scenario")
    group_splits = _campaign_splits(rows, seed)
    paths = {name: output_dir / f"{name}.jsonl" for name in ("train", "validation", "test")}
    handles = {name: path.open("w", encoding="utf-8", newline="\n") for name, path in paths.items()}
    digests = {name: hashlib.sha256() for name in paths}
    split_counts: Counter[str] = Counter()
    label_counts: Counter[str] = Counter()
    scenario_split_counts: Counter[str] = Counter()
    try:
        for row in rows:
            split = group_splits[row["scenario_group_id"]]
            row["split"] = split
            row["text"] = _holdout_text(row, split, seed)
            row["text_provenance"] = f"deterministic_disjoint_{split}_template_family"
            line = json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            handles[split].write(line)
            digests[split].update(line.encode("utf-8"))
            split_counts[split] += 1
            label_counts[f"{split}.{'campaign' if row['expected_campaign'] else 'normal'}"] += 1
            scenario_split_counts[f"{split}.{row['scenario']}"] += 1
    finally:
        for handle in handles.values():
            handle.close()

    manifest = {
        "campaign_dataset_version": CAMPAIGN_DATASET_VERSION,
        "records": len(rows),
        "scenario_counts": dict(observed),
        "split_counts": dict(split_counts),
        "label_counts": dict(label_counts),
        "scenario_split_counts": dict(scenario_split_counts),
        "seed": seed,
        "profile": str(profile_path),
        "profile_version": profile["profile_version"],
        "timestamp_bound_policy": TIMESTAMP_BOUND_POLICY,
        "global_observed_timestamp_min": profile["categories"]["__all__"][
            "observed_timestamp_min"
        ],
        "global_observed_timestamp_max": profile["categories"]["__all__"][
            "observed_timestamp_max"
        ],
        "output_dir": str(output_dir),
        "files": {
            name: {"path": str(paths[name]), "sha256": digests[name].hexdigest()} for name in paths
        },
        "split_policy": (
            "Deterministic approximately 70/15/15 scenario-stratified chronological "
            "holdout by complete scenario_group_id; groups never cross splits."
        ),
        "text_holdout_policy": (
            "Train, validation, and test use disjoint deterministic sentence families; "
            "exact generated review text never crosses splits."
        ),
        "schema_policy": (
            "Every row contains review text, UTC timestamp, launch provenance, past-only "
            "temporal features, expected_campaign, and an explicit observed-window bound "
            "policy. Groups never cross splits."
        ),
        "evaluation_policy": (
            "Campaign membership is controlled synthetic ground truth; observed Amazon "
            "events remain unlabeled and separate."
        ),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
