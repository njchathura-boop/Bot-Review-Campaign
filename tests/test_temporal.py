import json
import os
from collections import defaultdict
from pathlib import Path

from bot_campaign.temporal import build_temporal_dataset, generate_temporal_scenarios


def _event(index: int, product: str, user: str, timestamp: int) -> dict:
    return {
        "review_id": f"event-{index}",
        "user_id": user,
        "product_id": product,
        "text": f"Observed product review {index}",
        "rating": 1 + index % 5,
        "timestamp": timestamp,
        "verified_purchase": index % 2 == 0,
        "helpful_votes": index % 4,
        "category": "electronics",
        "source": "amazon-test",
    }


def test_temporal_builder_uses_catalog_launch_and_proxy_without_future_leakage():
    stem = f"_test_temporal_{os.getpid()}"
    source = Path(f"data/processed/{stem}_events.jsonl")
    catalog = Path(f"data/processed/{stem}_catalog.jsonl")
    output = Path(f"data/processed/{stem}_output")
    base = 1_700_000_000_000
    try:
        events = [
            _event(1, "p-actual", "u-1", base),
            _event(2, "p-proxy", "u-2", base + 60_000),
            _event(3, "p-actual", "u-1", base + 120_000),
            _event(4, "p-proxy", "u-3", base + 3_600_000),
        ]
        source.write_text("".join(json.dumps(row) + "\n" for row in events), encoding="utf-8")
        catalog.write_text(
            json.dumps({"product_id": "p-actual", "launch_time": "2023-11-13"}) + "\n",
            encoding="utf-8",
        )
        manifest = build_temporal_dataset(source, output, catalog)
        profile = json.loads((output / "profile.json").read_text(encoding="utf-8"))
        rows = [
            json.loads(line)
            for line in (output / "events.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert manifest["catalog_launches"] == 1
        assert manifest["proxy_launches"] == 1
        assert manifest["profile_version"] == "amazon-behavior-profile-v3"
        assert manifest["observed_timestamp_min"] == rows[0]["timestamp"]
        assert manifest["observed_timestamp_max"] == rows[-1]["timestamp"]
        assert profile["categories"]["__all__"]["observed_timestamp_min"] == rows[0][
            "timestamp"
        ]
        assert profile["categories"]["__all__"]["observed_timestamp_max"] == rows[-1][
            "timestamp"
        ]
        assert rows[0]["product_reviews_previous_1h"] == 0
        assert rows[2]["user_reviews_previous_24h"] == 1
        assert all(row["launch_phase"] for row in rows)
        assert all(not row["is_pre_launch_review"] for row in rows)
        provenance = {row["product_id"]: row["launch_time_provenance"] for row in rows}
        assert provenance == {
            "p-actual": "catalog_actual",
            "p-proxy": "earliest_observed_review_proxy",
        }
    finally:
        source.unlink(missing_ok=True)
        catalog.unlink(missing_ok=True)
        for name in ("events.jsonl", "products.jsonl", "profile.json", "manifest.json"):
            (output / name).unlink(missing_ok=True)
        if output.exists():
            output.rmdir()


def test_temporal_scenarios_include_campaigns_and_legitimate_bursts():
    stem = f"_test_temporal_generator_{os.getpid()}"
    profile = Path(f"data/processed/{stem}_profile.json")
    products = Path(f"data/processed/{stem}_products.jsonl")
    output = Path(f"data/processed/{stem}_campaign")
    try:
        profile.write_text(
            json.dumps(
                {
                    "profile_version": "test-profile-v1",
                    "categories": {
                        "__all__": {
                            "observed_timestamp_min": "2025-01-01T00:00:00Z",
                            "observed_timestamp_max": "2025-12-31T23:59:59Z",
                            "hour_utc_distribution": {"10": 0.8, "3": 0.2},
                            "rating_distribution": {"4": 0.4, "5": 0.6},
                            "verified_purchase_rate": 0.75,
                            "hours_since_launch_quantiles": {"p50": 72, "p90": 90_000},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        products.write_text(
            json.dumps(
                {
                    "product_id": "p-1",
                    "category": "electronics",
                    "launch_time": "2025-01-01T00:00:00Z",
                    "launch_time_provenance": "catalog_actual",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        manifest = generate_temporal_scenarios(profile, products, output, count=2_000, seed=7)
        rows = []
        for split in ("train", "validation", "test"):
            rows.extend(
                json.loads(line)
                for line in (output / f"{split}.jsonl").read_text(encoding="utf-8").splitlines()
            )
        assert manifest["records"] == 2_000
        assert manifest["campaign_dataset_version"] == "campaign-events-v3"
        assert sum(manifest["split_counts"].values()) == 2_000
        assert any(row["expected_campaign"] for row in rows)
        assert any(row["scenario"] == "legitimate_launch_burst" for row in rows)
        assert {row["scenario"] for row in rows} == {
            "organic",
            "legitimate_launch_burst",
            "coordinated_positive",
            "coordinated_negative",
            "paraphrased_campaign",
            "off_hour_campaign",
            "slow_drip_campaign",
            "multi_product_campaign",
        }
        assert all(row["launch_time"] for row in rows)
        assert all(row["timestamp"] and row["text"] for row in rows)
        assert all(
            "2025-01-01T00:00:00Z" <= row["timestamp"] <= "2025-12-31T23:59:59Z"
            for row in rows
        )
        assert all(
            row["timestamp_bound_policy"]
            == "reflect_into_product_and_category_observation_window"
            for row in rows
        )
        assert all(row["temporal_feature_version"] for row in rows)
        assert all(
            row["text_provenance"]
            == f"deterministic_disjoint_{row['split']}_template_family"
            for row in rows
        )
        assert all(not row["is_pre_launch_review"] for row in rows)
        assert all(row["hours_since_launch"] >= 0 for row in rows)
        assert all("product_reviews_previous_1h" in row for row in rows)
        assert all("user_reviews_previous_24h" in row for row in rows)
        assert any(row["product_reviews_previous_1h"] > 0 for row in rows)
        for split in ("train", "validation", "test"):
            assert {row["scenario"] for row in rows if row["split"] == split} == {
                "organic",
                "legitimate_launch_burst",
                "coordinated_positive",
                "coordinated_negative",
                "paraphrased_campaign",
                "off_hour_campaign",
                "slow_drip_campaign",
                "multi_product_campaign",
            }
        group_splits = defaultdict(set)
        groups = defaultdict(list)
        for row in rows:
            group_splits[row["scenario_group_id"]].add(row["split"])
            if row["campaign_id"]:
                groups[row["campaign_id"]].append(row)
        assert all(len(splits) == 1 for splits in group_splits.values())
        assert groups
        assert all(len(group) >= 5 for group in groups.values())
        assert all(
            len({row["product_id"] for row in group}) == 1
            for group in groups.values()
            if group[0]["scenario"] != "multi_product_campaign"
        )
        assert all(
            len({row["product_id"] for row in group}) == 3
            for group in groups.values()
            if group[0]["scenario"] == "multi_product_campaign"
        )
        assert all(len({row["scenario"] for row in group}) == 1 for group in groups.values())
        assert all(
            len({row["user_id"] for row in group}) == len(group) for group in groups.values()
        )

        text_by_split = {
            split: {row["text"] for row in rows if row["split"] == split}
            for split in ("train", "validation", "test")
        }
        assert not text_by_split["train"] & text_by_split["validation"]
        assert not text_by_split["train"] & text_by_split["test"]
        assert not text_by_split["validation"] & text_by_split["test"]

        group_end = defaultdict(dict)
        for row in rows:
            scenario = row["scenario"]
            group_id = row["scenario_group_id"]
            group_end[scenario][group_id] = max(
                group_end[scenario].get(group_id, ""), row["timestamp"]
            )
        for scenario, endings in group_end.items():
            split_endings = {
                split: [
                    ending
                    for group_id, ending in endings.items()
                    if group_splits[group_id] == {split}
                ]
                for split in ("train", "validation", "test")
            }
            assert max(split_endings["train"]) <= min(split_endings["validation"])
            assert max(split_endings["validation"]) <= min(split_endings["test"])
    finally:
        for path in (profile, products):
            path.unlink(missing_ok=True)
        for name in ("train.jsonl", "validation.jsonl", "test.jsonl", "manifest.json"):
            (output / name).unlink(missing_ok=True)
        if output.exists():
            output.rmdir()
