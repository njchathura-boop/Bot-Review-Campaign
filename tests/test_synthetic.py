import json
import os
from pathlib import Path

from bot_campaign.synthetic import generate_dataset


def test_synthetic_generation_is_reproducible_and_group_safe():
    counts = {
        "organic": 20,
        "coordinated_positive": 20,
        "coordinated_negative": 20,
        "paraphrased": 20,
        "slow_drip": 20,
        "multi_product": 20,
        "legitimate_burst": 20,
    }
    stem = f"_test_synthetic_{os.getpid()}"
    paths = [
        Path(f"data/processed/{stem}_first.jsonl"),
        Path(f"data/processed/{stem}_second.jsonl"),
    ]
    try:
        first = generate_dataset(paths[0], seed=7, counts=counts)
        second = generate_dataset(paths[1], seed=7, counts=counts)
        assert first["records"] == 140
        assert first["sha256"] == second["sha256"]

        rows = [
            json.loads(line)
            for line in paths[0].read_text(encoding="utf-8").splitlines()
        ]
        group_splits = {}
        for row in rows:
            group_splits.setdefault(row["group_id"], set()).add(row["split"])
        assert all(len(splits) == 1 for splits in group_splits.values())
        assert {row["synthetic_role"] for row in rows} == {
            "organic",
            "deceptive",
            "legitimate_burst",
        }
    finally:
        for path in paths:
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".manifest.json").unlink(missing_ok=True)
