import json
import os
from pathlib import Path

from bot_campaign.synthetic import augment_product_reviews


def test_product_augmentation_is_balanced_reproducible_and_train_only():
    stem = f"_test_augmentation_{os.getpid()}"
    source = Path(f"data/processed/{stem}_train.jsonl")
    first = Path(f"data/processed/{stem}_first.jsonl")
    second = Path(f"data/processed/{stem}_second.jsonl")
    try:
        rows = []
        for index in range(10):
            rows.append(
                {
                    "review_id": f"real-{index}",
                    "text": f"This product has great quality and works well. Example {index}.",
                    "label": index % 2,
                    "source": "local-product-reviews",
                    "group_id": f"real-{index}",
                    "synthetic": False,
                    "split": "train",
                }
            )
        source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        first_manifest = augment_product_reviews(source, first, count=20, seed=9)
        second_manifest = augment_product_reviews(source, second, count=20, seed=9)

        assert first_manifest["label_counts"] == {"genuine": 10, "deceptive": 10}
        assert first_manifest["sha256"] == second_manifest["sha256"]
        generated = [json.loads(line) for line in first.read_text(encoding="utf-8").splitlines()]
        assert all(row["synthetic"] is True and row["split"] == "train" for row in generated)
    finally:
        for path in (source, first, second):
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".manifest.json").unlink(missing_ok=True)
