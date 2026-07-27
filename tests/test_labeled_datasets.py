import json
import os
from pathlib import Path

import pytest

from bot_campaign.labeled_datasets import (
    build_training_set,
    canonical_product_review,
    prepare_product_reviews,
)


def test_product_review_boolean_maps_to_deceptive_label():
    row = canonical_product_review(
        {"text": "Excellent quality for this product.", "is_deceptive": True}, 1
    )
    assert row["label"] == 1
    assert row["source"] == "local-product-reviews"
    assert row["category"] is None
    assert row["rating"] is None
    assert row["field_provenance"] == {"text": "observed", "label": "observed"}
    assert "user_id" not in row and "timestamp" not in row


def test_kaggle_product_fields_are_preserved_without_fabricated_behavior():
    first = canonical_product_review(
        {
            "category": "Electronics_5",
            "rating": 5.0,
            "label": "CG",
            "text_": "The battery life is excellent for daily use.",
        }
    )
    second = canonical_product_review(
        {
            "category": "Home_and_Kitchen_5",
            "rating": 2.0,
            "label": "OR",
            "text_": "The pan handle became loose after one week.",
        }
    )
    assert first["label"] == 1 and second["label"] == 0
    assert first["source"] == "kaggle-mexwell-fake-reviews"
    assert first["category"] == "electronics"
    assert first["rating"] == 5.0
    assert first["field_provenance"]["rating"] == "observed"
    assert "user_id" not in first and "product_id" not in first
    assert "timestamp" not in first and "launch_time" not in first


def test_prepare_combines_local_jsonl_and_kaggle_csv():
    stem = f"_test_combined_{os.getpid()}"
    local = Path(f"data/processed/{stem}_local.jsonl")
    kaggle = Path(f"data/processed/{stem}_kaggle.csv")
    output = Path(f"data/processed/{stem}_output")
    try:
        local.write_text(
            json.dumps(
                {"text": "Useful item that arrived in sturdy packaging.", "is_deceptive": False}
            )
            + "\n",
            encoding="utf-8",
        )
        kaggle.write_text(
            'category,rating,label,text_\nElectronics_5,5,CG,"Perfect camera, buy it now!"\n',
            encoding="utf-8",
        )
        manifest = prepare_product_reviews([local, kaggle], output, seed=42)
        assert manifest["records"] == 2
        assert manifest["sources"] == {
            "local-product-reviews": 1,
            "kaggle-mexwell-fake-reviews": 1,
        }
        records = [
            json.loads(line)
            for split in ("train", "validation", "test")
            for line in (output / f"{split}.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert {record["category"] for record in records} == {
            "electronics",
            None,
        }
        assert all(record["field_provenance"] for record in records)
    finally:
        local.unlink(missing_ok=True)
        kaggle.unlink(missing_ok=True)
        for name in ("train.jsonl", "validation.jsonl", "test.jsonl", "manifest.json"):
            (output / name).unlink(missing_ok=True)
        if output.exists():
            output.rmdir()


def test_prepare_product_reviews_is_deterministic_and_split_safe():
    stem = f"_test_product_{os.getpid()}"
    source = Path(f"data/processed/{stem}.jsonl")
    first, second = Path(f"data/processed/{stem}_first"), Path(f"data/processed/{stem}_second")
    try:
        rows = [
            {"text": f"Unique product review number {index}", "is_deceptive": index % 2 == 0}
            for index in range(100)
        ]
        rows.append(rows[0])
        source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        first_manifest = prepare_product_reviews(source, first, seed=7)
        second_manifest = prepare_product_reviews(source, second, seed=7)

        assert first_manifest["records"] == 100
        assert first_manifest["duplicates"] == 1
        assert first_manifest["sha256"] == second_manifest["sha256"]
        split_ids = {
            split: {
                json.loads(line)["review_id"]
                for line in (first / f"{split}.jsonl").read_text(encoding="utf-8").splitlines()
            }
            for split in ("train", "validation", "test")
        }
        assert split_ids["train"].isdisjoint(split_ids["validation"] | split_ids["test"])
        assert split_ids["validation"].isdisjoint(split_ids["test"])
    finally:
        source.unlink(missing_ok=True)
        for directory in (first, second):
            for name in ("train.jsonl", "validation.jsonl", "test.jsonl", "manifest.json"):
                (directory / name).unlink(missing_ok=True)
            if directory.exists():
                directory.rmdir()


def test_training_mix_caps_synthetic_rows():
    synthetic = Path("data/processed/_test_mix_synthetic.jsonl")
    output = Path("data/processed/_test_training_mix.jsonl")
    try:
        with synthetic.open("w", encoding="utf-8") as handle:
            for index in range(20):
                row = {
                    "review_id": f"synthetic-{index}",
                    "text": f"Unique generated review {index}",
                    "synthetic": True,
                    "split": "train",
                }
                handle.write(json.dumps(row) + "\n")
        manifest = build_training_set(
            ["data/sample/labeled_reviews.jsonl"],
            synthetic,
            output,
            max_synthetic_fraction=0.25,
        )
        assert manifest["synthetic_fraction"] <= 0.25
        assert manifest["real_records"] == 20
    finally:
        for path in (
            synthetic,
            output,
            output.with_suffix(output.suffix + ".manifest.json"),
        ):
            path.unlink(missing_ok=True)


def test_training_mix_rejects_real_holdout_rows():
    stem = f"_test_holdout_{os.getpid()}"
    real = Path(f"data/processed/{stem}_real.jsonl")
    synthetic = Path(f"data/processed/{stem}_synthetic.jsonl")
    output = Path(f"data/processed/{stem}_output.jsonl")
    try:
        real.write_text(
            json.dumps({"review_id": "real-1", "text": "A held out review", "split": "test"})
            + "\n",
            encoding="utf-8",
        )
        synthetic.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="validation/test"):
            build_training_set([real], synthetic, output)
    finally:
        for path in (real, synthetic, output, output.with_suffix(".jsonl.manifest.json")):
            path.unlink(missing_ok=True)
