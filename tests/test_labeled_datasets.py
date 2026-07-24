import json
from pathlib import Path

from bot_campaign.labeled_datasets import (
    build_training_set,
    canonical_maide,
    canonical_ott_file,
)


def test_maide_source_one_maps_to_fake():
    row = canonical_maide(
        {"Upside_Review": "Excellent location", "Downside_Review": "", "Hotel Name": "Hotel A",
         "City Name": "Paris", "Review_Score": 9, "source": 1, "Review_Language": "English", "Sentiment": "POS"}, 1
    )
    assert row["label"] == 1
    assert row["rating"] == 4.5
    assert row["source"] == "maide-up-ai"
    assert row["synthetic_timestamp"] is True


def test_ott_directory_structure_preserves_label_and_group():
    root = Path("tests/fixtures/ott")
    deceptive = canonical_ott_file(
        root / "positive_polarity/deceptive_from_MTurk/fold1/d_hilton_1.txt", root
    )
    truthful = canonical_ott_file(
        root / "negative_polarity/truthful_from_Web/fold2/t_hilton_2.txt", root
    )
    assert deceptive["label"] == 1 and truthful["label"] == 0
    assert deceptive["group_id"] == truthful["group_id"] == "ott:hilton"


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
