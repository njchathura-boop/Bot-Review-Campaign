from pathlib import Path

from bot_campaign.labeled_datasets import canonical_maide, canonical_ott_file


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
