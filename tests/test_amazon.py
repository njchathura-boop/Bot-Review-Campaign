import json

import bot_campaign.amazon as amazon_module
from bot_campaign.amazon import (
    DATASET_ID,
    DATASET_REVISION,
    canonical_amazon_review,
    stream_amazon,
)


def test_amazon_mapping_uses_parent_asin_and_millisecond_timestamp():
    mapped = canonical_amazon_review(
        {
            "rating": 5.0, "title": "Useful", "text": "Worked for my needs",
            "asin": "CHILD", "parent_asin": "PARENT", "user_id": "USER",
            "timestamp": 1588687728923, "helpful_vote": 2, "verified_purchase": True,
        },
        "All_Beauty",
    )
    assert mapped["product_id"] == "PARENT"
    assert mapped["helpful_votes"] == 2
    assert mapped["schema_version"] == 1
    assert mapped["source_revision"] == DATASET_REVISION
    assert mapped["review_id"].startswith("amazon-")


def test_amazon_stream_reads_only_requested_lines_from_pinned_category(monkeypatch):
    source_rows = [
        {
            "rating": 4.0,
            "title": "Useful",
            "text": f"Worked for my needs {index}",
            "asin": "CHILD",
            "parent_asin": "PARENT",
            "user_id": f"USER-{index}",
            "timestamp": 1_588_687_728_923 + index,
            "helpful_vote": 2,
            "verified_purchase": True,
        }
        for index in range(3)
    ]
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def __iter__(self):
            return iter((json.dumps(row) + "\n").encode() for row in source_rows)

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        return FakeResponse()

    monkeypatch.setattr(amazon_module, "urlopen", fake_urlopen)
    rows = list(stream_amazon("Amazon_Fashion", limit=1))

    assert len(rows) == 1
    assert calls[0] == (
        f"https://huggingface.co/datasets/{DATASET_ID}/resolve/{DATASET_REVISION}/"
        "raw/review_categories/Amazon_Fashion.jsonl",
        amazon_module.DOWNLOAD_TIMEOUT_SECONDS,
    )
