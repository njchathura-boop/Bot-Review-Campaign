from bot_campaign.amazon import canonical_amazon_review


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
    assert mapped["review_id"].startswith("amazon-")

