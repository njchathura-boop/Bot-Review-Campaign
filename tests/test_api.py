from fastapi.testclient import TestClient

from bot_campaign.api import app


client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_campaign_endpoint():
    reviews = [
        {
            "review_id": f"a{index}", "user_id": f"u{index}", "product_id": "p1",
            "text": "Amazing sound quality and excellent battery life highly recommended",
            "rating": 5, "timestamp": f"2025-01-01T10:0{index}:00Z",
            "verified_purchase": False, "helpful_votes": 0,
        }
        for index in range(3)
    ]
    response = client.post("/detect_campaign", json={"reviews": reviews})
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_versioned_score_monitoring_and_lineage():
    response = client.post(
        "/v1/reviews/score",
        json={
            "review_id": "versioned-1",
            "user_id": "u-versioned",
            "product_id": "p-versioned",
            "text": "The battery lasted all week but the clasp is difficult to close.",
            "rating": 4,
            "timestamp": "2025-01-01T10:00:00Z",
            "verified_purchase": True,
            "helpful_votes": 1,
            "language": "en",
        },
    )
    assert response.status_code == 200
    assert response.json()["api_schema_version"] == "reviews.scored.v1"
    assert client.get("/v1/reviews/versioned-1/trust").status_code == 200
    assert client.get("/v1/lineage/predictions/versioned-1").status_code == 200
    assert client.get("/v1/monitoring/summary").json()["reviews_processed"] >= 1
    assert "review_scans_total" in client.get("/metrics").text


def test_demo_replay_populates_campaign_console():
    response = client.post("/v1/demo/replay", json={"scenario": "coordinated-positive"})
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    campaigns = client.get("/v1/campaigns").json()
    assert campaigns["count"] >= 1
