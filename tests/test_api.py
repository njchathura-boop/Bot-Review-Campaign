from fastapi.testclient import TestClient

from bot_campaign.api import create_app
from bot_campaign.config import Settings
from bot_campaign.data import load_reviews
from bot_campaign.model import ReviewScorer, train
from bot_campaign.runtime import TrustRuntime


reviews, _ = load_reviews("data/sample/labeled_reviews.jsonl", labeled=True)
bundle, _ = train(reviews, seed=42)
test_runtime = TrustRuntime(Settings.from_env(), scorer=ReviewScorer(bundle))
client = TestClient(create_app(runtime=test_runtime))


def test_health_endpoint():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"


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
    assert client.get("/v1/reviews/versioned-1/lineage").status_code == 200
    assert client.get("/v1/monitoring").json()["reviews_processed"] >= 1
    assert "review_scans_total" in client.get("/metrics").text


def test_demo_replay_populates_campaign_console():
    response = client.post("/v1/demo/replay", json={"scenario": "coordinated-positive"})
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    campaigns = client.get("/v1/campaigns").json()
    assert campaigns["count"] >= 1


def test_operations_never_claim_unchecked_dependencies_are_healthy():
    response = client.get("/v1/operations")
    assert response.status_code == 200
    services = response.json()["services"]
    external = [item for item in services if item["name"] not in {"FastAPI", "Model"}]
    assert all(item["status"] in {"configured", "not_configured"} for item in external)
