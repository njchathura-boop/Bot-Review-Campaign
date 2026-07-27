from fastapi.testclient import TestClient
from types import SimpleNamespace

import numpy as np

from bot_campaign.api import create_app
from bot_campaign.config import Settings
from bot_campaign.data import load_text_labels
from bot_campaign.model import ReviewScorer, train
from bot_campaign.runtime import TrustRuntime


reviews, _ = load_text_labels("data/sample/labeled_reviews.jsonl")
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
    assert client.get("/v1/lineage/predictions/versioned-1").status_code == 200
    assert client.get("/v1/lineage/current").status_code == 200
    assert client.get("/v1/monitoring").json()["reviews_processed"] >= 1
    assert client.get("/v1/monitoring/summary").status_code == 200
    assert client.get("/v1/ops/summary").status_code == 200
    assert "review_scans_total" in client.get("/metrics").text


def test_demo_replay_populates_campaign_console():
    response = client.post("/v1/demo/replay", json={"scenario": "coordinated-positive"})
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    campaigns = client.get("/v1/campaigns").json()
    assert campaigns["count"] >= 1


class _CampaignScorer:
    config = SimpleNamespace(feature_version="campaign-group-v1", threshold=0.8)

    def embed_texts(self, texts):
        return np.asarray([[1.0, index * 0.01] for index, _ in enumerate(texts)])

    def predict_with_review_embeddings(self, groups, embeddings):
        return np.asarray([0.97] * len(groups), dtype=np.float32)


def test_cross_product_replay_uses_hybrid_campaign_model_when_loaded():
    runtime = TrustRuntime(
        Settings.from_env(),
        scorer=ReviewScorer(bundle),
        campaign_scorer=_CampaignScorer(),
    )
    hybrid_client = TestClient(create_app(runtime=runtime))
    replay = hybrid_client.post(
        "/v1/demo/replay", json={"scenario": "coordinated-cross-product"}
    ).json()
    assert replay["campaign_engine"] == "hybrid-distilbert"
    assert replay["hybrid_scores"][0]["campaign_scope"] == "cross_product"
    campaigns = hybrid_client.get("/v1/campaigns").json()["items"]
    assert any(item["evidence"].get("model") == "bot-campaign-hybrid-distilbert" for item in campaigns)


def test_operations_never_claim_unchecked_dependencies_are_healthy():
    response = client.get("/v1/operations")
    assert response.status_code == 200
    services = response.json()["services"]
    external = [
        item
        for item in services
        if item["name"] not in {"FastAPI", "Review model", "Campaign model"}
    ]
    assert all(item["status"] in {"configured", "not_configured"} for item in external)
