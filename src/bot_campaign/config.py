from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from . import __version__


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    """Validated runtime configuration with safe local defaults."""

    app_version: str
    environment: str
    model_path: Path
    review_transformer_path: Path
    campaign_model_path: Path
    web_dir: Path
    dataset_version: str
    feature_version: str
    api_schema_version: str
    git_commit: str
    image_digest: str
    deployment_revision: str
    mlflow_run_id: str
    grafana_url: str
    grafana_health_url: str
    mlflow_url: str
    mlflow_health_url: str
    prometheus_url: str
    prometheus_health_url: str
    kibana_url: str
    kibana_health_url: str
    ray_dashboard_url: str
    ray_dashboard_health_url: str
    spark_master_ui_url: str
    campaign_candidate_limit: int
    streaming_enabled: bool
    kafka_bootstrap_servers: str
    raw_reviews_topic: str
    campaign_scores_topic: str
    campaign_alert_group_id: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_version=__version__,
            environment=os.getenv("APP_ENV", "local"),
            model_path=Path(
                os.getenv(
                    "MODEL_PATH",
                    str(PROJECT_ROOT / "artifacts" / "review_model.joblib"),
                )
            ),
            review_transformer_path=Path(
                os.getenv(
                    "REVIEW_TRANSFORMER_PATH",
                    str(PROJECT_ROOT / "artifacts" / "review_distilbert"),
                )
            ),
            campaign_model_path=Path(
                os.getenv(
                    "CAMPAIGN_MODEL_PATH",
                    str(PROJECT_ROOT / "artifacts" / "campaign_model"),
                )
            ),
            web_dir=Path(os.getenv("WEB_DIR", str(PROJECT_ROOT / "web"))),
            dataset_version=os.getenv("DATASET_VERSION", "demo-data-v1"),
            feature_version=os.getenv("FEATURE_VERSION", "feature-set-v2"),
            api_schema_version="reviews.scored.v1",
            git_commit=os.getenv("GIT_COMMIT", "local-uncommitted"),
            image_digest=os.getenv("IMAGE_DIGEST", "local-development"),
            deployment_revision=os.getenv(
                "DEPLOYMENT_REVISION", "docker-compose-local"
            ),
            mlflow_run_id=os.getenv("MLFLOW_RUN_ID", "local-baseline"),
            grafana_url=os.getenv("GRAFANA_URL", "http://localhost:3000"),
            grafana_health_url=os.getenv(
                "GRAFANA_HEALTH_URL", "http://localhost:3000/api/health"
            ),
            mlflow_url=os.getenv("MLFLOW_URL", "http://localhost:5001"),
            mlflow_health_url=os.getenv(
                "MLFLOW_HEALTH_URL", os.getenv("MLFLOW_URL", "http://localhost:5001")
            ),
            prometheus_url=os.getenv("PROMETHEUS_URL", "http://localhost:9090"),
            prometheus_health_url=os.getenv(
                "PROMETHEUS_HEALTH_URL", "http://localhost:9090/-/ready"
            ),
            kibana_url=os.getenv("KIBANA_URL", "http://localhost:5601"),
            kibana_health_url=os.getenv(
                "KIBANA_HEALTH_URL", "http://localhost:5601/api/status"
            ),
            ray_dashboard_url=os.getenv("RAY_DASHBOARD_URL", "http://localhost:8265"),
            ray_dashboard_health_url=os.getenv(
                "RAY_DASHBOARD_HEALTH_URL", "http://localhost:8265/api/jobs/"
            ),
            spark_master_ui_url=os.getenv(
                "SPARK_MASTER_UI_URL", "http://localhost:8082/json/"
            ),
            campaign_candidate_limit=max(
                20, min(int(os.getenv("CAMPAIGN_CANDIDATE_LIMIT", "100")), 500)
            ),
            streaming_enabled=os.getenv("CAMPAIGN_ALERTS_ENABLED", "false").lower()
            in {"1", "true", "yes", "on"},
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            raw_reviews_topic=os.getenv("KAFKA_RAW_REVIEWS_TOPIC", "reviews.raw.v1"),
            campaign_scores_topic=os.getenv(
                "KAFKA_CAMPAIGN_SCORES_TOPIC", "reviews.campaign-scores.v1"
            ),
            campaign_alert_group_id=os.getenv(
                "KAFKA_CAMPAIGN_ALERT_GROUP_ID", "campaign-api-materializer-v1"
            ),
        )
