from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .routes import campaigns, demo, operations, reviews
from .runtime import TrustRuntime
from .streaming_runtime import KafkaCampaignAlertConsumer


def create_app(
    settings: Settings | None = None, runtime: TrustRuntime | None = None
) -> FastAPI:
    settings = settings or Settings.from_env()
    service = runtime or TrustRuntime(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        consumer = None
        if service.settings.streaming_enabled:
            service.set_stream_state("starting")
            consumer = KafkaCampaignAlertConsumer(
                bootstrap_servers=service.settings.kafka_bootstrap_servers,
                topic=service.settings.campaign_scores_topic,
                group_id=service.settings.campaign_alert_group_id,
                on_message=service.ingest_campaign_score,
                on_state=service.set_stream_state,
            )
            consumer.start()
            app.state.campaign_alert_consumer = consumer
        else:
            service.set_stream_state("disabled")
        try:
            yield
        finally:
            if consumer:
                consumer.stop()

    app = FastAPI(
        title="Detectra Review Intelligence API",
        version=settings.app_version,
        lifespan=lifespan,
    )
    app.state.runtime = service
    app.include_router(reviews.router)
    app.include_router(campaigns.router)
    app.include_router(demo.router)
    app.include_router(operations.router)
    app.mount("/", StaticFiles(directory=settings.web_dir, html=True), name="web")
    return app


app = create_app()
