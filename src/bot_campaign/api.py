from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .routes import campaigns, demo, operations, reviews
from .runtime import TrustRuntime


def create_app(
    settings: Settings | None = None, runtime: TrustRuntime | None = None
) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Review Trust API", version=settings.app_version)
    app.state.runtime = runtime or TrustRuntime(settings)
    app.include_router(reviews.router)
    app.include_router(campaigns.router)
    app.include_router(demo.router)
    app.include_router(operations.router)
    app.mount("/", StaticFiles(directory=settings.web_dir, html=True), name="web")
    return app


app = create_app()
