from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse

from ..observability import prometheus_text
from ..runtime import TrustRuntime
from .dependencies import runtime


router = APIRouter(tags=["operations"])


@router.get("/health/live")
def live(service: TrustRuntime = Depends(runtime)):
    return {"status": "alive", "version": service.settings.app_version}


@router.get("/health/ready")
def ready(service: TrustRuntime = Depends(runtime)):
    if not service.model_ready:
        raise HTTPException(status_code=503, detail="Model artifact is unavailable")
    return {"status": "ready", "model_path": service.settings.model_path.name}


@router.get("/v1/operations")
def operations(service: TrustRuntime = Depends(runtime)):
    return service.operations()


@router.get("/v1/monitoring")
def monitoring(service: TrustRuntime = Depends(runtime)):
    return service.monitoring()


@router.get("/v1/lineage")
def lineage(service: TrustRuntime = Depends(runtime)):
    return service.lineage()


@router.get("/metrics", response_class=PlainTextResponse)
def metrics(service: TrustRuntime = Depends(runtime)):
    return prometheus_text(service.monitoring())
