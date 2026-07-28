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
    transformer = service.settings.review_transformer_path
    active_path = transformer if (transformer / "bundle.json").exists() else service.settings.model_path
    return {
        "status": "ready",
        "model_path": str(active_path),
        "model_version": service.model_version,
    }


@router.get("/v1/operations", include_in_schema=False)
@router.get("/v1/ops/summary")
def operations(service: TrustRuntime = Depends(runtime)):
    return service.operations()


@router.get("/v1/monitoring", include_in_schema=False)
@router.get("/v1/monitoring/summary")
def monitoring(service: TrustRuntime = Depends(runtime)):
    return service.monitoring()


@router.get("/v1/lineage", include_in_schema=False)
@router.get("/v1/lineage/current")
def lineage(service: TrustRuntime = Depends(runtime)):
    return service.lineage()


@router.get("/v1/lineage/predictions/{review_id}")
def prediction_lineage(review_id: str, service: TrustRuntime = Depends(runtime)):
    record = service.repository.get_review(review_id)
    if not record:
        raise HTTPException(status_code=404, detail="Prediction lineage not found")
    return record["lineage"]


@router.get("/metrics", response_class=PlainTextResponse)
def metrics(service: TrustRuntime = Depends(runtime)):
    return prometheus_text(service.monitoring())
