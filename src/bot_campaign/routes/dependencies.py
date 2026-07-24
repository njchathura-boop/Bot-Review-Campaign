from fastapi import HTTPException, Request

from ..runtime import ModelUnavailableError, TrustRuntime
from ..schemas import Review, ReviewPrediction


def runtime(request: Request) -> TrustRuntime:
    return request.app.state.runtime


def score(runtime_service: TrustRuntime, review: Review) -> ReviewPrediction:
    try:
        return runtime_service.score(review)
    except ModelUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
