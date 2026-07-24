from fastapi import APIRouter, Depends, HTTPException

from ..runtime import TrustRuntime
from ..schemas import BatchReviewRequest, Review, ReviewPrediction
from .dependencies import runtime, score


router = APIRouter(prefix="/v1/reviews", tags=["reviews"])


@router.post("/score", response_model=ReviewPrediction)
def score_review(
    review: Review, service: TrustRuntime = Depends(runtime)
) -> ReviewPrediction:
    return score(service, review)


@router.post("/batch-score", response_model=list[ReviewPrediction])
def score_reviews(
    request: BatchReviewRequest, service: TrustRuntime = Depends(runtime)
) -> list[ReviewPrediction]:
    return [score(service, review) for review in request.reviews]


@router.get("/recent")
def recent_reviews(limit: int = 50, service: TrustRuntime = Depends(runtime)):
    items = service.repository.recent(limit)
    return {"items": items, "count": len(items)}


@router.get("/{review_id}/trust")
def review_trust(review_id: str, service: TrustRuntime = Depends(runtime)):
    record = service.repository.get_review(review_id)
    if not record:
        raise HTTPException(status_code=404, detail="Review has not been scanned")
    return record


@router.get("/{review_id}/lineage")
def prediction_lineage(review_id: str, service: TrustRuntime = Depends(runtime)):
    record = service.repository.get_review(review_id)
    if not record:
        raise HTTPException(status_code=404, detail="Prediction lineage not found")
    return record["lineage"]
