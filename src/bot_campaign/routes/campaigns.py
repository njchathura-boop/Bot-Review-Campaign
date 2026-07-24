from fastapi import APIRouter, Depends, HTTPException

from ..runtime import TrustRuntime
from ..schemas import ModerationDecision
from .dependencies import runtime


router = APIRouter(prefix="/v1/campaigns", tags=["campaigns"])


@router.get("")
def campaigns(service: TrustRuntime = Depends(runtime)):
    items = service.repository.campaigns()
    return {"items": items, "count": len(items)}


@router.get("/{campaign_id}")
def campaign(campaign_id: str, service: TrustRuntime = Depends(runtime)):
    result = service.repository.get_campaign(campaign_id)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result


@router.post("/{campaign_id}/decision")
def moderate_campaign(
    campaign_id: str,
    decision: ModerationDecision,
    service: TrustRuntime = Depends(runtime),
):
    result = service.moderate(
        campaign_id, decision.decision, decision.moderator, decision.reason
    )
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result
