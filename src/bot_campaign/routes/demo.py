from fastapi import APIRouter, Depends, HTTPException

from ..runtime import TrustRuntime
from ..schemas import DemoReplayRequest
from .dependencies import runtime


router = APIRouter(prefix="/v1/demo", tags=["demo"])


@router.post("/replay")
def start_replay(
    request: DemoReplayRequest, service: TrustRuntime = Depends(runtime)
):
    return service.replay(request.scenario)


@router.get("/replay/{job_id}")
def replay_status(job_id: str, service: TrustRuntime = Depends(runtime)):
    result = service.repository.get_replay(job_id)
    if not result:
        raise HTTPException(status_code=404, detail="Replay job not found")
    return result
