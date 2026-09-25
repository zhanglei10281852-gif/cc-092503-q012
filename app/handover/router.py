from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.handover.engine import HandoverMergeService
from app.handover.schemas import DisputeDecisionIn, FormalReceiveIn, HandoverPackageIn

router = APIRouter(prefix="/api/handover", tags=["离线交接归并"])


@router.post("/packages", status_code=status.HTTP_201_CREATED)
def ingest_package(payload: HandoverPackageIn, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return HandoverMergeService(connection).ingest(principal, payload.model_dump())


@router.get("/stations")
def list_stations(principal: Principal = Depends(current_principal)):
    return {"stations": HandoverMergeService(get_connection()).list_stations(principal)}


@router.get("/stations/{station_code}")
def station_overview(station_code: str, principal: Principal = Depends(current_principal)):
    return HandoverMergeService(get_connection()).station_view(principal, station_code)


@router.get("/chains/{chain_id}")
def chain_detail(chain_id: int, principal: Principal = Depends(current_principal)):
    return HandoverMergeService(get_connection()).chain_view(principal, chain_id)


@router.get("/disputes")
def list_disputes(
    station_code: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    principal: Principal = Depends(current_principal),
):
    return {
        "disputes": HandoverMergeService(get_connection()).list_disputes(
            principal, station_code, status_filter
        )
    }


@router.post("/disputes/{dispute_id}/decisions")
def decide_dispute(
    dispute_id: int,
    payload: DisputeDecisionIn,
    principal: Principal = Depends(current_principal),
):
    with transaction(immediate=True) as connection:
        return HandoverMergeService(connection).decide(
            principal, dispute_id, payload.model_dump()
        )


@router.post("/chains/{chain_id}/receive", status_code=status.HTTP_201_CREATED)
def formal_receive(
    chain_id: int,
    payload: FormalReceiveIn,
    principal: Principal = Depends(current_principal),
):
    with transaction(immediate=True) as connection:
        return HandoverMergeService(connection).formal_receive(
            principal, chain_id, payload.model_dump()
        )
