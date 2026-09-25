from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.handover import HandoverService
from app.samples.handover_schemas import ConflictDecision, HandoverPackageCreate

router = APIRouter(prefix="/api/handover", tags=["离线交接包"])


@router.post("/packages", status_code=status.HTTP_201_CREATED)
def upload_package(payload: HandoverPackageCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return HandoverService(connection).upload(principal, payload.model_dump())


@router.get("/packages")
def list_packages(station_code: str | None = Query(default=None), principal: Principal = Depends(current_principal)):
    return HandoverService(get_connection()).list_packages(principal, station_code)


@router.get("/packages/{package_id}")
def get_package(package_id: int, principal: Principal = Depends(current_principal)):
    return HandoverService(get_connection()).get_package(principal, package_id)


@router.get("/chains/{station_code}")
def chain_view(station_code: str, principal: Principal = Depends(current_principal)):
    return HandoverService(get_connection()).chain_view(principal, station_code)


@router.get("/chains/{station_code}/samples/{sample_ref}")
def sample_chain_view(station_code: str, sample_ref: str, principal: Principal = Depends(current_principal)):
    return HandoverService(get_connection()).sample_view(principal, station_code, sample_ref)


@router.post("/chains/{station_code}/samples/{sample_ref}/promote", status_code=status.HTTP_201_CREATED)
def promote_sample(station_code: str, sample_ref: str, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return HandoverService(connection).promote(principal, station_code, sample_ref)


@router.get("/conflicts")
def list_conflicts(
    state: str | None = Query(default=None),
    conflict_type: str | None = Query(default=None),
    principal: Principal = Depends(current_principal),
):
    return HandoverService(get_connection()).list_conflicts(principal, state, conflict_type)


@router.post("/conflicts/{conflict_id}/decision")
def decide_conflict(conflict_id: int, payload: ConflictDecision, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return HandoverService(connection).decide(principal, conflict_id, payload.model_dump())
