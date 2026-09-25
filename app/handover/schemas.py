from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

EVENT_TYPES = {"collected", "sealed", "box_changed", "received", "note"}


class HandoverEvent(BaseModel):
    event_id: str = Field(min_length=2, max_length=100)
    event_type: str = Field(min_length=2, max_length=40)
    sample_code: str = Field(min_length=1, max_length=100)
    batch_code: str = Field(min_length=1, max_length=64)
    occurred_at: str = Field(min_length=10, max_length=40)
    actor: str = Field(default="", max_length=100)
    detail: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_type(self):
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"event_type 必须是 {sorted(EVENT_TYPES)} 之一")
        return self


class HandoverPackageIn(BaseModel):
    station_code: str = Field(min_length=2, max_length=64)
    station_seq: int = Field(ge=0)
    prev_digest: str = Field(default="", max_length=128)
    participants: list[str] = Field(min_length=1, max_length=50)
    events: list[HandoverEvent] = Field(min_length=1, max_length=500)
    digest: str = Field(min_length=8, max_length=128)


class DisputeDecisionIn(BaseModel):
    decision: Literal["adopt", "reject", "adopt_branch", "waive", "anchor"]
    note: str = Field(default="", max_length=500)
    # adopt_branch/adopt 时给出所选候选（见 dispute.detail.options 的 key）
    branch_digest: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def _validate_branch(self):
        if self.decision == "adopt_branch" and not self.branch_digest:
            raise ValueError("adopt_branch 必须提供 branch_digest")
        return self


class FormalReceiveIn(BaseModel):
    sample_type: str = Field(default="现场样品", min_length=1, max_length=100)
    quantity: float = Field(default=1.0, gt=0)
    unit: str = Field(default="份", min_length=1, max_length=20)
    location_id: int | None = Field(default=None, gt=0)
