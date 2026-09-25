from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class HandoverEvent(BaseModel):
    event_id: str = Field(min_length=4, max_length=100)
    sample_ref: str = Field(min_length=3, max_length=100)
    event_type: Literal["collected", "sealed", "reboxed", "received"]
    occurred_at: str = Field(min_length=10, max_length=40)
    actor: str = Field(min_length=1, max_length=100)
    batch_code: str = Field(min_length=3, max_length=64)
    details: dict[str, Any] = Field(default_factory=dict)


class HandoverPackageCreate(BaseModel):
    package_code: str = Field(min_length=4, max_length=100)
    station_code: str = Field(min_length=2, max_length=64)
    station_seq: int = Field(ge=1, le=1_000_000)
    prev_digest: str = Field(default="", max_length=64)
    package_digest: str | None = Field(default=None, min_length=64, max_length=64)
    participants: list[str] = Field(min_length=1, max_length=50)
    events: list[HandoverEvent] = Field(min_length=1, max_length=500)
    packaged_at: str = Field(min_length=10, max_length=40)

    @field_validator("prev_digest")
    @classmethod
    def check_prev_digest(cls, value: str) -> str:
        if value and not DIGEST_PATTERN.match(value):
            raise ValueError("前序摘要必须是 64 位十六进制字符")
        return value

    @field_validator("package_digest")
    @classmethod
    def check_package_digest(cls, value: str | None) -> str | None:
        if value is not None and not DIGEST_PATTERN.match(value):
            raise ValueError("包摘要必须是 64 位十六进制字符")
        return value

    @field_validator("participants")
    @classmethod
    def check_participants(cls, value: list[str]) -> list[str]:
        if any(not item.strip() for item in value):
            raise ValueError("参与人不能为空字符串")
        return value


class ConflictDecision(BaseModel):
    action: Literal[
        "select_package", "accept_gap", "keep_first",
        "keep_occurrence", "accept_reference", "reject_event",
    ]
    selected_package_digest: str | None = Field(default=None, min_length=64, max_length=64)
    rationale: str = Field(min_length=2, max_length=500)
