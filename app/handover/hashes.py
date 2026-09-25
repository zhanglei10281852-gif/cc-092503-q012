from __future__ import annotations

import hashlib
import json
from typing import Any

GENESIS_DIGEST = ""


def canonical_json(value: Any) -> str:
    """离线设备与服务共用的规范序列化：字典键排序、紧凑分隔、保留数组顺序。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def package_digest(envelope: dict[str, Any]) -> str:
    """对交接包的链式载荷求摘要，不含包自带的 digest 字段。"""
    payload = {key: envelope[key] for key in ("station_code", "station_seq", "prev_digest", "participants", "events")}
    return sha256_hex(canonical_json(payload))


def event_digest(event: dict[str, Any]) -> str:
    return sha256_hex(canonical_json(event))


def short(digest: str | None) -> str:
    return (digest or "")[:12]
