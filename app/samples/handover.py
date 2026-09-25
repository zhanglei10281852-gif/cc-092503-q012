from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditService

STAGE_RANK = {"collected": 1, "sealed": 2, "reboxed": 3, "received": 4}
REQUIRED_STAGES = ("collected", "sealed", "received")
EVENT_TYPE_LABELS = {"collected": "采集", "sealed": "封签", "reboxed": "更换运输箱", "received": "到库签收"}
ACTION_LABELS = {
    "promote": "转入正式接收",
    "adjudicate": "处理待裁决冲突",
    "await_predecessor": "等待站点补传前序包",
    "await_events": "等待站点补传后续事件",
}
DECISION_ACTIONS = {
    "fork": {"select_package"},
    "sequence_gap": {"accept_gap"},
    "duplicate_event": {"keep_first", "keep_occurrence"},
    "cross_batch_reference": {"accept_reference", "reject_event"},
}


def canonical_payload(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "station_code": data["station_code"],
        "station_seq": data["station_seq"],
        "prev_digest": data.get("prev_digest") or "",
        "participants": list(data["participants"]),
        "events": data["events"],
        "packaged_at": data["packaged_at"],
    }


def compute_package_digest(data: dict[str, Any]) -> str:
    canonical = json.dumps(canonical_payload(data), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _loads(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default


class HandoverService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------ 上传与归并
    def upload(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("handover.upload")
        digest = compute_package_digest(data)
        declared = data.get("package_digest")
        if declared and declared != digest:
            raise ValidationError(
                "包摘要与内容不符，哈希链校验失败",
                context={"declared": declared, "computed": digest},
            )
        existing = self._package_by_code(data["package_code"])
        if existing:
            if existing["package_digest"] != digest:
                raise ConflictError("交接包标识已被不同内容占用", context={"package_code": data["package_code"]})
            return {"package": self._package_view(existing), "replayed": True, "chain": self._chain_view(existing["station_code"])}
        same_content = self._package_by_digest(digest)
        if same_content:
            return {"package": self._package_view(same_content), "replayed": True, "chain": self._chain_view(same_content["station_code"])}
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            """INSERT INTO handover_packages(package_code,station_code,station_seq,prev_digest,participants_json,
               events_json,packaged_at,package_digest,received_by,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                data["package_code"], data["station_code"], data["station_seq"], data.get("prev_digest") or "",
                json.dumps(data["participants"], ensure_ascii=False), json.dumps(data["events"], ensure_ascii=False),
                data["packaged_at"], digest, principal.user_id, now,
            ),
        )
        package = self._package_by_id(cursor.lastrowid)
        self._recompute(data["station_code"], now)
        self.audit.record(
            principal, "handover.upload", "handover_package", str(package["id"]),
            after=self._package_view(package),
            metadata={"station_code": data["station_code"], "station_seq": data["station_seq"]},
        )
        return {"package": self._package_view(package), "replayed": False, "chain": self._chain_view(data["station_code"])}

    def list_packages(self, principal: Principal, station_code: str | None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        if station_code:
            rows = self.connection.execute(
                "SELECT * FROM handover_packages WHERE station_code=? ORDER BY station_seq,package_digest", (station_code,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM handover_packages ORDER BY station_code,station_seq,package_digest"
            ).fetchall()
        return [self._package_view(dict(row)) for row in rows]

    def get_package(self, principal: Principal, package_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        return self._package_view(self._package_by_id(package_id))

    # ------------------------------------------------------------------ 裁决
    def list_conflicts(self, principal: Principal, state: str | None, conflict_type: str | None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        clauses, params = [], []
        if state:
            clauses.append("state=?")
            params.append(state)
        if conflict_type:
            clauses.append("conflict_type=?")
            params.append(conflict_type)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            f"SELECT * FROM handover_conflicts{where} ORDER BY state DESC, conflict_type, conflict_code", tuple(params)
        ).fetchall()
        return [self._conflict_view(dict(row)) for row in rows]

    def decide(self, principal: Principal, conflict_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("handover.adjudicate")
        row = self.connection.execute("SELECT * FROM handover_conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not row:
            raise NotFoundError("冲突记录不存在")
        conflict = dict(row)
        if conflict["state"] != "pending":
            raise ConflictError("冲突已裁决，不能重复处理")
        conflict_type = conflict["conflict_type"]
        action = data["action"]
        if action not in DECISION_ACTIONS[conflict_type]:
            raise ValidationError(f"{conflict_type} 类型的冲突不支持 {action} 处理方式")
        details = _loads(conflict["details_json"], {})
        selected = data.get("selected_package_digest")
        if action in {"select_package", "keep_occurrence"}:
            if not selected:
                raise ValidationError("必须指定选定的包摘要")
            candidates = {item["package_digest"] for item in details.get("candidates", [])}
            candidates |= {item["package_digest"] for item in details.get("occurrences", [])}
            if selected not in candidates:
                raise ValidationError("选定的包摘要不在冲突候选中")
        now = to_storage(self.clock.now())
        resolution = {
            "manner": "manual",
            "action": action,
            "selected_package_digest": selected,
            "rationale": data["rationale"],
            "decided_by": principal.user_id,
            "decided_at": now,
        }
        self.connection.execute(
            "UPDATE handover_conflicts SET state='resolved',resolution_json=?,resolved_by=?,resolved_at=?,updated_at=? WHERE id=?",
            (json.dumps(resolution, ensure_ascii=False), principal.user_id, now, now, conflict_id),
        )
        stations = {conflict["station_code"]}
        stations |= {item["station_code"] for item in details.get("occurrences", [])}
        for station_code in sorted(stations):
            self._recompute(station_code, now)
        self.audit.record(
            principal, "handover.decide", "handover_conflict", str(conflict_id),
            before=conflict, after=self._conflict_view(self._conflict_by_id(conflict_id)),
            metadata={"action": action},
        )
        return {
            "conflict": self._conflict_view(self._conflict_by_id(conflict_id)),
            "chains": [self._chain_view(station_code) for station_code in sorted(stations)],
        }

    # ------------------------------------------------------------------ 正式接收
    def promote(self, principal: Principal, station_code: str, sample_ref: str) -> dict[str, Any]:
        principal.require("samples.write")
        chain = self._sample_chain_row(station_code, sample_ref)
        if chain["status"] == "received":
            sample = SampleRepository(self.connection).get(chain["received_sample_id"])
            return {"sample": sample, "chain": self._sample_view(chain), "replayed": True}
        if chain["status"] != "chain_ready":
            raise ConflictError(
                "保管链尚未形成唯一连续链，禁止正式接收",
                context={"status": chain["status"], "issues": _loads(chain["issues_json"], [])},
            )
        events = _loads(chain["events_json"], [])
        collected = next(item for item in events if item["event_type"] == "collected")
        received = next(item for item in events if item["event_type"] == "received")
        details = collected["details"]
        quantity = float(details.get("quantity") or 0)
        if quantity <= 0:
            raise ValidationError("采集事件缺少有效数量，无法正式接收")
        location_id = received["details"].get("location_id")
        if location_id is not None:
            found = self.connection.execute("SELECT id FROM storage_locations WHERE id=?", (location_id,)).fetchone()
            if not found:
                raise ValidationError("签收事件指定的保管位置不存在", context={"location_id": location_id})
        if self.connection.execute("SELECT id FROM samples WHERE sample_code=?", (sample_ref,)).fetchone():
            raise ConflictError("样品编码已存在，无法通过交接链重复接收", context={"sample_ref": sample_ref})
        now = to_storage(self.clock.now())
        batch_code = chain["batch_code"]
        batch_row = self.connection.execute("SELECT * FROM receipt_batches WHERE batch_code=?", (batch_code,)).fetchone()
        if batch_row:
            batch_id = batch_row["id"]
        else:
            expected = self.connection.execute(
                "SELECT COUNT(*) FROM handover_sample_chains WHERE station_code=? AND batch_code=?",
                (station_code, batch_code),
            ).fetchone()[0]
            cursor = self.connection.execute(
                """INSERT INTO receipt_batches(batch_code,project_code,received_by,received_at,expected_count,status,qr_payload,created_at,updated_at)
                   VALUES(?,?,?,?,?,'open',?,?,?)""",
                (
                    batch_code, details.get("project_code") or "HANDOVER", principal.user_id, now,
                    max(1, int(expected)), f"handover-batch:{station_code}:{batch_code}", now, now,
                ),
            )
            batch_id = cursor.lastrowid
        cursor = self.connection.execute(
            """INSERT INTO samples(sample_code,batch_id,collection_event_id,parent_sample_id,root_sample_id,sample_type,
               quantity,unit,lifecycle_state,location_id,custody_user_id,lineage_depth,created_at,updated_at)
               VALUES(?,?,NULL,NULL,NULL,?,?,?,'received',?,?,0,?,?)""",
            (
                sample_ref, batch_id, details.get("sample_type") or "未分类", quantity,
                details.get("unit") or "份", location_id, principal.user_id, now, now,
            ),
        )
        sample_id = cursor.lastrowid
        self.connection.execute("UPDATE samples SET root_sample_id=? WHERE id=?", (sample_id, sample_id))
        correlation_id = f"handover:{station_code}:{sample_ref}"
        for event in events:
            self.connection.execute(
                """INSERT INTO sample_events(sample_id,event_type,actor_user_id,quantity_delta,from_state,to_state,details_json,correlation_id,occurred_at)
                   VALUES(?,?,NULL,0,NULL,?,?,?,?)""",
                (
                    sample_id, f"handover.{event['event_type']}",
                    "received" if event["event_type"] == "received" else None,
                    json.dumps(
                        {
                            "event_id": event["event_id"],
                            "actor": event["actor"],
                            "station_code": station_code,
                            "package_code": event["source"]["package_code"],
                            "station_seq": event["source"]["station_seq"],
                        },
                        ensure_ascii=False,
                    ),
                    correlation_id, event["occurred_at"],
                ),
            )
        accepted = self.connection.execute("SELECT COUNT(*) FROM samples WHERE batch_id=?", (batch_id,)).fetchone()[0]
        self.connection.execute("UPDATE receipt_batches SET accepted_count=?,updated_at=? WHERE id=?", (accepted, now, batch_id))
        self.connection.execute(
            "UPDATE handover_sample_chains SET status='received',received_sample_id=?,updated_at=? WHERE id=?",
            (sample_id, now, chain["id"]),
        )
        sample = SampleRepository(self.connection).get(sample_id)
        self.audit.record(
            principal, "handover.promote", "sample", str(sample_id),
            after=sample, metadata={"station_code": station_code, "sample_ref": sample_ref, "batch_code": batch_code},
        )
        return {"sample": sample, "chain": self._sample_view(self._sample_chain_row(station_code, sample_ref)), "replayed": False}

    # ------------------------------------------------------------------ 视图
    def chain_view(self, principal: Principal, station_code: str) -> dict[str, Any]:
        principal.require("samples.read")
        return self._chain_view(station_code)

    def sample_view(self, principal: Principal, station_code: str, sample_ref: str) -> dict[str, Any]:
        principal.require("samples.read")
        return self._sample_view(self._sample_chain_row(station_code, sample_ref))

    def _chain_view(self, station_code: str) -> dict[str, Any]:
        rows = self.connection.execute(
            """SELECT s.position,s.adoption,s.rationale,s.recomputed_at,p.id AS package_id,p.package_code,
                      p.station_seq,p.package_digest,p.prev_digest,p.participants_json,p.events_json,p.packaged_at
               FROM handover_segments s JOIN handover_packages p ON p.id=s.package_id
               WHERE s.station_code=?
               ORDER BY CASE WHEN s.position IS NULL THEN 1 ELSE 0 END,s.position,s.station_seq,s.package_digest""",
            (station_code,),
        ).fetchall()
        if not rows:
            raise NotFoundError("该站点暂无交接包")
        segments = []
        head = None
        for row in rows:
            item = dict(row)
            if item["position"] is not None:
                head = item
            segments.append(
                {
                    "position": item["position"],
                    "adoption": item["adoption"],
                    "rationale": item["rationale"],
                    "package_id": item["package_id"],
                    "package_code": item["package_code"],
                    "station_seq": item["station_seq"],
                    "package_digest": item["package_digest"],
                    "prev_digest": item["prev_digest"],
                    "participants": _loads(item["participants_json"], []),
                    "event_count": len(_loads(item["events_json"], [])),
                    "packaged_at": item["packaged_at"],
                }
            )
        pending = self._pending_conflicts(station_code)
        chains = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM handover_sample_chains WHERE station_code=? ORDER BY sample_ref", (station_code,)
            ).fetchall()
        ]
        sample_chains = [
            {
                "sample_ref": chain["sample_ref"],
                "batch_code": chain["batch_code"],
                "status": chain["status"],
                "available_actions": self._action_views(_loads(chain["actions_json"], [])),
                "received_sample_id": chain["received_sample_id"],
            }
            for chain in chains
        ]
        actions = []
        if pending:
            actions.append("adjudicate")
        if any(segment["adoption"] == "orphan" for segment in segments):
            actions.append("await_predecessor")
        if any(chain["status"] == "chain_ready" for chain in chains):
            actions.append("promote")
        if pending:
            status = "blocked"
        elif any(segment["adoption"] == "orphan" for segment in segments):
            status = "building"
        else:
            status = "continuous"
        return {
            "station_code": station_code,
            "status": status,
            "head": (
                {"package_code": head["package_code"], "package_digest": head["package_digest"], "station_seq": head["station_seq"]}
                if head else None
            ),
            "segments": segments,
            "pending_conflicts": pending,
            "sample_chains": sample_chains,
            "available_actions": self._action_views(actions),
            "recomputed_at": rows[0]["recomputed_at"],
        }

    def _sample_view(self, chain: dict[str, Any]) -> dict[str, Any]:
        events = _loads(chain["events_json"], [])
        excluded = _loads(chain["excluded_json"], [])
        max_seq = max(
            [item["source"]["station_seq"] for item in events + excluded],
            default=0,
        )
        touching = [
            conflict for conflict in self._pending_conflicts(chain["station_code"])
            if self._touches(conflict, chain["sample_ref"], max_seq)
        ]
        return {
            "station_code": chain["station_code"],
            "sample_ref": chain["sample_ref"],
            "batch_code": chain["batch_code"],
            "status": chain["status"],
            "events": events,
            "excluded_events": excluded,
            "issues": _loads(chain["issues_json"], []),
            "pending_conflicts": touching,
            "available_actions": self._action_views(_loads(chain["actions_json"], [])),
            "received_sample_id": chain["received_sample_id"],
        }

    # ------------------------------------------------------------------ 确定性重算
    def _recompute(self, station_code: str, now: str) -> None:
        packages = self._station_packages(station_code)
        all_packages = self._all_packages()
        existing = {
            row["conflict_code"]: row
            for row in (
                dict(item)
                for item in self.connection.execute(
                    "SELECT * FROM handover_conflicts WHERE station_code=? OR conflict_code LIKE 'event:%'",
                    (station_code,),
                ).fetchall()
            )
        }
        manual = {}
        for code, row in existing.items():
            if row["state"] == "resolved" and row["resolution_json"]:
                resolution = json.loads(row["resolution_json"])
                if resolution.get("manner") == "manual":
                    manual[code] = resolution
        fork_choices = {
            resolution["selected_package_digest"]
            for code, resolution in manual.items()
            if code.startswith(f"{station_code}:fork:") and resolution.get("selected_package_digest")
        }
        duplicate_choices = {
            code[len("event:"):]: resolution.get("selected_package_digest")
            for code, resolution in manual.items()
            if code.startswith("event:")
        }
        xref_actions = {
            code: resolution.get("action")
            for code, resolution in manual.items()
            if ":xbatch:" in code
        }

        by_digest = {pkg["package_digest"]: pkg for pkg in packages}
        children: dict[str, list[dict[str, Any]]] = defaultdict(list)
        genesis = []
        for pkg in packages:
            prev = pkg["prev_digest"]
            if not prev:
                genesis.append(pkg)
            elif prev != pkg["package_digest"]:
                children[prev].append(pkg)
        for group in children.values():
            group.sort(key=lambda item: (item["station_seq"], item["package_digest"]))

        # 主链行走：从序号1的起始包出发，沿哈希链唯一推进；分叉处只跟随人工裁决的选定分支
        segments: dict[int, dict[str, Any]] = {}
        adopted_ids: set[int] = set()
        roots = [pkg for pkg in genesis if pkg["station_seq"] == 1]
        root = roots[0] if len(roots) == 1 else next((pkg for pkg in roots if pkg["package_digest"] in fork_choices), None)
        position = 0
        previous = None
        current = root
        while current is not None and current["id"] not in adopted_ids:
            adopted_ids.add(current["id"])
            position += 1
            reasons = []
            if previous is None:
                reasons.append("站点序号1且前序摘要为空，作为保管链起点")
            else:
                reasons.append(f"前序摘要衔接第{position - 1}段（{previous['package_digest'][:12]}…）")
                if current["station_seq"] != previous["station_seq"] + 1:
                    reasons.append(f"序号从{previous['station_seq']}跳至{current['station_seq']}，存在空洞")
                if current["package_digest"] in fork_choices:
                    reasons.append("所在分叉经人工裁决选定")
            segments[current["id"]] = {"position": position, "adoption": "adopted", "rationale": "；".join(reasons)}
            candidates = [item for item in children.get(current["package_digest"], []) if item["id"] not in adopted_ids]
            following = None
            if len(candidates) == 1:
                following = candidates[0]
            elif len(candidates) > 1:
                chosen = [item for item in candidates if item["package_digest"] in fork_choices]
                if len(chosen) == 1:
                    following = chosen[0]
            previous, current = current, following

        # 未采用包按前序结构归类：分叉待裁决、分叉落选、无法衔接的孤儿包
        for pkg in packages:
            if pkg["id"] in segments:
                continue
            adoption = "orphan"
            reasons = []
            prev = pkg["prev_digest"]
            if not prev:
                if pkg["station_seq"] != 1:
                    reasons.append(f"前序摘要为空但站点序号为{pkg['station_seq']}，缺少序号1的起始包")
                else:
                    adoption = "fork_loser" if f"{station_code}:fork:seq:1" in manual else "fork_pending"
                    reasons.append("存在多个序号1的起始包" + ("，人工裁决未选中本包" if adoption == "fork_loser" else "，等待人工裁决"))
            elif prev == pkg["package_digest"]:
                reasons.append("前序摘要指向自身，无法构成有效链节")
            elif prev not in by_digest:
                reasons.append(f"前序摘要{prev[:12]}…未匹配任何已收包，等待前序包到达")
            else:
                parent = by_digest[prev]
                siblings = children[prev]
                if parent["id"] not in adopted_ids:
                    reasons.append("前序包尚未接入主链，等待前序片段补齐")
                elif len(siblings) > 1:
                    if any(item["package_digest"] in fork_choices for item in siblings):
                        adoption = "fork_loser"
                        reasons.append("与前序包的其他后继构成分叉，人工裁决未选中本包")
                    else:
                        adoption = "fork_pending"
                        reasons.append("与前序包的其他后继构成分叉，等待人工裁决")
                else:
                    reasons.append("未被主链采用")
            segments[pkg["id"]] = {"position": None, "adoption": adoption, "rationale": "；".join(reasons)}

        # 败选分支的后代一并淘汰：不再触发新的分叉与空洞，也不阻塞样品链
        queue = [pkg for pkg in packages if segments[pkg["id"]]["adoption"] == "fork_loser"]
        while queue:
            loser = queue.pop(0)
            for child in children.get(loser["package_digest"], []):
                segment = segments[child["id"]]
                if child["id"] not in adopted_ids and segment["adoption"] != "fork_loser":
                    segment["adoption"] = "fork_loser"
                    segment["rationale"] = "所在分支已被人工裁决淘汰"
                    queue.append(child)

        def alive_groups() -> tuple[set[int], dict[int, list[dict[str, Any]]]]:
            alive_ids = {pkg["id"] for pkg in packages if segments[pkg["id"]]["adoption"] != "fork_loser"}
            groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for pkg in packages:
                if pkg["id"] in alive_ids:
                    groups[pkg["station_seq"]].append(pkg)
            return alive_ids, groups

        # 仍存活分支上的同序号竞争：补充标注或等待裁决
        alive_ids, alive_seq_groups = alive_groups()
        for seq, group in alive_seq_groups.items():
            if len(group) < 2:
                continue
            code = f"{station_code}:fork:seq:{seq}"
            for pkg in group:
                segment = segments[pkg["id"]]
                if segment["adoption"] == "adopted":
                    if code in manual and pkg["package_digest"] in fork_choices:
                        segment["rationale"] += "；同序号分叉经人工裁决确认本包"
                    elif code not in manual:
                        segment["rationale"] += "；存在同序号分叉包，等待人工裁决"
                elif segment["adoption"] == "orphan":
                    if code in manual:
                        if pkg["package_digest"] in fork_choices:
                            segment["rationale"] += "；人工裁决选定本包，但前序仍缺失，暂无法衔接"
                        else:
                            segment["adoption"] = "fork_loser"
                            segment["rationale"] += "；存在相同序号的其它包，人工裁决未选中本包"
                    else:
                        segment["adoption"] = "fork_pending"
                        segment["rationale"] += "；存在相同序号的其它包，分叉等待人工裁决"
        queue = [pkg for pkg in packages if segments[pkg["id"]]["adoption"] == "fork_loser"]
        while queue:
            loser = queue.pop(0)
            for child in children.get(loser["package_digest"], []):
                segment = segments[child["id"]]
                if child["id"] not in adopted_ids and segment["adoption"] != "fork_loser":
                    segment["adoption"] = "fork_loser"
                    segment["rationale"] = "所在分支已被人工裁决淘汰"
                    queue.append(child)
        alive_ids, alive_seq_groups = alive_groups()

        # 冲突检测：同序号分叉、同前序分叉、序号空洞、重复事件、跨批次引用
        detected: dict[str, dict[str, Any]] = {}
        seq_fork_sets = []
        for seq, group in sorted(alive_seq_groups.items()):
            if len(group) > 1:
                seq_fork_sets.append({item["package_digest"] for item in group})
                detected[f"{station_code}:fork:seq:{seq}"] = {
                    "conflict_type": "fork",
                    "sample_ref": None,
                    "details": {
                        "station_seq": seq,
                        "candidates": [self._candidate_view(item) for item in sorted(group, key=lambda i: i["package_digest"])],
                    },
                }
        for prev_digest, group in sorted(children.items()):
            alive_members = [item for item in group if item["id"] in alive_ids]
            if len(alive_members) > 1 and {item["package_digest"] for item in alive_members} not in seq_fork_sets:
                detected[f"{station_code}:fork:prev:{prev_digest[:16]}"] = {
                    "conflict_type": "fork",
                    "sample_ref": None,
                    "details": {
                        "prev_digest": prev_digest,
                        "candidates": [self._candidate_view(item) for item in sorted(alive_members, key=lambda i: i["package_digest"])],
                    },
                }
        max_seq = max(alive_seq_groups, default=0)
        for seq in range(1, max_seq + 1):
            if seq not in alive_seq_groups:
                detected[f"{station_code}:gap:seq:{seq}"] = {
                    "conflict_type": "sequence_gap",
                    "sample_ref": None,
                    "details": {"missing_seq": seq},
                }
        occurrences: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pkg in all_packages:
            for index, event in enumerate(pkg["events"]):
                occurrences[event["event_id"]].append(
                    {
                        "station_code": pkg["station_code"],
                        "station_seq": pkg["station_seq"],
                        "package_code": pkg["package_code"],
                        "package_digest": pkg["package_digest"],
                        "event_index": index,
                        "sample_ref": event["sample_ref"],
                    }
                )
        for group in occurrences.values():
            group.sort(key=lambda item: (item["station_code"], item["station_seq"], item["package_digest"], item["event_index"]))
        for event_id, group in sorted(occurrences.items()):
            if len(group) > 1:
                detected[f"event:{event_id}"] = {
                    "conflict_type": "duplicate_event",
                    "sample_ref": group[0]["sample_ref"],
                    "details": {"event_id": event_id, "occurrences": group},
                }
        sample_mentions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        alive_mentions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pkg in packages:
            for index, event in enumerate(pkg["events"]):
                mention = {"pkg": pkg, "index": index, "event": event}
                sample_mentions[event["sample_ref"]].append(mention)
                if pkg["id"] in alive_ids:
                    alive_mentions[event["sample_ref"]].append(mention)
        for mentions in list(sample_mentions.values()) + list(alive_mentions.values()):
            mentions.sort(key=lambda item: (item["pkg"]["station_seq"], item["pkg"]["package_digest"], item["index"]))
        home_batches = {}
        for sample_ref, mentions in sample_mentions.items():
            source = alive_mentions.get(sample_ref) or mentions
            collected = [item["event"]["batch_code"] for item in source if item["event"]["event_type"] == "collected"]
            home_batches[sample_ref] = collected[0] if collected else source[0]["event"]["batch_code"]
        for sample_ref, mentions in alive_mentions.items():
            home = home_batches[sample_ref]
            for item in mentions:
                event = item["event"]
                if event["batch_code"] != home:
                    detected[f"{station_code}:xbatch:{sample_ref}:{event['event_id']}"] = {
                        "conflict_type": "cross_batch_reference",
                        "sample_ref": sample_ref,
                        "details": {
                            "event_id": event["event_id"],
                            "expected_batch": home,
                            "found_batch": event["batch_code"],
                            "package_code": item["pkg"]["package_code"],
                            "package_digest": item["pkg"]["package_digest"],
                        },
                    }

        # 冲突落库：新增待裁决，已有人工裁决保留；空洞被补齐或分叉因上级裁决失效时自动关闭
        for code, info in sorted(detected.items()):
            details_json = json.dumps(info["details"], ensure_ascii=False, sort_keys=True)
            row = existing.get(code)
            if row is None:
                conflict_station = station_code
                if code.startswith("event:"):
                    conflict_station = info["details"]["occurrences"][0]["station_code"]
                self.connection.execute(
                    """INSERT INTO handover_conflicts(conflict_code,station_code,conflict_type,sample_ref,details_json,state,created_at,updated_at)
                       VALUES(?,?,?,?,?,'pending',?,?)""",
                    (code, conflict_station, info["conflict_type"], info["sample_ref"], details_json, now, now),
                )
            else:
                self.connection.execute(
                    "UPDATE handover_conflicts SET details_json=?,sample_ref=?,updated_at=? WHERE conflict_code=?",
                    (details_json, info["sample_ref"], now, code),
                )
        for code, row in existing.items():
            if (
                row["station_code"] == station_code
                and row["conflict_type"] in {"fork", "sequence_gap"}
                and row["state"] == "pending"
                and code not in detected
            ):
                if row["conflict_type"] == "sequence_gap":
                    resolution = {"manner": "auto", "action": "filled", "reason": "缺失序号的交接包已到达，空洞自动补齐"}
                else:
                    resolution = {"manner": "auto", "action": "superseded", "reason": "上级分叉已裁决或分支已失效，分叉冲突自动关闭"}
                self.connection.execute(
                    "UPDATE handover_conflicts SET state='resolved',resolution_json=?,resolved_at=?,updated_at=? WHERE conflict_code=?",
                    (json.dumps(resolution, ensure_ascii=False), now, now, code),
                )
        pending = {code: info for code, info in detected.items() if code not in manual}

        # 归并投影重建（原始包保持不变）
        self.connection.execute("DELETE FROM handover_segments WHERE station_code=?", (station_code,))
        for pkg in packages:
            segment = segments[pkg["id"]]
            self.connection.execute(
                """INSERT INTO handover_segments(station_code,package_id,package_digest,station_seq,position,adoption,rationale,recomputed_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (station_code, pkg["id"], pkg["package_digest"], pkg["station_seq"], segment["position"], segment["adoption"], segment["rationale"], now),
            )

        # 样品保管链装配
        previous_rows = {
            row["sample_ref"]: row
            for row in (
                dict(item)
                for item in self.connection.execute(
                    "SELECT * FROM handover_sample_chains WHERE station_code=?", (station_code,)
                ).fetchall()
            )
        }
        for sample_ref, mentions in sorted(sample_mentions.items()):
            home = home_batches[sample_ref]
            adopted_events = []
            excluded = []
            issues = []
            unresolved_mentions = []
            orphan_mentions = []
            for item in mentions:
                pkg, index, event = item["pkg"], item["index"], item["event"]
                segment = segments[pkg["id"]]
                base = {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "occurred_at": event["occurred_at"],
                    "actor": event["actor"],
                    "batch_code": event["batch_code"],
                    "details": event["details"],
                    "source": {
                        "package_code": pkg["package_code"],
                        "package_digest": pkg["package_digest"],
                        "station_seq": pkg["station_seq"],
                        "position": segment["position"],
                        "event_index": index,
                    },
                }
                if segment["adoption"] != "adopted":
                    excluded.append({**base, "rationale": f"所在包未接入主链：{segment['rationale']}"})
                    if segment["adoption"] in {"orphan", "fork_pending"}:
                        unresolved_mentions.append(pkg)
                    if segment["adoption"] == "orphan":
                        orphan_mentions.append(pkg)
                    continue
                rationale = f"来自第{segment['position']}段（包{pkg['package_code']}，序号{pkg['station_seq']}）"
                group = occurrences[event["event_id"]]
                if len(group) > 1:
                    choice = duplicate_choices.get(event["event_id"])
                    winner = next((entry for entry in group if entry["package_digest"] == choice), None) if choice else group[0]
                    if winner is None:
                        winner = group[0]
                    if winner["package_digest"] != pkg["package_digest"] or winner["event_index"] != index:
                        excluded.append({**base, "rationale": "同一事件在多个包中重复出现，本次出现未被采用"})
                        continue
                    rationale += "；事件存在重复出现，本次出现被采用"
                if event["batch_code"] != home:
                    code = f"{station_code}:xbatch:{sample_ref}:{event['event_id']}"
                    decision = xref_actions.get(code)
                    if decision == "accept_reference":
                        rationale += f"；批次{event['batch_code']}与采集批次{home}不一致，经人工裁决采纳"
                    else:
                        reason = "跨批次引用经人工裁决拒绝" if decision == "reject_event" else "事件批次与采集批次不一致，等待人工裁决"
                        excluded.append({**base, "rationale": reason})
                        continue
                adopted_events.append({**base, "rationale": rationale})
            adopted_events.sort(key=lambda item: (item["source"]["position"], item["source"]["event_index"]))
            max_rank = 0
            for event in adopted_events:
                rank = STAGE_RANK[event["event_type"]]
                if event["event_type"] == "reboxed":
                    if max_rank < STAGE_RANK["sealed"]:
                        issues.append("更换运输箱发生在封签之前")
                    elif max_rank >= STAGE_RANK["received"]:
                        issues.append("到库签收之后又发生更换运输箱")
                else:
                    if rank <= max_rank:
                        issues.append(f"事件阶段回退或重复：{EVENT_TYPE_LABELS[event['event_type']]}")
                    max_rank = max(max_rank, rank)
            present = {event["event_type"] for event in adopted_events}
            missing = [stage for stage in REQUIRED_STAGES if stage not in present]
            if missing:
                issues.append("缺少必要阶段：" + "、".join(EVENT_TYPE_LABELS[stage] for stage in missing))
            max_sample_seq = max((item["pkg"]["station_seq"] for item in alive_mentions.get(sample_ref, [])), default=0)
            touching = {
                code
                for code, info in pending.items()
                if self._touches_info(info, sample_ref, max_sample_seq)
            }
            previous_row = previous_rows.get(sample_ref)
            received_sample_id = None
            if previous_row and previous_row["status"] == "received":
                status = "received"
                received_sample_id = previous_row["received_sample_id"]
                if touching:
                    issues.append("正式接收后发现新的待裁决冲突，请人工核查")
            elif touching:
                status = "conflicted"
            elif issues or unresolved_mentions:
                status = "pending"
            else:
                status = "chain_ready"
            actions = []
            if status == "chain_ready":
                actions.append("promote")
            if status == "conflicted":
                actions.append("adjudicate")
            if orphan_mentions:
                actions.append("await_predecessor")
            if status == "pending" and issues:
                actions.append("await_events")
            self.connection.execute(
                """INSERT INTO handover_sample_chains(station_code,sample_ref,batch_code,status,events_json,excluded_json,
                       issues_json,actions_json,received_sample_id,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(station_code,sample_ref) DO UPDATE SET
                       batch_code=excluded.batch_code,status=excluded.status,events_json=excluded.events_json,
                       excluded_json=excluded.excluded_json,issues_json=excluded.issues_json,actions_json=excluded.actions_json,
                       received_sample_id=excluded.received_sample_id,updated_at=excluded.updated_at""",
                (
                    station_code, sample_ref, home, status,
                    json.dumps(adopted_events, ensure_ascii=False), json.dumps(excluded, ensure_ascii=False),
                    json.dumps(issues, ensure_ascii=False), json.dumps(actions, ensure_ascii=False),
                    received_sample_id, now, now,
                ),
            )

    # ------------------------------------------------------------------ 内部工具
    def _touches_info(self, info: dict[str, Any], sample_ref: str, max_sample_seq: int) -> bool:
        conflict_type = info["conflict_type"]
        details = info["details"]
        if conflict_type in {"duplicate_event", "cross_batch_reference"}:
            return info.get("sample_ref") == sample_ref
        if conflict_type == "fork":
            fork_seq = details.get("station_seq")
            if fork_seq is None:
                fork_seq = min((item["station_seq"] for item in details.get("candidates", [])), default=None)
            return fork_seq is not None and max_sample_seq >= fork_seq
        if conflict_type == "sequence_gap":
            return max_sample_seq > details["missing_seq"]
        return False

    def _touches(self, conflict: dict[str, Any], sample_ref: str, max_sample_seq: int) -> bool:
        return self._touches_info(
            {"conflict_type": conflict["conflict_type"], "sample_ref": conflict["sample_ref"], "details": conflict["details"]},
            sample_ref,
            max_sample_seq,
        )

    def _pending_conflicts(self, station_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM handover_conflicts WHERE state='pending' ORDER BY conflict_code").fetchall()
        result = []
        for row in rows:
            conflict = self._conflict_view(dict(row))
            if conflict["station_code"] == station_code:
                result.append(conflict)
            elif conflict["conflict_type"] == "duplicate_event" and station_code in {
                item["station_code"] for item in conflict["details"].get("occurrences", [])
            }:
                result.append(conflict)
        return result

    def _action_views(self, actions: list[str]) -> list[dict[str, str]]:
        return [{"action": action, "label": ACTION_LABELS[action]} for action in actions]

    def _candidate_view(self, pkg: dict[str, Any]) -> dict[str, Any]:
        return {
            "package_code": pkg["package_code"],
            "package_digest": pkg["package_digest"],
            "station_seq": pkg["station_seq"],
            "participants": pkg["participants"],
        }

    def _package_view(self, pkg: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": pkg["id"],
            "package_code": pkg["package_code"],
            "station_code": pkg["station_code"],
            "station_seq": pkg["station_seq"],
            "prev_digest": pkg["prev_digest"],
            "package_digest": pkg["package_digest"],
            "participants": _loads(pkg["participants_json"], []) if isinstance(pkg.get("participants_json"), str) else pkg.get("participants", []),
            "events": _loads(pkg["events_json"], []) if isinstance(pkg.get("events_json"), str) else pkg.get("events", []),
            "packaged_at": pkg["packaged_at"],
            "received_by": pkg["received_by"],
            "created_at": pkg["created_at"],
        }

    def _conflict_view(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "conflict_code": row["conflict_code"],
            "station_code": row["station_code"],
            "conflict_type": row["conflict_type"],
            "sample_ref": row["sample_ref"],
            "details": _loads(row["details_json"], {}),
            "state": row["state"],
            "resolution": _loads(row["resolution_json"], None),
            "resolved_by": row["resolved_by"],
            "resolved_at": row["resolved_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _conflict_by_id(self, conflict_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM handover_conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not row:
            raise NotFoundError("冲突记录不存在")
        return dict(row)

    def _package_by_id(self, package_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM handover_packages WHERE id=?", (package_id,)).fetchone()
        if not row:
            raise NotFoundError("交接包不存在")
        return dict(row)

    def _package_by_code(self, package_code: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM handover_packages WHERE package_code=?", (package_code,)).fetchone()
        return dict(row) if row else None

    def _package_by_digest(self, package_digest: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM handover_packages WHERE package_digest=?", (package_digest,)).fetchone()
        return dict(row) if row else None

    def _sample_chain_row(self, station_code: str, sample_ref: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM handover_sample_chains WHERE station_code=? AND sample_ref=?",
            (station_code, sample_ref),
        ).fetchone()
        if not row:
            raise NotFoundError("样品保管链不存在")
        return dict(row)

    def _station_packages(self, station_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM handover_packages WHERE station_code=? ORDER BY station_seq,package_digest",
            (station_code,),
        ).fetchall()
        return [self._parsed_package(dict(row)) for row in rows]

    def _all_packages(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM handover_packages ORDER BY station_code,station_seq,package_digest"
        ).fetchall()
        return [self._parsed_package(dict(row)) for row in rows]

    def _parsed_package(self, pkg: dict[str, Any]) -> dict[str, Any]:
        pkg["participants"] = _loads(pkg.pop("participants_json"), [])
        pkg["events"] = _loads(pkg.pop("events_json"), [])
        return pkg
