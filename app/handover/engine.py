from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.handover.hashes import GENESIS_DIGEST, canonical_json, event_digest, package_digest, sha256_hex, short
from app.handover.repository import HandoverRepository
from app.samples.repository import BatchRepository, LocationRepository, SampleRepository
from app.services.audit import AuditService

ADOPTED_MAIN = "位于唯一连续哈希链上，予以采用"
ADOPTED_PINNED_BRANCH = "位于人工裁决锁定的分支上，予以采用"
ADOPTED_FIRST_COPY = "重复事件默认保留最先到达的副本"
ADOPTED_PINNED_COPY = "人工裁决采用该副本"
ADOPTED_ANCHOR = "前序包缺失，经人工裁决锚定为链起点"
REJECTED_FORK = "处于分叉落选分支，不予采用"
REJECTED_DUPLICATE = "重复事件副本，未被采用"
REJECTED_CROSS_BATCH = "跨批次引用，未被采用"
REJECTED_BROKEN = "前序摘要指向缺失交接包且未获锚定裁决，不予采用"


def dispute_code(fingerprint: str) -> str:
    return "DSP-" + sha256_hex(fingerprint)[:16]


class HandoverMergeService:
    """离线交接包归并：原始包只追加，派生链每次确定性重算。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.repo = HandoverRepository(connection)
        self.samples = SampleRepository(connection)
        self.batches = BatchRepository(connection)
        self.locations = LocationRepository(connection)
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------
    # 接收交接包
    # ------------------------------------------------------------------

    def ingest(self, principal: Principal, envelope: dict[str, Any]) -> dict[str, Any]:
        principal.require("handover.ingest")
        station = envelope["station_code"]
        computed = package_digest(envelope)
        now = to_storage(self.clock.now())

        existing = self.repo.find_package_by_digest(station, computed)
        if existing:
            # 重复上传：原始包与归并结果都保持不变，仅留痕并返回原归并结果
            self.audit.record(
                principal, "handover.ingest.replayed", "handover_package", str(existing["id"]),
                metadata={"station_code": station, "station_seq": envelope["station_seq"]},
            )
            return self.station_view(principal, station, package_id=existing["id"], replayed=True)

        package_id = self.repo.insert_package(envelope, computed, now)
        self.audit.record(
            principal, "handover.ingest", "handover_package", str(package_id),
            after={"station_code": station, "station_seq": envelope["station_seq"], "digest": computed},
            metadata={"hash_valid": computed == envelope["digest"]},
        )
        self.recompute_station(principal, station)
        return self.station_view(principal, station, package_id=package_id, replayed=False)

    # ------------------------------------------------------------------
    # 确定性重算
    # ------------------------------------------------------------------

    def recompute_station(self, principal: Principal | None, station: str) -> None:
        now = to_storage(self.clock.now())
        packages = self.repo.list_station_packages(station)
        # 原始包不可变；只清理上一轮派生出的“未裁决”争议，已人工裁决的保持有效
        self.repo.delete_open_for_station(station)

        valid_packages: list[dict[str, Any]] = []
        for pkg in packages:
            if pkg["hash_valid"]:
                valid_packages.append(pkg)
            else:
                fingerprint = f"{station}:pkg:{pkg['id']}"
                if not self._fingerprint_decided(station, "hash_mismatch", fingerprint):
                    self.repo.insert_dispute(
                        {
                            "dispute_code": dispute_code(fingerprint),
                            "chain_id": None,
                            "station_code": station,
                            "dispute_type": "hash_mismatch",
                            "scope": "package",
                            "package_id": pkg["id"],
                            "station_seq": pkg["station_seq"],
                            "title": f"站点 {station} 序号 {pkg['station_seq']} 的交接包哈希校验失败",
                            "detail": {
                                "fingerprint": fingerprint,
                                "claimed_digest": pkg["claimed_digest"],
                                "computed_digest": pkg["computed_digest"],
                            },
                        },
                        now,
                    )

        sample_codes = sorted({ev["sample_code"] for pkg in valid_packages for ev in pkg["events"]})
        for sample_code in sample_codes:
            chain = self.repo.upsert_chain(station, sample_code, now)
            self._recompute_chain(chain, valid_packages, now)

    def _recompute_chain(self, chain: dict[str, Any], packages: list[dict[str, Any]], now: str) -> None:
        # 已正式接收的链即封存：后到包仍然入库，但不会静默改写已采信的保管链
        if self.repo.get_reception_by_chain(chain["id"]):
            self.repo.update_chain_state(
                chain["id"], chain["state"], chain["head_digest"], chain["last_station_seq"],
                chain["opening_batch_code"], True, now,
            )
            return
        station = chain["station_code"]
        sample_code = chain["sample_code"]
        chain_id = chain["id"]
        decisions = self._decision_index(chain_id)

        # 哈希校验失败的包不参与任何链（decide 直接重算时同样如此）
        packages = [pkg for pkg in packages if pkg["hash_valid"]]
        by_digest = {pkg["computed_digest"]: pkg for pkg in packages}
        children: dict[str, list[str]] = defaultdict(list)
        for pkg in packages:
            children[pkg["prev_digest"] or GENESIS_DIGEST].append(pkg["computed_digest"])
        for digests in children.values():
            digests.sort()

        carrying = [
            pkg for pkg in packages
            if any(ev["sample_code"] == sample_code for ev in pkg["events"])
        ]
        if not carrying:
            return

        # 沿 prev_digest 上行：返回（链上的祖先包含自身, 终止处）。
        # 终止处为 GENESIS 表示扎根到链首；否则指向缺失或哈希无效包，属于断链。
        def walk_up(start_digest: str) -> tuple[list[str], str]:
            path: list[str] = []
            seen: set[str] = set()
            cur = start_digest
            while cur and cur in by_digest and cur not in seen:
                seen.add(cur)
                path.append(cur)
                cur = by_digest[cur]["prev_digest"] or GENESIS_DIGEST
            return path, cur

        # 样品相关闭包 + 断链根（断链路径上最靠近缺失点的已知包）
        closure: set[str] = set()
        broken_roots: dict[str, str] = {}  # 最浅已知包摘要 -> 缺失的前序摘要
        for pkg in carrying:
            path, terminal = walk_up(pkg["computed_digest"])
            closure.update(path)
            if terminal != GENESIS_DIGEST:
                root_known = path[-1]
                broken_roots.setdefault(root_known, terminal)

        anchored: set[str] = set()
        for root_digest, missing_prev in sorted(broken_roots.items()):
            fingerprint = f"{station}:{sample_code}:broken:{short(root_digest)}"
            decision = decisions.get(("broken_link", fingerprint))
            if decision and decision["decision"] == "anchor":
                anchored.add(root_digest)
            else:
                self.repo.insert_dispute(
                    {
                        "dispute_code": dispute_code(fingerprint),
                        "chain_id": chain_id,
                        "station_code": station,
                        "dispute_type": "broken_link",
                        "scope": "chain",
                        "package_id": by_digest[root_digest]["id"],
                        "station_seq": by_digest[root_digest]["station_seq"],
                        "title": f"样品 {sample_code} 序号 {by_digest[root_digest]['station_seq']} 的前序交接包缺失",
                        "detail": {
                            "fingerprint": fingerprint,
                            "package_digest": root_digest,
                            "missing_prev_digest": missing_prev,
                            "options": [
                                {"key": root_digest, "label": f"锚定包 {short(root_digest)} 为该链起点"}
                            ],
                        },
                    },
                    now,
                )

        # 从每个允许的根（链首 + 人工锚定包）出发，沿闭包走出确定性主链
        roots = [GENESIS_DIGEST] + sorted(anchored)
        open_forks: list[dict[str, Any]] = []
        pinned_branch_digests: set[str] = set()
        spine: list[dict[str, Any]] = []
        root_segments: list[list[int]] = []
        for root in roots:
            current = root
            segment_seqs: list[int] = []
            while True:
                # 无人工裁决时确定性地默认先到分支（按原始包入库顺序）
                options = sorted(
                    (d for d in children.get(current, []) if d in closure),
                    key=lambda d: by_digest[d]["id"],
                )
                if not options:
                    break
                chosen = options[0]
                if len(options) > 1:
                    fingerprint = f"{station}:{sample_code}:fork:{short(current) or 'GENESIS'}"
                    decision = decisions.get(("fork", fingerprint))
                    if decision:
                        pinned = decision.get("resolution", {}).get("branch_digest")
                        if pinned in options:
                            chosen = pinned
                            pinned_branch_digests.add(chosen)
                        # 人工裁决的分支不会被静默改选：即使出现新的候选也保持 pinned
                    else:
                        open_forks.append(
                            {
                                "fingerprint": fingerprint,
                                "fork_prev": current,
                                "station_seq": by_digest[chosen]["station_seq"],
                                "options": options,
                            }
                        )
                chosen_pkg = by_digest[chosen]
                spine.append(chosen_pkg)
                segment_seqs.append(chosen_pkg["station_seq"])
                current = chosen
            if segment_seqs:
                root_segments.append(segment_seqs)

        spine_ids = {pkg["id"] for pkg in spine}
        # 断链且未被锚定路径上的已知包，用于区分“断链弃用”与“分叉弃用”
        broken_pkg_ids: set[int] = set()
        for root_digest in broken_roots:
            if root_digest in anchored:
                continue
            for pkg in carrying:
                if pkg["id"] in spine_ids:
                    continue
                path, terminal = walk_up(pkg["computed_digest"])
                if terminal != GENESIS_DIGEST and root_digest in path:
                    broken_pkg_ids.add(pkg["id"])

        # ---- 记录落库与采用理由 ----
        records: list[dict[str, Any]] = []
        candidate_events: list[dict[str, Any]] = []
        for pkg in carrying:
            on_spine = pkg["id"] in spine_ids
            for index, ev in enumerate(pkg["events"]):
                if ev["sample_code"] != sample_code:
                    continue
                candidate_events.append({"pkg": pkg, "index": index, "ev": ev, "on_spine": on_spine})

        # 重复事件：相同 event_id 或相同内容摘要，按不相交集合合并成冲突组
        by_event_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_content: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for cand in candidate_events:
            by_event_id[cand["ev"]["event_id"]].append(cand)
            by_content[event_digest(cand["ev"])].append(cand)

        parent = list(range(len(candidate_events)))

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

        position = {id(c): i for i, c in enumerate(candidate_events)}
        for grouped in (*by_event_id.values(), *by_content.values()):
            if len({c["pkg"]["id"] for c in grouped}) > 1:
                first = position[id(grouped[0])]
                for other in grouped[1:]:
                    union(first, position[id(other)])

        groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for i, cand in enumerate(candidate_events):
            groups[find(i)].append(cand)
        duplicate_groups: list[dict[str, Any]] = []
        cand_group_id: dict[int, int] = {}
        for members in groups.values():
            if len({c["pkg"]["id"] for c in members}) > 1:
                membership = sorted(f"{c['pkg']['id']}:{c['index']}" for c in members)
                gid = int(sha256_hex(canonical_json(membership))[:12], 16)
                duplicate_groups.append(
                    {
                        "gid": gid,
                        "event_ids": sorted({c["ev"]["event_id"] for c in members}),
                        "candidates": sorted(members, key=lambda c: (c["pkg"]["id"], c["index"])),
                        "same_content": len({event_digest(c["ev"]) for c in members}) == 1,
                    }
                )
                for cand in members:
                    cand_group_id[id(cand)] = gid

        # 每组事件只产生一个待裁决事项；默认确定性地采用先到包中的副本
        adopted_pkg_by_group: dict[int, int] = {}
        group_fingerprint: dict[int, str] = {}
        for group in duplicate_groups:
            label = group["event_ids"][0] if len(group["event_ids"]) == 1 else f"{len(group['event_ids'])}个关联事件"
            fingerprint = f"{station}:{sample_code}:dup:{group['gid']}"
            group_fingerprint[group["gid"]] = fingerprint
            decision = decisions.get(("duplicate_event", fingerprint))
            cands = group["candidates"]
            if decision:
                chosen_pkg = None if decision["decision"] == "reject" else decision.get("resolution", {}).get("package_id")
                if chosen_pkg is not None and any(c["pkg"]["id"] == chosen_pkg for c in cands):
                    adopted_pkg_by_group[group["gid"]] = chosen_pkg
            else:
                self.repo.insert_dispute(
                    {
                        "dispute_code": dispute_code(fingerprint),
                        "chain_id": chain_id,
                        "station_code": station,
                        "dispute_type": "duplicate_event",
                        "scope": "chain",
                        "package_id": cands[0]["pkg"]["id"],
                        "station_seq": cands[0]["pkg"]["station_seq"],
                        "title": f"样品 {sample_code} 事件 {label} 存在重复副本",
                        "detail": {
                            "fingerprint": fingerprint,
                            "event_ids": group["event_ids"],
                            "same_content": group["same_content"],
                            "options": [
                                {
                                    "key": str(c["pkg"]["id"]),
                                    "label": f"序号 {c['pkg']['station_seq']} 包 {short(c['pkg']['computed_digest'])} 中的事件",
                                }
                                for c in cands
                            ],
                        },
                    },
                    now,
                )
            adopted_pkg_by_group.setdefault(group["gid"], cands[0]["pkg"]["id"])

        # 跨批次引用
        batch_codes = sorted({c["ev"]["batch_code"] for c in candidate_events})
        cross_fingerprint = f"{station}:{sample_code}:cross:{':'.join(batch_codes)}"
        opening_batch = batch_codes[0]
        if len(batch_codes) > 1:
            # 已裁决过归属批次且该批次仍在候选中时，裁决持续有效（即使又出现新批次）
            prior_cross = max(
                (
                    d
                    for d in decisions.values()
                    if d["dispute_type"] == "cross_batch"
                    and d["decision"] == "adopt_branch"
                    and d.get("resolution", {}).get("batch_code") in batch_codes
                ),
                key=lambda d: d["id"],
                default=None,
            )
            if prior_cross:
                opening_batch = prior_cross["resolution"]["batch_code"]
                known_batches = {o["key"] for o in prior_cross["detail"].get("options", [])}
                expanded = set(batch_codes) - known_batches
                if expanded:
                    # 归属裁决不被静默改选；但出现全新批次仍需再次送裁
                    self.repo.insert_dispute(
                        {
                            "dispute_code": dispute_code(cross_fingerprint),
                            "chain_id": chain_id,
                            "station_code": station,
                            "dispute_type": "cross_batch",
                            "scope": "chain",
                            "title": f"样品 {sample_code} 出现新的跨批次引用：{', '.join(sorted(expanded))}",
                            "detail": {
                                "fingerprint": cross_fingerprint,
                                "current_owner": opening_batch,
                                "new_batches": sorted(expanded),
                                "options": [{"key": code, "label": f"批次 {code}"} for code in batch_codes],
                            },
                        },
                        now,
                    )
            else:
                self.repo.insert_dispute(
                    {
                        "dispute_code": dispute_code(cross_fingerprint),
                        "chain_id": chain_id,
                        "station_code": station,
                        "dispute_type": "cross_batch",
                        "scope": "chain",
                        "title": f"样品 {sample_code} 的记录跨越多个批次：{', '.join(batch_codes)}",
                        "detail": {
                            "fingerprint": cross_fingerprint,
                            "options": [{"key": code, "label": f"批次 {code}"} for code in batch_codes],
                        },
                    },
                    now,
                )
                # 确定性默认：采用序号最小链路上最先出现的批次
                opening_batch = min(
                    candidate_events,
                    key=lambda c: (0 if c["on_spine"] else 1, c["pkg"]["station_seq"], c["index"]),
                )["ev"]["batch_code"]

        for fork in open_forks:
            self.repo.insert_dispute(
                {
                    "dispute_code": dispute_code(fork["fingerprint"]),
                    "chain_id": chain_id,
                    "station_code": station,
                    "dispute_type": "fork",
                    "scope": "chain",
                    "package_id": by_digest[fork["options"][0]]["id"],
                    "station_seq": fork["station_seq"],
                    "title": f"样品 {sample_code} 在序号 {fork['station_seq']} 出现分叉",
                    "detail": {
                        **fork,
                        "options": [
                            {"key": digest, "label": f"分支包 {short(digest)}（序号 {by_digest[digest]['station_seq']}）"}
                            for digest in fork["options"]
                        ],
                    },
                },
                now,
            )

        human_pinned_groups = {
            gid
            for gid, fingerprint in group_fingerprint.items()
            if (d := decisions.get(("duplicate_event", fingerprint))) and d["decision"] != "reject"
        }
        rejected_groups = {
            gid
            for gid, fingerprint in group_fingerprint.items()
            if decisions.get(("duplicate_event", fingerprint), {}).get("decision") == "reject"
        }

        anchored_pkg_ids = {by_digest[d]["id"] for d in anchored}

        for cand in candidate_events:
            pkg, ev = cand["pkg"], cand["ev"]
            gid = cand_group_id.get(id(cand))
            record = {
                "package_id": pkg["id"],
                "station_seq": pkg["station_seq"],
                "event_index": cand["index"],
                "sample_code": sample_code,
                "event_id": ev["event_id"],
                "record_digest": f"{pkg['computed_digest']}:{cand['index']}",
                "content": ev,
            }
            if not cand["on_spine"]:
                record["adopted"] = False
                record["adoption_reason"] = (
                    REJECTED_BROKEN if pkg["id"] in broken_pkg_ids else REJECTED_FORK
                )
            elif gid is not None and gid in rejected_groups:
                record["adopted"] = False
                record["adoption_reason"] = REJECTED_DUPLICATE
            elif gid is not None and adopted_pkg_by_group.get(gid) != pkg["id"]:
                record["adopted"] = False
                record["adoption_reason"] = REJECTED_DUPLICATE
            elif ev["batch_code"] != opening_batch:
                record["adopted"] = False
                record["adoption_reason"] = REJECTED_CROSS_BATCH
            else:
                record["adopted"] = True
                if gid is not None:
                    record["adoption_reason"] = (
                        ADOPTED_PINNED_COPY if gid in human_pinned_groups else ADOPTED_FIRST_COPY
                    )
                elif pkg["computed_digest"] in pinned_branch_digests:
                    record["adoption_reason"] = ADOPTED_PINNED_BRANCH
                elif pkg["id"] in anchored_pkg_ids:
                    record["adoption_reason"] = ADOPTED_ANCHOR
                else:
                    record["adoption_reason"] = ADOPTED_MAIN
            records.append(record)

        # ---- 序号空洞：每个连续段内部检测缺失，锚定段起点之前不报空洞 ----
        gaps: list[int] = []
        for seqs in root_segments:
            ordered = sorted(set(seqs))
            for expected in range(ordered[0], ordered[-1] + 1):
                if expected not in ordered:
                    gaps.append(expected)
        waived_gaps = {
            d["resolution"].get("missing_seq")
            for d in self.repo.list_decided_for_chains([chain_id])
            if d["dispute_type"] == "sequence_gap" and d.get("resolution")
        }
        for missing in gaps:
            fingerprint = f"{station}:{sample_code}:gap:{missing}"
            if missing in waived_gaps:
                continue
            self.repo.insert_dispute(
                {
                    "dispute_code": dispute_code(fingerprint),
                    "chain_id": chain_id,
                    "station_code": station,
                    "dispute_type": "sequence_gap",
                    "scope": "chain",
                    "station_seq": missing,
                    "title": f"样品 {sample_code} 的保管链缺少序号 {missing}",
                    "detail": {"fingerprint": fingerprint, "missing_seq": missing, "options": []},
                },
                now,
            )

        self.repo.replace_records(chain_id, records, now)

        # ---- 链状态 ----
        chain_open = [
            d for d in self.repo.list_open_disputes(station) if d["chain_id"] == chain_id
        ]
        types_open = {d["dispute_type"] for d in chain_open}
        if not types_open:
            state = "unique"
        elif "fork" in types_open:
            state = "forked"
        elif {"sequence_gap", "broken_link"} & types_open:
            state = "gapped"
        else:
            state = "conflicted"

        adopted_records = [r for r in records if r["adopted"]]
        head_digest = ""
        last_seq = None
        if adopted_records:
            last_record = max(adopted_records, key=lambda r: (r["station_seq"], r["event_index"], r["package_id"]))
            last_pkg = next(p for p in carrying if p["id"] == last_record["package_id"])
            head_digest = last_pkg["computed_digest"]
            last_seq = last_pkg["station_seq"]

        # 一旦有人工裁决即锁定为“人工已介入”，后续确定性重算不得静默改选
        locked = bool(chain["locked"]) or bool(self.repo.list_decided_for_chains([chain_id]))
        self.repo.update_chain_state(
            chain_id, state, head_digest, last_seq,
            opening_batch if adopted_records else None, locked, now,
        )

    def _decision_index(self, chain_id: int) -> dict[tuple[str, str], dict[str, Any]]:
        decided = self.repo.list_decided_for_chains([chain_id])
        index: dict[tuple[str, str], dict[str, Any]] = {}
        for dispute in decided:
            fingerprint = dispute["detail"].get("fingerprint")
            if fingerprint:
                index[(dispute["dispute_type"], fingerprint)] = dispute
        return index

    def _fingerprint_decided(self, station: str, dispute_type: str, fingerprint: str) -> bool:
        for dispute in self.repo.list_disputes(station, "decided"):
            if dispute["dispute_type"] == dispute_type and dispute["detail"].get("fingerprint") == fingerprint:
                return True
        return False

    # ------------------------------------------------------------------
    # 人工裁决
    # ------------------------------------------------------------------

    def decide(self, principal: Principal, dispute_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("handover.decide")
        dispute = self.repo.get_dispute(dispute_id)
        if not dispute:
            raise NotFoundError("待裁决事项不存在")
        if dispute["status"] != "open":
            raise ConflictError("该事项已经裁决，不能重复裁决")
        now = to_storage(self.clock.now())
        resolution: dict[str, Any] = {}

        kind = dispute["dispute_type"]
        decision = data["decision"]
        if kind == "fork":
            if decision != "adopt_branch":
                raise ValidationError("分叉必须通过 adopt_branch 选定一个分支")
            options = {item["key"] for item in dispute["detail"].get("options", [])}
            if data.get("branch_digest") not in options:
                raise ValidationError("branch_digest 不在该分叉的候选分支中")
            resolution = {"branch_digest": data["branch_digest"], "fork_prev": dispute["detail"].get("fork_prev")}
        elif kind == "duplicate_event":
            if decision not in {"adopt", "reject"}:
                raise ValidationError("重复事件只能 adopt 或 reject")
            if decision == "adopt":
                options = {int(item["key"]) for item in dispute["detail"].get("options", [])}
                try:
                    package_id = int(data["branch_digest"])
                except (TypeError, ValueError):
                    raise ValidationError("adopt 重复事件需在 branch_digest 中给出候选包 id")
                if package_id not in options:
                    raise ValidationError("候选包不在该重复事件的范围内")
                resolution = {"package_id": package_id}
        elif kind == "sequence_gap":
            if decision != "waive":
                raise ValidationError("序号空洞只能 waive 豁免（需确保包确属漏交而非丢失）")
            resolution = {"missing_seq": dispute["detail"].get("missing_seq"), "waived": True}
        elif kind == "cross_batch":
            if decision != "adopt_branch":
                raise ValidationError("跨批次引用必须通过 adopt_branch 选定归属批次")
            options = {item["key"] for item in dispute["detail"].get("options", [])}
            if data.get("branch_digest") not in options:
                raise ValidationError("branch_digest 不在候选批次中")
            resolution = {"batch_code": data["branch_digest"]}
        elif kind == "hash_mismatch":
            if decision != "reject":
                raise ValidationError("哈希校验失败的包只能 reject 弃用")
            resolution = {"rejected": True}
        elif kind == "broken_link":
            if decision not in {"anchor", "reject"}:
                raise ValidationError("断链只能 anchor 锚定为链起点，或 reject 弃用")
            if decision == "anchor":
                options = {item["key"] for item in dispute["detail"].get("options", [])}
                if data.get("branch_digest") not in options:
                    raise ValidationError("branch_digest 不在可锚定的候选包中")
                resolution = {"package_digest": data["branch_digest"], "anchored": True}
            else:
                resolution = {"rejected": True}

        before = dict(dispute)
        self.repo.decide_dispute(dispute_id, decision, principal.user_id, data.get("note", ""), resolution, now)
        decided = self.repo.get_dispute(dispute_id)
        self.audit.record(
            principal, "handover.decide", "handover_dispute", str(dispute_id),
            before=before, after=decided,
        )
        if dispute["chain_id"]:
            chain = self.repo.get_chain(dispute["chain_id"])
            self._recompute_chain(chain, self.repo.list_station_packages(chain["station_code"]), now)
        return decided

    # ------------------------------------------------------------------
    # 正式接收
    # ------------------------------------------------------------------

    def formal_receive(self, principal: Principal, chain_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("handover.receive")
        chain = self.repo.get_chain(chain_id)
        if not chain:
            raise NotFoundError("保管链不存在")
        if chain["state"] != "unique":
            raise ConflictError("只有形成唯一连续链、且无未决冲突的样品才能正式接收")
        existing = self.repo.get_reception_by_chain(chain_id)
        if existing:
            sample = self.samples.get(existing["sample_id"])
            return {"replayed": True, "reception": existing, "sample": sample, "chain": self.chain_view(principal, chain_id)}
        records = [r for r in self.repo.list_records(chain_id) if r["adopted"]]
        if not any(r["content"]["event_type"] == "collected" for r in records):
            raise ConflictError("保管链缺少采集起点，不能正式接收")
        if data.get("location_id"):
            self.locations.get(data["location_id"])

        now = to_storage(self.clock.now())
        batch_code = chain["opening_batch_code"]
        batch_row = self.connection.execute(
            "SELECT * FROM receipt_batches WHERE batch_code=?", (batch_code,)
        ).fetchone()
        if batch_row:
            batch = dict(batch_row)
        else:
            payload = f"handover-chain:{chain['chain_code']}"
            batch = self.batches.create(
                {
                    "batch_code": batch_code,
                    "project_code": records[0]["content"].get("detail", {}).get("project_code") or "OFFLINE-HANDOVER",
                    "expected_count": 1,
                },
                principal.user_id, payload, now,
            )
        if self.samples.by_code(chain["sample_code"]):
            raise ConflictError(f"样品编码 {chain['sample_code']} 已存在正式档案")
        sample = self.samples.create(
            {
                "sample_code": chain["sample_code"],
                "batch_id": batch["id"],
                "sample_type": data["sample_type"],
                "quantity": data["quantity"],
                "unit": data["unit"],
                "lifecycle_state": "received",
                "location_id": data.get("location_id"),
                "custody_user_id": principal.user_id,
                "lineage_depth": 0,
            },
            now,
        )
        for record in records:
            self.samples.append_event(
                sample["id"],
                f"handover.{record['content']['event_type']}",
                principal.user_id,
                record["content"]["occurred_at"],
                details={
                    "handover_event_id": record["event_id"],
                    "station_code": chain["station_code"],
                    "station_seq": record["station_seq"],
                    "package_id": record["package_id"],
                    "record_digest": record["record_digest"],
                    "actor": record["content"].get("actor", ""),
                    "detail": record["content"].get("detail", {}),
                },
            )
        reception_id = self.repo.insert_reception(
            chain_id, sample["id"], batch["id"], principal.user_id, chain["head_digest"], now
        )
        self.batches.update_counts(batch["id"], now)
        self.connection.execute(
            "UPDATE handover_chains SET locked=1,updated_at=? WHERE id=?", (now, chain_id)
        )
        reception = dict(self.connection.execute(
            "SELECT * FROM handover_receptions WHERE id=?", (reception_id,)
        ).fetchone())
        self.audit.record(
            principal, "handover.receive", "handover_chain", str(chain_id),
            after={"reception_id": reception_id, "sample_id": sample["id"], "batch_id": batch["id"]},
        )
        return {"replayed": False, "reception": reception, "sample": sample, "chain": self.chain_view(principal, chain_id)}

    # ------------------------------------------------------------------
    # 查询视图
    # ------------------------------------------------------------------

    def station_view(self, principal: Principal, station: str, *, package_id: int | None = None, replayed: bool = False) -> dict[str, Any]:
        principal.require("handover.read")
        packages = self.repo.list_station_packages(station)
        if not packages:
            raise NotFoundError("站点尚无交接包")
        chains = [self._chain_summary(c) for c in self.repo.list_station_chains(station)]
        return {
            "station_code": station,
            "replayed": replayed,
            "ingested_package_id": package_id,
            "package_count": len(packages),
            "packages": [
                {
                    "id": pkg["id"],
                    "station_seq": pkg["station_seq"],
                    "prev_digest": pkg["prev_digest"],
                    "digest": pkg["computed_digest"],
                    "claimed_digest": pkg["claimed_digest"],
                    "hash_valid": bool(pkg["hash_valid"]),
                    "participants": pkg["participants"],
                    "event_count": len(pkg["events"]),
                    "received_at": pkg["received_at"],
                }
                for pkg in packages
            ],
            "chains": chains,
            "open_disputes": self.repo.list_open_disputes(station),
            "available_actions": sorted({action for c in chains for action in c["available_actions"]}),
        }

    def list_stations(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require("handover.read")
        return self.repo.list_stations()

    def _chain_summary(self, chain: dict[str, Any]) -> dict[str, Any]:
        open_disputes = [
            d for d in self.repo.list_open_disputes(chain["station_code"])
            if d["chain_id"] == chain["id"]
        ]
        reception = self.repo.get_reception_by_chain(chain["id"])
        actions: list[str] = []
        if reception:
            actions.append("view_sample")
        elif chain["state"] == "unique":
            actions.append("formal_receive")
        else:
            actions.append("resolve_disputes")
        return {
            "chain_id": chain["id"],
            "chain_code": chain["chain_code"],
            "station_code": chain["station_code"],
            "sample_code": chain["sample_code"],
            "state": chain["state"],
            "locked": bool(chain["locked"]),
            "head_digest": chain["head_digest"],
            "last_station_seq": chain["last_station_seq"],
            "opening_batch_code": chain["opening_batch_code"],
            "open_dispute_count": len(open_disputes),
            "received_sample_id": reception["sample_id"] if reception else None,
            "available_actions": actions,
        }

    def chain_view(self, principal: Principal, chain_id: int) -> dict[str, Any]:
        principal.require("handover.read")
        chain = self.repo.get_chain(chain_id)
        if not chain:
            raise NotFoundError("保管链不存在")
        records = self.repo.list_records(chain_id)
        disputes = [
            d for d in self.repo.list_disputes(chain["station_code"], None)
            if d["chain_id"] == chain_id
        ]
        summary = self._chain_summary(chain)
        return {
            **summary,
            "segments": [
                {
                    "package_id": r["package_id"],
                    "station_seq": r["station_seq"],
                    "event_index": r["event_index"],
                    "event_id": r["event_id"],
                    "event": r["content"],
                    "adopted": r["adopted"],
                    "adoption_reason": r["adoption_reason"],
                    "record_digest": r["record_digest"],
                }
                for r in records
            ],
            "disputes": [
                {
                    "id": d["id"],
                    "dispute_code": d["dispute_code"],
                    "type": d["dispute_type"],
                    "status": d["status"],
                    "title": d["title"],
                    "station_seq": d["station_seq"],
                    "detail": d["detail"],
                    "decision": d["decision"],
                    "decision_note": d["decision_note"],
                }
                for d in disputes
            ],
        }

    def list_disputes(self, principal: Principal, station: str | None, status_filter: str | None) -> list[dict[str, Any]]:
        principal.require("handover.read")
        return self.repo.list_disputes(station, status_filter)
