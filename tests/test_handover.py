from __future__ import annotations

from app.handover.hashes import canonical_json, sha256_hex


def _digest(envelope: dict) -> str:
    payload = {
        key: envelope[key]
        for key in ("station_code", "station_seq", "prev_digest", "participants", "events")
    }
    return sha256_hex(canonical_json(payload))


def event(event_id, event_type, sample_code="SAM-1", batch_code="B-OFF-1", **extra):
    base = {
        "event_id": event_id,
        "event_type": event_type,
        "sample_code": sample_code,
        "batch_code": batch_code,
        "occurred_at": extra.pop("occurred_at", "2026-09-20T08:00:00+00:00"),
        "actor": extra.pop("actor", "张采样"),
        "detail": extra.pop("detail", {}),
    }
    base.update(extra)
    return base


def package(station, seq, prev, events, *, participants=None, digest=None):
    envelope = {
        "station_code": station,
        "station_seq": seq,
        "prev_digest": prev,
        "participants": participants or ["张采样", "李运输"],
        "events": events,
        "digest": "pending",
    }
    envelope["digest"] = digest or _digest(envelope)
    return envelope


def chain_of(station, events_by_seq, *, participants=None):
    """生成顺序哈希链，返回 {seq: envelope}。"""
    envelopes = {}
    prev = ""
    for seq in sorted(events_by_seq):
        pkg = package(station, seq, prev, events_by_seq[seq], participants=participants)
        envelopes[seq] = pkg
        prev = pkg["digest"]
    return envelopes


def ingest(client, admin, envelope):
    response = client.post("/api/handover/packages", headers=admin["headers"], json=envelope)
    assert response.status_code == 201, response.text
    return response.json()


STATION = "ST-A"


def _station(client, admin, station=STATION):
    response = client.get(f"/api/handover/stations/{station}", headers=admin["headers"])
    assert response.status_code == 200, response.text
    return response.json()


def _chain(client, admin, chain_id):
    response = client.get(f"/api/handover/chains/{chain_id}", headers=admin["headers"])
    assert response.status_code == 200, response.text
    return response.json()


def _chain_id(view, sample_code="SAM-1"):
    matches = [c for c in view["chains"] if c["sample_code"] == sample_code]
    assert len(matches) == 1
    return matches[0]["chain_id"]


def _dispute(view, dispute_type):
    matches = [d for d in view["open_disputes"] if d["dispute_type"] == dispute_type]
    assert len(matches) == 1, [d["dispute_type"] for d in view["open_disputes"]]
    return matches[0]


def test_full_chain_receive_and_replay(client, admin):
    chain = chain_of(
        STATION,
        {
            0: [event("EV-0", "collected")],
            1: [event("EV-1", "sealed")],
            2: [event("EV-2", "box_changed")],
            3: [event("EV-3", "received")],
        },
    )
    for seq in (0, 1, 2, 3):
        view = ingest(client, admin, chain[seq])
    assert view["chains"][0]["state"] == "unique"
    assert view["available_actions"] == ["formal_receive"]

    # 重复上传：返回原归并结果
    replay = ingest(client, admin, chain[2])
    assert replay["replayed"] is True
    assert replay["ingested_package_id"] is not None
    packages = _station(client, admin)["packages"]
    assert len(packages) == 4  # 原始包不重复落库

    chain_id = _chain_id(view)
    detail = _chain(client, admin, chain_id)
    assert [s["station_seq"] for s in detail["segments"]] == [0, 1, 2, 3]
    assert all(s["adopted"] for s in detail["segments"])
    assert all("采用" in s["adoption_reason"] for s in detail["segments"])

    received = client.post(
        f"/api/handover/chains/{chain_id}/receive",
        headers=admin["headers"],
        json={"sample_type": "水样", "quantity": 500, "unit": "mL"},
    )
    assert received.status_code == 201, received.text
    body = received.json()
    assert body["sample"]["lifecycle_state"] == "received"
    sample_id = body["sample"]["id"]

    sample_detail = client.get(f"/api/samples/{sample_id}", headers=admin["headers"])
    handover_events = [e for e in sample_detail.json()["events"] if e["event_type"].startswith("handover.")]
    assert [e["event_type"] for e in handover_events] == [
        "handover.collected",
        "handover.sealed",
        "handover.box_changed",
        "handover.received",
    ]

    # 正式接收幂等
    again = client.post(
        f"/api/handover/chains/{chain_id}/receive",
        headers=admin["headers"],
        json={},
    )
    assert again.status_code == 201
    assert again.json()["replayed"] is True


def test_late_predecessor_closes_broken_link(client, admin):
    """先到高序号包（前序缺失），后到的前序包触发确定性重算并闭合链。"""
    chain = chain_of(
        STATION,
        {
            0: [event("EV-0", "collected")],
            1: [event("EV-1", "sealed")],
            2: [event("EV-2", "received")],
        },
    )
    view = ingest(client, admin, chain[2])
    assert view["chains"][0]["state"] == "gapped"
    dispute = _dispute(view, "broken_link")
    assert dispute["detail"]["missing_prev_digest"] == chain[1]["digest"]

    ingest(client, admin, chain[0])
    view = ingest(client, admin, chain[1])
    assert view["chains"][0]["state"] == "unique"
    assert view["open_disputes"] == []

    detail = _chain(client, admin, _chain_id(view))
    assert [s["station_seq"] for s in detail["segments"]] == [0, 1, 2]


def test_middle_package_replaced_is_detected(client, admin):
    """seq1 被替换：seq2 的前序摘要指向已缺失的原始 seq1。"""
    chain = chain_of(
        STATION,
        {
            0: [event("EV-0", "collected")],
            1: [event("EV-1", "sealed")],
            2: [event("EV-2", "received")],
        },
    )
    original_seq1_digest = chain[1]["digest"]
    tampered_seq1 = package(
        STATION, 1, chain[0]["digest"], [event("EV-1", "sealed", detail={"box": "替换箱"})]
    )
    assert tampered_seq1["digest"] != original_seq1_digest

    ingest(client, admin, chain[0])
    ingest(client, admin, tampered_seq1)
    view = ingest(client, admin, chain[2])
    dispute = _dispute(view, "broken_link")
    assert dispute["detail"]["package_digest"] == chain[2]["digest"]

    # 锚定 seq2 为起点后仍然序号不连续：seq2 与 seq0/1 同链，seq2 起点 2，seq0 段 0..1，无空洞
    decided = client.post(
        f"/api/handover/disputes/{dispute['id']}/decisions",
        headers=admin["headers"],
        json={"decision": "anchor", "branch_digest": chain[2]["digest"], "note": "运输方确认补传"},
    )
    assert decided.status_code == 200, decided.text
    view = _station(client, admin)
    # seq0..替换seq1 段连续；seq2 锚定独立段，链上无未决分叉
    assert all(d["dispute_type"] != "broken_link" for d in view["open_disputes"])


def test_sequence_gap_must_be_resolved(client, admin):
    chain = chain_of(
        STATION,
        {
            0: [event("EV-0", "collected")],
            2: [event("EV-2", "received")],
        },
    )
    # 手工把 seq2 的 prev 指向 seq0（中间 seq1 空洞）
    chain[2] = package(STATION, 2, chain[0]["digest"], [event("EV-2", "received")])
    ingest(client, admin, chain[0])
    view = ingest(client, admin, chain[2])
    assert view["chains"][0]["state"] == "gapped"
    dispute = _dispute(view, "sequence_gap")
    assert dispute["detail"]["missing_seq"] == 1

    # 未裁决前不能接收
    chain_id = _chain_id(view)
    blocked = client.post(
        f"/api/handover/chains/{chain_id}/receive", headers=admin["headers"], json={}
    )
    assert blocked.status_code == 409

    waived = client.post(
        f"/api/handover/disputes/{dispute['id']}/decisions",
        headers=admin["headers"],
        json={"decision": "waive", "note": "站点确认该序号留空"},
    )
    assert waived.status_code == 200
    view = _station(client, admin)
    assert view["chains"][0]["state"] == "unique"
    assert view["chains"][0]["locked"] is True


def test_fork_requires_decision_and_pin_is_sticky(client, admin):
    seq0 = package(STATION, 0, "", [event("EV-0", "collected")])
    seq1a = package(STATION, 1, seq0["digest"], [event("EV-1A", "sealed", detail={"box": "A"})])
    seq1b = package(STATION, 1, seq0["digest"], [event("EV-1B", "sealed", detail={"box": "B"})])
    ingest(client, admin, seq0)
    ingest(client, admin, seq1a)
    view = ingest(client, admin, seq1b)
    assert view["chains"][0]["state"] == "forked"
    dispute = _dispute(view, "fork")
    assert {o["key"] for o in dispute["detail"]["options"]} == {seq1a["digest"], seq1b["digest"]}

    # 人工选择分支 B
    decided = client.post(
        f"/api/handover/disputes/{dispute['id']}/decisions",
        headers=admin["headers"],
        json={"decision": "adopt_branch", "branch_digest": seq1b["digest"], "note": "B 箱封签完整"},
    )
    assert decided.status_code == 200
    detail = _chain(client, admin, _chain_id(view))
    adopted = {(s["station_seq"], s["event"]["detail"].get("box")) for s in detail["segments"] if s["adopted"]}
    assert (1, "B") in adopted
    rejected = [s for s in detail["segments"] if not s["adopted"]]
    assert rejected and rejected[0]["adoption_reason"].startswith("处于分叉落选分支")
    assert any("人工裁决锁定" in s["adoption_reason"] for s in detail["segments"] if s["adopted"])

    # 之后到达挂在落选分支 A 上的续包，不能把锁定的选择静默改选
    seq2a = package(STATION, 2, seq1a["digest"], [event("EV-2A", "box_changed")])
    view = ingest(client, admin, seq2a)
    # 分叉事项早已裁决（不再 open），链维持唯一采用 B 分支
    assert all(d["dispute_type"] != "fork" for d in view["open_disputes"])
    detail = _chain(client, admin, _chain_id(view))
    assert not any(s["adopted"] and s["event_id"] == "EV-2A" for s in detail["segments"])
    assert view["chains"][0]["locked"] is True


def test_duplicate_event_conflict(client, admin):
    seq0 = package(STATION, 0, "", [event("EV-0", "collected")])
    seq1 = package(STATION, 1, seq0["digest"], [event("EV-1", "sealed")])
    # 重发包：同一 event_id 但内容不同（中间包被替换的一种表现）
    seq1_dup = package(
        STATION, 1, seq0["digest"], [event("EV-1", "sealed", detail={"seal": "另一个封签号"})]
    )
    # 两者 digest 相同会被当成重复上传，这里 prev 相同、events 不同，digest 必不同；但同 prev 又构成分叉
    assert seq1["digest"] != seq1_dup["digest"]
    ingest(client, admin, seq0)
    ingest(client, admin, seq1)
    view = ingest(client, admin, seq1_dup)
    types = {d["dispute_type"] for d in view["open_disputes"]}
    assert "duplicate_event" in types

    chain_id = _chain_id(view)
    detail = _chain(client, admin, chain_id)
    dup_segments = [s for s in detail["segments"] if s["event_id"] == "EV-1"]
    assert len(dup_segments) == 2
    assert sum(1 for s in dup_segments if s["adopted"]) == 1  # 默认先到先得
    adopted_first = next(s for s in dup_segments if s["adopted"])
    assert adopted_first["adoption_reason"] == "重复事件默认保留最先到达的副本"

    # 链存在分叉，不能接收
    assert client.post(
        f"/api/handover/chains/{chain_id}/receive", headers=admin["headers"], json={}
    ).status_code == 409


def test_cross_batch_reference(client, admin):
    seq0 = package(STATION, 0, "", [event("EV-0", "collected", batch_code="BATCH-X")])
    seq1 = package(STATION, 1, seq0["digest"], [event("EV-1", "sealed", batch_code="BATCH-Y")])
    ingest(client, admin, seq0)
    view = ingest(client, admin, seq1)
    dispute = _dispute(view, "cross_batch")
    assert {o["key"] for o in dispute["detail"]["options"]} == {"BATCH-X", "BATCH-Y"}

    detail = _chain(client, admin, _chain_id(view))
    cross = next(s for s in detail["segments"] if s["station_seq"] == 1)
    assert cross["adopted"] is False
    assert cross["adoption_reason"] == "跨批次引用，未被采用"

    decided = client.post(
        f"/api/handover/disputes/{dispute['id']}/decisions",
        headers=admin["headers"],
        json={"decision": "adopt_branch", "branch_digest": "BATCH-X", "note": "归属首批"},
    )
    assert decided.status_code == 200


def test_hash_mismatch_package_is_excluded(client, admin):
    seq0 = package(STATION, 0, "", [event("EV-0", "collected")])
    bogus = dict(seq0)
    bogus["digest"] = "deadbeef" * 8
    view = ingest(client, admin, bogus)
    dispute = _dispute(view, "hash_mismatch")
    assert dispute["scope"] == "package"
    pkg = view["packages"][0]
    assert pkg["hash_valid"] is False
    # 哈希无效包不形成任何样品链
    assert view["chains"] == []


def test_receive_requires_collection_origin(client, admin):
    seq0 = package(STATION, 0, "", [event("EV-9", "note")])
    view = ingest(client, admin, seq0)
    chain_id = _chain_id(view)
    response = client.post(
        f"/api/handover/chains/{chain_id}/receive", headers=admin["headers"], json={}
    )
    assert response.status_code == 409
    assert "采集起点" in response.text


def test_seeded_chain_is_frozen_after_receive(client, admin):
    """正式接收后再到的新包不会静默改写已封存链。"""
    chain = chain_of(STATION, {0: [event("EV-0", "collected")], 1: [event("EV-1", "received")]})
    ingest(client, admin, chain[0])
    view = ingest(client, admin, chain[1])
    chain_id = _chain_id(view)
    received = client.post(
        f"/api/handover/chains/{chain_id}/receive", headers=admin["headers"], json={}
    )
    assert received.status_code == 201

    late = package(STATION, 2, chain[1]["digest"], [event("EV-2", "box_changed")])
    view = ingest(client, admin, late)
    target = next(c for c in view["chains"] if c["sample_code"] == "SAM-1")
    assert target["received_sample_id"] is not None
    detail = _chain(client, admin, chain_id)
    assert all(s["event_id"] != "EV-2" for s in detail["segments"])


def test_permissions_split_by_role(client, admin):
    """审批人可以裁决但不能接包；样品管理员负责接包与接收。"""
    client.post(
        "/api/users",
        headers=admin["headers"],
        json={
            "username": "approver1",
            "password": "Approver!23456",
            "display_name": "审批人甲",
            "role_codes": ["approver"],
        },
    )
    login = client.post(
        "/api/auth/login",
        json={"username": "approver1", "password": "Approver!23456", "client_label": "t"},
    )
    assert login.status_code == 200, login.text
    approver = {"headers": {"Authorization": f"Bearer {login.json()['token']}"}}

    seq0 = package(STATION, 0, "", [event("EV-0", "collected")])
    denied = client.post("/api/handover/packages", headers=approver["headers"], json=seq0)
    assert denied.status_code == 403
    assert client.get("/api/handover/disputes", headers=approver["headers"]).status_code == 200
