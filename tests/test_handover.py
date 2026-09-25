from __future__ import annotations

from app.samples.handover import compute_package_digest


def make_event(event_id, sample_ref="FS-001", event_type="collected", batch_code="TR-01",
               occurred_at="2026-09-20T08:00:00+00:00", details=None):
    if details is None:
        details = (
            {"sample_type": "土壤", "quantity": 500, "unit": "g", "project_code": "P-ALPHA"}
            if event_type == "collected"
            else {}
        )
    return {
        "event_id": event_id,
        "sample_ref": sample_ref,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "actor": "站点人员甲",
        "batch_code": batch_code,
        "details": details,
    }


def make_package(package_code, seq, prev_digest="", events=None, station="ST-01"):
    return {
        "package_code": package_code,
        "station_code": station,
        "station_seq": seq,
        "prev_digest": prev_digest,
        "participants": ["站点人员甲", "站点人员乙"],
        "events": events if events is not None else [make_event(f"EV-{package_code}")],
        "packaged_at": "2026-09-20T08:00:00+00:00",
    }


def full_chain(station="ST-01", sample_ref="FS-001", batch_code="TR-01"):
    events = [
        [make_event("EV-1", sample_ref, "collected", batch_code, "2026-09-20T08:00:00+00:00")],
        [make_event("EV-2", sample_ref, "sealed", batch_code, "2026-09-20T10:00:00+00:00", {"seal_id": "SEAL-1"})],
        [make_event("EV-3", sample_ref, "reboxed", batch_code, "2026-09-21T09:00:00+00:00", {"from_box": "BX-1", "to_box": "BX-2"})],
        [make_event("EV-4", sample_ref, "received", batch_code, "2026-09-23T15:00:00+00:00")],
    ]
    packages = []
    prev = ""
    for index, package_events in enumerate(events, start=1):
        package = make_package(f"{station}-PKG-{index:04d}", index, prev, package_events, station)
        packages.append(package)
        prev = compute_package_digest(package)
    return packages


def upload(client, admin, package, expected=201):
    response = client.post("/api/handover/packages", headers=admin["headers"], json=package)
    assert response.status_code == expected, response.text
    return response.json()


def action_codes(view):
    return [item["action"] for item in view["available_actions"]]


def test_out_of_order_packages_merge_into_continuous_chain(client, admin):
    p1, p2, p3, p4 = full_chain()
    first = upload(client, admin, p2)
    assert first["replayed"] is False
    assert first["chain"]["status"] == "blocked"
    assert first["chain"]["segments"][0]["adoption"] == "orphan"
    assert "await_predecessor" in action_codes(first["chain"])
    gaps = [item for item in first["chain"]["pending_conflicts"] if item["conflict_type"] == "sequence_gap"]
    assert [item["details"]["missing_seq"] for item in gaps] == [1]

    second = upload(client, admin, p1)
    chain = second["chain"]
    assert chain["status"] == "continuous"
    assert [segment["package_code"] for segment in chain["segments"]] == [p1["package_code"], p2["package_code"]]
    assert chain["segments"][0]["position"] == 1
    assert chain["segments"][1]["position"] == 2
    assert all(segment["rationale"] for segment in chain["segments"])
    assert chain["head"]["package_digest"] == compute_package_digest(p2)
    resolved = client.get("/api/handover/conflicts?state=resolved", headers=admin["headers"]).json()
    assert any(item["conflict_type"] == "sequence_gap" for item in resolved)

    upload(client, admin, p3)
    fourth = upload(client, admin, p4)
    sample = fourth["chain"]["sample_chains"][0]
    assert sample["sample_ref"] == "FS-001"
    assert sample["status"] == "chain_ready"
    assert "promote" in action_codes(sample)


def test_duplicate_upload_returns_original_merge_result(client, admin):
    p1, p2, *_ = full_chain()
    first = upload(client, admin, p1)
    again = upload(client, admin, p1)
    assert again["replayed"] is True
    assert again["package"]["id"] == first["package"]["id"]
    upload(client, admin, p2)
    replay = upload(client, admin, p1)
    assert replay["replayed"] is True
    assert replay["package"]["package_code"] == p1["package_code"]
    assert len(replay["chain"]["segments"]) == 2


def test_same_package_code_with_different_content_is_rejected(client, admin):
    p1, *_ = full_chain()
    upload(client, admin, p1)
    tampered = dict(p1, events=[make_event("EV-TAMPER", event_type="sealed")])
    response = client.post("/api/handover/packages", headers=admin["headers"], json=tampered)
    assert response.status_code == 409


def test_declared_digest_mismatch_fails_hash_verification(client, admin):
    p1, *_ = full_chain()
    payload = dict(p1, package_digest="0" * 64)
    response = client.post("/api/handover/packages", headers=admin["headers"], json=payload)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_sequence_gap_detected_and_filled_by_late_predecessor(client, admin):
    p1, p2, p3, p4 = full_chain()
    upload(client, admin, p1)
    upload(client, admin, p3)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    assert chain["status"] == "blocked"
    gaps = [item for item in chain["pending_conflicts"] if item["conflict_type"] == "sequence_gap"]
    assert [item["details"]["missing_seq"] for item in gaps] == [2]
    assert "adjudicate" in action_codes(chain)
    assert "await_predecessor" in action_codes(chain)

    upload(client, admin, p2)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    assert chain["status"] == "continuous"
    assert [segment["station_seq"] for segment in chain["segments"]] == [1, 2, 3]
    resolved = client.get("/api/handover/conflicts?state=resolved", headers=admin["headers"]).json()
    assert any(
        item["conflict_type"] == "sequence_gap" and item["resolution"]["manner"] == "auto"
        for item in resolved
    )
    upload(client, admin, p4)
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "chain_ready"


def test_fork_requires_adjudication_and_manual_choice_is_sticky(client, admin):
    p1, p2, p3, p4 = full_chain()
    alt2 = make_package(
        "ST-01-PKG-9002", 2, compute_package_digest(p1),
        [make_event("EV-ALT-2", event_type="sealed", details={"seal_id": "SEAL-9"})],
    )
    alt3 = make_package(
        "ST-01-PKG-9003", 3, compute_package_digest(alt2),
        [make_event("EV-ALT-3", event_type="reboxed")],
    )
    upload(client, admin, p1)
    upload(client, admin, p2)
    upload(client, admin, alt2)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    assert chain["status"] == "blocked"
    forks = [item for item in chain["pending_conflicts"] if item["conflict_type"] == "fork"]
    assert len(forks) == 1
    conflict_id = forks[0]["id"]

    denied = client.post("/api/handover/chains/ST-01/samples/FS-001/promote", headers=admin["headers"])
    assert denied.status_code == 409

    decision = client.post(
        f"/api/handover/conflicts/{conflict_id}/decision",
        headers=admin["headers"],
        json={
            "action": "select_package",
            "selected_package_digest": compute_package_digest(p2),
            "rationale": "与站点纸质交接单核对一致",
        },
    )
    assert decision.status_code == 200, decision.text
    chain = decision.json()["chains"][0]
    adoption = {segment["package_code"]: segment["adoption"] for segment in chain["segments"]}
    assert adoption[p2["package_code"]] == "adopted"
    assert adoption[alt2["package_code"]] == "fork_loser"

    upload(client, admin, alt3)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    adoption = {segment["package_code"]: segment["adoption"] for segment in chain["segments"]}
    assert adoption[p2["package_code"]] == "adopted"
    assert adoption[alt2["package_code"]] == "fork_loser"
    assert adoption[alt3["package_code"]] == "fork_loser"
    assert chain["pending_conflicts"] == []

    upload(client, admin, p3)
    result = upload(client, admin, p4)
    assert result["chain"]["sample_chains"][0]["status"] == "chain_ready"


def test_duplicate_event_sent_to_adjudication(client, admin):
    p1 = make_package("ST-01-PKG-0001", 1, "", [make_event("EV-1", event_type="collected")])
    p2 = make_package(
        "ST-01-PKG-0002", 2, compute_package_digest(p1),
        [make_event("EV-1", event_type="collected"), make_event("EV-2", event_type="sealed")],
    )
    p3 = make_package("ST-01-PKG-0003", 3, compute_package_digest(p2), [make_event("EV-3", event_type="received")])
    for package in (p1, p2, p3):
        upload(client, admin, package)
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "conflicted"
    conflict = next(item for item in view["pending_conflicts"] if item["conflict_type"] == "duplicate_event")
    assert [event["event_id"] for event in view["events"]] == ["EV-1", "EV-2", "EV-3"]
    excluded = [event for event in view["excluded_events"] if event["event_id"] == "EV-1"]
    assert len(excluded) == 1
    assert "重复" in excluded[0]["rationale"]

    decision = client.post(
        f"/api/handover/conflicts/{conflict['id']}/decision",
        headers=admin["headers"],
        json={"action": "keep_first", "rationale": "首次出现为原始记录"},
    )
    assert decision.status_code == 200, decision.text
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "chain_ready"


def test_cross_batch_reference_requires_adjudication(client, admin):
    p1 = make_package("ST-01-PKG-0001", 1, "", [make_event("EV-1", event_type="collected", batch_code="TR-01")])
    p2 = make_package(
        "ST-01-PKG-0002", 2, compute_package_digest(p1),
        [make_event("EV-2", event_type="sealed", batch_code="TR-02")],
    )
    p3 = make_package(
        "ST-01-PKG-0003", 3, compute_package_digest(p2),
        [make_event("EV-3", event_type="received", batch_code="TR-01")],
    )
    for package in (p1, p2, p3):
        upload(client, admin, package)
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "conflicted"
    xref = next(item for item in view["pending_conflicts"] if item["conflict_type"] == "cross_batch_reference")
    assert xref["details"]["expected_batch"] == "TR-01"
    assert xref["details"]["found_batch"] == "TR-02"
    assert any(event["event_id"] == "EV-2" for event in view["excluded_events"])

    decision = client.post(
        f"/api/handover/conflicts/{xref['id']}/decision",
        headers=admin["headers"],
        json={"action": "accept_reference", "rationale": "确认换箱时批次标签误贴，实物无误"},
    )
    assert decision.status_code == 200, decision.text
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "chain_ready"
    adopted = next(event for event in view["events"] if event["event_id"] == "EV-2")
    assert "人工裁决采纳" in adopted["rationale"]


def test_promote_creates_formal_receipt_and_is_idempotent(client, admin):
    for package in full_chain():
        upload(client, admin, package)
    promoted = client.post("/api/handover/chains/ST-01/samples/FS-001/promote", headers=admin["headers"])
    assert promoted.status_code == 201, promoted.text
    body = promoted.json()
    assert body["replayed"] is False
    sample = body["sample"]
    assert sample["sample_code"] == "FS-001"
    assert sample["lifecycle_state"] == "received"
    assert sample["quantity"] == 500

    again = client.post("/api/handover/chains/ST-01/samples/FS-001/promote", headers=admin["headers"])
    assert again.status_code == 201
    assert again.json()["replayed"] is True
    assert again.json()["sample"]["id"] == sample["id"]

    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"]).json()
    assert [event["event_type"] for event in detail["events"]] == [
        "handover.collected", "handover.sealed", "handover.reboxed", "handover.received",
    ]
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "received"
    assert view["received_sample_id"] == sample["id"]


def test_promote_rejected_when_chain_not_ready(client, admin):
    p1, p2, p3, _ = full_chain()
    for package in (p1, p2, p3):
        upload(client, admin, package)
    response = client.post("/api/handover/chains/ST-01/samples/FS-001/promote", headers=admin["headers"])
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"


def test_gap_can_be_accepted_manually(client, admin):
    p1 = make_package("ST-01-PKG-0001", 1, "", [make_event("EV-1", event_type="collected")])
    p2 = make_package("ST-01-PKG-0002", 2, compute_package_digest(p1), [make_event("EV-2", event_type="sealed")])
    p4 = make_package("ST-01-PKG-0004", 4, compute_package_digest(p2), [make_event("EV-4", event_type="received")])
    for package in (p1, p2, p4):
        upload(client, admin, package)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    assert chain["status"] == "blocked"
    gap = next(item for item in chain["pending_conflicts"] if item["conflict_type"] == "sequence_gap")
    assert gap["details"]["missing_seq"] == 3
    adoption = {segment["package_code"]: segment["adoption"] for segment in chain["segments"]}
    assert adoption[p4["package_code"]] == "adopted"
    jumped = next(segment for segment in chain["segments"] if segment["package_code"] == p4["package_code"])
    assert "空洞" in jumped["rationale"]

    decision = client.post(
        f"/api/handover/conflicts/{gap['id']}/decision",
        headers=admin["headers"],
        json={"action": "accept_gap", "rationale": "站点书面确认序号3未启用"},
    )
    assert decision.status_code == 200, decision.text
    view = client.get("/api/handover/chains/ST-01/samples/FS-001", headers=admin["headers"]).json()
    assert view["status"] == "chain_ready"


def test_reverse_order_arrival_converges_to_same_chain(client, admin):
    packages = full_chain()
    for package in reversed(packages):
        upload(client, admin, package)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    assert chain["status"] == "continuous"
    assert [segment["package_code"] for segment in chain["segments"]] == [package["package_code"] for package in packages]
    assert [segment["position"] for segment in chain["segments"]] == [1, 2, 3, 4]
    assert chain["sample_chains"][0]["status"] == "chain_ready"
    assert client.get("/api/handover/conflicts?state=pending", headers=admin["headers"]).json() == []


def test_conflict_decision_validates_action_and_candidate(client, admin):
    p1, p2, *_ = full_chain()
    alt2 = make_package(
        "ST-01-PKG-9002", 2, compute_package_digest(p1),
        [make_event("EV-ALT-2", event_type="sealed")],
    )
    upload(client, admin, p1)
    upload(client, admin, p2)
    upload(client, admin, alt2)
    chain = client.get("/api/handover/chains/ST-01", headers=admin["headers"]).json()
    conflict = chain["pending_conflicts"][0]
    wrong_action = client.post(
        f"/api/handover/conflicts/{conflict['id']}/decision",
        headers=admin["headers"],
        json={"action": "accept_gap", "rationale": "动作与冲突类型不匹配"},
    )
    assert wrong_action.status_code == 422
    wrong_digest = client.post(
        f"/api/handover/conflicts/{conflict['id']}/decision",
        headers=admin["headers"],
        json={"action": "select_package", "selected_package_digest": "1" * 64, "rationale": "摘要不在候选中"},
    )
    assert wrong_digest.status_code == 422
