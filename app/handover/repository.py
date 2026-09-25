from __future__ import annotations

import json
import sqlite3
from typing import Any


class HandoverRepository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    # ---- packages -------------------------------------------------------

    def insert_package(self, envelope: dict[str, Any], computed_digest: str, now: str) -> int:
        cursor = self.connection.execute(
            """INSERT INTO handover_packages(
                   station_code,station_seq,prev_digest,claimed_digest,computed_digest,hash_valid,
                   participants_json,events_json,raw_json,received_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                envelope["station_code"],
                envelope["station_seq"],
                envelope.get("prev_digest", ""),
                envelope["digest"],
                computed_digest,
                1 if computed_digest == envelope["digest"] else 0,
                json.dumps(envelope["participants"], ensure_ascii=False),
                json.dumps(envelope["events"], ensure_ascii=False),
                json.dumps(envelope, ensure_ascii=False, sort_keys=True),
                now,
            ),
        )
        return int(cursor.lastrowid)

    def find_package_by_digest(self, station_code: str, computed_digest: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM handover_packages WHERE station_code=? AND computed_digest=? ORDER BY id LIMIT 1",
            (station_code, computed_digest),
        ).fetchone()
        return dict(row) if row else None

    def get_package(self, package_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM handover_packages WHERE id=?", (package_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_station_packages(self, station_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM handover_packages WHERE station_code=? ORDER BY id",
            (station_code,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["participants"] = json.loads(item.pop("participants_json"))
            item["events"] = json.loads(item.pop("events_json"))
            result.append(item)
        return result

    def list_stations(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """SELECT station_code,
                          COUNT(*) AS package_count,
                          MIN(station_seq) AS min_seq,
                          MAX(station_seq) AS max_seq,
                          MIN(received_at) AS first_received_at,
                          MAX(received_at) AS last_received_at
                   FROM handover_packages GROUP BY station_code ORDER BY station_code"""
            ).fetchall()
        ]

    # ---- chains ---------------------------------------------------------

    def upsert_chain(self, station_code: str, sample_code: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            """INSERT INTO handover_chains(chain_code,station_code,sample_code,state,created_at,updated_at)
               VALUES(?,?,?,'conflicted',?,?)
               ON CONFLICT(station_code,sample_code) DO UPDATE SET updated_at=excluded.updated_at""",
            (f"CHAIN-{station_code}-{sample_code}", station_code, sample_code, now, now),
        )
        return self.get_chain_by_sample(station_code, sample_code)

    def get_chain(self, chain_id: int) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM handover_chains WHERE id=?", (chain_id,)).fetchone()
        return dict(row) if row else None

    def get_chain_by_sample(self, station_code: str, sample_code: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM handover_chains WHERE station_code=? AND sample_code=?",
            (station_code, sample_code),
        ).fetchone()
        return dict(row)

    def list_station_chains(self, station_code: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM handover_chains WHERE station_code=? ORDER BY sample_code",
                (station_code,),
            ).fetchall()
        ]

    def update_chain_state(
        self,
        chain_id: int,
        state: str,
        head_digest: str,
        last_station_seq: int | None,
        opening_batch_code: str | None,
        locked: bool,
        now: str,
    ) -> None:
        self.connection.execute(
            """UPDATE handover_chains
               SET state=?,head_digest=?,last_station_seq=?,opening_batch_code=?,locked=?,updated_at=?
               WHERE id=?""",
            (state, head_digest, last_station_seq, opening_batch_code, 1 if locked else 0, now, chain_id),
        )

    # ---- records (derived, rebuilt on every recompute) ------------------

    def replace_records(self, chain_id: int, records: list[dict[str, Any]], now: str) -> None:
        self.connection.execute("DELETE FROM handover_chain_records WHERE chain_id=?", (chain_id,))
        self.connection.executemany(
            """INSERT INTO handover_chain_records(
                   chain_id,package_id,station_seq,event_index,sample_code,event_id,
                   record_digest,content_json,adopted,adoption_reason,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    chain_id,
                    record["package_id"],
                    record["station_seq"],
                    record["event_index"],
                    record["sample_code"],
                    record["event_id"],
                    record["record_digest"],
                    json.dumps(record["content"], ensure_ascii=False, sort_keys=True),
                    1 if record["adopted"] else 0,
                    record["adoption_reason"],
                    now,
                )
                for record in records
            ],
        )

    def list_records(self, chain_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM handover_chain_records WHERE chain_id=? ORDER BY station_seq,event_index,id",
            (chain_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["content"] = json.loads(item.pop("content_json"))
            item["adopted"] = bool(item["adopted"])
            result.append(item)
        return result

    # ---- disputes -------------------------------------------------------

    def delete_open_for_station(self, station_code: str) -> None:
        self.connection.execute(
            "DELETE FROM handover_disputes WHERE station_code=? AND status='open'",
            (station_code,),
        )

    def insert_dispute(self, dispute: dict[str, Any], now: str) -> int:
        cursor = self.connection.execute(
            """INSERT INTO handover_disputes(
                   dispute_code,chain_id,station_code,dispute_type,scope,package_id,station_seq,
                   title,detail_json,status,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?, 'open',?,?)""",
            (
                dispute["dispute_code"],
                dispute.get("chain_id"),
                dispute["station_code"],
                dispute["dispute_type"],
                dispute["scope"],
                dispute.get("package_id"),
                dispute.get("station_seq"),
                dispute["title"],
                json.dumps(dispute_detail(dispute), ensure_ascii=False, sort_keys=True),
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def get_dispute(self, dispute_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM handover_disputes WHERE id=?", (dispute_id,)
        ).fetchone()
        if not row:
            return None
        return hydrate_dispute(row)

    def list_disputes(self, station_code: str | None, status_filter: str | None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if station_code:
            clauses.append("station_code=?")
            params.append(station_code)
        if status_filter:
            clauses.append("status=?")
            params.append(status_filter)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.connection.execute(
            "SELECT * FROM handover_disputes" + where + " ORDER BY status DESC, id DESC",
            tuple(params),
        ).fetchall()
        result = []
        for row in rows:
            result.append(hydrate_dispute(row))
        return result

    def list_open_disputes(self, station_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM handover_disputes WHERE station_code=? AND status='open' ORDER BY id",
            (station_code,),
        ).fetchall()
        return [hydrate_dispute(row) for row in rows]

    def list_decided_for_chains(self, chain_ids: list[int]) -> list[dict[str, Any]]:
        if not chain_ids:
            return []
        placeholders = ",".join("?" for _ in chain_ids)
        rows = self.connection.execute(
            f"SELECT * FROM handover_disputes WHERE status='decided' AND chain_id IN ({placeholders}) ORDER BY id",
            tuple(chain_ids),
        ).fetchall()
        return [hydrate_dispute(row) for row in rows]

    def decide_dispute(
        self,
        dispute_id: int,
        decision: str,
        decided_by: int,
        note: str,
        resolution: dict[str, Any],
        now: str,
    ) -> None:
        self.connection.execute(
            """UPDATE handover_disputes
               SET status='decided',decision=?,decided_by=?,decided_at=?,decision_note=?,
                   resolution_json=?,updated_at=?
               WHERE id=?""",
            (
                decision,
                decided_by,
                now,
                note,
                json.dumps(resolution, ensure_ascii=False, sort_keys=True),
                now,
                dispute_id,
            ),
        )

    # ---- reception ------------------------------------------------------

    def get_reception_by_chain(self, chain_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM handover_receptions WHERE chain_id=?", (chain_id,)
        ).fetchone()
        return dict(row) if row else None

    def insert_reception(
        self,
        chain_id: int,
        sample_id: int,
        batch_id: int,
        accepted_by: int,
        head_digest: str,
        now: str,
    ) -> int:
        cursor = self.connection.execute(
            """INSERT INTO handover_receptions(chain_id,sample_id,batch_id,accepted_by,head_digest,accepted_at)
               VALUES(?,?,?,?,?,?)""",
            (chain_id, sample_id, batch_id, accepted_by, head_digest, now),
        )
        return int(cursor.lastrowid)


def dispute_detail(dispute: dict[str, Any]) -> dict[str, Any]:
    return dispute.get("detail") or {}


def hydrate_dispute(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["detail"] = json.loads(item.pop("detail_json"))
    raw_resolution = item.pop("resolution_json", None)
    if raw_resolution:
        item["resolution"] = json.loads(raw_resolution)
    return item
