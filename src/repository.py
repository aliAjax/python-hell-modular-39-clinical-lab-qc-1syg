import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS lot_instruments (
                    qc_lot_id TEXT NOT NULL,
                    instrument_id TEXT NOT NULL,
                    observed_run_id TEXT,
                    legacy INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(qc_lot_id, instrument_id)
                );
                CREATE INDEX IF NOT EXISTS idx_lot_inst_lot
                    ON lot_instruments(qc_lot_id);
                CREATE TABLE IF NOT EXISTS freeze_causes (
                    cause_key TEXT PRIMARY KEY,
                    freeze_order_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS work_items (
                    id TEXT PRIMARY KEY,
                    freeze_order_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    target_kind TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_work_freeze
                    ON work_items(freeze_order_id, status);
            """)
        self._schema_version = 2

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # -- shared lot / instrument links -------------------------------------

    def upsert_lot_instrument(self, qc_lot_id, instrument_id, observed_run_id=None, legacy=False):
        """Record that an instrument has used a shared QC lot.

        Observed links upgrade legacy ones. Legacy links (reconstructed at
        upgrade time) never overwrite an observed relationship. Returns True
        when a new link row was inserted.
        """
        now = utcnow()
        with self._connect() as connection:
            if legacy:
                cursor = connection.execute(
                    "INSERT INTO lot_instruments(qc_lot_id, instrument_id, observed_run_id, legacy, created_at) "
                    "VALUES (?, ?, ?, 1, ?) "
                    "ON CONFLICT(qc_lot_id, instrument_id) DO NOTHING",
                    (qc_lot_id, instrument_id, observed_run_id, now),
                )
            else:
                cursor = connection.execute(
                    "INSERT INTO lot_instruments(qc_lot_id, instrument_id, observed_run_id, legacy, created_at) "
                    "VALUES (?, ?, ?, 0, ?) "
                    "ON CONFLICT(qc_lot_id, instrument_id) DO UPDATE SET "
                    "legacy = 0, "
                    "observed_run_id = COALESCE(excluded.observed_run_id, observed_run_id)",
                    (qc_lot_id, instrument_id, observed_run_id, now),
                )
            return cursor.rowcount > 0

    def list_lot_instruments(self, qc_lot_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM lot_instruments WHERE qc_lot_id = ? ORDER BY created_at, instrument_id",
                (qc_lot_id,),
            ).fetchall()
        return [
            {
                "qc_lot_id": row["qc_lot_id"],
                "instrument_id": row["instrument_id"],
                "observed_run_id": row["observed_run_id"],
                "legacy": bool(row["legacy"]),
            }
            for row in rows
        ]

    # -- freeze orders ------------------------------------------------------

    def get_freeze_by_cause(self, cause_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT freeze_order_id FROM freeze_causes WHERE cause_key = ?",
                (cause_key,),
            ).fetchone()
        return row["freeze_order_id"] if row else None

    def insert_freeze_order(self, entity, cause_key, items, audit_detail):
        """Atomically create the freeze order, its cause record and work items.

        Returns (freeze_order_id, True) on success, or (existing_id, False)
        when another freeze with the same cause already exists.
        """
        now = utcnow()
        payload = json.dumps(entity["data"], ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT freeze_order_id FROM freeze_causes WHERE cause_key = ?",
                (cause_key,),
            ).fetchone()
            if existing:
                connection.rollback()
                return existing["freeze_order_id"], False
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (
                    entity["id"],
                    "freeze_order",
                    entity["status"],
                    payload,
                    entity["created_by"],
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO freeze_causes(cause_key, freeze_order_id, created_at) VALUES (?, ?, ?)",
                (cause_key, entity["id"], now),
            )
            for item in items:
                connection.execute(
                    "INSERT INTO work_items(id, freeze_order_id, phase, item_type, target_kind, "
                    "target_id, status, payload, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        item["id"],
                        entity["id"],
                        item["phase"],
                        item["item_type"],
                        item["target_kind"],
                        item["target_id"],
                        item["status"],
                        json.dumps(item.get("payload") or {}, ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
                "to_status, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity["id"],
                    audit_detail["actor_id"],
                    audit_detail["actor_role"],
                    "open",
                    None,
                    entity["status"],
                    json.dumps(audit_detail["detail"], ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            connection.commit()
            return entity["id"], True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def list_work_items(self, freeze_order_id, status=None):
        clauses = ["freeze_order_id = ?"]
        params = [freeze_order_id]
        if status:
            clauses.append("status = ?")
            params.append(status)
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM work_items WHERE " + " AND ".join(clauses) + " ORDER BY created_at, id",
                params,
            ).fetchall()
        return [self._work_item_from_row(row) for row in rows]

    @staticmethod
    def _work_item_from_row(row):
        return {
            "id": row["id"],
            "freeze_order_id": row["freeze_order_id"],
            "phase": row["phase"],
            "item_type": row["item_type"],
            "target_kind": row["target_kind"],
            "target_id": row["target_id"],
            "status": row["status"],
            "payload": json.loads(row["payload"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def claim_work_item(self, item_id):
        """Atomically claim a pending work item. Returns the item or None."""
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM work_items WHERE id = ?", (item_id,)
            ).fetchone()
            if not row or row["status"] != "pending":
                connection.rollback()
                return None
            connection.execute(
                "UPDATE work_items SET status = 'in_progress', updated_at = ? WHERE id = ?",
                (now, item_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_work_item(item_id)

    def get_work_item(self, item_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM work_items WHERE id = ?", (item_id,)
            ).fetchone()
        return self._work_item_from_row(row) if row else None

    def complete_work_item(self, item_id):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "UPDATE work_items SET status = 'completed', updated_at = ? WHERE id = ?",
                (now, item_id),
            )

    def fail_work_item(self, item_id):
        """Return a claimed item to pending so a retry only picks leftovers."""
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "UPDATE work_items SET status = 'pending', updated_at = ? WHERE id = ? AND status = 'in_progress'",
                (now, item_id),
            )

    def add_work_items(self, freeze_order_id, items):
        now = utcnow()
        with self._connect() as connection:
            for item in items:
                connection.execute(
                    "INSERT INTO work_items(id, freeze_order_id, phase, item_type, target_kind, "
                    "target_id, status, payload, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        item["id"],
                        freeze_order_id,
                        item["phase"],
                        item["item_type"],
                        item["target_kind"],
                        item["target_id"],
                        item["status"],
                        json.dumps(item.get("payload") or {}, ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )

    def work_item_counts(self, freeze_order_id, phase):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS n FROM work_items WHERE freeze_order_id = ? AND phase = ? "
                "GROUP BY status",
                (freeze_order_id, phase),
            ).fetchall()
        counts = {"pending": 0, "in_progress": 0, "completed": 0}
        for row in rows:
            counts[row["status"]] = int(row["n"])
        return counts

    def compare_and_swap_status(self, entity_id, expected_statuses, new_status, data, actor):
        """Conditionally move an entity to a new status inside one transaction.

        Returns the updated entity dict, or None when the current status does
        not match any of expected_statuses.
        """
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row or row["status"] not in expected_statuses:
                connection.rollback()
                return None
            from_status = row["status"]
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                (new_status, payload, now, entity_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id), from_status

    def append_freeze_audit(self, entity_id, actor, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
                "to_status, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor.user_id,
                    actor.role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
