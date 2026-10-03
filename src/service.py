from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        # Tests may inject a fault: {work_item_type: exception} raised while
        # processing the matching item, leaving the freeze order resumable.
        self.faults = {}

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        if kind == "freeze_order":
            return self.open_freeze_order(actor, data or {}, idempotency_key)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if kind == "qc_run":
            # A run on a shared lot is evidence that this instrument uses it.
            # Legacy runs (pre-upgrade data) get their scope backfilled later
            # by upgrade_legacy_lot_links, one instrument at a time.
            if not payload.get("legacy_observed"):
                self.repository.upsert_lot_instrument(
                    payload.get("qc_lot_id"),
                    payload.get("instrument_id"),
                    observed_run_id=entity_id,
                    legacy=False,
                )
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = entity["kind"]
        if kind == "freeze_order" and action == "resume":
            return self.resume_freeze(actor, entity_id)
        if kind == "freeze_order" and action == "recover":
            return self.recover_freeze(actor, entity_id, data or {}, expected_version)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        if kind == "freeze_order" and action == "review" and entity["status"] not in (
            "frozen",
            "review_rejected",
        ):
            # A concurrent reviewer already moved this order: report the
            # freshest blockers rather than a bare state-machine error.
            raise ConflictError(
                "freeze order %s is already %s" % (entity_id, entity["status"]),
                details=self._review_conflict_details(entity),
            )
        try:
            next_status, patch = self.rules.validate_transition(
                actor, entity, action, dict(data or {}), self._lookup
            )
        except ConflictError as exc:
            if kind == "freeze_order" and action == "review":
                exc.details.update(self._review_conflict_details(entity))
            raise
        merged = dict(entity["data"])
        merged.update(patch)
        try:
            updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        except ConflictError as exc:
            if kind == "freeze_order" and action == "review":
                latest = self.repository.get_entity(entity_id)
                if latest:
                    exc.details.update(self._review_conflict_details(latest))
            raise
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # -- freeze isolation / recovery ---------------------------------------

    def open_freeze_order(self, actor, data, idempotency_key=None):
        """Open one freeze order per root cause and fan out quarantine work."""
        if actor.role not in ("supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed to open freeze orders" % actor.role)
        trigger_run_id = data.get("trigger_qc_run_id")
        reason = str(data.get("reason", "")).strip()
        if not trigger_run_id:
            raise ValidationError("trigger_qc_run_id is required")
        if not reason:
            raise ValidationError("reason is required")
        trigger_run = self.repository.get_entity(trigger_run_id)
        if not trigger_run or trigger_run["kind"] != "qc_run":
            raise ValidationError("trigger qc run does not exist")
        lot_id = trigger_run["data"].get("qc_lot_id")
        assay_id = trigger_run["data"].get("assay_id")
        trigger_instrument_id = trigger_run["data"].get("instrument_id")
        lot = self.repository.get_entity(lot_id)
        if not lot:
            raise ValidationError("qc lot of trigger run disappeared")

        cause_key = data.get("cause_key") or ("qc_run:%s" % trigger_run_id)

        # Observed post-upgrade links make the lot shared; legacy links only
        # describe the trigger instrument itself.
        trigger_is_legacy = bool(trigger_run["data"].get("legacy_observed"))
        instrument_ids = [trigger_instrument_id]
        if not trigger_is_legacy:
            for link in self.repository.list_lot_instruments(lot_id):
                if link["legacy"]:
                    continue
                if link["instrument_id"] not in instrument_ids:
                    instrument_ids.append(link["instrument_id"])

        waiting_batches, released_snapshots = self._collect_lot_batches(
            assay_id, instrument_ids, trigger_run
        )

        freeze_id = data.get("id") or str(uuid4())
        freeze_data = {
            "assay_id": assay_id,
            "qc_lot_id": lot_id,
            "lot_no": lot["data"].get("lot_no"),
            "trigger_qc_run_id": trigger_run_id,
            "trigger_instrument_id": trigger_instrument_id,
            "instrument_ids": instrument_ids,
            "reason": reason,
            "legacy_scope": trigger_is_legacy,
            "opened_by": actor.user_id,
            "held_batch_ids": [batch["id"] for batch in waiting_batches],
            "released_batch_snapshot_ids": [item["batch_id"] for item in released_snapshots],
            "released_batch_snapshots": released_snapshots,
            "blocked_instrument_ids": [],
            "reviewed_by": None,
            "decision": None,
            "recovered_by": None,
        }
        items = []
        for instrument_id in instrument_ids:
            items.append(
                self._work_item(
                    freeze_id,
                    "quarantine",
                    "block_instrument",
                    "instrument",
                    instrument_id,
                )
            )
        for batch in waiting_batches:
            items.append(
                self._work_item(
                    freeze_id,
                    "quarantine",
                    "hold_batch",
                    "result_batch",
                    batch["id"],
                    payload={
                        "assay_id": assay_id,
                        "instrument_id": batch["data"].get("instrument_id"),
                        "planned_status": batch["status"],
                        "planned_version": batch["version"],
                    },
                )
            )
        items.append(
            self._work_item(
                freeze_id,
                "quarantine",
                "record_snapshot",
                "freeze_order",
                freeze_id,
                payload={"released_batch_snapshots": released_snapshots},
            )
        )
        freeze_entity = {
            "id": freeze_id,
            "kind": "freeze_order",
            "status": "freezing",
            "data": freeze_data,
            "created_by": actor.user_id,
        }
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        freeze_order_id, created = self.repository.insert_freeze_order(
            freeze_entity,
            cause_key,
            items,
            {
                "actor_id": actor.user_id,
                "actor_role": actor.role,
                "detail": {
                    "cause_key": cause_key,
                    "qc_lot_id": lot_id,
                    "instrument_ids": instrument_ids,
                    "held_batch_ids": freeze_data["held_batch_ids"],
                    "released_batch_snapshot_ids": freeze_data["released_batch_snapshot_ids"],
                    "legacy_scope": trigger_is_legacy,
                },
            },
        )
        if not created:
            existing_order = self.repository.get_entity(freeze_order_id)
            raise ConflictError(
                "freeze order %s already exists for this cause" % freeze_order_id,
                details={
                    "freeze_order_id": freeze_order_id,
                    "freeze_status": existing_order["status"],
                    "instrument_ids": existing_order["data"].get("instrument_ids", []),
                    "blocking_holds": list(existing_order["data"].get("held_batch_ids", [])),
                    "already_existing": True,
                },
            )
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, freeze_order_id)
        return self._process_pending(actor, freeze_order_id, "quarantine", target="frozen")

    def _collect_lot_batches(self, assay_id, instrument_ids, trigger_run):
        waiting = []
        snapshots = []
        trigger_at = str(trigger_run["data"].get("run_at", ""))
        for batch in self.repository.list_entities(kind="result_batch"):
            if batch["data"].get("assay_id") != assay_id:
                continue
            if batch["data"].get("instrument_id") not in instrument_ids:
                continue
            run = self.repository.get_entity(batch["data"].get("qc_run_id", ""))
            if not run or run["data"].get("qc_lot_id") != trigger_run["data"].get("qc_lot_id"):
                continue
            if batch["status"] in ("waiting", "intercepted"):
                # Batches already intercepted by another freeze still belong
                # in scope: recovery must wait for every freeze to lift.
                waiting.append(batch)
            elif batch["status"] == "released":
                snapshots.append(
                    {
                        "batch_id": batch["id"],
                        "status": batch["status"],
                        "version": batch["version"],
                        "instrument_id": batch["data"].get("instrument_id"),
                        "qc_run_id": batch["data"].get("qc_run_id"),
                        "run_at": batch["data"].get("run_at"),
                        "patient_count": batch["data"].get("patient_count"),
                        "released_by": batch["data"].get("released_by"),
                        "recorded_at": batch["updated_at"],
                        "evidence": "original released record preserved at freeze open",
                        "trigger_run_at": trigger_at,
                    }
                )
        return waiting, snapshots

    @staticmethod
    def _work_item(freeze_order_id, phase, item_type, target_kind, target_id, payload=None):
        return {
            "id": str(uuid4()),
            "freeze_order_id": freeze_order_id,
            "phase": phase,
            "item_type": item_type,
            "target_kind": target_kind,
            "target_id": target_id,
            "status": "pending",
            "payload": payload or {},
        }

    def _process_pending(self, actor, freeze_order_id, phase, target):
        """Process pending work items for a phase; failures stay resumable."""
        for item in self.repository.list_work_items(freeze_order_id, status="pending"):
            if item["phase"] != phase:
                continue
            claimed = self.repository.claim_work_item(item["id"])
            if not claimed:
                continue
            try:
                self._handle_work_item(actor, claimed)
            except Exception:
                self.repository.fail_work_item(item["id"])
                raise
            self.repository.complete_work_item(item["id"])
        counts = self.repository.work_item_counts(freeze_order_id, phase)
        if counts["pending"] == 0 and counts["in_progress"] == 0:
            freeze = self.repository.get_entity(freeze_order_id)
            if freeze and freeze["status"] != target:
                result = self.repository.compare_and_swap_status(
                    freeze_order_id,
                    {
                        "quarantine": {"freezing"},
                        "recovery": {"recovering"},
                    }[phase],
                    target,
                    freeze["data"],
                    actor,
                )
                if result:
                    updated, from_status = result
                    self.repository.append_freeze_audit(
                        freeze_order_id,
                        actor,
                        phase + "_complete",
                        from_status,
                        target,
                        {"completed_items": counts["completed"]},
                    )
                    return updated
        return self.repository.get_entity(freeze_order_id)

    def _handle_work_item(self, actor, item):
        freeze = self.repository.get_entity(item["freeze_order_id"])
        handler = getattr(self, "_handle_" + item["item_type"])
        handler(actor, freeze, item)

    def _fault(self, item_type):
        fault = self.faults.get(item_type)
        if fault is None:
            return None
        return fault() if callable(fault) else fault

    def _handle_block_instrument(self, actor, freeze, item):
        instrument = self.repository.get_entity(item["target_id"])
        if not instrument:
            return
        fault = self._fault("block_instrument")
        if fault is not None:
            raise fault
        data = dict(instrument["data"])
        ids = list(data.get("blocked_by_freeze_order_ids", []))
        if freeze["id"] not in ids:
            ids.append(freeze["id"])
        data["blocked_by_freeze_order_ids"] = ids
        status = "blocked" if instrument["status"] == "ready" else instrument["status"]
        self.repository.update_entity(instrument["id"], instrument["version"], status, data)
        self.repository.append_freeze_audit(
            instrument["id"],
            actor,
            "freeze_block",
            instrument["status"],
            status,
            {"freeze_order_id": freeze["id"], "qc_lot_id": freeze["data"].get("qc_lot_id")},
        )
        freeze_data = dict(freeze["data"])
        blocked = list(freeze_data.get("blocked_instrument_ids", []))
        if instrument["id"] not in blocked:
            blocked.append(instrument["id"])
        freeze_data["blocked_instrument_ids"] = blocked
        self.repository.update_entity(freeze["id"], freeze["version"], freeze["status"], freeze_data)

    def _handle_hold_batch(self, actor, freeze, item):
        batch = self.repository.get_entity(item["target_id"])
        if not batch:
            return
        if batch["status"] not in ("waiting", "intercepted"):
            # The batch moved on (e.g. released between planning and
            # processing): nothing to force.
            return
        fault = self._fault("hold_batch")
        if fault is not None:
            raise fault
        data = dict(batch["data"])
        ids = list(data.get("freeze_order_ids", []))
        if freeze["id"] in ids:
            return
        ids.append(freeze["id"])
        data["freeze_order_ids"] = ids
        data["held_by_freeze_order_id"] = freeze["id"]
        data["hold_reason"] = freeze["data"].get("reason")
        data["held_qc_lot_id"] = freeze["data"].get("qc_lot_id")
        from_status = batch["status"]
        self.repository.update_entity(batch["id"], batch["version"], "intercepted", data)
        self.repository.append_freeze_audit(
            batch["id"],
            actor,
            "freeze_hold",
            from_status,
            "intercepted",
            {"freeze_order_id": freeze["id"], "qc_lot_id": freeze["data"].get("qc_lot_id")},
        )

    def _handle_record_snapshot(self, actor, freeze, item):
        # Released batches are never rewritten; the evidence is already kept
        # on the freeze order. This item only marks that evidence captured.
        fault = self._fault("record_snapshot")
        if fault is not None:
            raise fault

    def _handle_unblock_instrument(self, actor, freeze, item):
        instrument = self.repository.get_entity(item["target_id"])
        if not instrument:
            return
        data = dict(instrument["data"])
        ids = [fid for fid in data.get("blocked_by_freeze_order_ids", []) if fid != freeze["id"]]
        data["blocked_by_freeze_order_ids"] = ids
        next_status = "ready" if instrument["status"] == "blocked" and not ids else instrument["status"]
        self.repository.update_entity(instrument["id"], instrument["version"], next_status, data)
        self.repository.append_freeze_audit(
            instrument["id"],
            actor,
            "freeze_unblock",
            instrument["status"],
            next_status,
            {"freeze_order_id": freeze["id"]},
        )

    def _handle_release_hold(self, actor, freeze, item):
        batch = self.repository.get_entity(item["target_id"])
        if not batch:
            return
        if batch["status"] != "intercepted":
            return
        if freeze["id"] not in batch["data"].get("freeze_order_ids", []):
            # This batch never ended up held by this freeze order; leave any
            # other reason's interception untouched.
            return
        data = dict(batch["data"])
        remaining = [fid for fid in data.get("freeze_order_ids", []) if fid != freeze["id"]]
        data["freeze_order_ids"] = remaining
        data["lifted_by_freeze_order_id"] = freeze["id"]
        if remaining:
            # Another freeze still covers this batch: keep it intercepted.
            data["held_by_freeze_order_id"] = remaining[-1]
            next_status = "intercepted"
        else:
            data.pop("held_by_freeze_order_id", None)
            data.pop("hold_reason", None)
            data.pop("held_qc_lot_id", None)
            next_status = "waiting"
        self.repository.update_entity(batch["id"], batch["version"], next_status, data)
        self.repository.append_freeze_audit(
            batch["id"],
            actor,
            "freeze_lift",
            "intercepted",
            next_status,
            {"freeze_order_id": freeze["id"], "remaining_freeze_order_ids": remaining},
        )

    def resume_freeze(self, actor, freeze_order_id):
        """Retry a partial freeze or recovery, doing only remaining items."""
        if actor.role not in ("operator", "supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed to resume freeze orders" % actor.role)
        freeze = self.repository.get_entity(freeze_order_id)
        if not freeze or freeze["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        if freeze["status"] == "freezing":
            return self._process_pending(actor, freeze_order_id, "quarantine", target="frozen")
        if freeze["status"] == "recovering":
            return self._process_pending(actor, freeze_order_id, "recovery", target="recovered")
        # Already finished: resume is an idempotent no-op returning the order.
        return freeze

    def recover_freeze(self, actor, freeze_order_id, data=None, expected_version=None):
        """After a passed review, plan and run the recovery work items."""
        if actor.role not in ("supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed to recover freeze orders" % actor.role)
        freeze = self.repository.get_entity(freeze_order_id)
        if not freeze or freeze["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        if freeze["status"] == "review_rejected":
            raise ConflictError("freeze order review was rejected; it cannot be recovered")
        if freeze["status"] != "reviewed":
            raise ConflictError("freeze order must be reviewed and approved before recovery")
        expected = int(expected_version) if expected_version is not None else freeze["version"]
        if expected != freeze["version"]:
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected, freeze["version"]),
                details=self._review_conflict_details(freeze),
            )
        items = []
        for instrument_id in freeze["data"].get("instrument_ids", []):
            items.append(
                self._work_item(
                    freeze_order_id, "recovery", "unblock_instrument", "instrument", instrument_id
                )
            )
        for batch_id in freeze["data"].get("held_batch_ids", []):
            items.append(
                self._work_item(
                    freeze_order_id, "recovery", "release_hold", "result_batch", batch_id
                )
            )
        updated_data = dict(freeze["data"])
        updated_data["recovered_by"] = actor.user_id
        updated_data["recovery_note"] = (data or {}).get("note")
        result = self.repository.compare_and_swap_status(
            freeze_order_id, {"reviewed"}, "recovering", updated_data, actor
        )
        if not result:
            latest = self.repository.get_entity(freeze_order_id)
            raise ConflictError(
                "freeze order %s is already %s" % (freeze_order_id, latest["status"]),
                details=self._review_conflict_details(latest),
            )
        updated, from_status = result
        self.repository.add_work_items(freeze_order_id, items)
        self.repository.append_freeze_audit(
            freeze_order_id, actor, "recover", from_status, "recovering",
            {"planned_items": len(items)},
        )
        return self._process_pending(actor, freeze_order_id, "recovery", target="recovered")

    def _review_conflict_details(self, freeze):
        """Fresh blockers shown to the reviewer who lost the race."""
        held = []
        for batch_id in freeze["data"].get("held_batch_ids", []):
            batch = self.repository.get_entity(batch_id)
            if batch and batch["status"] == "intercepted":
                held.append(
                    {
                        "batch_id": batch["id"],
                        "instrument_id": batch["data"].get("instrument_id"),
                        "status": batch["status"],
                        "version": batch["version"],
                    }
                )
        return {
            "freeze_order_id": freeze["id"],
            "freeze_status": freeze["status"],
            "reviewed_by": freeze["data"].get("reviewed_by"),
            "decision": freeze["data"].get("decision"),
            "blocked_instrument_ids": list(freeze["data"].get("blocked_instrument_ids", [])),
            "blocking_holds": held,
            "released_batch_snapshot_ids": list(
                freeze["data"].get("released_batch_snapshot_ids", [])
            ),
        }

    # -- legacy data upgrade -----------------------------------------------

    def upgrade_legacy_lot_links(self, actor):
        """Backfill single-instrument lot scope for pre-upgrade QC runs.

        Old QC data has no shared-lot relationship recorded. Each legacy run
        contributes a legacy link for its own instrument only; links marked
        legacy never widen a freeze to instruments with no observed sharing.
        """
        if actor.role not in ("supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed to upgrade legacy links" % actor.role)
        inserted = []
        for run in self.repository.list_entities(kind="qc_run"):
            lot_id = run["data"].get("qc_lot_id")
            instrument_id = run["data"].get("instrument_id")
            if not lot_id or not instrument_id:
                continue
            created = self.repository.upsert_lot_instrument(
                lot_id, instrument_id, observed_run_id=None, legacy=True
            )
            if created:
                inserted.append(
                    {"qc_lot_id": lot_id, "instrument_id": instrument_id, "run_id": run["id"]}
                )
        if inserted:
            self.repository.append_freeze_audit(
                "legacy-upgrade",
                actor,
                "legacy_lot_link_upgrade",
                None,
                "completed",
                {"links": inserted, "count": len(inserted)},
            )
        return {"backfilled_links": inserted, "count": len(inserted)}
