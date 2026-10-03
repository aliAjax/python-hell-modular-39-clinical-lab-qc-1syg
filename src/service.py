from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import RuleEngine

# 连续偏差类失控标志：出现这些标志说明是批号级系统误差，需要隔离整批
SYSTEMATIC_FLAGS = ("bias_high", "bias_low", "trend_up", "trend_down")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        # 写入失败注入钩子：在每个冻结项写入前调用，抛出异常可模拟写入失败
        self.on_freeze_item = None

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
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
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        # 乐观锁：后到者版本过期时直接看到最新状态，而非先撞状态机
        if expected_version is not None and expected != entity["version"]:
            latest = self.repository.get_entity(entity_id)
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected, entity["version"]),
                details={"latest": latest},
            )
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        try:
            updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        except ConflictError as exc:
            # 后到者看到最新状态与阻塞项
            latest = self.repository.get_entity(entity_id)
            details = dict(getattr(exc, "details", None) or {})
            details["latest"] = latest
            raise ConflictError(str(exc), details=details)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        # 连续偏差自动隔离：批号级系统误差触发整批冻结
        if (
            entity["kind"] == "qc_run"
            and action == "evaluate"
            and updated["status"] == "rejected"
            and updated["data"].get("qc_lot_id")
        ):
            flags = [f for f in (updated["data"].get("flags") or []) if f in SYSTEMATIC_FLAGS]
            if flags:
                self.create_freeze_order(actor, updated["data"]["qc_lot_id"], flags[0])
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

    # ------------------------------------------------------------------
    # 冻结单：同一批号同一原因只建一份，按批号找出受影响仪器与待放行批次
    # ------------------------------------------------------------------
    def create_freeze_order(self, actor, qc_lot_id, reason, scope="lot", instrument_id=None, idempotency_key=None):
        lot = self.repository.get_entity(qc_lot_id)
        if not lot or lot["kind"] != "qc_lot":
            raise ValidationError("qc lot does not exist")
        reason = str(reason or "").strip()
        if not reason:
            raise ValidationError("freeze reason is required")
        reason_key = "%s::%s" % (qc_lot_id, reason)
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                order = self.repository.get_entity(existing)
                if order and order["kind"] == "freeze_order":
                    return {
                        "freeze_order": order,
                        "items": self._list_freeze_items(order["id"]),
                        "created": False,
                    }
        # 同一原因只建一份：复用未解除的同原因冻结单
        order = None
        for candidate in self.repository.list_entities(kind="freeze_order", status="active"):
            if candidate["data"].get("reason_key") == reason_key:
                order = candidate
                break
        if order is None:
            data = {
                "qc_lot_id": qc_lot_id,
                "reason": reason,
                "reason_key": reason_key,
                "scope": scope,
                "instrument_id": instrument_id,
            }
            order = self.repository.create_entity(str(uuid4()), "freeze_order", "active", data, actor.user_id)
            self.audit.record(
                order["id"], actor, "freeze", None, "active",
                {"qc_lot_id": qc_lot_id, "reason": reason, "scope": scope},
            )
            if idempotency_key:
                self.repository.save_idempotency(actor.user_id, idempotency_key, order["id"])
            created = True
        else:
            created = False
        # 逐项写入受影响仪器与待放行批次；失败后重试只补未完成项
        items = self._populate_freeze_items(actor, order)
        return {"freeze_order": order, "items": items, "created": created}

    def _populate_freeze_items(self, actor, order):
        lot_id = order["data"]["qc_lot_id"]
        reason = order["data"]["reason"]
        # 受影响仪器：所有在该批号上做过质控的仪器
        instrument_ids = []
        seen = set()
        for run in self.repository.list_entities(kind="qc_run"):
            if run["data"].get("qc_lot_id") == lot_id:
                iid = run["data"].get("instrument_id")
                if iid and iid not in seen:
                    seen.add(iid)
                    instrument_ids.append(iid)
        # 待放行批次：waiting 状态且其质控结果使用该批号
        targets = [(iid, "instrument", iid) for iid in instrument_ids]
        for batch in self.repository.list_entities(kind="result_batch", status="waiting"):
            run_id = batch["data"].get("qc_run_id")
            run = self.repository.get_entity(run_id) if run_id else None
            if run and run["data"].get("qc_lot_id") == lot_id:
                targets.append((batch["id"], "result_batch", batch["data"].get("instrument_id")))
        items = []
        for target_id, target_type, instrument_id in targets:
            item_key = "%s:%s:%s" % (order["id"], target_type, target_id)
            existing = self._find_freeze_item(item_key)
            if existing:
                items.append(existing)
                continue
            if self.on_freeze_item:
                self.on_freeze_item(order, target_type, target_id)
            data = {
                "freeze_order_id": order["id"],
                "item_key": item_key,
                "target_type": target_type,
                "target_id": target_id,
                "qc_lot_id": lot_id,
                "instrument_id": instrument_id,
                "reason": reason,
                "blocked_at": utcnow(),
            }
            item = self.repository.create_entity(str(uuid4()), "freeze_item", "blocked", data, actor.user_id)
            self.audit.record(
                item["id"], actor, "freeze_item", None, "blocked",
                {"freeze_order_id": order["id"], "target_type": target_type, "target_id": target_id},
            )
            items.append(item)
        return items

    def _find_freeze_item(self, item_key):
        for item in self.repository.list_entities(kind="freeze_item"):
            if item["data"].get("item_key") == item_key:
                return item
        return None

    def _list_freeze_items(self, freeze_order_id):
        return [
            item for item in self.repository.list_entities(kind="freeze_item")
            if item["data"].get("freeze_order_id") == freeze_order_id
        ]

    def list_freeze_orders(self):
        return self.repository.list_entities(kind="freeze_order")

    def list_freeze_items(self, freeze_order_id=None):
        items = self.repository.list_entities(kind="freeze_item")
        if freeze_order_id:
            items = [i for i in items if i["data"].get("freeze_order_id") == freeze_order_id]
        return items

    def get_freeze_order(self, freeze_order_id):
        order = self.repository.get_entity(freeze_order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        return {"freeze_order": order, "items": self._list_freeze_items(freeze_order_id)}

    def review_freeze_order(self, actor, freeze_order_id, review_note, expected_version=None):
        order = self.repository.get_entity(freeze_order_id)
        if not order or order["kind"] != "freeze_order":
            raise NotFoundError("freeze order not found: " + freeze_order_id)
        expected = int(expected_version) if expected_version is not None else order["version"]
        # 乐观锁：后到者版本过期时看到最新冻结单与阻塞项
        if expected_version is not None and expected != order["version"]:
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected, order["version"]),
                details={"freeze_order": self.repository.get_entity(freeze_order_id),
                         "items": self._list_freeze_items(freeze_order_id)},
            )
        next_status, patch = self.rules.validate_transition(
            actor, order, "review",
            {"reviewer_id": actor.user_id, "review_note": review_note},
            self._lookup,
        )
        merged = dict(order["data"])
        merged.update(patch)
        expected = int(expected_version) if expected_version is not None else order["version"]
        try:
            updated = self.repository.update_entity(freeze_order_id, expected, next_status, merged)
        except ConflictError as exc:
            # 后到者看到最新冻结单与阻塞项
            raise ConflictError(
                str(exc),
                details={"freeze_order": self.repository.get_entity(freeze_order_id),
                         "items": self._list_freeze_items(freeze_order_id)},
            )
        # 复核通过：解除该单下所有阻塞项；已放行批次保留原记录不动
        items = self._list_freeze_items(freeze_order_id)
        now = utcnow()
        for item in items:
            if item["status"] == "blocked":
                item_data = dict(item["data"])
                item_data["released_at"] = now
                item_data["release_basis"] = review_note or ("review by " + actor.user_id)
                self.repository.update_entity(item["id"], item["version"], "released", item_data)
                self.audit.record(
                    item["id"], actor, "release", "blocked", "released",
                    {"freeze_order_id": freeze_order_id},
                )
        return {"freeze_order": updated, "items": self._list_freeze_items(freeze_order_id)}

    # ------------------------------------------------------------------
    # 旧数据升级：旧质控没有共享批号关系，按单仪器范围补全 qc_lot_id
    # ------------------------------------------------------------------
    def backfill_lot_relationships(self, actor):
        backfilled = []
        for run in self.repository.list_entities(kind="qc_run"):
            if run["data"].get("qc_lot_id"):
                continue
            instrument_id = run["data"].get("instrument_id")
            lot_id = None
            # 单仪器范围：仅依据该仪器自身的历史质控推断批号，不跨仪器共享
            for other in self.repository.list_entities(kind="qc_run"):
                if other["id"] == run["id"]:
                    continue
                if other["data"].get("instrument_id") != instrument_id:
                    continue
                if other["data"].get("qc_lot_id"):
                    lot_id = other["data"]["qc_lot_id"]
                    break
            if not lot_id:
                assay_id = run["data"].get("assay_id")
                for lot in self.repository.list_entities(kind="qc_lot", status="active"):
                    if lot["data"].get("assay_id") == assay_id:
                        lot_id = lot["id"]
                        break
            if lot_id:
                run_data = dict(run["data"])
                run_data["qc_lot_id"] = lot_id
                run_data["lot_backfilled"] = True
                updated = self.repository.update_entity(run["id"], run["version"], run["status"], run_data)
                self.audit.record(
                    run["id"], actor, "backfill_lot", run["status"], run["status"],
                    {"qc_lot_id": lot_id, "scope": "instrument"},
                )
                backfilled.append(updated)
        return {"backfilled": len(backfilled), "runs": backfilled}
