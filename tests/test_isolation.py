import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class IsolationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "isolation.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_shared_lot(self):
        assay = self.service.create(
            self.supervisor, "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1,
             "rule_config": {"limit_sd": 3, "consecutive_n": 4, "consecutive_sd": 1, "trend_n": 4}},
        )
        lot = self.service.create(
            self.supervisor, "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "a"})
        inst_a = self.service.create(
            self.supervisor, "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        inst_b = self.service.create(
            self.supervisor, "instrument",
            {"name": "Analyzer B", "serial": "B-200", "calibration_due": "2099-01-01"},
        )
        return assay, lot, inst_a, inst_b

    def _accepted_run(self, assay, lot, instrument, value, run_at):
        run = self.service.create(
            self.supervisor, "qc_run",
            {"assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": instrument["id"],
             "value": value, "run_at": run_at},
        )
        return self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "a"})

    def _waiting_batch(self, assay, instrument, run, run_at):
        return self.service.create(
            self.supervisor, "result_batch",
            {"assay_id": assay["id"], "instrument_id": instrument["id"], "qc_run_id": run["id"],
             "run_at": run_at, "patient_count": 10},
        )

    def test_freeze_order_dedup_by_reason(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        first = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        second = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        self.assertEqual(first["freeze_order"]["id"], second["freeze_order"]["id"])
        self.assertFalse(second["created"])
        # 不同原因 -> 不同冻结单
        third = self.service.create_freeze_order(self.supervisor, lot["id"], "trend_up")
        self.assertNotEqual(first["freeze_order"]["id"], third["freeze_order"]["id"])

    def test_finds_affected_instruments_and_pending_batches(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        run_a = self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        run_b = self._accepted_run(assay, lot, inst_b, 5.0, "2026-09-27T08:05:00Z")
        batch_a = self._waiting_batch(assay, inst_a, run_a, "2026-09-27T09:00:00Z")
        batch_b = self._waiting_batch(assay, inst_b, run_b, "2026-09-27T09:05:00Z")
        result = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        target_keys = {(i["data"]["target_type"], i["data"]["target_id"]) for i in result["items"]}
        self.assertIn(("instrument", inst_a["id"]), target_keys)
        self.assertIn(("instrument", inst_b["id"]), target_keys)
        self.assertIn(("result_batch", batch_a["id"]), target_keys)
        self.assertIn(("result_batch", batch_b["id"]), target_keys)
        self.assertEqual(len(result["items"]), 4)

    def test_release_blocked_until_review_passes(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        run_a = self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        batch_a = self._waiting_batch(assay, inst_a, run_a, "2026-09-27T09:00:00Z")
        self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        with self.assertRaises(ConflictError):
            self.service.transition(self.supervisor, batch_a["id"], "release", {"reviewer_id": "r"})
        # 复核通过后放行
        order = self.service.list_freeze_orders()[0]
        self.service.review_freeze_order(self.supervisor, order["id"], "review passed")
        released = self.service.transition(self.supervisor, batch_a["id"], "release", {"reviewer_id": "r"})
        self.assertEqual(released["status"], "released")

    def test_released_batches_keep_their_records(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        run_a = self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        batch_a = self._waiting_batch(assay, inst_a, run_a, "2026-09-27T09:00:00Z")
        released = self.service.transition(self.supervisor, batch_a["id"], "release", {"reviewer_id": "r"})
        self.assertEqual(released["status"], "released")
        result = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        batch_ids = [i["data"]["target_id"] for i in result["items"] if i["data"]["target_type"] == "result_batch"]
        self.assertNotIn(batch_a["id"], batch_ids)
        after = self.service.get(batch_a["id"])
        self.assertEqual(after["status"], "released")
        self.assertEqual(after["data"]["released_by"], "qc-supervisor")

    def test_concurrent_review_one_succeeds_loser_sees_latest(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        result = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        order = result["freeze_order"]
        v = order["version"]
        first = self.service.review_freeze_order(self.supervisor, order["id"], "ok", expected_version=v)
        self.assertEqual(first["freeze_order"]["status"], "released")
        with self.assertRaises(ConflictError) as ctx:
            self.service.review_freeze_order(self.supervisor, order["id"], "ok2", expected_version=v)
        details = ctx.exception.details
        self.assertIn("freeze_order", details)
        self.assertEqual(details["freeze_order"]["status"], "released")
        self.assertIn("items", details)

    def test_concurrent_release_one_succeeds_loser_sees_latest(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        run_a = self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        batch_a = self._waiting_batch(assay, inst_a, run_a, "2026-09-27T09:00:00Z")
        v = batch_a["version"]
        first = self.service.transition(self.supervisor, batch_a["id"], "release", {"reviewer_id": "r1"}, expected_version=v)
        self.assertEqual(first["status"], "released")
        with self.assertRaises(ConflictError) as ctx:
            self.service.transition(self.supervisor, batch_a["id"], "release", {"reviewer_id": "r2"}, expected_version=v)
        self.assertIn("latest", ctx.exception.details)
        self.assertEqual(ctx.exception.details["latest"]["status"], "released")

    def test_resumable_item_writes_no_duplicate_audit(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        run_a = self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        run_b = self._accepted_run(assay, lot, inst_b, 5.0, "2026-09-27T08:05:00Z")
        self._waiting_batch(assay, inst_a, run_a, "2026-09-27T09:00:00Z")
        self._waiting_batch(assay, inst_b, run_b, "2026-09-27T09:05:00Z")
        calls = {"n": 0}

        def hook(order, target_type, target_id):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("write failed")

        self.service.on_freeze_item = hook
        with self.assertRaises(RuntimeError):
            self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        order = self.service.list_freeze_orders()[0]
        self.assertEqual(len(self.service.list_freeze_items(order["id"])), 1)
        # 重试：只补未完成项，不重复审计
        self.service.on_freeze_item = None
        result = self.service.create_freeze_order(self.supervisor, lot["id"], "bias_high")
        self.assertEqual(len(result["items"]), 4)
        item_audits = [a for a in self.service.audit_log() if a["action"] == "freeze_item"]
        self.assertEqual(len(item_audits), 4)

    def test_legacy_backfill_by_single_instrument_scope(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        self._accepted_run(assay, lot, inst_a, 5.0, "2026-09-27T08:00:00Z")
        # 旧质控没有共享批号关系：直接插入无 qc_lot_id 的质控结果
        legacy = self.service.repository.create_entity(
            "legacy-run-1", "qc_run", "pending",
            {"assay_id": assay["id"], "instrument_id": inst_a["id"],
             "value": 5.0, "run_at": "2026-09-27T08:05:00Z"},
            self.supervisor.user_id,
        )
        self.assertNotIn("qc_lot_id", legacy["data"])
        result = self.service.backfill_lot_relationships(self.supervisor)
        self.assertEqual(result["backfilled"], 1)
        updated = self.service.get(legacy["id"])
        self.assertEqual(updated["data"]["qc_lot_id"], lot["id"])
        self.assertTrue(updated["data"]["lot_backfilled"])

    def test_auto_freeze_on_continuous_bias(self):
        assay, lot, inst_a, inst_b = self._setup_shared_lot()
        self._accepted_run(assay, lot, inst_a, 5.12, "2026-09-27T08:00:00Z")
        self._accepted_run(assay, lot, inst_a, 5.13, "2026-09-27T08:05:00Z")
        self._accepted_run(assay, lot, inst_a, 5.14, "2026-09-27T08:10:00Z")
        run = self.service.create(
            self.supervisor, "qc_run",
            {"assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": inst_a["id"],
             "value": 5.15, "run_at": "2026-09-27T08:15:00Z"},
        )
        run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "a"})
        self.assertEqual(run["status"], "rejected")
        self.assertIn("bias_high", run["data"]["flags"])
        orders = self.service.list_freeze_orders()
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["data"]["reason"], "bias_high")


if __name__ == "__main__":
    unittest.main()
