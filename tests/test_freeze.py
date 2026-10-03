import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FreezeWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "freeze.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.reviewer_a = Actor("reviewer-a", "supervisor")
        self.reviewer_b = Actor("reviewer-b", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _world(self, legacy_run=False):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "SHARED-LOT", "target": 5.0, "sd": 0.1,
             "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "a"})
        instruments, runs, batches = [], [], []
        for serial in ("I-1", "I-2"):
            instrument = self.service.create(
                self.supervisor,
                "instrument",
                {"name": "Analyzer " + serial, "serial": serial, "calibration_due": "2099-01-01"},
            )
            run = self.service.create(
                self.supervisor,
                "qc_run",
                {
                    "assay_id": assay["id"],
                    "qc_lot_id": lot["id"],
                    "instrument_id": instrument["id"],
                    "value": 5.02,
                    "run_at": "2026-09-27T08:00:00Z",
                    **({"legacy_observed": True} if legacy_run and serial == "I-1" else {}),
                },
            )
            run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "op"})
            batch = self.service.create(
                self.supervisor,
                "result_batch",
                {
                    "assay_id": assay["id"],
                    "instrument_id": instrument["id"],
                    "qc_run_id": run["id"],
                    "run_at": "2026-09-27T09:00:00Z",
                    "patient_count": 5,
                },
            )
            instruments.append(instrument)
            runs.append(run)
            batches.append(batch)
        return assay, lot, instruments, runs, batches

    def _release(self, batch):
        return self.service.transition(
            self.supervisor, batch["id"], "release", {"reviewer_id": "r-1"}
        )

    def test_shared_lot_freeze_quarantines_all_instruments_and_waiting_batches(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {
                "trigger_qc_run_id": runs[0]["id"],
                "reason": "consecutive positive bias on shared lot",
            },
        )
        self.assertEqual(freeze["status"], "frozen")
        self.assertEqual(set(freeze["data"]["instrument_ids"]), {i["id"] for i in instruments})
        for batch in batches:
            held = self.service.get(batch["id"])
            self.assertEqual(held["status"], "intercepted")
            self.assertIn(freeze["id"], held["data"]["freeze_order_ids"])
        for instrument in instruments:
            blocked = self.service.get(instrument["id"])
            self.assertEqual(blocked["status"], "blocked")
            self.assertIn(freeze["id"], blocked["data"]["blocked_by_freeze_order_ids"])
        with self.assertRaises(ConflictError):
            self._release(batches[0])

    def test_same_cause_opens_only_one_freeze_order(self):
        assay, lot, instruments, runs, batches = self._world()
        first = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        with self.assertRaises(ConflictError) as caught:
            self.service.open_freeze_order(
                Actor("other-sup", "supervisor"),
                {"trigger_qc_run_id": runs[0]["id"], "reason": "bias again"},
            )
        self.assertEqual(caught.exception.details["freeze_order_id"], first["id"])
        self.assertTrue(caught.exception.details["already_existing"])
        freezes = self.service.list("freeze_orders")
        self.assertEqual(len(freezes), 1)

    def test_released_batch_keeps_original_record_and_evidence_snapshot(self):
        assay, lot, instruments, runs, batches = self._world()
        released = self._release(batches[1])
        released_version = released["version"]
        released_by = released["data"]["released_by"]
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        untouched = self.service.get(released["id"])
        self.assertEqual(untouched["status"], "released")
        self.assertEqual(untouched["version"], released_version)
        self.assertEqual(untouched["data"]["released_by"], released_by)
        self.assertEqual(freeze["data"]["released_batch_snapshot_ids"], [released["id"]])
        snapshot = freeze["data"]["released_batch_snapshots"][0]
        self.assertEqual(snapshot["batch_id"], released["id"])
        self.assertEqual(snapshot["status"], "released")
        self.assertEqual(snapshot["version"], released_version)
        self.assertEqual(snapshot["released_by"], released_by)
        # The released batch itself has no freeze marker written onto it.
        self.assertNotIn("freeze_order_ids", untouched["data"])

    def test_concurrent_review_only_one_wins_and_loser_sees_latest_blockers(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        winner = self.service.transition(
            self.reviewer_a,
            freeze["id"],
            "review",
            {"reviewer_id": "reviewer-a", "decision": "approved", "comment": "lot confirmed bad"},
            expected_version=freeze["version"],
        )
        self.assertEqual(winner["status"], "reviewed")
        with self.assertRaises(ConflictError) as caught:
            self.service.transition(
                self.reviewer_b,
                freeze["id"],
                "review",
                {"reviewer_id": "reviewer-b", "decision": "approved", "comment": "agree"},
                expected_version=freeze["version"],
            )
        details = caught.exception.details
        self.assertEqual(details["freeze_status"], "reviewed")
        self.assertEqual(details["reviewed_by"], "reviewer-a")
        blocked_hold_ids = {item["batch_id"] for item in details["blocking_holds"]}
        self.assertEqual(blocked_hold_ids, {b["id"] for b in batches})
        self.assertEqual(set(details["blocked_instrument_ids"]), {i["id"] for i in instruments})

    def test_release_conflict_carries_latest_blocking_items(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        with self.assertRaises(ConflictError) as caught:
            self._release(batches[0])
        details = caught.exception.details
        self.assertEqual(details["freeze_order_id"], freeze["id"])
        self.assertIn(batches[0]["id"], details["blocking_holds"])

    def test_review_then_recover_lifts_holds_and_unblocks(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        reviewed = self.service.transition(
            self.reviewer_a,
            freeze["id"],
            "review",
            {"reviewer_id": "reviewer-a", "decision": "approved"},
        )
        recovered = self.service.recover_freeze(
            self.supervisor, reviewed["id"], {"note": "new lot in use on all instruments"}
        )
        self.assertEqual(recovered["status"], "recovered")
        for batch in batches:
            lifted = self.service.get(batch["id"])
            self.assertEqual(lifted["status"], "waiting")
            self.assertEqual(lifted["data"]["freeze_order_ids"], [])
            self.assertEqual(lifted["data"]["lifted_by_freeze_order_id"], freeze["id"])
            released = self._release(lifted)
            self.assertEqual(released["status"], "released")
        for instrument in instruments:
            ready = self.service.get(instrument["id"])
            self.assertEqual(ready["status"], "ready")
            self.assertEqual(ready["data"]["blocked_by_freeze_order_ids"], [])

    def test_cannot_release_before_review_even_if_review_started(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        with self.assertRaises(ConflictError):
            self._release(batches[0])
        self.service.transition(
            self.reviewer_a,
            freeze["id"],
            "review",
            {"reviewer_id": "reviewer-a", "decision": "approved"},
        )
        # Reviewed but not yet recovered: still blocked.
        with self.assertRaises(ConflictError):
            self._release(batches[0])

    def test_rejected_review_blocks_recovery_until_re_review_passes(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        rejected = self.service.transition(
            self.reviewer_a,
            freeze["id"],
            "review",
            {"reviewer_id": "reviewer-a", "decision": "rejected", "comment": "need more evidence"},
        )
        self.assertEqual(rejected["status"], "review_rejected")
        with self.assertRaises(ConflictError):
            self.service.recover_freeze(self.supervisor, freeze["id"], {})
        approved = self.service.transition(
            self.reviewer_b,
            freeze["id"],
            "review",
            {"reviewer_id": "reviewer-b", "decision": "approved", "comment": "evidence complete"},
        )
        self.assertEqual(approved["status"], "reviewed")
        recovered = self.service.recover_freeze(self.supervisor, freeze["id"], {})
        self.assertEqual(recovered["status"], "recovered")

    def test_resume_after_write_failure_keeps_unfinished_and_skips_audit(self):
        assay, lot, instruments, runs, batches = self._world()

        class Boom(RuntimeError):
            pass

        calls = {"n": 0}

        def fail_on_second_hold():
            calls["n"] += 1
            if calls["n"] == 2:
                raise Boom("disk full")

        self.service.faults["hold_batch"] = fail_on_second_hold
        with self.assertRaises(Boom):
            self.service.open_freeze_order(
                self.supervisor,
                {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
            )
        freezes = self.service.list("freeze_orders")
        self.assertEqual(len(freezes), 1)
        freeze = freezes[0]
        self.assertEqual(freeze["status"], "freezing")

        audits_before = {
            row["entity_id"]: len(self.service.audit_log(row["entity_id"]))
            for row in self.service.audit_log()
        }
        pending = self.service.repository.list_work_items(freeze["id"], status="pending")
        completed = self.service.repository.list_work_items(freeze["id"], status="completed")
        self.assertTrue(pending)
        self.assertTrue(completed)

        # Retry after the fault clears: only leftovers run, no duplicate audit.
        self.service.faults.clear()
        resumed = self.service.resume_freeze(self.supervisor, freeze["id"])
        self.assertEqual(resumed["status"], "frozen")
        for entity_id, before in audits_before.items():
            after = len(self.service.audit_log(entity_id))
            # A completed item must not be audited again on retry.
            self.assertLessEqual(after, before + 1, "duplicate audit on retry for %s" % entity_id)
        for batch in batches:
            self.assertEqual(self.service.get(batch["id"])["status"], "intercepted")
        # Resuming again is a no-op.
        again = self.service.resume_freeze(self.supervisor, freeze["id"])
        self.assertEqual(again["status"], "frozen")

    def test_legacy_upgrade_backfills_single_instrument_scope(self):
        assay, lot, instruments, runs, batches = self._world(legacy_run=True)
        # Before upgrade the legacy run has no shared-lot link recorded.
        links = self.service.repository.list_lot_instruments(lot["id"])
        # I-2's run created an observed link at creation time; I-1 (legacy) did not.
        linked = {row["instrument_id"] for row in links}
        self.assertNotIn(instruments[0]["id"], linked)
        result = self.service.upgrade_legacy_lot_links(self.supervisor)
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["backfilled_links"][0]["instrument_id"], instruments[0]["id"])
        links = self.service.repository.list_lot_instruments(lot["id"])
        by_instrument = {row["instrument_id"]: row for row in links}
        self.assertTrue(by_instrument[instruments[0]["id"]]["legacy"])
        self.assertFalse(by_instrument[instruments[1]["id"]]["legacy"])
        # Idempotent: running again adds nothing and writes no new audit.
        audit_before = len(self.service.audit_log("legacy-upgrade"))
        again = self.service.upgrade_legacy_lot_links(self.supervisor)
        self.assertEqual(again["count"], 0)
        self.assertEqual(len(self.service.audit_log("legacy-upgrade")), audit_before)

    def test_freeze_from_legacy_run_scopes_to_trigger_instrument_only(self):
        assay, lot, instruments, runs, batches = self._world(legacy_run=True)
        self.service.upgrade_legacy_lot_links(self.supervisor)
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "legacy bias"},
        )
        self.assertTrue(freeze["data"]["legacy_scope"])
        self.assertEqual(freeze["data"]["instrument_ids"], [instruments[0]["id"]])
        self.assertEqual(self.service.get(batches[0]["id"])["status"], "intercepted")
        # The other instrument on the same lot is untouched: legacy links do
        # not widen a freeze without an observed sharing relationship.
        self.assertEqual(self.service.get(instruments[1]["id"])["status"], "ready")
        other_batch = self.service.get(batches[1]["id"])
        self.assertEqual(other_batch["status"], "waiting")
        # That other batch can still be released.
        self.assertEqual(self._release(other_batch)["status"], "released")

    def test_opener_cannot_review_own_freeze(self):
        assay, lot, instruments, runs, batches = self._world()
        freeze = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "bias"},
        )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.supervisor,
                freeze["id"],
                "review",
                {"reviewer_id": "qc-supervisor", "decision": "approved"},
            )

    def test_two_freeze_orders_on_different_causes_both_block(self):
        assay, lot, instruments, runs, batches = self._world()
        run3 = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instruments[0]["id"],
                "value": 5.01,
                "run_at": "2026-09-27T10:00:00Z",
            },
        )
        run3 = self.service.transition(self.supervisor, run3["id"], "evaluate", {"evaluated_by": "op"})
        first = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": runs[0]["id"], "reason": "morning bias"},
        )
        second = self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": run3["id"], "reason": "recurring afternoon bias"},
        )
        self.assertNotEqual(first["id"], second["id"])
        held = self.service.get(batches[0]["id"])
        self.assertEqual(set(held["data"]["freeze_order_ids"]), {first["id"], second["id"]})
        # Recovering only the first keeps the batch blocked by the second.
        reviewed = self.service.transition(
            self.reviewer_a, first["id"], "review",
            {"reviewer_id": "reviewer-a", "decision": "approved"},
        )
        self.service.recover_freeze(self.supervisor, reviewed["id"], {})
        still_held = self.service.get(batches[0]["id"])
        self.assertEqual(still_held["status"], "intercepted")
        self.assertEqual(still_held["data"]["freeze_order_ids"], [second["id"]])
        with self.assertRaises(ConflictError):
            self._release(batches[0])
        # Recovering the second finally releases the hold.
        reviewed2 = self.service.transition(
            self.reviewer_b, second["id"], "review",
            {"reviewer_id": "reviewer-b", "decision": "approved"},
        )
        self.service.recover_freeze(self.supervisor, reviewed2["id"], {})
        self.assertEqual(self.service.get(batches[0]["id"])["status"], "waiting")


if __name__ == "__main__":
    unittest.main()
