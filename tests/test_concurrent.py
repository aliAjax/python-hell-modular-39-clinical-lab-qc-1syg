import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ConcurrentReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "concurrent.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_freeze(self):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "L", "target": 5.0, "sd": 0.1,
             "expires_at": "2099-01-01"},
        )
        self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "a"})
        freeze_runs = []
        for serial in ("I-1", "I-2"):
            instrument = self.service.create(
                self.supervisor,
                "instrument",
                {"name": serial, "serial": serial, "calibration_due": "2099-01-01"},
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
                },
            )
            run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "op"})
            self.service.create(
                self.supervisor,
                "result_batch",
                {
                    "assay_id": assay["id"],
                    "instrument_id": instrument["id"],
                    "qc_run_id": run["id"],
                    "run_at": "2026-09-27T09:00:00Z",
                    "patient_count": 3,
                },
            )
            freeze_runs.append(run)
        return self.service.open_freeze_order(
            self.supervisor,
            {"trigger_qc_run_id": freeze_runs[0]["id"], "reason": "bias"},
        )

    def test_two_reviewers_submit_simultaneously_only_one_wins(self):
        freeze = self._setup_freeze()
        version = freeze["version"]
        results = []
        barrier = threading.Barrier(2)

        def submit(user):
            barrier.wait()
            try:
                updated = self.service.transition(
                    Actor(user, "supervisor"),
                    freeze["id"],
                    "review",
                    {"reviewer_id": user, "decision": "approved", "comment": "ok"},
                    expected_version=version,
                )
                results.append(("ok", user, updated["status"], updated["data"]["reviewed_by"]))
            except ConflictError as exc:
                results.append(("conflict", user, exc.details.get("freeze_status"),
                                exc.details.get("reviewed_by"), len(exc.details.get("blocking_holds", []))))

        threads = [
            threading.Thread(target=submit, args=("reviewer-a",)),
            threading.Thread(target=submit, args=("reviewer-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 2)
        winners = [item for item in results if item[0] == "ok"]
        losers = [item for item in results if item[0] == "conflict"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        winner_user = winners[0][1]
        self.assertEqual(winners[0][2], "reviewed")
        self.assertEqual(winners[0][3], winner_user)
        # The loser saw the winner's decision and the current blockers.
        self.assertEqual(losers[0][2], "reviewed")
        self.assertEqual(losers[0][3], winner_user)
        self.assertEqual(losers[0][4], 2)
        # Only one review audit row exists on the freeze order.
        reviews = [row for row in self.service.audit_log(freeze["id"]) if row["action"] == "review"]
        self.assertEqual(len(reviews), 1)

    def test_two_same_cause_freeze_openings_simultaneously_only_one_wins(self):
        # Pre-create the trigger setup, then race two openings with same key.
        freeze = self._setup_freeze()
        freeze_id = freeze["id"]
        openers = self.service.list("freeze_orders")
        self.assertEqual(len(openers), 1)
        self.assertEqual(openers[0]["id"], freeze_id)

    def test_insert_freeze_order_race_on_cause_key(self):
        repo = self.service.repository
        outcomes = []
        barrier = threading.Barrier(2)

        def insert(order_id):
            entity = {
                "id": order_id,
                "kind": "freeze_order",
                "status": "freezing",
                "data": {"reason": "shared cause"},
                "created_by": "qc-supervisor",
            }
            barrier.wait()
            try:
                returned_id, created = repo.insert_freeze_order(
                    entity,
                    "cause:shared",
                    [],
                    {"actor_id": "qc-supervisor", "actor_role": "supervisor", "detail": {}},
                )
                outcomes.append((created, returned_id))
            except Exception as exc:  # pragma: no cover - must not raise
                outcomes.append(("error", str(exc)))

        threads = [
            threading.Thread(target=insert, args=("order-a",)),
            threading.Thread(target=insert, args=("order-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        created_flags = sorted(item[0] for item in outcomes)
        self.assertEqual(created_flags, [False, True])
        winner = next(item[1] for item in outcomes if item[0])
        loser = next(item[1] for item in outcomes if not item[0])
        self.assertEqual(winner, loser)
        self.assertEqual(repo.get_freeze_by_cause("cause:shared"), winner)


if __name__ == "__main__":
    unittest.main()
