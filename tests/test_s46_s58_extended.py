"""Extended, isolated regressions for S46/S58 transaction safety and math parity.

Run from the workspace with:
    python work/exceptions-stage2/tests/test_s46_s58_extended.py
No production imports, database connections, or package installs are used.
"""
import asyncio
import sys
import unittest
from datetime import date
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from test_regressions import (  # noqa: E402
    DriverError,
    FakeEngine,
    SqlAlchemyStub,
    load_context_module,
)


class S46S58ExtendedTests(unittest.TestCase):
    def test_nonretryable_mysql_error_rolls_back_whole_write_for_both_services(self):
        for service in ("46", "58"):
            with self.subTest(service=service):
                original = [{"event_type": "old", "value": 7}]
                sasha = FakeEngine(
                    rows=[dict(row) for row in original],
                    fail_codes=[1064],  # syntax error: must not be retried
                )
                brain = FakeEngine(source_rows=[(date(2026, 1, 1), 100.0), (date(2026, 1, 2), 101.0)])
                module = load_context_module(ROOT / f"{service}/context_idx.py")

                with SqlAlchemyStub(), self.assertRaises(DriverError) as caught:
                    asyncio.run(module.build_index(sasha, brain))

                self.assertEqual(caught.exception.args[0], 1064)
                self.assertEqual(sasha.rows, original, "DELETE/partial inserts must rollback")
                self.assertEqual(sasha.begin_count, 2, "one DDL transaction plus one write attempt")
                self.assertEqual(sasha.fail_codes, [])

    def test_event_aggregates_match_legacy_delta_math_for_both_services(self):
        # Legacy calculation classifies each adjacent delta, aggregates the new
        # value, and averages absolute delta by occurrence count.
        values = [100.0, 101.0, 100.0, 100.0]
        expected = {
            "rate_hike": (1, 101.0, 1.0, 101.0, 101.0),
            "rate_cut": (1, 100.0, 1.0, 100.0, 100.0),
            "rate_unchanged": (1, 100.0, 0.0, 100.0, 100.0),
        }
        rows = [(date(2026, 1, index + 1), value) for index, value in enumerate(values)]

        for service in ("46", "58"):
            with self.subTest(service=service):
                sasha = FakeEngine()
                brain = FakeEngine(source_rows=rows)
                module = load_context_module(ROOT / f"{service}/context_idx.py")
                with SqlAlchemyStub():
                    asyncio.run(module.build_index(sasha, brain))

                actual = {row["event_type"]: row for row in sasha.rows}
                self.assertEqual(set(actual), set(expected))
                for event, (count, avg, abs_avg, low, high) in expected.items():
                    row = actual[event]
                    self.assertEqual(row["cnt"], count)
                    self.assertAlmostEqual(row["avg_value"], avg)
                    self.assertAlmostEqual(row["avg_abs_change"], abs_avg)
                    self.assertAlmostEqual(row["min_value"], low)
                    self.assertAlmostEqual(row["max_value"], high)


if __name__ == "__main__":
    unittest.main(verbosity=2)
