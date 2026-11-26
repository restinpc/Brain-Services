from datetime import datetime
from decimal import Decimal
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import urllib.request

PARSER_DIR = Path(__file__).resolve().parents[1] / "parser"
sys.path.insert(0, str(PARSER_DIR))
import fxcm_h1_parser as parser


class Cursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.db.executed.append((sql, params))
        if "FROM brain.vlad_mt_rates_h1" in sql:
            self.db.selected = True

    def fetchall(self):
        return self.db.source_rows

    def executemany(self, sql, rows):
        self.db.insert_sql = sql
        self.db.insert_rows = list(rows)
        self.rowcount = len(self.db.insert_rows)

    def close(self):
        pass


class FakeDb:
    def __init__(self, source_rows):
        self.source_rows = source_rows
        self.executed = []
        self.insert_sql = None
        self.insert_rows = []
        self.commits = 0
        self.rollbacks = 0
        self.selected = False

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def rate_row(hour="2026-11-26 13:00:00"):
    start = datetime.fromisoformat(hour)
    return (
        start, start.replace(hour=start.hour + 1), 120, 120, 0,
        Decimal("1.15000"), Decimal("1.15100"), Decimal("1.14900"), Decimal("1.15050"),
        Decimal("1.15010"), Decimal("1.15110"), Decimal("1.14910"), Decimal("1.15060"),
        Decimal("0.00010"), Decimal("0.00010"), "a" * 64, 1795700000,
    )


class RatesBridgeTests(unittest.TestCase):
    def test_registered_parser_routes_to_rates_table_without_external_fetch(self):
        db = FakeDb([rate_row()])
        args = type("Args", (), {"start": datetime(2025, 8, 1).date(), "end": datetime(2026, 11, 27).date()})()
        with patch.object(urllib.request, "urlopen", side_effect=AssertionError("external fetch")) as urlopen:
            result = parser.run(db, parser.TABLE, args)
            urlopen.assert_not_called()
        self.assertFalse(hasattr(parser, "weekly_data"))
        self.assertFalse(hasattr(parser, "BASE"))
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["source"], "ROBOFOREX_MT5")
        self.assertEqual((result["rows_seen"], result["rows_added"]), (1, 1))
        self.assertTrue(db.selected)
        self.assertIn("source_id=%s", db.executed[0][0])
        self.assertIn("quality='MATCHED'", db.executed[0][0])
        self.assertTrue(db.insert_sql.startswith("INSERT IGNORE INTO vlad_model100_fxcm_h1"))
        self.assertEqual(db.insert_rows[0][0:3], ("ROBOFOREX_MT5", "EURUSD", "SPOT_BID_ASK_H1"))
        self.assertEqual(db.insert_rows[0][3], datetime(2026, 11, 26, 13))
        self.assertEqual(db.insert_rows[0][21], parser.RATES_PARSER_VERSION)
        self.assertEqual(db.insert_rows[0][22], parser.RATES_SCHEMA_VERSION)
        self.assertIn("INSERT IGNORE", db.insert_sql)
        self.assertTrue(any(params and params[0] == "ROBOFOREX_MT5_EURUSD_H1" for _, params in db.executed))
        self.assertEqual((db.commits, db.rollbacks), (1, 0))

    def test_rejects_non_rates_source_before_any_external_call(self):
        db = FakeDb([rate_row()])
        args = type("Args", (), {"source": "fxcm", "start": datetime(2025, 8, 1).date(), "end": datetime(2026, 11, 27).date()})()
        with self.assertRaisesRegex(ValueError, "rates-v2"):
            parser.run(db, parser.TABLE, args)
        self.assertFalse(db.executed)

    def test_bad_payload_hash_rolls_back_without_writing(self):
        row = list(rate_row())
        row[15] = "not-a-sha256"
        db = FakeDb([tuple(row)])
        with self.assertRaisesRegex(ValueError, "payload hash"):
            parser.run_rates_v2(db, parser.TABLE)
        self.assertFalse(db.insert_rows)
        self.assertEqual((db.commits, db.rollbacks), (0, 1))

    def test_empty_delta_is_successful_and_does_not_create_rows(self):
        db = FakeDb([])
        result = parser.run_rates_v2(db, parser.TABLE)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual((result["rows_seen"], result["rows_added"]), (0, 0))
        self.assertFalse(db.insert_rows)
        self.assertEqual(db.commits, 1)


if __name__ == "__main__":
    unittest.main()
