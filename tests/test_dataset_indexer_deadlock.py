import asyncio
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from shared import dataset_indexer


class _Result:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class _Deadlock(Exception):
    def __init__(self):
        self.orig = SimpleNamespace(args=(1213, "Deadlock found"))


class _Connection:
    def __init__(self, engine):
        self.engine = engine

    async def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT id, `json`" in sql:
            if self.engine.select_error:
                raise RuntimeError("read failed")
            return _Result([(1, '["event_type"]')])
        if "MIN(`date_dt`) AS _date_added" in sql:
            return _Result([("growth", datetime(2026, 1, 1))])
        if sql.lstrip().startswith("INSERT IGNORE"):
            self.engine.insert_calls += 1
            if self.engine.insert_calls <= self.engine.deadlocks:
                raise _Deadlock()
            return _Result(rowcount=1)
        return _Result()


class _Context:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        return _Connection(self.engine)

    async def __aexit__(self, *exc):
        return False


class _Engine:
    def __init__(self, deadlocks=0, select_error=False):
        self.deadlocks = deadlocks
        self.select_error = select_error
        self.insert_calls = 0

    def connect(self):
        return _Context(self)

    def begin(self):
        return _Context(self)


class DatasetIndexerDeadlockTest(unittest.TestCase):
    def call(self, engine):
        async def get_columns(*_args):
            return {"event_type": "`event_type` VARCHAR(64) NULL"}

        with patch.object(dataset_indexer, "_get_col_definitions", get_columns), patch.object(
            dataset_indexer.asyncio, "sleep", return_value=None
        ):
            return asyncio.run(
                dataset_indexer.parse_indexes(engine, "events", "date_dt")
            )

    def test_deadlock_retries_same_idempotent_batch_and_succeeds(self):
        engine = _Engine(deadlocks=2)
        self.assertTrue(self.call(engine))
        self.assertEqual(engine.insert_calls, 3)

    def test_exhausted_deadlock_is_reported_as_failure(self):
        engine = _Engine(deadlocks=9)
        self.assertFalse(self.call(engine))
        self.assertEqual(engine.insert_calls, 3)

    def test_source_select_failure_is_reported_as_failure(self):
        self.assertFalse(self.call(_Engine(select_error=True)))


if __name__ == "__main__":
    unittest.main()
