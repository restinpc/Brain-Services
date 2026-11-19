import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("gdelt_writer", ROOT / "parser" / "GDELT.py")
gdelt_writer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gdelt_writer)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.conn.events.append(("execute", sql, params))

    def executemany(self, sql, values):
        self.conn.events.append(("executemany", sql, len(values)))
        if self.conn.fail_insert:
            raise RuntimeError("simulated insert failure")

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __init__(self, fail_insert=False):
        self.events = []
        self.fail_insert = fail_insert

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.events.append(("commit",))

    def rollback(self):
        self.events.append(("rollback",))


class GdeltWriterSnapshotLockTests(unittest.TestCase):
    def make_store(self, fail_insert=False):
        store = object.__new__(gdelt_writer.NewsSink)
        store.conn = FakeConnection(fail_insert)
        store.table = "sasha_gdelt_news"
        store.out = None
        row = {column: None for column in gdelt_writer.DB_COLUMNS}
        row["record_key"] = "record-key"
        row["provider"] = "gdelt"
        row["record_type"] = "event"
        return store, row

    def test_source_writer_holds_snapshot_lock_through_commit(self):
        store, row = self.make_store()
        store.save_rows([row])
        events = store.conn.events
        lock_index = next(i for i, e in enumerate(events) if e[0] == "execute" and "GET_LOCK" in e[1])
        insert_index = next(i for i, e in enumerate(events) if e[0] == "executemany")
        commit_index = next(i for i, e in enumerate(events) if e[0] == "commit")
        release_index = next(i for i, e in enumerate(events) if e[0] == "execute" and "RELEASE_LOCK" in e[1])
        self.assertLess(lock_index, insert_index)
        self.assertLess(insert_index, commit_index)
        self.assertLess(commit_index, release_index)

    def test_source_writer_releases_lock_after_rolling_back_failed_batch(self):
        store, row = self.make_store(fail_insert=True)
        with self.assertRaisesRegex(RuntimeError, "simulated insert failure"):
            store.save_rows([row])
        events = store.conn.events
        rollback_index = next(i for i, e in enumerate(events) if e[0] == "rollback")
        release_index = next(i for i, e in enumerate(events) if e[0] == "execute" and "RELEASE_LOCK" in e[1])
        self.assertLess(rollback_index, release_index)
