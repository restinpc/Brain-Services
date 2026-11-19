"""Model 94 source selection and mapped price query regression tests."""
import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))
stub = types.ModuleType("brain_framework")
stub.get_service_config = lambda: {}
stub.run_standard_model = lambda *a, **k: None
with patch.dict(sys.modules, {"brain_framework": stub}):
    spec = importlib.util.spec_from_file_location("model94_table_selection", ROOT / "94" / "model.py")
    model94 = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = model94
    spec.loader.exec_module(model94)


class _Mappings:
    def all(self):
        return [{"condition_id": "c1", "price": 0.5, "family": "FX", "orientation": "NEUTRAL", "signature": "sig", "end_date": None}]


class _Result:
    def mappings(self):
        return _Mappings()


class _Connection:
    def __init__(self):
        self.sql = None
        self.params = None
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        return None
    async def execute(self, sql, params=None):
        self.sql = str(sql)
        self.params = params
        return _Result()


class _Engine:
    def __init__(self):
        self.connection = _Connection()
    def connect(self):
        return self.connection


class Model94TableSelectionTests(unittest.TestCase):
    def test_source_selector_checks_candidates_in_order(self):
        with patch.object(model94, "_table_exists", new=AsyncMock(side_effect=[False, True])) as exists:
            result = asyncio.run(model94._first_existing_table(object(), "missing", "prices"))
        self.assertEqual(result, "prices")
        self.assertEqual(exists.await_count, 2)

    def test_missing_price_source_returns_no_timestamp(self):
        with patch.object(model94, "_table_exists", new=AsyncMock(return_value=False)):
            result = asyncio.run(model94._source_max_ts(object(), "prices", "parser"))
        self.assertIsNone(result)

    def test_price_chunk_joins_mapping_and_binds_window(self):
        engine = _Engine()
        with patch.object(model94, "_first_existing_table", new=AsyncMock(return_value="prices")):
            rows = asyncio.run(model94._load_price_chunk(engine, "prices", "parser", "mapped", "start", "end"))
        self.assertEqual(rows[0]["condition_id"], "c1")
        self.assertIn("JOIN `mapped` m ON m.condition_id = p.condition_id", engine.connection.sql)
        self.assertEqual(engine.connection.params, {"start": "start", "end": "end"})


if __name__ == "__main__":
    unittest.main()
