import ast
import asyncio
import copy
import importlib.util
import sys
import types
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class Result:
    def __init__(self, rows=()): self.rows = list(rows)
    def fetchall(self): return list(self.rows)
    def scalar(self): return None
    def first(self): return None

class DriverError(Exception): pass

class FakeConnection:
    def __init__(self, engine): self.engine = engine
    async def execute(self, statement, parameters=None):
        sql = str(statement).strip()
        if sql.startswith('SELECT date_iso, value'): return Result(self.engine.source_rows)
        if sql.startswith('CREATE TABLE IF NOT EXISTS'): return Result()
        if sql.startswith('DELETE FROM'): self.engine.rows.clear(); return Result()
        if sql.startswith('INSERT INTO'):
            if self.engine.fail_codes:
                code = self.engine.fail_codes.pop(0)
                raise DriverError(code, 'injected transient database failure')
            row = dict(parameters or {})
            prior = next((r for r in self.engine.rows if r['event_type'] == row['event_type']), None)
            if 'ON DUPLICATE KEY UPDATE' in sql and prior is not None: prior.update(row)
            else: self.engine.rows.append(row)
            return Result()
        return Result()

class ConnectContext:
    def __init__(self, engine): self.engine = engine
    async def __aenter__(self): return FakeConnection(self.engine)
    async def __aexit__(self, *args): return False

class BeginContext:
    def __init__(self, engine): self.engine = engine; self.snapshot = None
    async def __aenter__(self):
        self.engine.begin_count += 1
        self.snapshot = copy.deepcopy(self.engine.rows)
        return FakeConnection(self.engine)
    async def __aexit__(self, typ, value, tb):
        if typ is not None: self.engine.rows[:] = self.snapshot
        return False

class FakeEngine:
    def __init__(self, source_rows=(), rows=(), fail_codes=()):
        self.source_rows = list(source_rows); self.rows = list(rows)
        self.fail_codes = list(fail_codes); self.begin_count = 0
    def connect(self): return ConnectContext(self)
    def begin(self): return BeginContext(self)

class SqlAlchemyStub:
    def __enter__(self):
        self.old = sys.modules.get('sqlalchemy')
        mod = types.ModuleType('sqlalchemy'); mod.text = lambda sql: sql
        sys.modules['sqlalchemy'] = mod
    def __exit__(self, *args):
        if self.old is None: sys.modules.pop('sqlalchemy', None)
        else: sys.modules['sqlalchemy'] = self.old

def load_context_module(path):
    with SqlAlchemyStub():
        spec = importlib.util.spec_from_file_location('context_idx_test', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module

async def noop(*args, **kwargs): return None
async def yes(*args, **kwargs): return True
async def first(engine, *names): return names[0]
async def none(*args): return None
async def mapped(*args): return {'markets': 0}

class ExceptionsStage2Tests(unittest.TestCase):
    def test_s94_source_engine_is_defined_and_uses_service_source(self):
        path = ROOT / '94/model.py'
        tree = ast.parse(path.read_text(encoding='utf-8'))
        fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'enrich_dataset')
        seen = []
        async def market(engine, *args): seen.append(engine); return []
        ns = {
            'get_service_config': lambda: {'dataset': {'parser_table':'parser','markets_table':'markets','prices_table':'prices','enriched_table':'out','map_table':'map','state_table':'state','engine':'vlad'}, 'model':{}},
            '_ensure_enriched_tables': noop, '_table_exists': yes, '_first_existing_table': first,
            '_load_market_meta': market, '_prepare_map_table': mapped, '_source_max_ts': none,
            'datetime': __import__('datetime').datetime, 'timedelta': __import__('datetime').timedelta,
        }
        exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'), ns)
        vlad, brain = object(), object()
        with SqlAlchemyStub(): result = asyncio.run(ns['enrich_dataset'](vlad, brain))
        self.assertEqual(seen, [vlad]); self.assertEqual(result['mode'], 'empty')

    def test_s46_s58_retry_full_idempotent_write_transaction(self):
        for service in ('46', '58'):
            with self.subTest(service=service):
                module = load_context_module(ROOT / f'{service}/context_idx.py')
                sasha = FakeEngine(rows=[{'event_type':'rate_hike','id':1}], fail_codes=[1213, 1062])
                brain = FakeEngine(source_rows=[(date(2026,1,1),100.0),(date(2026,1,2),101.0),(date(2026,1,3),100.0)])
                with SqlAlchemyStub(): result = asyncio.run(module.build_index(sasha, brain))
                self.assertEqual(result['contexts_total'], 3)
                self.assertEqual(result['rows_total'], 3)
                self.assertEqual(result['events_total'], 2)
                self.assertEqual({r['event_type'] for r in sasha.rows}, {'rate_hike','rate_cut','rate_unchanged'})
                self.assertEqual(sasha.begin_count, 4)
                self.assertEqual(sasha.fail_codes, [])

    def test_context_ids_used_in_weight_codes_survive_rebuild(self):
        for service in ('46','58'):
            module=load_context_module(ROOT / f'{service}/context_idx.py')
            sasha=FakeEngine(rows=[{'event_type':name,'id':i} for i,name in enumerate(('rate_hike','rate_cut','rate_unchanged'),1)])
            brain=FakeEngine(source_rows=[(date(2026,1,1),100.0),(date(2026,1,2),101.0)])
            with SqlAlchemyStub():
                asyncio.run(module.build_index(sasha,brain))
                asyncio.run(module.build_index(sasha,brain))
            self.assertEqual({r['event_type']:r.get('id') for r in sasha.rows},{'rate_hike':1,'rate_cut':2,'rate_unchanged':3})

    def test_s100_reads_news_from_schema_that_owns_source_table(self):
        import tomllib
        cfg = tomllib.loads((ROOT / '100/config.toml').read_text(encoding='utf-8'))
        available = {'brain': {'sasha_gdelt_news'}, 'vlad': set()}
        source_engine = cfg['dataset'].get('source_engine', 'vlad')
        self.assertIn(cfg['dataset']['parser_table'], available[source_engine])

if __name__ == '__main__': unittest.main(verbosity=2)
