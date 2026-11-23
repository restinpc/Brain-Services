import asyncio
import copy
import unittest
from pathlib import Path
from test_regressions import SqlAlchemyStub, load_context_module, Result

ROOT=Path(__file__).resolve().parents[1]

class Connection:
    def __init__(self, engine): self.engine=engine
    async def execute(self, statement, parameters=None):
        sql=str(statement).strip()
        if 'TRUNCATE' in sql: raise AssertionError('rebuild must preserve committed identities')
        if sql.startswith('SELECT id, occurrence_count'): return Result([(1,3),(2,3),(3,3)])
        if sql.startswith('SELECT id FROM'):
            self.engine.locked=True
            return Result()
        if sql.startswith('INSERT INTO'):
            assert self.engine.locked
            self.engine.writes+=1
            if self.engine.fail_after==self.engine.writes: raise RuntimeError('injected write failure')
            key=parameters['wc']
            if key not in self.engine.rows:
                self.engine.rows[key]={'id':max([v['id'] for v in self.engine.rows.values()]+[0])+1,**parameters}
            return Result()
        return Result()

class Scope:
    def __init__(self, engine, transaction=False): self.engine=engine; self.transaction=transaction
    async def __aenter__(self):
        self.snapshot=copy.deepcopy(self.engine.rows)
        return Connection(self.engine)
    async def __aexit__(self, typ, value, tb):
        if self.transaction and typ: self.engine.rows=self.snapshot
        return False

class Engine:
    def __init__(self):
        self.rows={'1_0_0':{'id':37,'wc':'1_0_0','ctx_id':1,'mode':0,'shift':0}}
        self.fail_after=None; self.writes=0; self.locked=False
    def begin(self): return Scope(self,True)
    def connect(self): return Scope(self)

class WeightPublicationTests(unittest.TestCase):
    def test_repeated_rebuild_preserves_ids_and_exact_codes(self):
        expected={f'{ctx}_{mode}_{shift}' for ctx in (1,2,3) for mode in (0,1) for shift in range(31)}
        for model in ('46','58'):
            engine=Engine();module=load_context_module(ROOT/model/'weights.py')
            with SqlAlchemyStub():
                asyncio.run(module.build_weights(engine))
                before=copy.deepcopy(engine.rows)
                asyncio.run(module.build_weights(engine))
            self.assertEqual(set(engine.rows),expected)
            self.assertEqual(engine.rows,before)
            self.assertEqual(engine.rows['1_0_0']['id'],37)

    def test_partial_failure_keeps_previous_committed_weights(self):
        for model in ('46','58'):
            engine=Engine();before=copy.deepcopy(engine.rows);engine.fail_after=4
            module=load_context_module(ROOT/model/'weights.py')
            with SqlAlchemyStub(),self.assertRaises(RuntimeError):
                asyncio.run(module.build_weights(engine))
            self.assertEqual(engine.rows,before)

if __name__=='__main__':unittest.main()
