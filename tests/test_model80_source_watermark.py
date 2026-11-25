import ast,asyncio,pathlib,sys,types,unittest
from unittest.mock import AsyncMock
ROOT=pathlib.Path(__file__).resolve().parents[1]
source=ast.parse((ROOT/'80/model.py').read_text())
fn=next(n for n in source.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='enrich_dataset')
fn.returns=None
class Result:
 def scalar(self):return 1
class Conn:
 async def __aenter__(self):return self
 async def __aexit__(self,*args):pass
 async def execute(self,*args):return Result()
class Engine:
 def connect(self):return Conn()
class SourceWatermarkTest(unittest.IsolatedAsyncioTestCase):
 async def test_modes_preserve_dataset_stats_and_publish_cursor_vector(self):
  for mode in ('noop','full','incremental'):
   stats={'mode':mode,'source_max_ids':{'cnn':4,'nyt':5,'twp':6,'wsj':7,'tgd':8},'events_written':3}
   scope={'SOURCES':dict.fromkeys(stats['source_max_ids']),'get_service_config':lambda:{},'_enrich_unlocked':AsyncMock(return_value=stats.copy()),'_sync_shared_brain_indexes':AsyncMock(return_value={'mode':'noop'}),'datetime':__import__('datetime').datetime}
   fake=types.ModuleType('sqlalchemy');fake.text=lambda x:x
   old=sys.modules.get('sqlalchemy');sys.modules['sqlalchemy']=fake
   try:
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'model80-enrich','exec'),scope)
    out=await scope['enrich_dataset'](Engine(),Engine())
   finally:
    if old is None:sys.modules.pop('sqlalchemy',None)
    else:sys.modules['sqlalchemy']=old
   self.assertEqual(out['source_watermark'],'cnn:4;nyt:5;tgd:8;twp:6;wsj:7')
   for k,v in stats.items():self.assertEqual(out[k],v)
 async def test_incomplete_cursor_is_not_claimed_as_watermark(self):
  scope={'SOURCES':dict.fromkeys(('cnn','nyt','twp','wsj','tgd')),'get_service_config':lambda:{},'_enrich_unlocked':AsyncMock(return_value={'mode':'locked'}),'_sync_shared_brain_indexes':AsyncMock(return_value={})}
  fake=types.ModuleType('sqlalchemy');fake.text=lambda x:x
  old=sys.modules.get('sqlalchemy');sys.modules['sqlalchemy']=fake
  try:
   exec(compile(ast.Module(body=[fn],type_ignores=[]),'model80-enrich','exec'),scope)
   out=await scope['enrich_dataset'](Engine(),Engine())
  finally:
   if old is None:sys.modules.pop('sqlalchemy',None)
   else:sys.modules['sqlalchemy']=old
  self.assertNotIn('source_watermark',out)
if __name__=='__main__':unittest.main()
