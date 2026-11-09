"""Offline production reconciliation tests; no DB, model execution or training."""
import ast,asyncio,bisect,importlib.util,sys,types,unittest
from datetime import datetime,timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch,AsyncMock
import numpy as np
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'shared'));sys.path.insert(0,str(ROOT/'parser'))
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path)
 module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module
stub=types.ModuleType('brain_framework');stub.get_service_config=lambda:{};stub.run_standard_model=lambda *a,**k:None
with patch.dict(sys.modules,{'brain_framework':stub}):
 m92=load('sync_model92','92/model.py');m94=load('sync_model94','94/model.py')
rl=load('sync_reverse','shared/reverse_learning.py')
from brain_strict_reduction import strict_masked_reduce
gdelt=load('sync_gdelt','parser/GDELT.py')
def framework_functions():
 tree=ast.parse((ROOT/'shared/brain_framework.py').read_text())
 names={'_filter_rates_lte','_filter_dataset_lte','_append_np_rates_row','_dt_to_ts'}
 selected=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
 ns={'np':np,'bisect':bisect,'_list_view':lambda r,a,b:r[a:b]}
 exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+selected,type_ignores=[])),'framework-selected','exec'),ns)
 return ns
class SyncTests(unittest.TestCase):
 def test_framework_cutoff_excludes_future(self):
  ns=framework_functions();d=datetime(2026,1,1)
  s=SimpleNamespace(global_rates={'x':[{'date':d},{'date':d+timedelta(hours=1)}]},global_rates_dates={})
  self.assertEqual(len(ns['_filter_rates_lte']('x',d,s)),1)
 def test_framework_dataset_cutoff(self):
  ns=framework_functions();d=datetime(2026,1,1);s=SimpleNamespace(FILTER_DATASET_BY_DATE=True,dataset=[1,2],dataset_dates=[d,d+timedelta(hours=1)])
  self.assertEqual(ns['_filter_dataset_lte'](d,s),[1])
 def test_framework_append_preserves_ohlc(self):
  ns=framework_functions();ns['_dt_to_ts']=lambda dt:int(dt.timestamp())
  keys=['dates_ns','t1','ranges','ext_min','ext_max','close','open','max','min']
  s=SimpleNamespace(np_rates={'x':{k:np.array([],dtype=np.int64 if k=='dates_ns' else np.float64) for k in keys}})
  ns['_append_np_rates_row']('x',datetime(2026,1,1),1,2,s,close=3,open_=2,max_=4,min_=1)
  self.assertTrue(all(len(x)==1 for x in s.np_rates['x'].values()));self.assertEqual(s.np_rates['x']['max'][0],4)
 def test_strict_reduction_order_and_future(self):
  a=np.array([1e16,1.,-1e16,9.]);hits=np.ones(4);weights=np.ones(4);masks=np.ones((4,1),dtype=np.uint8);dates=np.arange(4,dtype=np.int64);ends=np.array([1,2,3,100]);valid=np.ones(4,dtype=np.uint8)
  result=strict_masked_reduce(a,hits,weights,masks,dates,ends,valid,4,99,4)
  self.assertEqual(result[0,0],0.);self.assertEqual(result[0,2],3.)
 def test_strict_reduction_invalid_shape(self):
  with self.assertRaises(ValueError):strict_masked_reduce(np.ones(2),np.ones(1),np.ones(2),np.ones((2,1),dtype=np.uint8),np.arange(2),np.arange(2),np.ones(2,dtype=np.uint8),2,9,9)
 def test_reverse_precision_no_training(self):
  records=[SimpleNamespace(codes=['a'],sign=1,base_amp=1.),SimpleNamespace(codes=['b'],sign=-1,base_amp=1.)]
  self.assertEqual(rl.total_precision({'a':2.,'b':-3.},records),1.)
  self.assertEqual(rl.total_precision({},records),0.)
 def test_reverse_exception_not_converted_to_empty(self):
  tree=ast.parse((ROOT/'shared/reverse_learning.py').read_text())
  for n in ast.walk(tree):
   if isinstance(n,ast.Try) and any(isinstance(x,ast.Await) and isinstance(x.value,ast.Call) and getattr(x.value.func,'id',None)=='active_codes_at' for stmt in n.body for x in ast.walk(stmt)):
    self.fail('Provider exception can be swallowed')
 def test_model92_strict_dependency(self):
  self.assertEqual(Path(m92.strict_masked_reduce.__code__.co_filename).name,'brain_strict_reduction.py')
 def test_model94_empty_mapping_no_connection(self):
  engine=SimpleNamespace(connect=lambda:(_ for _ in ()).throw(AssertionError('unexpected DB')))
  with patch.object(m94,'_first_existing_table',AsyncMock(return_value='prices')):
   self.assertEqual(asyncio.run(m94._load_price_chunk(engine,'prices','parser',{},datetime(2026,1,1),datetime(2026,1,2))),[])
 def test_model94_missing_source_no_connection(self):
  with patch.object(m94,'_first_existing_table',AsyncMock(return_value=None)):
   self.assertIsNone(asyncio.run(m94._source_max_ts(object(),'prices','parser')))
 def test_model94_sql_bindings_and_mapping(self):
  class Result:
   def mappings(self):return self
   def all(self):return [{'condition_id':'c','price':.5,'price_timestamp':datetime(2026,1,1)}]
  class Conn:
   async def __aenter__(self):return self
   async def __aexit__(self,*a):pass
   async def execute(self,sql,params):
    self_params=params;assert params['condition_ids']==['c'];assert 'condition_ids' in sql._bindparams
    return Result()
  with patch.object(m94,'_first_existing_table',AsyncMock(return_value='prices')):
   out=asyncio.run(m94._load_price_chunk(SimpleNamespace(connect=lambda:Conn()),'prices','parser',{'c':dict(family='FX',orientation=1,signature='s',end_date=None)},datetime(2026,1,1),datetime(2026,1,2)))
   self.assertEqual(out[0]['family'],'FX')
 def test_model94_required_canonical_fail_before_map(self):
  cfg={'dataset':dict(parser_table='legacy',source_database='brain',markets_table='markets',prices_table='prices',enriched_table='e',map_table='m',state_table='s')}
  with patch.object(m94,'get_service_config',return_value=cfg),patch.object(m94,'_ensure_enriched_tables',AsyncMock()),patch.object(m94,'_table_exists',AsyncMock(return_value=False)),patch.object(m94,'_prepare_map_table',AsyncMock()) as mapping:
   with self.assertRaisesRegex(RuntimeError,'canonical_parser_source_missing'):asyncio.run(m94.enrich_dataset(object(),object()))
   mapping.assert_not_called()
 def test_gdelt_registered_argument(self):
  args=['GDELT.py','host','user','secret-fixture','brain','table','fields','--recent-hours 24 --max-units 8']
  out=gdelt.expand_registered_argument(args);self.assertEqual(out[7:],['--recent-hours','24','--max-units','8'])
  normal=['GDELT.py','--recent-hours','24'];self.assertEqual(gdelt.expand_registered_argument(normal),normal)
if __name__=='__main__':unittest.main()
