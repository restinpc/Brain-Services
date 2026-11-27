"""Actual ReverseStore cold feature preparation: concurrency, failure and cancellation."""
import ast,asyncio,gc,weakref,os
from pathlib import Path
from datetime import datetime,timedelta
p=Path(os.getenv("BRAIN_TEST_REVERSE_FILE","/brain/Brain-Services/shared/reverse_learning.py"))
tree=ast.parse(p.read_text());c=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=="ReverseStore")
methods=[n for n in c.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in ("__init__","_get_train_lock","maybe_retrain")]
unit=ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(name="Store",bases=[],keywords=[],body=methods,decorator_list=[])],type_ignores=[]))
dates=[datetime(2025,1,1),datetime(2025,1,2)];seq=[(d,(-1 if i else 1)) for i,d in enumerate(dates)]
key=(tuple((int(d.timestamp()),s) for d,s in seq),(("a",),("b",)),0,20,0.1,0.95,0,"mean")
ns={"asyncio":asyncio,"weakref":weakref,"_seq_codes_cache":{},"_SEQ_CODES_CACHE_MAX":1024,"_train_at_date_cache":{key:({"learned":1.25},0.99,3,2)},"collect_extremums_back":lambda *a,**kw:seq,"collect_extremums_forward":lambda *a,**kw:seq}
exec(compile(unit,str(p),"exec",flags=__import__("__future__").annotations.compiler_flag),ns)
async def invoke(store,provider,tag="same",day=3):
 return await store.maybe_retrain(pair=1,day_flag=0,control_date=datetime(2025,1,day),params_hash="same",np_simple_rates={},active_codes_at=provider,model_tag=tag)
async def main():
 failures=[]
 ns["_seq_codes_cache"].clear();store=ns["Store"](object(),port=8942);calls=[]
 async def provider(d):
  calls.append(d);await asyncio.sleep(.005);return ["a" if d==dates[0] else "b"]
 results=await asyncio.gather(invoke(store,provider,day=3),invoke(store,provider,day=4))
 if len(calls)!=2:failures.append(f"cold identical sequence fetched {len(calls)} times, expected2")
 assert results==[({"learned":1.25},0.99)]*2
 print("cold_calls",len(calls))
 # Failed or cancelled preparation is never published as empty/successful codes.
 for cancellation in (False,True):
  ns["_seq_codes_cache"].clear();started=asyncio.Event()
  async def bad(d):
   started.set()
   if cancellation:await asyncio.Event().wait()
   raise ValueError("provider unavailable")
  task=asyncio.create_task(invoke(store,bad));await started.wait()
  if cancellation:task.cancel()
  try:await task
  except (ValueError,asyncio.CancelledError):pass
  else:raise AssertionError("provider failure swallowed")
  assert not ns["_seq_codes_cache"]
  calls.clear();assert await asyncio.wait_for(invoke(store,provider),1)==({"learned":1.25},0.99)
  assert len(calls)==2
 print("PASS exception/cancellation leave no partial sequence and retry releases ownership")
 ns["_seq_codes_cache"].clear();entered=set();barrier=asyncio.Event()
 def independent(tag):
  async def fn(d):
   entered.add(tag)
   if len(entered)==2:barrier.set()
   await asyncio.wait_for(barrier.wait(),1)
   return ["a" if d==dates[0] else "b"]
  return fn
 await asyncio.gather(invoke(store,independent("x"),tag="x"),invoke(store,independent("y"),tag="y"))
 print("PASS distinct model parameter scopes prepare concurrently")
 assert not failures,failures
 print("PASS identical cold sequences deduplicated; trained results and order unchanged")
asyncio.run(main())
