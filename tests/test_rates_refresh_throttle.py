"""Actual rate refresh and RW gate: a fresh candle cache must not queue a writer."""
import ast,asyncio,bisect,copy as _copy,os
from contextlib import asynccontextmanager
from datetime import datetime,timedelta
from pathlib import Path
from types import SimpleNamespace
import numpy as np
path=Path(os.getenv("BRAIN_TEST_FRAMEWORK","/brain/Brain-Services/shared/brain_framework.py"))
tree=ast.parse(path.read_text())
body=[n for n in tree.body if isinstance(n,(ast.ClassDef,ast.AsyncFunctionDef)) and n.name in ("_AsyncRWGate","_refresh_rates","_refresh_rates_unlocked")]
ns={"asyncio":asyncio,"asynccontextmanager":asynccontextmanager,"datetime":datetime,"timedelta":timedelta,"_copy":_copy,"np":np,"bisect":bisect,"text":lambda x:x,"log":lambda *a,**kw:None}
exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])),str(path),"exec",flags=__import__("__future__").annotations.compiler_flag),ns)
class SQL:
 def __init__(self):self.reads=0
 def connect(self):return self
 async def __aenter__(self):return self
 async def __aexit__(self,*a):pass
 async def execute(self,*a,**kw):
  self.reads+=1;await asyncio.sleep(.01)
  return SimpleNamespace(mappings=lambda:SimpleNamespace(all=lambda:[]))
def state(fresh):
 table="brain_rates_fixture";d=datetime(2025,1,1)
 s=SimpleNamespace(last_rates_refresh=({table:datetime.now()} if fresh else {}),DYNAMIC_POLL_SECONDS=300,state_gate=ns["_AsyncRWGate"](),global_rates_dates={table:[d]},global_rates={table:[]},rates={},candle_ranges={},last_candles={},extremums={},avg_range={},np_rates={},engine_brain=SQL(),NODE_NAME="fixture")
 return table,s
async def main():
 failures=[]
 table,s=state(True);hold=await s.state_gate.acquire_read();task=asyncio.create_task(ns["_refresh_rates"](table,s))
 await asyncio.sleep(.03)
 if not task.done():failures.append("Fresh refresh queued behind long reader")
 if s.state_gate.snapshot()["waiting_writers"]:failures.append("Fresh refresh advertised a writer and blocked unrelated readers")
 task.cancel()
 try:await task
 except asyncio.CancelledError:pass
 await s.state_gate.release_read(hold)
 assert s.engine_brain.reads==0
 print("fresh_under_reader",not failures)
 # A real due refresh must retain the write fence; no weakening of ownership.
 table,s=state(False);hold=await s.state_gate.acquire_read();task=asyncio.create_task(ns["_refresh_rates"](table,s));await asyncio.sleep(.01)
 assert not task.done() and s.engine_brain.reads==0 and s.state_gate.snapshot()["waiting_writers"]==1
 await s.state_gate.release_read(hold);await task;assert s.engine_brain.reads==1
 assert not any(s.state_gate.snapshot().values())
 print("PASS due refresh waits for reader and releases writer")
 # Both callers can observe stale metadata before acquisition; inner check deduplicates SQL.
 table,s=state(False);await asyncio.gather(ns["_refresh_rates"](table,s),ns["_refresh_rates"](table,s));assert s.engine_brain.reads==1
 print("PASS concurrent due refresh performs one SQL read")
 table,s=state(True);s.state_gate=None;await ns["_refresh_rates"](table,s);assert s.engine_brain.reads==0
 print("PASS no-gate compatibility and unchanged freshness threshold")
 assert not failures,failures
asyncio.run(main())
