"""Actual MySQL read path for rate refresh, with isolated schema and no production writes."""
import ast,asyncio,bisect,copy as _copy,json,os,subprocess
from datetime import datetime,timedelta
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine
path=Path(os.getenv("BRAIN_TEST_FRAMEWORK","/brain/Brain-Services/shared/brain_framework.py"))
body=[n for n in ast.parse(path.read_text()).body if isinstance(n,(ast.ClassDef,ast.AsyncFunctionDef)) and n.name in ("_AsyncRWGate","_refresh_rates","_refresh_rates_unlocked")]
errors=[]
ns={"asyncio":asyncio,"asynccontextmanager":asynccontextmanager,"datetime":datetime,"timedelta":timedelta,"_copy":_copy,"np":np,"bisect":bisect,"text":text,"log":lambda *a,**kw:errors.append((a,kw))}
exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])),str(path),"exec",flags=__import__("__future__").annotations.compiler_flag),ns)
# Credentials remain in memory, never printed or saved.
cfg=json.loads(subprocess.check_output(["php","-r","require '/brain/Brain-Server/config.php';echo json_encode($_SERVER['config']);"]))
url=URL.create("mysql+aiomysql",username=cfg["sql_login"],password=cfg["sql_pass"],host=cfg["sql_server"],port=int(cfg.get("sql_port",3306)))
engine=create_async_engine(url,pool_pre_ping=True);schema="brain_v74_rate_throttle_"+str(os.getpid())
class Reader:
 def __init__(self,c):self.c=c;self.reads=0
 def connect(self):return self
 async def __aenter__(self):return self
 async def __aexit__(self,*a):await self.c.rollback()
 async def execute(self,*a,**kw):self.reads+=1;return await self.c.execute(*a,**kw)
async def main():
 async with engine.connect() as c:
  await c.exec_driver_sql("SET SESSION sql_log_bin=0")
  version=(await c.exec_driver_sql("SELECT VERSION()")).scalar();assert version.startswith("8.4."),version
  await c.exec_driver_sql("CREATE DATABASE "+schema);await c.exec_driver_sql("USE "+schema)
  try:
   await c.exec_driver_sql("CREATE TABLE brain_rates_fixture(date DATETIME PRIMARY KEY,open DOUBLE,close DOUBLE,max DOUBLE,min DOUBLE,t1 DOUBLE)")
   await c.exec_driver_sql("INSERT INTO brain_rates_fixture VALUES('2025-01-01',1,2,3,0,1),('2025-01-02',2,3,4,1,1),('2025-01-03',3,4,5,2,1)");await c.commit()
   table="brain_rates_fixture";reader=Reader(c);dates=[datetime(2025,1,i) for i in range(1,4)]
   rows=[{"date":d,"open":float(i),"close":float(i+1),"max":float(i+2),"min":float(i-1)} for i,d in enumerate(dates,1)]
   s=SimpleNamespace(last_rates_refresh={},DYNAMIC_POLL_SECONDS=300,state_gate=ns["_AsyncRWGate"](),global_rates_dates={table:dates},global_rates={table:rows},rates={table:{d:1. for d in dates}},candle_ranges={table:{d:3. for d in dates}},last_candles={table:[(d,True) for d in dates]},extremums={table:{"min":set(),"max":set()}},avg_range={table:3.},np_rates={table:None},engine_brain=reader,NODE_NAME="fixture")
   before=repr(s.global_rates)
   await asyncio.gather(ns["_refresh_rates"](table,s),ns["_refresh_rates"](table,s))
   assert not errors,errors
   assert reader.reads==1 and repr(s.global_rates)==before
   hold=await s.state_gate.acquire_read();task=asyncio.create_task(ns["_refresh_rates"](table,s));await asyncio.sleep(.03)
   assert task.done() and not s.state_gate.snapshot()["waiting_writers"];await task;await s.state_gate.release_read(hold)
   assert reader.reads==1
   print(json.dumps({"mysql":version,"concurrent_due_sql_reads":reader.reads,"fresh_request_under_reader":"PASS","rate_values_and_order_unchanged":True}))
  finally:
   await c.rollback();await c.exec_driver_sql("DROP DATABASE "+schema);await c.commit()
 await engine.dispose()
asyncio.run(main())
