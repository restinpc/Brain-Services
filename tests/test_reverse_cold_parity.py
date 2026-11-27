import asyncio,importlib.util,sys,pickle,os
from datetime import datetime,timedelta
import numpy as np
sys.path.insert(0,'/brain/Brain-Services/shared')
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m
old=load("v74_reverse_baseline",os.environ["BRAIN_TEST_REVERSE_BASELINE"])
new=load("v74_reverse_candidate",os.getenv("BRAIN_TEST_REVERSE_FILE","/brain/Brain-Services/shared/reverse_learning.py"))
dates=[datetime(2025,1,1)+timedelta(days=i) for i in range(20)]
close=np.array([100.+(-1)**i*(i+1) for i in range(20)],dtype=np.float64)
rates={"dates_ns":np.array([int(d.timestamp()) for d in dates],dtype=np.int64),"close":close,"max":close+1.,"min":close-1.}
async def run(m,mode):
 m._seq_codes_cache.clear();m._train_at_date_cache.clear()
 store=m.ReverseStore(object(),port=8942);calls=[]
 async def provider(d):
  calls.append(d);await asyncio.sleep(.001);return ["shared","even" if d.day%2==0 else "odd"]
 async def calc(d):
  return await store.maybe_retrain(pair=1,day_flag=1,control_date=d,params_hash="fixture",np_simple_rates=rates,active_codes_at=provider,train_mode=mode,max_iter=5,extremum_limit=6,extremum_interval=3,skip_db_writes=True,model_tag="same")
 result=await asyncio.gather(calc(dates[-1]+timedelta(days=1)),calc(dates[-1]+timedelta(days=2)))
 return result,len(calls)
async def main():
 for mode in range(5):
  a,ca=await run(old,mode);b,cb=await run(new,mode)
  assert pickle.dumps(a,protocol=5)==pickle.dumps(b,protocol=5),(mode,a,b)
  assert cb<ca,(mode,ca,cb)
  print({"mode":mode,"bitwise_equal":True,"original_provider_calls":ca,"candidate_provider_calls":cb})
asyncio.run(main())
