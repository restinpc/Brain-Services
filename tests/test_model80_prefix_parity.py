"""Compare exact production aggregates before/after sharing an immutable prefix."""
import ast
import bisect
from collections import OrderedDict
from datetime import datetime, timedelta
import pickle
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any
import numpy as np

NAMES = {
    '_var_allows_row', '_var_eligibility_mask', '_rate_timestamp_first_index',
    '_frame_date', '_ctx_reverse', '_current_rows', '_previous_direction',
    '_aggregate_analogs_by_var', '_mode1_score', '_add_code',
    '_compute_all_slots_for_date',
}

def load(source, label):
    tree = ast.parse(source)
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]
    assert len(selected) == len(NAMES)
    ns = dict(np=np, datetime=datetime, timedelta=timedelta, bisect=bisect, Any=Any,
              VAR_RANGE=list(range(8)), TYPES_RANGE=list(range(5)), SHIFT_WINDOW=12,
              TOPIC_FOCUS_MIN=.36, CLUSTER_SIM_MIN=.70, NOVELTY_MIN=.35,
              _CACHE_LOCK=threading.Lock(), _RATE_TS_INDEX_CACHE=OrderedDict(),
              _RATE_TS_INDEX_CACHE_MAX=8,
              _nlp_cfg=lambda: {'train_cutoff': datetime(2000, 1, 1)})
    ns['_VAR_MASK_TO_VARS'] = tuple(tuple(v for v in range(8) if m & (1 << v)) for m in range(256))
    exec(compile(ast.Module(body=selected, type_ignores=[]), label, 'exec'), ns)
    return ns['_compute_all_slots_for_date']

class Counted(list):
    visits = 0
    def __getitem__(self, key):
        type(self).visits += 1
        return super().__getitem__(key)

repo=Path(os.environ.get('BRAIN_TEST_SERVICES_ROOT',Path(__file__).resolve().parents[1]))
reference_source=subprocess.check_output(['git','-C',str(repo),'show','a36e12b:80/model.py'],text=True)
candidate_path=Path(os.environ.get('BRAIN_TEST_MODEL80_CANDIDATE',repo/'80/model.py'))
reference = load(reference_source,'Model80 before prefix reuse')
candidate = load(candidate_path.read_text(),str(candidate_path))
base = datetime(2025, 1, 1)
target = base + timedelta(hours=1900)
source = []
for hour in list(range(0, 1900, 4)) + list(range(1876, 1902)):
    for event in range(3):
        for copy in range(4 if hour >= 1876 else 1):
            source.append(dict(date=base + timedelta(hours=hour, minutes=copy * 3),
                               event_type=f'e{event}', quality_score=[.2,.65,1.2][event],
                               topic_focus=[.2,.5,.8][event], cluster_similarity=.8,
                               novelty=.4, confirmation_count=event+1))
source.sort(key=lambda r: r['date'])
by_key = {f'e{e}': Counted([r for r in source if r['event_type']==f'e{e}']) for e in range(3)}
dates = np.array([int((base+timedelta(hours=i//2)).timestamp()) for i in range(4100)], dtype=np.int64)
rates = dict(dates_ns=dates, open=np.ones(len(dates)), close=np.full(len(dates),2.),
             t1=np.array(([1e16,1e-9,-1e16,17.25]*1025)),
             ext_min=np.array(([False,True]*2050)), ext_max=np.array(([True,False]*2050)))
index = dict(by_key=by_key, key_dates={k:[r['date'] for r in v] for k,v in by_key.items()},
             ctx_index={str(e): {'id':e+1,'event_type':f'e{e}'} for e in range(3)},
             full_dataset=source, dates=[r['date'] for r in source], np_rates=rates,
             dataset_timestamps=np.array([int(r['date'].timestamp()) for r in source]))
for daily in (False,True):
    index['is_daily']=daily
    Counted.visits=0; start=time.perf_counter(); before=reference(source,target,index)
    before_s=time.perf_counter()-start; old_visits=Counted.visits
    Counted.visits=0; start=time.perf_counter(); after=candidate(source,target,index)
    after_s=time.perf_counter()-start; new_visits=Counted.visits
    assert pickle.dumps(before,protocol=5)==pickle.dumps(after,protocol=5), 'FLOAT_OR_ORDER_MISMATCH'
    assert sum(map(len,before.values()))>0, 'VACUOUS_FIXTURE'
    assert new_visits < old_visits, 'PREFIX_NOT_REUSED'
    print(dict(daily=daily, bitwise_equal=True, original_visits=old_visits,
               optimized_visits=new_visits, original_seconds=round(before_s,4),
               optimized_seconds=round(after_s,4)))
for state in ({**index,'np_rates':None},{**index,'by_key':{}}, {**index,'full_dataset':[]}):
    assert reference(source,target,state)==candidate(source,target,state)
print('PASS: missing rates, empty indexes/data, duplicate timestamps, causal boundaries and FP64 order')
