"""Compare full captured history with bounded live-style inputs, SELECT only."""
import json,sys,time
from datetime import timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'shared')]
from shared.model100_rich_context import V2FeatureBuilder as FeatureBuilder
from shared.model100_h1_features import Config,clock,digest,canonical
from shared.model100_rich_context import RichContextBuilder
from shared.model100_rich_runtime import RUNTIME,read_input


def run():
    results={}
    for mode in ('RESEARCH','STRICT_ASOF'):
        start=time.monotonic();meta=json.loads((RUNTIME/(mode+'-capture.json')).read_text())
        data=read_input(meta['base']);extra=read_input(meta['extra'])
        schema=json.loads(json.loads((RUNTIME/'registration.json').read_text())[mode]['schema_json'])
        full=FeatureBuilder(data,Config(mode=mode),schema);rich=RichContextBuilder(full,extra)
        hours=full.cutoffs('2025-01-01',meta['end']);selected=[hours[i*(len(hours)-1)//11] for i in range(12)]
        checks=[]
        for at in selected:
            t=clock(at);lo=t-timedelta(days=10);short={**data}
            short['prices']=[r for r in data['prices'] if lo<=clock(r['date'])<=t]
            short['news']=[r for r in data['news'] if mode=='STRICT_ASOF' or clock(r['date_dt'])>=lo]
            short['proxy']=[r for r in data['proxy'] if clock(r['event_time_utc'])>=lo]
            short['gdelt']=[r for r in data['gdelt'] if clock(r['available_at'])>=lo]
            inc=FeatureBuilder(short,Config(mode=mode),schema);rr=RichContextBuilder(inc,extra)
            a=full.build_at(t);b=inc.build_at(t)
            if a!=b:raise ValueError('FIXED_FULL_INCREMENTAL_MISMATCH:'+at)
            x=rich.build_at(t);y=rr.build_at(t)
            if x!=y:raise ValueError('RICH_FULL_INCREMENTAL_MISMATCH:'+at)
            checks.append(dict(cutoff=at,fixed_sha256=a['row_hash'],rich_sha256=digest(x)))
        results[mode]=dict(real_bounded_snapshot_checks=checks,seconds=time.monotonic()-start)
        print(canonical({mode:results[mode]}),flush=True)
    (RUNTIME/'incremental-equivalence.json').write_text(canonical(results))


if __name__=='__main__':run()
