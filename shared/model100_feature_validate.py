"""Reproducible, no-DB all-hours audit and full/incremental comparison."""
import argparse
import bisect
import gzip
import hashlib
import json
import time
import calendar
from datetime import timedelta
from collections import defaultdict
from pathlib import Path

from .model100_h1_features import Config, FeatureBuilder, GROUPS, canonical, clock, read_dataset, write_dataset


def run(snapshot_path,full_directory,incremental_directory):
    snapshot=json.loads(gzip.decompress(Path(snapshot_path).read_bytes()))
    full=Path(full_directory);schema=json.loads((full/'schema.json').read_text())
    c=schema['config'];config=Config(mode=c['mode'],groups=tuple(c['groups']),news_windows=tuple(c['news_windows']),
        topic_count=c['topic_count'],price_windows=tuple(c['price_windows']),stale_price_hours=c['stale_price_hours'])
    b=FeatureBuilder(snapshot,config,schema)
    manifest=json.loads((full/'manifest.json').read_text())
    first=next(read_dataset(full))['timestamp'];last=manifest['last_cutoff'];hours=b.cutoffs(first,last)
    t=time.perf_counter();split=len(hours)//2
    write_dataset(b,hours[:split],incremental_directory)
    first_seconds=time.perf_counter()-t
    t=time.perf_counter();write_dataset(b,hours,incremental_directory)
    append_seconds=time.perf_counter()-t
    # Independent selection oracle: parsed once, indexed by availability.
    macros=defaultdict(list)
    for r in snapshot.get('macro',[]):
        if r.get('edition')=='REVISION':continue
        at=clock(r['available_at']);until=clock(r.get('valid_until_audit'));eligible=r.get('eligible_feature')
        if r.get('role')=='PRIVATE_EMPLOYMENT_PROXY':until=at+timedelta(days=45);eligible=True
        elif r.get('feature_kind')=='FORECAST_OF_TARGET' and r.get('reference_period'):
            d=clock(str(r['reference_period'])[:10]);month=d.month+(2 if r.get('role','').startswith('GDP_FORECAST') or 'GDP' in r['indicator'] else 0)
            bound=d.replace(month=month,day=calendar.monthrange(d.year,month)[1])+timedelta(days=1)
            until=min(until,bound) if until else bound;eligible=at<until
        key='.'.join(str(x) for x in (r['source'],r['indicator'],r.get('region'),r.get('target_horizon')) if x not in (None,''))
        if eligible:
            macros[key].append((at.timestamp(),until.timestamp() if until else float('inf'),
                str(r.get('reference_period') or ''),str(r.get('event_identity') or ''),float(r['value'])))
    macros={k:sorted(v) for k,v in macros.items()};times={k:[x[0] for x in v] for k,v in macros.items()}
    errors=defaultdict(int);n=0;chain=hashlib.sha256();examples=[]
    inc=iter(read_dataset(incremental_directory))
    for row in read_dataset(full):
        n+=1;other=next(inc,None)
        if row!=other:errors['incremental_full_mismatch']+=1
        if row!=b.build_at(row['timestamp']):errors['live_batch_mismatch']+=1
        if row!=b.build_at(row['timestamp']):errors['repeat_mismatch']+=1
        if row['schema_hash']!=b.schema_hash:errors['schema_mismatch']+=1
        vector=b.vectorize(row)
        if len(vector)!=2*len(b.schema['feature_order']):errors['vector_shape']+=1
        t=clock(row['timestamp']);epoch=t.timestamp()
        if any('t1' in k or 'target_value' in k for k in row['features']):errors['target_column']+=1
        for group,proof in row['provenance'].items():
            available=proof.get('available_at') or proof.get('latest_available_at')
            if available and clock(available)>t:errors['future_'+group.split('.')[0]]+=1
        price=row['provenance'].get('price')
        if price and clock(price['bar_open']).timestamp()+3600>epoch:errors['open_bar']+=1
        for key in schema['macro_keys']:
            aa=macros.get(key,[]);hi=bisect.bisect_right(times.get(key,[]),epoch)
            eligible=[x for x in aa[:hi] if epoch<x[1]]
            expected=max(eligible,key=lambda x:(x[2],x[0],x[3]))[4] if eligible else None
            actual=row['features']['macro.'+key+'.value']
            if actual!=expected:errors['macro_oracle_mismatch']+=1
        chain.update(row['row_hash'].encode())
        if len(examples)<3 and (n==1 or row['features'].get('news.missing')==0):examples.append(row)
    if next(inc,None) is not None:errors['incremental_extra_rows']+=1
    result={'rows':n,'full_incremental_equal':not errors.get('incremental_full_mismatch'),
            'first_half_seconds':first_seconds,'append_seconds':append_seconds,'errors':dict(errors),
            'all_hour_row_hash_chain':chain.hexdigest(),'schema_hash':b.schema_hash,
            'vector_dimensions':2*len(b.schema['feature_order']),'macro_sources':sorted({k.split('.')[0] for k in schema['macro_keys']}),
            'scope':'read-only cached inputs; no training, source writes or DB calls'}
    (full/'validation.json').write_text(canonical(result))
    (full/'examples.json').write_text(json.dumps(examples,indent=2))
    if errors:raise ValueError('ALL_HOURS_AUDIT_FAILED:'+canonical(result))
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(__doc__);p.add_argument('--snapshot',required=True);p.add_argument('--full',required=True);p.add_argument('--incremental',required=True)
    a=p.parse_args();print(canonical(run(a.snapshot,a.full,a.incremental)))
