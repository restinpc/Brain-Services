"""Offline/replay entrypoint. Existing engines; no service/model registration."""
import argparse
import asyncio
import gzip
import json
import time
import hashlib
from collections import Counter
from pathlib import Path

from .model100_h1_features import Config, FeatureBuilder, GROUPS, canonical, read_dataset, write_dataset, _atomic
from .model100_feature_sources import load_snapshot, merge_snapshots


async def capture(start,end,config,refresh_since=None):
    # Reuse Brain's existing credential/config/engine construction. Never log it.
    from dotenv import load_dotenv
    import sys
    repo=Path(__file__).resolve().parents[1]
    load_dotenv('/brain/Brain-Services/.env',override=False)
    sys.path.insert(0,str(repo/'shared'))
    from common import build_engines
    vlad,brain,super_engine=build_engines()
    try:
        return await load_snapshot(brain,vlad,start,end,config,refresh_since=refresh_since)
    finally:
        for e in (vlad,brain,super_engine):await e.dispose()


def summarize(directory,elapsed):
    n=0;missing=Counter();nonzero=Counter();counts=Counter();first=last=None
    for row in read_dataset(directory):
        n+=1;first=first or row['timestamp'];last=row['timestamp']
        for key,value in row['features'].items():
            counts[key]+=1
            if value is None:missing[key]+=1
            elif value!=0:nonzero[key]+=1
    schema=json.loads((Path(directory)/'schema.json').read_text())
    return {'rows':n,'first_cutoff':first,'last_cutoff':last,'build_seconds':elapsed,
            'features_by_group':{k:len(v) for k,v in schema['features_by_group'].items()},
            'feature_count':sum(map(len,schema['features_by_group'].values())),
            'null_percent':{k:round(100*missing[k]/n,4) for k in counts} if n else {},
            'group_any_data_hours':{k:n-int(nonzero[k+'.missing']) for k in schema['features_by_group']},
            'policy':'no targets, weights, model calls or database mutations'}


def main():
    p=argparse.ArgumentParser(__doc__)
    p.add_argument('--start',required=True);p.add_argument('--end',required=True)
    p.add_argument('--mode',choices=('STRICT_ASOF','RESEARCH'),default='STRICT_ASOF')
    p.add_argument('--groups',default=','.join(GROUPS[:-1]))
    p.add_argument('--output',required=True);p.add_argument('--snapshot')
    p.add_argument('--schema');p.add_argument('--chunk-size',type=int,default=128)
    p.add_argument('--refresh',action='store_true',help='Bounded source refresh; never rebuild committed H1 silently')
    a=p.parse_args();config=Config(mode=a.mode,groups=tuple(a.groups.split(',')))
    output=Path(a.output);output.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((output/'manifest.json').read_text()) if (output/'manifest.json').exists() else {}
    source=Path(a.snapshot) if a.snapshot else output/manifest.get('input_reference','inputs.json.gz')
    t=time.perf_counter()
    if source.exists():data=json.loads(gzip.decompress(source.read_bytes()))
    else:
        data=asyncio.run(capture(a.start,a.end,config))
    if a.refresh and manifest.get('last_cutoff'):
        from datetime import timedelta
        from .model100_h1_features import clock
        begin=max(clock(a.start),clock(manifest['last_cutoff'])-timedelta(days=10))
        data=merge_snapshots(data,asyncio.run(capture(begin,a.end,config,manifest['last_cutoff'])))
    if data.get('capture_mode',config.mode)!=config.mode:
        raise ValueError('STRICT_RESEARCH_SNAPSHOT_MISMATCH')
    capture_seconds=time.perf_counter()-t
    schema_path=Path(a.schema) if a.schema else output/'schema.json'
    schema=json.loads(schema_path.read_text()) if schema_path.exists() else None
    builder=FeatureBuilder(data,config,schema)
    encoded=canonical(data).encode();name='inputs-'+hashlib.sha256(encoded).hexdigest()+'.json.gz'
    if not (output/name).exists():_atomic(output/name,gzip.compress(encoded,mtime=0))
    t=time.perf_counter();state=write_dataset(builder,builder.cutoffs(a.start,a.end),output,a.chunk_size,input_reference=name)
    elapsed=time.perf_counter()-t
    audit=summarize(output,elapsed)|{'capture_seconds':capture_seconds,'schema_hash':builder.schema_hash,
             'normalization_diagnostics':dict(builder.diagnostics),'input_diagnostics':data.get('diagnostics',{}),
             'input_rows':{k:len(data.get(k,[])) for k in ('prices','macro','news','proxy','gdelt','cftc')},
             'mode':config.mode,'source_identity':data.get('source_identity',{})}
    _atomic(output/'audit.json',canonical(audit).encode());print(canonical(audit))


if __name__=='__main__':main()
