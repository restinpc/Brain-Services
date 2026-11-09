"""SELECT-only full DB/file equivalence and reader timings. No model calls."""
import asyncio
import json
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'shared'))
from shared.model100_feature_store import read_features
from shared.model100_h1_features import read_dataset,canonical


async def run():
    load_dotenv('/brain/Brain-Services/.env',override=False)
    from common import build_engines
    v,b,s=build_engines();base=Path('/brain/model100-featurebuilder-20261109');out={}
    try:
        for mode,name in [('RESEARCH','research-release'),('STRICT_ASOF','strict-release')]:
            expected=iter(read_dataset(base/name));n=0;t=time.perf_counter()
            async for row in read_features(b,'model100-h1-v1hex-20261109',mode,'2025-01-01T00:00:00Z','2026-11-09T09:00:00Z'):
                if row!=next(expected,None):raise ValueError('DB_SOURCE_ROW_MISMATCH:'+row['timestamp'])
                n+=1
            if next(expected,None) is not None or n!=11443:raise ValueError('HISTORY_COUNT_MISMATCH')
            out[mode]={'source_rows_equal':n,'full_decode_and_comparison_seconds':time.perf_counter()-t}
            for limit in (1,100):
                t=time.perf_counter();count=0
                async for row in read_features(b,'model100-h1-v1hex-20261109',mode,'2026-10-01T00:00:00Z','2026-11-09T09:00:00Z',limit=limit):count+=1
                out[mode][f'read_{limit}_seconds']=time.perf_counter()-t
                if count!=limit:raise ValueError('READER_COUNT_MISMATCH')
        (base/'mysql-store/db-equivalence.json').write_text(canonical(out));print(canonical(out))
    finally:
        for e in (v,b,s):await e.dispose()


async def benchmark():
    load_dotenv('/brain/Brain-Services/.env',override=False)
    from common import build_engines
    import resource
    from datetime import datetime,timezone
    v,b,s=build_engines();out={}
    try:
        for mode in ('RESEARCH','STRICT_ASOF'):
            out[mode]={}
            for limit in (1,100,None):
                t=time.perf_counter();n=0
                async for row in read_features(b,'model100-h1-v1hex-20261109',mode,'2025-01-01T00:00:00Z',datetime.now(timezone.utc),limit=limit):n+=1
                out[mode][str(limit or 'all')]={'rows':n,'seconds':time.perf_counter()-t}
        out['max_reader_rss_kb']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        p=Path('/brain/model100-featurebuilder-20261109/mysql-store/read-performance.json');p.write_text(canonical(out));print(canonical(out))
    finally:
        for e in (v,b,s):await e.dispose()


if __name__=='__main__':asyncio.run(benchmark() if '--benchmark-only' in sys.argv else run())
