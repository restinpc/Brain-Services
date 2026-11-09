"""Read-only whole-history dry run; bounded output, no model/source writes."""
import asyncio,json,sys,time,resource,gzip,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'shared')]
from shared.model100_rich_context import V2FeatureBuilder as FeatureBuilder
from shared.model100_h1_features import Config,clock,canonical,digest
from shared.model100_rich_context import RichContextBuilder,unpack
from shared.model100_rich_runtime import RUNTIME,read_input
from shared.model100_rich_store import encode_context


def check_clocks(value,t):
    if isinstance(value,dict):
        if value.get('available_at') and clock(value['available_at'])>t:raise ValueError('FUTURE_AVAILABLE_AT')
        if value.get('bar_close_utc') and clock(value['bar_close_utc'])>t:raise ValueError('OPEN_BAR')
        for v in value.values():check_clocks(v,t)
    elif isinstance(value,list):
        for v in value:check_clocks(v,t)


_BASE=None
_RICH=None
_SCHEMA=None
_MODE=None


def worker_init(mode):
    global _BASE,_RICH,_SCHEMA,_MODE
    _MODE=mode
    meta=json.loads((RUNTIME/(mode+'-capture.json')).read_text())
    data=read_input(meta['base']);extra=read_input(meta['extra'])
    reg=json.loads((RUNTIME/'registration.json').read_text())[mode]
    _SCHEMA=json.loads(reg['schema_json'])
    _BASE=FeatureBuilder(data,Config(mode=mode),_SCHEMA)
    _RICH=RichContextBuilder(_BASE,extra)


def worker_shard(task):
    index,hours=task;start=time.monotonic();lines=[];checks=[];counts={k:0 for k in _RICH.groups}
    news_ids=set();macro_ids=set();sizes=[];comparison_hours=0
    for at in hours:
        t=clock(at);x=_RICH.build_at(t);check_clocks(x,t)
        p=encode_context(_BASE.build_at(t),x,_SCHEMA)
        lines.append(canonical(p));size=len(p['rich_context'].encode());sizes.append(size)
        groups=x['groups']
        comparison_hours+=int(any(r.get('nowcast_comparisons') for r in groups['macro']['series_state'].values()))
        for k in counts:
            g=groups[k];counts[k]+=int(bool(g.get('events') or g.get('series_state') or g.get('h1_sequence') or g.get('state')))
        news_ids.update(e['event_id'] for e in groups['news']['events'])
        macro_ids.update(e['event_identity'] for e in groups['macro']['releases'])
        checks.append(dict(cutoff=t.isoformat(),context_sha256=p['context_sha256'],rich_sha256=p['rich_sha256'],bytes=size))
    raw=('\n'.join(lines)+'\n').encode();compressed=gzip.compress(raw,compresslevel=6,mtime=0)
    dest=RUNTIME/(_MODE+'-verified-shards');dest.mkdir(exist_ok=True)
    path=dest/(str(index).zfill(5)+'.jsonl.gz')
    from shared.model100_h1_features import _atomic
    _atomic(path,compressed)
    shard=dict(path=str(path),rows=len(lines),first_cutoff=hours[0],last_cutoff=hours[-1],
               sha256=hashlib.sha256(raw).hexdigest(),file_sha256=hashlib.sha256(compressed).hexdigest())
    return dict(shard=shard,checks=checks,counts=counts,news_ids=sorted(news_ids),macro_ids=sorted(macro_ids),
                total_bytes=sum(sizes),max_bytes=max(sizes),seconds=time.monotonic()-start,
                peak_rss_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,nowcast_comparison_hours=comparison_hours)


async def run():
    from dotenv import load_dotenv
    from common import build_engines
    from sqlalchemy import text
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing
    load_dotenv('/brain/Brain-Services/.env',override=False)
    v,b,s=build_engines();results={}
    try:
        for mode in ('RESEARCH','STRICT_ASOF'):
            start=time.monotonic();meta=json.loads((RUNTIME/(mode+'-capture.json')).read_text())
            reg=json.loads((RUNTIME/'registration.json').read_text())[mode];schema=json.loads(reg['schema_json'])
            async with b.connect() as c:
                rows=(await c.execute(text("SELECT cutoff_utc FROM vlad_model100_h1_features WHERE dataset_version='model100-h1-v1hex-20261109' AND mode=:m AND cutoff_utc<=:e ORDER BY cutoff_utc"),dict(m=mode,e=clock(meta['end']).replace(tzinfo=None)))).all()
            hours=[clock(r[0]).isoformat() for r in rows]
            tasks=[(i//256,hours[i:i+256]) for i in range(0,len(hours),256)]
            counts={k:0 for k in ('news','macro','price','gdelt','binance','cftc','fxcm')}
            news_ids=set();macro_ids=set();shards=[];total_bytes=maximum=peak=0;n=0;comparison_hours=0
            with (RUNTIME/(mode+'-dryrun-sha.jsonl')).open('w') as f:
                # Four low-priority processes on the verified 112-CPU B1.
                # Workers read pinned files only; none has a DB writer/engine.
                with ProcessPoolExecutor(max_workers=4,mp_context=multiprocessing.get_context('spawn'),initializer=worker_init,initargs=(mode,)) as pool:
                    for item in pool.map(worker_shard,tasks):
                        shards.append(item['shard']);n+=item['shard']['rows']
                        for k in counts:counts[k]+=item['counts'][k]
                        news_ids.update(item['news_ids']);macro_ids.update(item['macro_ids'])
                        total_bytes+=item['total_bytes'];maximum=max(maximum,item['max_bytes']);peak=max(peak,item['peak_rss_kb'])
                        comparison_hours+=item['nowcast_comparison_hours']
                        for check in item['checks']:f.write(canonical(check)+'\n')
                        print(canonical(dict(mode=mode,rows=n,seconds=time.monotonic()-start)),flush=True)
            result=dict(rows=n,hours_with_group=counts,distinct_selected_news=len(news_ids),distinct_selected_macro=len(macro_ids),
                        average_rich_bytes=total_bytes/n,maximum_rich_bytes=maximum,total_rich_bytes=total_bytes,
                        seconds=time.monotonic()-start,peak_worker_rss_kb=peak,workers=4,capture_sha256=digest(meta),clock_checks='ALL_ROWS_PASSED',nowcast_comparison_hours=comparison_hours)
            (RUNTIME/(mode+'-verified-manifest.json')).write_text(canonical(dict(schema_hash=digest(schema),capture=meta,rows=n,shards=shards)))
            (RUNTIME/(mode+'-full-dryrun.json')).write_text(canonical(result));results[mode]=result;print(canonical(result),flush=True)
        (RUNTIME/'full-dryrun.json').write_text(canonical(results))
    finally:
        for e in (v,b,s):await e.dispose()


if __name__=='__main__':asyncio.run(run())
