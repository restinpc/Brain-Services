"""Refresh bounded rich delta and exact NLP metadata; no source/DB mutation."""
import asyncio,json,sys
from datetime import timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'shared')]
from shared.model100_h1_features import canonical,clock
from shared.model100_feature_sources import serial,stream_rows
from shared.model100_rich_runtime import RUNTIME,read_input,retain
from shared.model100_rich_sources import enrich


async def run():
    from dotenv import load_dotenv
    from common import build_engines
    load_dotenv('/brain/Brain-Services/.env',override=False)
    v,b,s=build_engines()
    try:
        metas={};wanted={};min_date=None;max_date=None
        for mode in ('RESEARCH','STRICT_ASOF'):
            meta=json.loads((RUNTIME/(mode+'-capture.json')).read_text());metas[mode]=meta
            data=read_input(meta['base'])
            wanted[mode]={(r['press'],int(r['source_id'])):r.get('nlp_updated_at') for r in data['news']}
            dates=[clock(r.get('date_dt')) for r in data['news'] if clock(r.get('date_dt'))]
            if dates:
                min_date=min(min_date,min(dates)) if min_date else min(dates)
                max_date=max(max_date,max(dates)) if max_date else max(dates)
        rows={}
        if min_date:
            async with v.connect() as c:
                await c.exec_driver_sql("SET time_zone='+00:00'");await c.rollback();await c.exec_driver_sql('SET TRANSACTION READ ONLY')
                sql='SELECT press,source_id,feed,event_type,cluster_id,cluster_similarity,updated_at AS nlp_updated_at FROM vlad_news_algo_events WHERE date_dt>=:start AND date_dt<=:end ORDER BY date_dt,id'
                async for row in stream_rows(c,sql,dict(start=min_date.replace(tzinfo=None),end=max_date.replace(tzinfo=None))):
                    r=serial(row);key=(r['press'],int(r['source_id']))
                    if any(w.get(key)==r['nlp_updated_at'] for w in wanted.values()):rows[key]=r
                await c.rollback()
        summary={}
        for mode,meta in metas.items():
            data=read_input(meta['base']);extra=read_input(meta['extra']);end=clock(meta['end'])
            old={(r['press'],int(r['source_id'])):r for r in extra.get('news',[])}
            # Latest four days plus one hour of bucket warmup, not the full
            # GDELT archive. This also captures new closed proxy bars/metadata.
            delta=await enrich(b,v,data,end-timedelta(hours=1),end)
            begin=end-timedelta(days=4)
            for prefix,diagnostic in [('GEO','gdelt_geo'),('GKG','gdelt_gkg')]:
                if delta.get('diagnostics',{}).get(diagnostic):continue
                extra['gdelt']=[r for r in extra.get('gdelt',[]) if not r['kind'].startswith(prefix) or clock(r['available_at'])<begin]
                extra['gdelt'].extend(r for r in delta['gdelt'] if r['kind'].startswith(prefix) and clock(r['available_at'])>=begin)
            proxy={r['event_time_utc']:r for r in extra.get('proxy',[])}
            proxy.update({r['event_time_utc']:r for r in delta.get('proxy',[])})
            extra['proxy']=sorted(proxy.values(),key=lambda r:r['event_time_utc'])
            extra['cftc']=delta['cftc']
            extra['macro_revisions']=delta['macro_revisions']
            matched=[]
            for key,at in sorted(wanted[mode].items()):
                current=rows.get(key);prior=old.get(key)
                r=current if current and current['nlp_updated_at']==at else prior if prior and prior['nlp_updated_at']==at else None
                if r:matched.append(r)
            extra['news']=matched
            extra['diagnostics']={**extra.get('diagnostics',{}),**delta.get('diagnostics',{})}
            meta['extra']=retain(extra,mode+'-extra-final')
            (RUNTIME/(mode+'-capture.json')).write_text(canonical(meta))
            summary[mode]=dict(news_versions_matched=len(matched),news_feed_available=sum(bool(r.get('feed')) for r in matched),
                               gdelt_context_items=len(extra['gdelt']),extra_sha256=meta['extra']['sha256'],delta_days=4,
                               diagnostics=extra['diagnostics'])
        (RUNTIME/'metadata-preparation.json').write_text(canonical(summary));print(canonical(summary),flush=True)
    finally:
        for e in (v,b,s):await e.dispose()


if __name__=='__main__':asyncio.run(run())
