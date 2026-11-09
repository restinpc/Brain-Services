"""SELECT-only enrichment; retains row-version evidence, not full news text."""
import json
from collections import Counter
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from .model100_feature_sources import stream_rows, serial
from .model100_h1_features import clock, digest, canonical


async def enrich(brain_engine, vlad_engine, snapshot, start, end):
    params={'start':(clock(start)-timedelta(days=4)).replace(tzinfo=None),
            'end':clock(end).replace(tzinfo=None)}
    out={'news':[], 'gdelt':[], 'cftc':[], 'fxcm':[], 'proxy':[],
         'macro_revisions':[r for r in snapshot.get('macro',[]) if r.get('edition')=='REVISION'],
         'diagnostics':{}}
    wanted={(r['press'],int(r['source_id'])):r.get('nlp_updated_at') for r in snapshot.get('news',[])}
    async with vlad_engine.connect() as c:
        await c.exec_driver_sql("SET time_zone='+00:00'")
        await c.rollback();await c.exec_driver_sql('SET TRANSACTION READ ONLY')
        sql='SELECT press,source_id,event_type,cluster_id,updated_at AS nlp_updated_at FROM vlad_news_algo_events WHERE date_dt>=:start AND date_dt<=:end ORDER BY date_dt,id'
        async for row in stream_rows(c,sql,params):
            r=serial(row)
            if wanted.get((r['press'],int(r['source_id'])))==r['nlp_updated_at']:
                out['news'].append(r)
        await c.rollback()
    async with brain_engine.connect() as c:
        await c.exec_driver_sql("SET time_zone='+00:00'")
        await c.rollback();await c.exec_driver_sql('SET TRANSACTION READ ONLY')
        await c.exec_driver_sql('SET SESSION MAX_EXECUTION_TIME=120000')
        for key,table in [('cftc','vlad_model100_cftc_tff_euro_fx'),('fxcm','vlad_model100_fxcm_h1')]:
            sql=f'SELECT * FROM {table}'
            if key=='fxcm':sql+=' WHERE event_time_utc>=:start AND event_time_utc<:end ORDER BY event_time_utc'
            else:sql+=' ORDER BY report_date'
            async for r in stream_rows(c,sql,params):
                out[key].append(serial(r))
        async for r in stream_rows(c,'SELECT * FROM vlad_model100_binance_eurusdt_h1 WHERE event_time_utc>=:start AND event_time_utc<:end ORDER BY event_time_utc',params):
            out['proxy'].append(serial(r))
        # Country/category counts cover ALL geo rows. Limit retained categories,
        # retaining omitted total; never mistake selected samples for all events.
        pub='GREATEST(snapshot_at,loaded_at)' if snapshot['capture_mode']=='STRICT_ASOF' else 'snapshot_at'
        sql=f'''SELECT CEIL(UNIX_TIMESTAMP({pub})/3600)*3600 AS epoch,country,category,
                COUNT(*) n,SUM(tone) tone_sum,COUNT(tone) tone_count,MIN(id) min_id,MAX(id) max_id
                FROM vlad_gdelt_geo_events WHERE snapshot_at>=:start AND snapshot_at<:end
                AND {pub}<=:cutoff GROUP BY epoch,country,category ORDER BY epoch,country,category'''
        buckets={}
        try:
            chunk=params['start']
            while chunk<params['end']:
                stop=min(chunk+timedelta(days=14),params['end'])
                args=dict(start=chunk,end=stop,cutoff=params['end'])
                for r in (await c.execute(text(sql),args)).mappings():
                        epoch=int(r['epoch'])
                        if datetime.fromtimestamp(epoch,timezone.utc)>clock(end):continue
                        b=buckets.setdefault(epoch,dict(countries=Counter(),categories=Counter(),count=0,
                            tone_sum=0.,tone_count=0,first_id=int(r['min_id']),last_id=int(r['max_id'])))
                        b['countries'][r['country'] or 'UNKNOWN']+=int(r['n'])
                        b['categories'][r['category'] or 'UNKNOWN']+=int(r['n'])
                        b['count']+=int(r['n']);b['tone_sum']+=float(r['tone_sum'] or 0);b['tone_count']+=int(r['tone_count'])
                        b['first_id']=min(b['first_id'],int(r['min_id']));b['last_id']=max(b['last_id'],int(r['max_id']))
                chunk=stop
            for epoch,b in sorted(buckets.items()):
                countries=sorted(b.pop('countries').items(),key=lambda kv:(-kv[1],kv[0]))
                b['countries']=dict(countries[:32]);b['omitted_country_mentions']=sum(v for _,v in countries[32:])
                b['categories']=dict(sorted(b['categories'].items()))
                b.update(available_at=datetime.fromtimestamp(epoch,timezone.utc).isoformat(),
                         row_id=f'geo:{epoch}',kind='GEO_COUNTRY_CATEGORY_BUCKET',
                         evidence_kind='SOURCE_ROWSET_AGGREGATE_NOT_DOCUMENT_SHA')
                b['source_rowset_sha256']=digest(b);out['gdelt'].append(b)
        except Exception as e:
            out['diagnostics']['gdelt_geo']={'status':'UNAVAILABLE','error_type':type(e).__name__}
        # GKG event samples: deterministic four earliest stable ids per
        # availability hour, with the complete hourly count retained.
        pub='GREATEST(available_at,loaded_at)' if snapshot['capture_mode']=='STRICT_ASOF' else 'available_at'
        outer_pub='GREATEST(n.available_at,n.loaded_at)' if snapshot['capture_mode']=='STRICT_ASOF' else 'n.available_at'
        sql=f'''SELECT n.id,n.record_key,n.event_id,n.story_id,n.source,
                n.v2_themes,n.v2_locations,n.v2_tone,{outer_pub} AS effective_available,
                ranked.epoch,ranked.rn,ranked.total FROM (SELECT id,
                CEIL(UNIX_TIMESTAMP({pub})/3600)*3600 AS epoch,
                ROW_NUMBER() OVER(PARTITION BY CEIL(UNIX_TIMESTAMP({pub})/3600) ORDER BY id) rn,
                COUNT(*) OVER(PARTITION BY CEIL(UNIX_TIMESTAMP({pub})/3600)) total
                FROM sasha_gdelt_news WHERE available_at>=:start AND available_at<=:end AND {pub}<=:end
                ) ranked JOIN sasha_gdelt_news n ON n.id=ranked.id WHERE ranked.rn<=4 ORDER BY ranked.epoch,n.id'''
        try:
            for r in (await c.execute(text(sql),params)).mappings():
                at=datetime.fromtimestamp(int(r['epoch']),timezone.utc)
                if at>clock(end):continue
                parse=lambda v:json.loads(v) if isinstance(v,str) else v
                themes=parse(r['v2_themes']) or [];locations=parse(r['v2_locations']) or []
                countries=sorted(set(x.get('country_code') for x in locations if isinstance(x,dict) and x.get('country_code')))
                rr=dict(row_id='gkg:'+str(r['id']),kind='GKG_EVENT_SAMPLE',available_at=at.isoformat(),
                        effective_available=str(r['effective_available']),record_key=r['record_key'],
                        event_id=r['event_id'],story_id=r['story_id'],source=r['source'],
                        countries={k:1 for k in countries},categories={},
                        themes=sorted(set(x.get('theme') for x in themes if isinstance(x,dict) and x.get('theme')))[:32],
                        tone=parse(r['v2_tone']),hour_total=int(r['total']),sample_rank=int(r['rn']),
                        selection='FIRST_4_SOURCE_IDS_PER_AVAILABILITY_HOUR')
                rr['source_rowset_sha256']=digest(rr);out['gdelt'].append(rr)
        except Exception as e:
            out['diagnostics']['gdelt_gkg']={'status':'UNAVAILABLE','error_type':type(e).__name__}
        await c.rollback()
    # Fresh captures and reloaded pinned captures have exactly the same types.
    # SQL DECIMAL values remain exact decimal strings, not binary doubles.
    return json.loads(canonical(out))
