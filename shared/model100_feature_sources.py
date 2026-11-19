"""SELECT-only streaming input adapter using existing Brain AsyncEngines.

No raw texts, credentials, or database rows are written by this adapter. It
does not call enrich_dataset, _fit_artifact, model(), or Framework build_app.
"""
from __future__ import annotations

import hashlib
import json
import ast
from collections import Counter
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

try:
    from .model100_h1_features import Config, clock, digest
    from .model100_macro.model100_feature_contract import compile_rows
    from .model100_macro.model100_source_policy import load_policy
except ImportError:
    from model100_h1_features import Config, clock, digest
    from model100_macro.model100_feature_contract import compile_rows
    from model100_macro.model100_source_policy import load_policy

POLICY_PATH = Path(__file__).with_name('model100_macro') / 'model100_v1_sources.json'
NEWS_TABLES = {'cnn':'brain_cnn_news','nyt':'brain_nyt_news','twp':'brain_twp_news','wsj':'brain_wsj_news','tgd':'brain_tgd_news'}


def existing_nlp_contract(repo):
    """Execute only Service80's existing pure config/signature functions.

    No importing a model or monkeypatching a running Framework module. The
    signature establishes that the persisted artifact matches its fit cutoff.
    """
    import tomllib
    model=Path(repo)/'80/model.py';tree=ast.parse(model.read_text())
    cfg=tomllib.loads((Path(repo)/'80/config.toml').read_text())
    ns={'datetime':datetime,'hashlib':hashlib,'get_service_config':lambda:cfg}
    for node in tree.body:
        if isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name):
            name=node.targets[0].id
            if name.startswith('DEFAULT_') or name=='ALGO_VERSION':ns[name]=ast.literal_eval(node.value)
    functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('_nlp_cfg','_signature')]
    module=ast.Module(body=ast.parse('from __future__ import annotations').body+functions,type_ignores=[])
    exec(compile(ast.fix_missing_locations(module),str(model),'exec'),ns)
    normalized=ns['_nlp_cfg']()
    return normalized['train_cutoff'].isoformat(),ns['_signature'](normalized),hashlib.sha256(model.read_bytes()).hexdigest()


def serial(row):
    return {k: v.isoformat() if isinstance(v, datetime) else v for k,v in row.items()}


async def stream_rows(conn, sql, params=None):
    """Same server-side partition approach as brain_framework._load_dataset."""
    result = await conn.stream(text(sql), params or {}, execution_options={'yield_per':4096})
    try:
        async for part in result.partitions(4096):
            keys = result.keys()
            for row in part:
                yield dict(zip(keys,row))
    finally:
        await result.close()


def text_proof(row):
    """A hash alone is not proof that existing NLP used this text version."""
    ex = row.get('publication_time_evidence') or {}
    ex = json.loads(ex) if isinstance(ex,str) else ex
    path = ex.get('text_snapshot_path'); sha = ex.get('text_snapshot_sha256')
    if not path or not sha:
        return False, sha
    p = Path(path).resolve()
    allowed = Path('/brain/Brain-Server/runtime/news-time-evidence').resolve()
    if not p.is_relative_to(allowed) or not p.is_file():
        return False, sha
    raw = p.read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha:
        return False, sha
    try:
        content = json.loads(raw)
    except (ValueError,UnicodeError):
        return False, sha
    matches = all(str(content.get(k,''))==str(row.get(k,'')) for k in ('title','text'))
    return matches,sha


async def load_snapshot(engine_brain, engine_vlad, start, end, config=Config(), b1_hostname='techno-kur-brain01.local',refresh_since=None):
    """Read consistent per-database snapshots; client has no mutation methods.

    Timestamp fields of type TIMESTAMP are fetched with session UTC. DATETIME
    fields below follow each collector's documented UTC contract. updated_at of
    existing NLP is an availability LOWER BOUND for its current row version,
    never the historical first receipt of the article.
    """
    start,end=clock(start),clock(end)
    params={'start':(start-timedelta(days=10)).replace(tzinfo=None),'end':end.replace(tzinfo=None)}
    data={k:[] for k in ('prices','macro','news','proxy','gdelt','cftc')}
    diagnostics={};policy=load_policy(POLICY_PATH)
    async with AsyncExitStack() as stack:
        brain=await stack.enter_async_context(engine_brain.connect())
        vlad=None
        if 'news' in config.groups:
            try:
                vlad=await stack.enter_async_context(engine_vlad.connect())
            except Exception as exc:
                diagnostics['news_engine']={'status':'UNAVAILABLE','error_type':type(exc).__name__}
        await brain.exec_driver_sql("SET time_zone='+00:00'")
        if vlad is not None:await vlad.exec_driver_sql("SET time_zone='+00:00'")
        # End setup transactions before requesting the SELECT-only snapshots.
        await brain.rollback()
        if vlad is not None:await vlad.rollback()
        await brain.exec_driver_sql('SET TRANSACTION READ ONLY')
        await brain.exec_driver_sql('SET SESSION MAX_EXECUTION_TIME=120000')
        if vlad is not None:await vlad.exec_driver_sql('SET TRANSACTION READ ONLY')
        hostname=(await brain.execute(text('SELECT @@hostname'))).scalar()
        if b1_hostname and hostname!=b1_hostname:
            raise ValueError('B1_AUTHORITATIVE_SOURCE_REQUIRED')
        data['source_identity']={'brain_hostname':hostname,'price_table':'brain.sasha_rates_eur_usd','nlp_table':'vlad.vlad_news_algo_events'}
        tables={r[0] for r in (await brain.execute(text('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()'))).all()}
        vlad_tables={r[0] for r in (await vlad.execute(text('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()'))).all()} if vlad is not None else set()
        data['capture_mode']=config.mode
        data['capture_groups']=list(config.groups)
        data['capture_start']=params['start'].isoformat()
        for table in ('vlad_model100_primary_source_records','vlad_model100_binance_eurusdt_h1','vlad_model100_cftc_tff_euro_fx','sasha_gdelt_news','vlad_gdelt_geo_events'):
            if table not in tables:diagnostics[table]={'status':'TABLE_MISSING'}
        async for r in stream_rows(brain,"SELECT date,open,close,`max`,`min`,loaded_at AS row_updated_at FROM sasha_rates_eur_usd WHERE date>=:start AND date<:end ORDER BY date",params):
            data['prices'].append(serial(r))
        if 'macro' in config.groups and 'vlad_model100_primary_source_records' in tables:
            raw=[]
            async for r in stream_rows(brain,"SELECT source_id,record_key,record_kind,event_type,region,title,http_status,source_published_at_utc,first_received_at_utc,observation_period_start,vintage_month,numeric_value,unit,asof_class,release_clock_evidence,canonical_url,content_sha256,extra_json FROM vlad_model100_primary_source_records WHERE numeric_value IS NOT NULL AND asof_class<>'UNUSABLE' ORDER BY source_id,record_key,first_received_at_utc"):
                raw.append(serial(r))
            data['macro'],diagnostics['macro_contract']=compile_rows(raw,policy)
            diagnostics['macro_raw_numeric_rows']=len(raw)
            diagnostics['macro_eligible_rows']=sum(r['eligible_feature'] for r in data['macro'])
            data['macro_policy_sha256']=hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
        if 'news' in config.groups and vlad is not None and {'vlad_news_algo_events','vlad_news_algo_events_artifact'}<=vlad_tables:
            artifact=(await vlad.execute(text("SELECT signature,sha256 FROM vlad_news_algo_events_artifact WHERE artifact_key='main'"))).mappings().first()
            # This is metadata of the existing frozen pipeline; never unpickle or refit it.
            data['nlp_artifact']=dict(artifact) if artifact else {}
            cutoff,expected_signature,code_sha=existing_nlp_contract(Path(__file__).resolve().parents[1])
            data['nlp_fit_cutoff']=cutoff
            data['nlp_contract_code_sha256']=code_sha
            nlp_ok=artifact and artifact['signature']==expected_signature
            if not nlp_ok:
                diagnostics['news']={'status':'EXISTING_NLP_ARTIFACT_SIGNATURE_MISMATCH'}
            if config.mode=='RESEARCH' and nlp_ok:
                extra=' OR updated_at>=:refresh_since' if refresh_since else ''
                sql="SELECT id,press,source_id,date_dt,topic_id,quality_score,novelty,confirmation_count,topic_focus,cluster_similarity,updated_at AS nlp_updated_at FROM vlad_news_algo_events WHERE (date_dt>=:start"+extra+") AND date_dt<=:end ORDER BY date_dt,press,source_id"
                args=dict(params,refresh_since=clock(refresh_since).replace(tzinfo=None)) if refresh_since else params
                async for r in stream_rows(vlad,sql,args):
                    r=serial(r);r['nlp_artifact_sha']=(artifact or {}).get('sha256');data['news'].append(r)
            elif nlp_ok:
                # Sidecar and NLP live in different existing engines. Join only
                # the small set with a real Brain receipt; no million-row text fetch.
                candidates={}; per_press=Counter()
                for press,table in NEWS_TABLES.items():
                    if table not in tables or 'brain_news_publication_time' not in tables:
                        per_press[press+'_table_missing']+=1;continue
                    sql=f"""SELECT p.*,n.title,n.text FROM brain_news_publication_time p
                            JOIN {table} n ON n.id=p.news_id
                            WHERE p.source=:press AND p.first_seen_at_utc IS NOT NULL
                            AND p.first_seen_at_utc<=:end"""
                    async for r in stream_rows(brain,sql,dict(params,press=press)):
                        ex=r.get('publication_time_evidence') or {};ex=json.loads(ex) if isinstance(ex,str) else ex
                        if ex.get('first_seen_status')!='OBSERVED_BRAIN_RECEIPT':
                            per_press[press+'_receipt_unverified']+=1;continue
                        verified,sha=text_proof(r)
                        observed=clock(ex.get('text_first_observed_at_utc'))
                        if not verified or not observed:
                            per_press[press+'_text_version_unverified']+=1;continue
                        candidates[(press,int(r['news_id']))]={**serial(r),'text_sha':sha,'text_version_verified':True,
                            'text_observed_at':observed.isoformat(),'source_day':ex.get('source_day'),
                            'nlp_artifact_sha':(artifact or {}).get('sha256')}
                for press in NEWS_TABLES:
                    ids=sorted(k[1] for k in candidates if k[0]==press)
                    for offset in range(0,len(ids),500):
                        batch=ids[offset:offset+500];args={f'id{i}':v for i,v in enumerate(batch)};args['press']=press
                        sql="SELECT press,source_id,date_dt,topic_id,quality_score,novelty,confirmation_count,topic_focus,updated_at AS nlp_updated_at FROM vlad_news_algo_events WHERE press=:press AND source_id IN ("+','.join(':id'+str(i) for i in range(len(batch)))+")"
                        async for r in stream_rows(vlad,sql,args):
                            c=candidates[(press,int(r['source_id']))]
                            # Never export title/text: NLP already exists and only
                            # its verified row version is reused.
                            c={k:v for k,v in c.items() if k not in ('title','text','publication_time_evidence')}
                            data['news'].append({**c,**serial(r)})
                diagnostics['news']=dict(per_press)
        if 'proxy' in config.groups and 'vlad_model100_binance_eurusdt_h1' in tables:
            sql="""SELECT event_time_utc,window_end_utc,open_price,close_price,base_volume_eur,
                   aggressor_buy_volume_eur,signed_trade_delta_eur,trade_count,feature_ready_at_utc,
                   brain_first_received_at_utc,source_archive_sha256 AS original_sha256,source_archive_uri AS source_url
                   FROM vlad_model100_binance_eurusdt_h1 WHERE event_time_utc<=:end ORDER BY event_time_utc"""
            async for r in stream_rows(brain,sql,params):data['proxy'].append(serial(r))
        if 'gdelt' in config.groups:
            # Aggregate at a conservative ceiling-hour of actual availability.
            # COUNT/SUM retain structural/tone fields without loading GKG text.
            for kind,table,pub,receipt,tone in [
                ('gkg','sasha_gdelt_news','available_at','loaded_at',"CAST(JSON_UNQUOTE(JSON_EXTRACT(v2_tone,'$.tone')) AS DOUBLE)"),
                ('geo','vlad_gdelt_geo_events','snapshot_at','loaded_at','tone')]:
                if table not in tables:
                    continue
                available=f'GREATEST({pub},{receipt})' if config.mode=='STRICT_ASOF' else pub
                where=f'{available}>=:start AND {available}<=:end'
                # Legacy GKG v2_tone may be a provider list, not an object.
                if kind=='gkg':tone="COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(v2_tone,'$.tone')) AS DOUBLE),CAST(JSON_UNQUOTE(JSON_EXTRACT(v2_tone,'$[0]')) AS DOUBLE))"
                conflict="SUM(category IN ('conflict','war','protest','terror'))" if kind=='geo' else '0'
                sql=f"""SELECT CEIL(UNIX_TIMESTAMP({available})/3600)*3600 AS available_epoch,
                      COUNT(*) AS count,COALESCE(SUM({tone}),0) AS tone_sum,COUNT({tone}) AS tone_count,
                      {conflict} AS conflict_count,SUM(COALESCE({tone},0)<0) AS negative_count,
                      MIN(id) AS min_id,MAX(id) AS max_id FROM {table} WHERE {where}
                      GROUP BY available_epoch ORDER BY available_epoch"""
                try:
                    buckets=[]
                    async for r in stream_rows(brain,sql,params):
                        at=datetime.fromtimestamp(int(r.pop('available_epoch')),timezone.utc)
                        if at>end:continue
                        buckets.append({**r,'available_at':at.isoformat(),'kind':kind,'row_id':f'{table}:{r["min_id"]}:{r["max_id"]}',
                                              'original_sha256':None,'evidence_kind':'DB_ROWSET_AGGREGATE_SHA','source_url':'brain://'+table})
                    data['gdelt'].extend(buckets)
                except Exception as exc:
                    diagnostics['gdelt_'+kind]={'status':'UNAVAILABLE','error_type':type(exc).__name__}
        if 'cftc' in config.groups and 'vlad_model100_cftc_tff_euro_fx' in tables:
            async for r in stream_rows(brain,"SELECT report_date,positions_as_of_date,source_available_at_utc,brain_first_received_at_utc,open_interest,net_dealer,net_asset_manager,net_leveraged_money,source_archive_sha256 AS original_sha256,source_archive_uri AS source_url FROM vlad_model100_cftc_tff_euro_fx ORDER BY report_date"):
                data['cftc'].append(serial(r))
        await brain.rollback()
        if vlad is not None:await vlad.rollback()
    data['diagnostics']=diagnostics
    return data


async def prepare_from_framework(state,start,end,config=Config(),schema=None):
    """Existing Framework engines in, unified builder out; no model hook changes."""
    try:
        from .model100_h1_features import FeatureBuilder
    except ImportError:
        from model100_h1_features import FeatureBuilder
    data=await load_snapshot(state.engine_brain,state.engine_vlad,start,end,config)
    return FeatureBuilder(data,config,schema)


def merge_snapshots(old,new):
    """Merge bounded new-hour reads, keeping unchanged observed NLP versions.

    Changed committed features are rejected later by the prefix guard, never
    silently rewritten. Macro reads are tiny and always cover the full registry.
    """
    if old.get('capture_mode')!=new.get('capture_mode'):
        raise ValueError('STRICT_RESEARCH_SNAPSHOT_MISMATCH')
    out={**old,**new}
    keys={'prices':lambda r:str(r['date']),'news':lambda r:(r['press'],r['source_id']),
          'proxy':lambda r:str(r['event_time_utc']),'gdelt':lambda r:(r['kind'],r['available_at']),
          'cftc':lambda r:str(r['report_date'])}
    for group,key in keys.items():
        rows={key(r):r for r in old.get(group,[])}
        for r in new.get(group,[]):
            if group=='gdelt' and clock(r['available_at'])<=clock(new['capture_start']):continue
            previous=rows.get(key(r))
            if group=='prices' and previous and all(previous.get(k)==r.get(k) for k in ('open','close','max','min')):
                end=clock(r['date'])+timedelta(hours=1)
                evidence=[clock(v.get('row_updated_at')) for v in (previous,r)]
                evidence=[v for v in evidence if v and v>=end]
                if evidence:r={**r,'row_updated_at':min(evidence).isoformat()}
            if group=='news' and previous:
                fields=('date_dt','published_at_utc','first_seen_at_utc','text_observed_at','text_sha','nlp_artifact_sha',
                        'topic_id','quality_score','novelty','confirmation_count','topic_focus')
                if all(previous.get(k)==r.get(k) for k in fields):
                    r={**r,'nlp_updated_at':min(str(previous.get('nlp_updated_at')),str(r.get('nlp_updated_at')))}
            rows[key(r)]=r
        out[group]=list(rows.values())
    return out
