"""Immutable MySQL rows; SELECT interface for existing Brain AsyncEngines."""
import json
import math
from datetime import datetime,timezone
from .model100_h1_features import VERSION,canonical,digest,clock

TABLE='vlad_model100_h1_features'
SETS='vlad_model100_h1_feature_sets'


def parsed(v):
    return json.loads(v) if isinstance(v,(str,bytes)) else v


def closed_hours(hours,now):
    return [h for h in hours if clock(h)<=clock(now).replace(minute=0,second=0,microsecond=0)]


def pending_market_hours(db,version,mode,last,now):
    """Recover ascending gaps and late source arrivals without rescanning years."""
    from datetime import timedelta
    c=db.cursor()
    try:
        c.execute(f'''SELECT r.date FROM sasha_rates_eur_usd r
            WHERE r.date>=%s AND r.date<%s AND NOT EXISTS
            (SELECT 1 FROM {TABLE} f WHERE f.dataset_version=%s AND f.mode=%s
             AND f.cutoff_utc=DATE_ADD(r.date,INTERVAL 1 HOUR)) ORDER BY r.date LIMIT 168''',
            ((clock(last)-timedelta(days=7,hours=1)).replace(tzinfo=None),clock(now).replace(tzinfo=None),version,mode))
        hours=[(clock(row[0])+timedelta(hours=1)).isoformat() for row in c]
        return sorted(set(closed_hours(hours,now)))
    finally:c.close()


def source_changes(old,new):
    """Compare overlapping retained source identities; no deletion inference."""
    specs={
        'prices':(('date',),('open','close','max','min')),
        'news':(('press','source_id'),('date_dt','text_sha','nlp_artifact_sha','topic_id','quality_score','novelty','confirmation_count','topic_focus')),
        'macro':(('source','indicator','reference_period','event_identity','edition','available_at'),('value','available_at','edition','original_sha256','reference_period')),
        'proxy':(('event_time_utc',),('open_price','close_price','base_volume_eur','signed_trade_delta_eur')),
        'gdelt':(('kind','available_at'),('count','tone_sum','tone_count','negative_count','conflict_count'))}
    changes=[]
    for group,(identity,fields) in specs.items():
        rows={tuple(str(r.get(k)) for k in identity):r for r in old.get(group,[])}
        for r in new.get(group,[]):
            key=tuple(str(r.get(k)) for k in identity);before=rows.get(key)
            if before is None:continue
            a={k:before.get(k) for k in fields};b={k:r.get(k) for k in fields}
            if a!=b:changes.append(dict(group=group,identity=key,old_sha256=digest(a),new_sha256=digest(b)))
    return {'changed_overlapping_identities':len(changes),'changes_sha256':digest(changes),'examples':changes[:20]}


def encode_row(row,schema,version):
    keys=schema['feature_order']
    if row['schema_hash']!=digest(schema) or row['mode']!=schema['config']['mode']:
        raise ValueError('SCHEMA_MODE_MISMATCH')
    if set(row['features'])!=set(keys) or set(row['missing_masks'])!=set(keys):
        raise ValueError('FEATURE_ORDER_MISMATCH')
    if any('t1' in k or 'target_value' in k for k in keys):raise ValueError('TARGET_COLUMN_FORBIDDEN')
    if digest({k:v for k,v in row.items() if k!='row_hash'})!=row['row_hash']:
        raise ValueError('ROW_HASH_MISMATCH')
    vals=[row['features'][k] for k in keys];masks=[row['missing_masks'][k] for k in keys]
    if any(m not in (0,1) or m!=int(v is None) for v,m in zip(vals,masks)):
        raise ValueError('MASK_MISMATCH')
    if any(v is not None and (not isinstance(v,(int,float)) or not math.isfinite(v)) for v in vals):
        raise ValueError('INVALID_FEATURE_NUMBER')
    # MySQL JSON numeric parsing can change the final IEEE754 bit. Hex strings
    # preserve float bits, including signed zero; ints/null remain native JSON.
    encoded=[v.hex() if isinstance(v,float) else v for v in vals]
    packed={'feature_values':canonical(encoded),'missing_masks':canonical(masks),'provenance':canonical(row['provenance'])}
    return dict(packed,dataset_version=version,mode=row['mode'],cutoff_utc=clock(row['timestamp']).replace(tzinfo=None),
                schema_hash=row['schema_hash'],row_hash=row['row_hash'],builder_version=VERSION,value_encoding='IEEE754_HEX_V1',
                payload_sha256=digest({k:parsed(v) for k,v in packed.items()}))


def decode_row(record,schema):
    r=dict(record);values=parsed(r['feature_values']);masks=parsed(r['missing_masks']);proof=parsed(r['provenance'])
    if r.get('value_encoding')!='IEEE754_HEX_V1':raise ValueError('UNSUPPORTED_VALUE_ENCODING')
    keys=schema['feature_order']
    if r['schema_hash']!=digest(schema) or r['mode']!=schema['config']['mode']:
        raise ValueError('SCHEMA_MODE_MISMATCH')
    if len(values)!=len(keys) or len(masks)!=len(keys):raise ValueError('ARRAY_LENGTH_MISMATCH')
    if digest(dict(feature_values=values,missing_masks=masks,provenance=proof))!=r['payload_sha256']:
        raise ValueError('PAYLOAD_SHA_MISMATCH')
    values=[float.fromhex(v) if isinstance(v,str) else v for v in values]
    at=clock(r['cutoff_utc']).isoformat()
    row=dict(timestamp=at,date_dt=at,mode=r['mode'],schema_hash=r['schema_hash'],
             features=dict(zip(keys,values)),missing_masks=dict(zip(keys,masks)),provenance=proof)
    if digest(row)!=r['row_hash']:raise ValueError('ROW_HASH_MISMATCH')
    row['row_hash']=r['row_hash']
    return row


def insert_rows(db,rows,schema,version,batch_size=64):
    """Commit bounded batches. Existing identical keys are no-ops, never UPDATE."""
    new=seen=0;pending=0;c=db.cursor(dictionary=True)
    try:
        for row in rows:
            p=encode_row(row,schema,version)
            c.execute(f'SELECT row_hash,payload_sha256 FROM {TABLE} WHERE dataset_version=%s AND mode=%s AND cutoff_utc=%s',
                      (version,p['mode'],p['cutoff_utc']))
            old=c.fetchone()
            if old:
                if any(old[k]!=p[k] for k in ('row_hash','payload_sha256')):raise ValueError('IMMUTABLE_HISTORY_CONFLICT')
                seen+=1;continue
            cols=tuple(p)
            c.execute(f'INSERT INTO {TABLE} ('+','.join(cols)+') VALUES ('+','.join('%s' for _ in cols)+')',tuple(p[k] for k in cols))
            new+=1;pending+=1
            if pending>=batch_size:db.commit();pending=0
        db.commit()
    except Exception:
        db.rollback();raise
    finally:c.close()
    return {'inserted':new,'unchanged':seen}


async def read_features(engine,version,mode,start,end=None,limit=None):
    """Server-side async generator; caller receives reconstructed checked rows."""
    from sqlalchemy import text
    async with engine.connect() as c:
        # cutoff_utc is DATETIME by contract; JSON clocks are explicit UTC.
        # Do not change a pooled Framework connection's session timezone.
        s=(await c.execute(text(f'SELECT schema_json FROM {SETS} WHERE dataset_version=:v AND mode=:m'),dict(v=version,m=mode))).scalar_one()
        schema=parsed(s)
        sql=f'SELECT * FROM {TABLE} WHERE dataset_version=:v AND mode=:m AND cutoff_utc>=:a AND cutoff_utc<=:b ORDER BY cutoff_utc'
        if limit is not None:
            if not isinstance(limit,int) or limit<1:raise ValueError('INVALID_LIMIT')
            sql+=' LIMIT '+str(limit)
        args=dict(v=version,m=mode,a=clock(start).replace(tzinfo=None),b=clock(end or start).replace(tzinfo=None))
        result=await c.stream(text(sql),args,execution_options={'yield_per':256})
        try:
            async for r in result.mappings():yield decode_row(r,schema)
        finally:await result.close()


async def read_schema(engine,version,mode):
    from sqlalchemy import text
    async with engine.connect() as c:
        return parsed((await c.execute(text(f'SELECT schema_json FROM {SETS} WHERE dataset_version=:v AND mode=:m'),dict(v=version,m=mode))).scalar_one())


def feature_vector(row,schema):
    """Reuse the tested builder vectorizer without rebuilding source inputs."""
    from types import SimpleNamespace
    from .model100_h1_features import FeatureBuilder
    return FeatureBuilder.vectorize(SimpleNamespace(schema=schema,schema_hash=digest(schema)),row)
