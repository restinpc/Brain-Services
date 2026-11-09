"""Immutable V2 payloads in the existing table, with bounded streaming reads."""
import json
from .model100_h1_features import canonical, digest, clock
from .model100_feature_store import TABLE, SETS, encode_row, decode_row, parsed
from .model100_rich_context import VERSION, DATASET, GROUPS, MAX_BYTES, unpack

FIXED_COLUMNS='dataset_version,mode,cutoff_utc,schema_hash,feature_values,missing_masks,provenance,row_hash,payload_sha256,builder_version,value_encoding'


def encode_context(row,rich,schema,version=DATASET):
    if rich['cutoff_utc']!=row['timestamp'] or rich['mode']!=row['mode']:
        raise ValueError('RICH_CUTOFF_MODE_MISMATCH')
    if rich['version']!=VERSION or schema.get('rich_contract',{}).get('version')!=VERSION:
        raise ValueError('RICH_SCHEMA_MISMATCH')
    for k,v in rich['groups'].items():
        if rich['group_sha256'].get(k)!=digest(v):raise ValueError('RICH_GROUP_SHA_MISMATCH')
    raw=canonical(rich)
    if len(raw.encode())>MAX_BYTES:raise ValueError('RICH_CONTEXT_SIZE_LIMIT')
    p=encode_row(row,schema,version)
    p['builder_version']=VERSION
    p.update(rich_context=raw,rich_sha256=digest(rich))
    p['context_sha256']=digest(dict(row_hash=p['row_hash'],payload_sha256=p['payload_sha256'],rich_sha256=p['rich_sha256']))
    return p


def insert_contexts(db,rows,schema,version=DATASET):
    new=seen=0;c=db.cursor(dictionary=True)
    try:
        for row,rich in rows:
            p=encode_context(row,rich,schema,version)
            c.execute(f'SELECT context_sha256 FROM {TABLE} WHERE dataset_version=%s AND mode=%s AND cutoff_utc=%s',
                      (version,p['mode'],p['cutoff_utc']))
            old=c.fetchone()
            if old:
                if old['context_sha256']!=p['context_sha256']:raise ValueError('IMMUTABLE_RICH_HISTORY_CONFLICT')
                seen+=1;continue
            cols=tuple(p)
            c.execute(f'INSERT INTO {TABLE} ('+','.join(cols)+') VALUES ('+','.join('%s' for _ in cols)+')',tuple(p[k] for k in cols))
            new+=1
            if new%16==0:db.commit()
        db.commit()
    except Exception:
        db.rollback();raise
    finally:c.close()
    return dict(inserted=new,unchanged=seen)


async def get_context_range(engine,start,end,mode,version=DATASET,groups=GROUPS,numeric=True):
    """Server-side read. Selected groups are independently hash checked.

    Full reads additionally validate the complete context SHA. Fixed-only
    callers do not transfer rich_context; no filesystem snapshot dependency.
    """
    from sqlalchemy import text
    if set(groups)-set(GROUPS):raise ValueError('UNKNOWN_RICH_GROUP')
    async with engine.connect() as c:
        schema=parsed((await c.execute(text(f'SELECT schema_json FROM {SETS} WHERE dataset_version=:v AND mode=:m'),
                                             dict(v=version,m=mode))).scalar_one())
        columns=FIXED_COLUMNS if numeric else 'cutoff_utc,mode,row_hash,payload_sha256,rich_sha256,context_sha256'
        if numeric:columns+=',rich_sha256,context_sha256'
        full=set(groups)==set(GROUPS)
        if full:columns+=',rich_context'
        elif groups:
            columns+=",JSON_EXTRACT(rich_context,'$.group_sha256') AS group_sha256"
            columns+=''.join(",JSON_EXTRACT(rich_context,'$.groups."+g+"') AS rich_"+g for g in groups)
        result=await c.stream(text(f'SELECT {columns} FROM {TABLE} WHERE dataset_version=:v AND mode=:m AND cutoff_utc>=:a AND cutoff_utc<=:b ORDER BY cutoff_utc'),
                              dict(v=version,m=mode,a=clock(start).replace(tzinfo=None),b=clock(end).replace(tzinfo=None)),
                              execution_options={'yield_per':16})
        try:
            async for r in result.mappings():
                if digest(dict(row_hash=r['row_hash'],payload_sha256=r['payload_sha256'],rich_sha256=r['rich_sha256']))!=r['context_sha256']:
                    raise ValueError('CONTEXT_SHA_MISMATCH')
                out={'timestamp':clock(r['cutoff_utc']).isoformat(),'mode':r['mode'],'dataset_version':version}
                if numeric:out['numeric']=decode_row(r,schema)
                if full:
                    rich=parsed(r['rich_context'])
                    if digest(rich)!=r['rich_sha256']:raise ValueError('RICH_SHA_MISMATCH')
                    out['rich_context']=unpack(rich)
                elif groups:
                    hashes=parsed(r['group_sha256']);selected={g:parsed(r['rich_'+g]) for g in groups}
                    if any(digest(v)!=hashes[g] for g,v in selected.items()):raise ValueError('GROUP_SHA_MISMATCH')
                    out['rich_context']={'groups':unpack(selected)}
                yield out
        finally:await result.close()


async def get_context(engine,T,mode,version=DATASET,groups=GROUPS,numeric=True):
    async for r in get_context_range(engine,T,T,mode,version,groups,numeric):return r
    return None
