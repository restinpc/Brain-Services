"""ID168 extension. V1 commits first; V2 has independent recovery/status."""
import asyncio
import gzip
import hashlib
import json
from datetime import datetime,timedelta,timezone
from pathlib import Path
from .model100_h1_features import Config,canonical,digest,clock,_atomic
from .model100_feature_store import TABLE,SETS,parsed
from .model100_feature_cli import capture
from .model100_rich_context import V2FeatureBuilder as FeatureBuilder
from .model100_rich_context import RichContextBuilder,VERSION,DATASET,GROUPS,MAX_BYTES,contract
from .model100_rich_store import insert_contexts

ROOT=Path(__file__).resolve().parents[1]
RUNTIME=Path('/brain/model100-featurebuilder-20261109/v2-runtime')
V1='model100-h1-v1hex-20261109'


def code_hash():
    names=['shared/model100_rich_context.py','shared/model100_rich_sources.py',
           'shared/model100_rich_store.py','shared/model100_rich_runtime.py',
           'shared/model100_h1_features.py','shared/model100_feature_sources.py',
           'shared/model100_feature_cli.py','shared/model100_feature_store.py',
           'parser/model100_feature_store_parser.py']
    return digest({n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in names})




async def capture_extra(data,start,end):
    from dotenv import load_dotenv
    import sys
    load_dotenv('/brain/Brain-Services/.env',override=False)
    sys.path.insert(0,str(ROOT/'shared'))
    from common import build_engines
    from .model100_rich_sources import enrich
    v,b,s=build_engines()
    try:return await enrich(b,v,data,start,end)
    finally:
        for e in (v,b,s):await e.dispose()


def retain(data,prefix):
    raw=canonical(data).encode();sha=hashlib.sha256(raw).hexdigest()
    p=RUNTIME/(prefix+'-'+sha+'.json.gz')
    if not p.exists():_atomic(p,gzip.compress(raw,mtime=0))
    return dict(path=str(p),sha256=sha)


def read_input(ref):
    p=Path(ref['path']).resolve()
    if not p.is_relative_to(RUNTIME) or not p.is_file():raise ValueError('UNTRUSTED_V2_CAPTURE')
    raw=gzip.decompress(p.read_bytes())
    if hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('V2_CAPTURE_SHA_MISMATCH')
    return json.loads(raw)


def prepared_rows(mode,schema,due):
    from .model100_feature_store import decode_row
    manifest=json.loads((RUNTIME/(mode+'-verified-manifest.json')).read_text())
    if manifest['schema_hash']!=digest(schema):raise ValueError('V2_SHARD_SCHEMA_MISMATCH')
    wanted={clock(t) for t in due};found=set()
    for shard in manifest['shards']:
        if clock(shard['last_cutoff'])<min(wanted) or clock(shard['first_cutoff'])>max(wanted):continue
        p=Path(shard['path']).resolve()
        if not p.is_relative_to(RUNTIME):raise ValueError('UNTRUSTED_SHARD_PATH')
        raw_file=p.read_bytes()
        if hashlib.sha256(raw_file).hexdigest()!=shard['file_sha256']:raise ValueError('V2_SHARD_FILE_SHA')
        raw=gzip.decompress(raw_file)
        if hashlib.sha256(raw).hexdigest()!=shard['sha256']:raise ValueError('V2_SHARD_CONTENT_SHA')
        for line in raw.splitlines():
            p=json.loads(line);t=clock(p['cutoff_utc'])
            if t not in wanted:continue
            if p['dataset_version']!=DATASET or p['mode']!=mode:raise ValueError('V2_SHARD_IDENTITY')
            rich=parsed(p['rich_context'])
            if digest(rich)!=p['rich_sha256']:raise ValueError('V2_SHARD_RICH_SHA')
            if digest(dict(row_hash=p['row_hash'],payload_sha256=p['payload_sha256'],rich_sha256=p['rich_sha256']))!=p['context_sha256']:raise ValueError('V2_SHARD_CONTEXT_SHA')
            found.add(t);yield decode_row(p,schema),rich
    if found!=wanted:raise ValueError('V2_SHARD_CUTOFF_MISSING')


def update_v2(db,history=False):
    out={};now=datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0)
    for mode in ('RESEARCH','STRICT_ASOF'):
        key='V2_'+mode
        try:
            c=db.cursor(dictionary=True)
            c.execute(f'SELECT * FROM {SETS} WHERE dataset_version=%s AND mode=%s',(DATASET,mode));reg=c.fetchone()
            if not reg:
                c.close();out[key]=dict(status='SUCCESS_UNCHANGED',detail='V2_NOT_REGISTERED');continue
            schema=parsed(reg['schema_json'])
            if reg['builder_code_sha256']!=code_hash() or schema.get('rich_contract')!=contract():raise ValueError('V2_NEW_VERSION_REQUIRED')
            lower=datetime(2025,1,1,tzinfo=timezone.utc) if history else now-timedelta(days=7)
            upper=now
            if history:
                manifest=json.loads((RUNTIME/(mode+'-verified-manifest.json')).read_text())
                upper=min(now,clock(manifest['shards'][-1]['last_cutoff']))
            c.execute(f'''SELECT v.cutoff_utc FROM {TABLE} v WHERE v.dataset_version=%s AND v.mode=%s
                         AND v.cutoff_utc>=%s
                         AND v.cutoff_utc<=%s AND NOT EXISTS(SELECT 1 FROM {TABLE} r
                         WHERE r.dataset_version=%s AND r.mode=v.mode AND r.cutoff_utc=v.cutoff_utc)
                         ORDER BY v.cutoff_utc LIMIT 256''',(V1,mode,lower.replace(tzinfo=None),upper.replace(tzinfo=None),DATASET))
            due=[clock(r['cutoff_utc']).isoformat() for r in c.fetchall()];c.close()
            if not due:
                result=dict(status='SUCCESS_UNCHANGED',inserted=0)
            elif history:
                counts=insert_contexts(db,prepared_rows(mode,schema,due),schema)
                result=dict(status='SUCCESS_NEW_DATA' if counts['inserted'] else 'SUCCESS_UNCHANGED',
                            **counts,first_cutoff=due[0],last_cutoff=due[-1],input='VERIFIED_IMMUTABLE_V2_SHARDS',batch_limit=256)
            else:
                meta=json.loads((RUNTIME/(mode+'-capture.json')).read_text())
                latest=RUNTIME/(mode+'-latest-capture.json')
                if clock(due[-1])>clock(meta['end']) and latest.is_file():
                    cached=json.loads(latest.read_text())
                    if clock(cached['end'])>=clock(due[-1]) and clock(cached.get('start')) and clock(cached['start'])<=clock(due[0]):meta=cached
                if clock(due[-1])<=clock(meta['end']):
                    data=read_input(meta['base']);extra=read_input(meta['extra'])
                else:
                    start=clock(due[0])-timedelta(days=4)
                    data=asyncio.run(capture(start,due[-1],Config(mode=mode)))
                    extra=asyncio.run(capture_extra(data,start,due[-1]))
                    meta=dict(base=retain(data,mode+'-base'),extra=retain(extra,mode+'-extra'),start=start.isoformat(),end=due[-1])
                    # Reproducibility evidence only; the original history capture
                    # remains pinned so a retry cannot silently change old rows.
                    _atomic(RUNTIME/(mode+'-latest-capture.json'),canonical(meta).encode())
                base=FeatureBuilder(data,Config(mode=mode),schema)
                rich=RichContextBuilder(base,extra)
                admitted=set(base.cutoffs(due[0],due[-1]))
                if any(t not in admitted for t in due):raise ValueError('V2_MARKET_GRID_MISMATCH')
                counts=insert_contexts(db,((base.build_at(t),rich.build_at(t)) for t in due),schema)
                result=dict(status='SUCCESS_NEW_DATA' if counts['inserted'] else 'SUCCESS_UNCHANGED',
                            **counts,first_cutoff=due[0],last_cutoff=due[-1],capture=meta,
                            rich_source_diagnostics=extra.get('diagnostics',{}),batch_limit=256)
            c=db.cursor();c.execute(f'UPDATE {SETS} SET last_run_at_utc=UTC_TIMESTAMP(6),last_status=%s,status_json=%s WHERE dataset_version=%s AND mode=%s',
                                   (result['status'],canonical(result),DATASET,mode));db.commit();c.close();out[key]=result
        except Exception as e:
            db.rollback();out[key]=dict(status='FAILED_RETRY_NEXT_NATIVE_TICK',error_type=type(e).__name__,reason=str(e)[:160])
            c=db.cursor();c.execute(f'UPDATE {SETS} SET last_run_at_utc=UTC_TIMESTAMP(6),last_status=%s,status_json=%s WHERE dataset_version=%s AND mode=%s',
                                   (out[key]['status'],canonical(out[key]),DATASET,mode));db.commit();c.close()
    return out
