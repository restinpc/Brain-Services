#!/usr/bin/env python3
"""Native parser: append immutable H1 inputs; no model or source mutations."""
import asyncio
import fcntl
import gzip
import hashlib
import json
import os
import sys
from datetime import datetime,timedelta,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from shared.model100_h1_features import FeatureBuilder,Config,VERSION,canonical,clock,digest,read_dataset,_atomic
from shared.model100_feature_cli import capture
from shared.model100_feature_store import TABLE,SETS,insert_rows,parsed,closed_hours,source_changes,pending_market_hours

RUNTIME=Path('/brain/model100-featurebuilder-20261109/mysql-store')
DATASET='model100-h1-v1hex-20261109'
BASE=Path('/brain/model100-featurebuilder-20261109')
MODES={'RESEARCH':'research-release','STRICT_ASOF':'strict-release'}


def require_fence():
    p=Path('/run/lock/brain-native-parser-writers')/(hashlib.sha256(TABLE.encode()).hexdigest()+'.lock')
    if p.is_symlink() or p.stat().st_uid!=0:raise RuntimeError('UNTRUSTED_WRITER_LEASE')
    with p.open('r+') as h:
        try:fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            ancestors={os.getpid()};pid=os.getppid()
            while pid>1 and pid not in ancestors:
                ancestors.add(pid)
                try:pid=int(Path('/proc/'+str(pid)+'/stat').read_text().rpartition(') ')[2].split()[1])
                except (OSError,ValueError,IndexError):break
            st=p.stat()
            for line in Path('/proc/locks').read_text().splitlines():
                fields=line.split()
                if len(fields)<6 or fields[1]!='FLOCK':continue
                try:
                    major,minor,inode=fields[5].split(':')
                    match=(int(major,16),int(minor,16),int(inode))==(os.major(st.st_dev),os.minor(st.st_dev),st.st_ino)
                    if match and int(fields[4]) in ancestors:return
                except (ValueError,IndexError):continue
            raise RuntimeError('WRITERFENCE_OWNER_NOT_ANCESTOR')
        fcntl.flock(h,fcntl.LOCK_UN)
    raise RuntimeError('NATIVE_PHP_WRITERFENCE_REQUIRED')


def source_code_hash():
    names=['shared/model100_h1_features.py','shared/model100_feature_sources.py','shared/model100_feature_cli.py']
    return digest({n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in names})


def registry(db,mode,schema):
    c=db.cursor(dictionary=True)
    c.execute(f'SELECT * FROM {SETS} WHERE dataset_version=%s AND mode=%s',(DATASET,mode));r=c.fetchone()
    if not r:raise RuntimeError('ROOT_SCHEMA_REGISTRATION_REQUIRED')
    if r['schema_hash']!=digest(schema) or r['builder_code_sha256']!=source_code_hash():raise ValueError('EXPLICIT_NEW_DATASET_VERSION_REQUIRED')
    c.close();return r


def status(db,mode,state,extra):
    c=db.cursor(dictionary=True)
    c.execute(f'SELECT status_json FROM {SETS} WHERE dataset_version=%s AND mode=%s',(DATASET,mode));old=c.fetchone()
    previous=parsed(old['status_json']) if old and old.get('status_json') else {}
    if 'input_sha256' not in extra and previous.get('input_sha256'):extra={**extra,'input_sha256':previous['input_sha256']}
    c.execute(f'UPDATE {SETS} SET last_run_at_utc=UTC_TIMESTAMP(6),last_status=%s,status_json=%s WHERE dataset_version=%s AND mode=%s',
              (state,canonical(extra),DATASET,mode));db.commit();c.close()


def watermark(db,mode):
    c=db.cursor();c.execute(f'SELECT MAX(cutoff_utc) FROM {TABLE} WHERE dataset_version=%s AND mode=%s',(DATASET,mode));v=c.fetchone()[0];c.close();return clock(v)


def import_history(db):
    result={}
    for mode,name in MODES.items():
        p=BASE/name;schema=json.loads((p/'schema.json').read_text());manifest=json.loads((p/'manifest.json').read_text())
        registry(db,mode,schema)
        if manifest['rows']!=11443 or schema['config']['mode']!=mode or manifest['schema_hash']!=digest(schema):
            raise ValueError('HISTORICAL_MANIFEST_MISMATCH')
        snapshot=(p/manifest['input_reference']).read_bytes();raw=gzip.decompress(snapshot)
        if hashlib.sha256(raw).hexdigest()!=manifest['input_reference'].removeprefix('inputs-').removesuffix('.json.gz'):
            raise ValueError('INPUT_SNAPSHOT_SHA_MISMATCH')
        # read_dataset checks every admitted shard before yielding its rows.
        result[mode]=insert_rows(db,read_dataset(p),schema,DATASET)
        status(db,mode,'HISTORY_IMPORTED',{**result[mode],'input_sha256':hashlib.sha256(raw).hexdigest()})
    return result


def update(db):
    result={};now=datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0)
    for mode in MODES:
        try:
            c=db.cursor(dictionary=True);c.execute(f'SELECT * FROM {SETS} WHERE dataset_version=%s AND mode=%s',(DATASET,mode));reg=c.fetchone();c.close()
            if not reg:raise ValueError('SCHEMA_REGISTRY_MISSING')
            schema=parsed(reg['schema_json']);registry(db,mode,schema)
            last=watermark(db,mode)
            if last is None:raise ValueError('HISTORICAL_IMPORT_REQUIRED')
            due=pending_market_hours(db,DATASET,mode,last,now)
            if not due:
                result[mode]={'status':'SUCCESS_UNCHANGED','last_cutoff':last.isoformat(),
                              'upstream_last_close':last.isoformat(),
                              'upstream_lag_hours':(now-last).total_seconds()/3600}
                status(db,mode,'SUCCESS_UNCHANGED',result[mode]);continue
            # A finite recovery wave bounds memory/work; next native tick resumes.
            due=due[:168];end=due[-1]
            data=asyncio.run(capture(clock(due[0])-timedelta(days=4),end,Config(mode=mode)))
            b=FeatureBuilder(data,Config(mode=mode),schema)
            # Keep source versions for reproducibility; future model reads MySQL only.
            encoded=canonical(data).encode();sha=hashlib.sha256(encoded).hexdigest()
            dest=RUNTIME/'inputs'/('inputs-'+sha+'.json.gz');dest.parent.mkdir(parents=True,exist_ok=True)
            if not dest.exists():_atomic(dest,gzip.compress(encoded,mtime=0))
            old_state=parsed(reg.get('status_json')) if reg.get('status_json') else {}
            comparison={'status':'NO_PREVIOUS_CAPTURE'};old_sha=old_state.get('input_sha256')
            if old_sha:
                for old_path in (RUNTIME/'inputs'/('inputs-'+old_sha+'.json.gz'),BASE/MODES[mode]/('inputs-'+old_sha+'.json.gz')):
                    if old_path.is_file():
                        raw=gzip.decompress(old_path.read_bytes())
                        if hashlib.sha256(raw).hexdigest()!=old_sha:raise ValueError('PREVIOUS_CAPTURE_SHA_MISMATCH')
                        comparison=source_changes(json.loads(raw),data);break
            admitted=set(b.cutoffs(due[0],end))
            rows=(b.build_at(h) for h in due if h in admitted)
            counts=insert_rows(db,rows,schema,DATASET)
            skipped=[h for h in due if h not in admitted]
            outcome='PARTIAL_COVERAGE_INVALID_MARKET_BAR' if skipped else 'SUCCESS_NEW_DATA' if counts['inserted'] else 'SUCCESS_UNCHANGED'
            result[mode]={'status':outcome,**counts,'skipped_market_hours':skipped,
                          'input_sha256':sha,'last_cutoff':watermark(db,mode).isoformat(),'source_changes':comparison,'source_diagnostics':data.get('diagnostics',{})}
            status(db,mode,result[mode]['status'],result[mode])
        except Exception as exc:
            result[mode]={'status':'FAILED_RETRY_NEXT_NATIVE_TICK','error_type':type(exc).__name__,'reason':str(exc)[:160]}
            status(db,mode,result[mode]['status'],result[mode])
    return result


def main(argv=None):
    os.umask(0o007)
    if os.uname().nodename!='techno-kur-brain01.local':raise RuntimeError('B1_RUNTIME_ONLY')
    args=sys.argv[1:] if argv is None else argv
    if len(args)<6 or args[0]!=TABLE:raise ValueError('REGISTERED_TARGET_MISMATCH')
    require_fence()
    import mysql.connector
    db=mysql.connector.connect(host=args[1],port=int(args[2]),user=args[3],password=args[4],database=args[5],autocommit=False,connection_timeout=15)
    try:
        c=db.cursor();c.execute("SET time_zone='+00:00'");c.execute('SELECT @@hostname');host=c.fetchone()[0]
        if host!='techno-kur-brain01.local':raise ValueError('B1_AUTHORITATIVE_ONLY')
        c.execute("SELECT GET_LOCK('model100-h1-feature-store',0)")
        if c.fetchone()[0]!=1:raise RuntimeError('WRITER_BUSY')
        c.close()
        action=args[6] if len(args)>6 else 'normal'
        if action not in ('normal','import','v2-history'):raise ValueError('INVALID_RUN_MODE')
        from shared.model100_rich_runtime import update_v2
        if action=='v2-history':
            result=update(db)
            result.update(update_v2(db,history=True))
        elif action=='import':result=import_history(db)
        else:
            result=update(db)
            result.update(update_v2(db))
            result.update({'HISTORY_'+k:v for k,v in update_v2(db,history=True).items()})
        line={'at_utc':datetime.now(timezone.utc).isoformat(),'action':action,'dataset_version':DATASET,'modes':result}
        RUNTIME.mkdir(parents=True,exist_ok=True)
        with (RUNTIME/'runs.jsonl').open('a') as f:f.write(canonical(line)+'\n')
        print(canonical(line))
        if action in ('normal','v2-history') and any(v.get('status') not in ('SUCCESS_NEW_DATA','SUCCESS_UNCHANGED') for v in result.values()):raise SystemExit(1)
    finally:db.close()


if __name__=='__main__':main()
