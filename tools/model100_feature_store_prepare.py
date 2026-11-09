"""Root migration input preparation; no database credentials or database writes."""
import gzip
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from parser.model100_feature_store_parser import BASE,RUNTIME,MODES,DATASET,source_code_hash
from shared.model100_h1_features import VERSION,digest,canonical,_atomic


def prepare(apply=False):
    paths=['shared/model100_h1_features.py','shared/model100_feature_sources.py','shared/model100_feature_cli.py',
           'parser/model100_semantics.py','parser/model100_primary_sources.py']
    paths += [str(p.relative_to(ROOT)) for p in (ROOT/'shared/model100_macro').iterdir() if p.suffix in ('.py','.json')]
    for name in paths:
        reference=subprocess.check_output(['git','show','8e08f594f92e9209d8e29c1f4227248eb8ca479c:'+name],cwd=ROOT)
        if (ROOT/name).read_bytes()!=reference:raise ValueError('ORIGINAL_BUILDER_CODE_DRIFT:'+name)
    registration={}
    for mode,name in MODES.items():
        p=BASE/name;schema=json.loads((p/'schema.json').read_text());m=json.loads((p/'manifest.json').read_text())
        v=json.loads((p/'validation.json').read_text())
        if m['rows']!=11443 or v['errors'] or not v['full_incremental_equal'] or m['schema_hash']!=digest(schema):
            raise ValueError('HISTORICAL_VALIDATION_FAILED')
        raw=gzip.decompress((p/m['input_reference']).read_bytes())
        if 'inputs-'+hashlib.sha256(raw).hexdigest()+'.json.gz'!=m['input_reference']:raise ValueError('INPUT_SNAPSHOT_SHA_MISMATCH')
        for shard in m['shards']:
            if hashlib.sha256((p/shard['file']).read_bytes()).hexdigest()!=shard['sha256']:raise ValueError('SHARD_SHA_MISMATCH')
        registration[mode]=dict(dataset_version=DATASET,schema_hash=m['schema_hash'],schema_json=canonical(schema),
                                builder_code_sha256=source_code_hash(),builder_version=VERSION)
    if apply:
        if os.geteuid()!=0:raise PermissionError('ROOT_REQUIRED')
        RUNTIME.mkdir(parents=True,exist_ok=True);os.chown(RUNTIME,0,48);os.chmod(RUNTIME,0o2770)
        target=RUNTIME/'schema-registration.json'
        if target.exists() and json.loads(target.read_text())!=registration:raise ValueError('EXISTING_REGISTRATION_CONFLICT')
        _atomic(target,canonical(registration).encode())
    print(canonical({'verified_code_files':len(paths),'verified_modes':list(MODES),'dataset_version':DATASET,'applied':apply}))


if __name__=='__main__':prepare('--apply' in sys.argv)
