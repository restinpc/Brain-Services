"""Root preparation only: pin verified sources/schema; no database mutation."""
import asyncio,gzip,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'shared')]
from shared.model100_rich_context import V2FeatureBuilder as FeatureBuilder
from shared.model100_h1_features import canonical,digest,Config
from shared.model100_feature_sources import merge_snapshots
from shared.model100_rich_runtime import RUNTIME,DATASET,VERSION,contract,code_hash,retain


async def prepare():
    from dotenv import load_dotenv
    from sqlalchemy import text
    from common import build_engines
    load_dotenv('/brain/Brain-Services/.env',override=False)
    v,b,s=build_engines();reg={};base=RUNTIME.parent
    try:
        async with b.connect() as c:
            for mode,name in [('RESEARCH','research-release'),('STRICT_ASOF','strict-release')]:
                p=base/name;m=json.loads((p/'manifest.json').read_text());raw=gzip.decompress((p/m['input_reference']).read_bytes())
                if hashlib.sha256(raw).hexdigest() not in m['input_reference']:raise ValueError('ORIGINAL_CAPTURE_SHA')
                data=json.loads(raw)
                old=(await c.execute(text("SELECT status_json FROM vlad_model100_h1_feature_sets WHERE dataset_version='model100-h1-v1hex-20261109' AND mode=:m"),dict(m=mode))).scalar_one()
                old=json.loads(old) if isinstance(old,str) else old
                sha=old.get('input_sha256');latest=base/'mysql-store/inputs'/('inputs-'+str(sha)+'.json.gz')
                if sha and latest.is_file():
                    raw=gzip.decompress(latest.read_bytes())
                    if hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('LATEST_V1_CAPTURE_SHA')
                    data=merge_snapshots(data,json.loads(raw))
                meta=json.loads((RUNTIME/(mode+'-preflight.json')).read_text())
                extra_path=RUNTIME/(mode+'-extras-'+meta['extras_sha256']+'.json.gz')
                raw=gzip.decompress(extra_path.read_bytes())
                if hashlib.sha256(raw).hexdigest()!=meta['extras_sha256']:raise ValueError('RICH_CAPTURE_SHA')
                extra=json.loads(raw)
                schema=json.loads((p/'schema.json').read_text());schema['rich_contract']=contract()
                # Verify the existing builder accepts the enriched schema.
                builder=FeatureBuilder(data,Config(mode=mode),schema)
                end=max(builder.market_cutoffs).isoformat()
                capture=dict(base=retain(data,mode+'-base'),extra=dict(path=str(extra_path),sha256=meta['extras_sha256']),end=end)
                (RUNTIME/(mode+'-capture.json')).write_text(canonical(capture))
                reg[mode]=dict(dataset_version=DATASET,schema_hash=digest(schema),schema_json=canonical(schema),
                               builder_code_sha256=code_hash(),builder_version=VERSION)
        (RUNTIME/'registration.json').write_text(canonical(reg));print(canonical({m:dict(schema_hash=r['schema_hash'],code_sha256=r['builder_code_sha256']) for m,r in reg.items()}))
    finally:
        for e in (v,b,s):await e.dispose()


if __name__=='__main__':
    if '--refresh-code-only' in sys.argv:
        reg=json.loads((RUNTIME/'registration.json').read_text())
        for mode,r in reg.items():
            if json.loads(r['schema_json'])['rich_contract']!=contract():raise ValueError('RICH_CONTRACT_CHANGED_NEW_VERSION_REQUIRED')
            r['builder_code_sha256']=code_hash()
        (RUNTIME/'registration.json').write_text(canonical(reg));print('CODE_HASH_REFRESHED_PINNED_INPUTS_UNCHANGED')
    else:asyncio.run(prepare())
