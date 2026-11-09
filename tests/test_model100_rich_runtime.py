import gzip,hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from shared.model100_h1_features import FeatureBuilder,Config,canonical,digest
from shared.model100_rich_context import RichContextBuilder,contract
from shared.model100_rich_store import encode_context,insert_contexts
from shared import model100_rich_runtime as runtime


class RuntimeTests(unittest.TestCase):
    def fixture(self):
        data=dict(prices=[],macro=[],news=[],proxy=[],gdelt=[])
        config=Config(mode='RESEARCH')
        schema=FeatureBuilder(data,config).schema;schema['rich_contract']=contract()
        b=FeatureBuilder(data,config,schema=schema);at='2026-01-01T15:00:00+00:00'
        row=b.build_at(at);rich=RichContextBuilder(b).build_at(at)
        return at,schema,row,rich,encode_context(row,rich,schema)

    def shard(self,p,schema,record):
        raw=(canonical(record)+'\n').encode();compressed=gzip.compress(raw,mtime=0)
        q=p/'s.gz';q.write_bytes(compressed)
        manifest=dict(schema_hash=digest(schema),shards=[dict(path=str(q),sha256=hashlib.sha256(raw).hexdigest(),file_sha256=hashlib.sha256(compressed).hexdigest(),first_cutoff=record['cutoff_utc'].isoformat(),last_cutoff=record['cutoff_utc'].isoformat())])
        (p/'RESEARCH-verified-manifest.json').write_text(canonical(manifest))
        return q

    def test_verified_shard_reconstructs(self):
        at,schema,row,rich,record=self.fixture()
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.shard(p,schema,record)
            with patch.object(runtime,'RUNTIME',p):self.assertEqual(list(runtime.prepared_rows('RESEARCH',schema,[at])),[(row,rich)])

    def test_corrupt_shard_rejected(self):
        at,schema,row,rich,record=self.fixture()
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);q=self.shard(p,schema,record);q.write_bytes(q.read_bytes()+b'bad')
            with patch.object(runtime,'RUNTIME',p),self.assertRaisesRegex(ValueError,'FILE_SHA'):list(runtime.prepared_rows('RESEARCH',schema,[at]))

    def test_schema_mismatch_rejected(self):
        at,schema,row,rich,record=self.fixture()
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);self.shard(p,schema,record);other={**schema,'tampered':1}
            with patch.object(runtime,'RUNTIME',p),self.assertRaisesRegex(ValueError,'SCHEMA'):list(runtime.prepared_rows('RESEARCH',other,[at]))

    def test_bad_group_never_committed(self):
        from unittest.mock import MagicMock
        at,schema,row,rich,record=self.fixture();rich['groups']['news']['total_24h']=100
        db=MagicMock()
        with self.assertRaises(ValueError):insert_contexts(db,[(row,rich)],schema)
        db.commit.assert_not_called();db.rollback.assert_called_once()


if __name__=='__main__':unittest.main()
