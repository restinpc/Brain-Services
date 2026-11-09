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

    def test_recovery_starts_from_last_progress_not_current_day(self):
        from shared.model100_h1_features import clock
        self.assertEqual(runtime.live_lower_bound('2026-01-01','2026-03-01'),clock('2026-02-22'))
        self.assertEqual(runtime.live_lower_bound('2026-01-01',None),clock('2026-01-01T01:00:00Z'))

    def test_successful_history_does_not_hide_failed_live(self):
        from unittest.mock import MagicMock
        db=MagicMock();c=db.cursor.return_value
        c.fetchone.return_value=dict(status_json=canonical(dict(bootstrap_end_utc='2026-01-01',lanes=dict(live=dict(status='FAILED_RETRY_NEXT_NATIVE_TICK')))))
        runtime.write_status(db,'RESEARCH',True,dict(status='SUCCESS_UNCHANGED',inserted=0))
        args=c.execute.call_args.args[1];state=json.loads(args[1])
        self.assertEqual(args[0],'FAILED_RETRY_NEXT_NATIVE_TICK')
        self.assertEqual(state['lanes']['history']['status'],'SUCCESS_UNCHANGED')
        self.assertEqual(state['bootstrap_end_utc'],'2026-01-01')

    def no_pending_without_files(self,history):
        from unittest.mock import MagicMock
        state=dict(bootstrap_end_utc='2026-01-01',lanes={})
        reg=dict(schema_json=canonical(dict(rich_contract=contract())),builder_code_sha256='code',status_json=canonical(state))
        cursors=[]
        for _ in range(2):
            main=MagicMock();main.fetchone.side_effect=[reg] if history else [reg,dict(last_cutoff=None)];main.fetchall.return_value=[]
            status=MagicMock();status.fetchone.return_value=dict(status_json=canonical(state));cursors.extend([main,status])
        db=MagicMock();db.cursor.side_effect=cursors
        with patch.object(runtime,'code_hash',return_value='code'),patch.object(Path,'read_text',side_effect=AssertionError('FILE_DEPENDENCY')):
            result=runtime.update_v2(db,history=history)
        self.assertTrue(all(r['status']=='SUCCESS_UNCHANGED' for r in result.values()))

    def test_live_no_pending_needs_no_snapshot(self):self.no_pending_without_files(False)

    def test_history_no_pending_needs_no_manifest(self):self.no_pending_without_files(True)

    def test_root_disabled_v2_does_not_block_v1_or_require_files(self):
        from unittest.mock import MagicMock
        db=MagicMock();db.cursor.return_value.fetchone.return_value=dict(schema_json='{}',status_json='{"enabled":false}')
        with patch.object(runtime,'code_hash',side_effect=AssertionError('CODE_NOT_NEEDED')):
            result=runtime.update_v2(db)
        self.assertTrue(all(v.get('detail')=='V2_DISABLED_BY_ROOT' for v in result.values()))
        db.commit.assert_not_called()

    def test_single_context_closes_stream_before_return(self):
        import asyncio
        from shared import model100_rich_store as store
        closed=[]
        async def rows(*args,**kwargs):
            try:yield {'ok':True}
            finally:closed.append(True)
        with patch.object(store,'get_context_range',rows):
            value=asyncio.run(store.get_context(None,'2026-01-01','RESEARCH'))
        self.assertEqual(value,{'ok':True});self.assertEqual(closed,[True])


if __name__=='__main__':unittest.main()
