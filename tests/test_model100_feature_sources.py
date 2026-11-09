"""Offline source adapter checks; no DB or model execution."""
import ast
import hashlib
import unittest
from pathlib import Path

from shared.model100_feature_sources import existing_nlp_contract, merge_snapshots, text_proof


class SourceTests(unittest.TestCase):
    def test_existing_nlp_contract_is_reused_without_fit(self):
        repo=Path(__file__).resolve().parents[1]
        cutoff,signature,code=existing_nlp_contract(repo)
        self.assertEqual(cutoff,'2025-01-15T00:00:00')
        self.assertEqual(len(signature),64)
        self.assertEqual(code,hashlib.sha256((repo/'80/model.py').read_bytes()).hexdigest())

    def test_source_text_proof_rejects_external_path(self):
        row={'publication_time_evidence':{'text_snapshot_path':'/etc/passwd','text_snapshot_sha256':'a'*64}}
        self.assertFalse(text_proof(row)[0])

    def test_merge_retains_unchanged_observed_nlp_version(self):
        news=dict(press='cnn',source_id=1,date_dt='2025-01-02T12:00:00',topic_id=1,
                  quality_score=.5,nlp_updated_at='2025-01-02T12:01:00')
        old={'capture_mode':'STRICT_ASOF','news':[news],'prices':[],'macro':[]}
        new={'capture_mode':'STRICT_ASOF','capture_start':'2025-01-01T00:00:00',
             'news':[dict(news,nlp_updated_at='2025-01-03T00:00:00')],'prices':[],'macro':[]}
        self.assertEqual(merge_snapshots(old,new)['news'][0]['nlp_updated_at'],news['nlp_updated_at'])
        new['news'][0]['quality_score']=.6
        self.assertEqual(merge_snapshots(old,new)['news'][0]['nlp_updated_at'],'2025-01-03T00:00:00')

    def test_no_cross_mode_merge_and_no_partial_first_bucket(self):
        old={'capture_mode':'STRICT_ASOF','gdelt':[]}
        new={'capture_mode':'RESEARCH','gdelt':[]}
        with self.assertRaisesRegex(ValueError,'STRICT_RESEARCH'):merge_snapshots(old,new)
        new={'capture_mode':'STRICT_ASOF','capture_start':'2025-01-01T00:00:00Z',
             'gdelt':[dict(kind='geo',available_at='2025-01-01T00:00:00Z',count=1),
                      dict(kind='geo',available_at='2025-01-01T01:00:00Z',count=2)]}
        self.assertEqual(len(merge_snapshots(old,new)['gdelt']),1)

    def test_unchanged_ohlc_keeps_first_observed_complete_version(self):
        row=dict(date='2025-01-02T15:00:00Z',open=1.0,close=1.1,max=1.2,min=.9,
                 row_updated_at='2025-01-02T16:40:00Z')
        old={'capture_mode':'STRICT_ASOF','prices':[row]}
        new={'capture_mode':'STRICT_ASOF','prices':[dict(row,row_updated_at='2025-01-03T00:00:00Z')]}
        self.assertEqual(merge_snapshots(old,new)['prices'][0]['row_updated_at'],'2025-01-02T16:40:00+00:00')
        old['prices'][0]=dict(row,row_updated_at='2025-01-02T15:40:00Z')
        self.assertEqual(merge_snapshots(old,new)['prices'][0]['row_updated_at'],'2025-01-03T00:00:00+00:00')

    def test_source_adapter_contains_no_database_mutations(self):
        source=(Path(__file__).resolve().parents[1]/'shared/model100_feature_sources.py').read_text()
        tree=ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node,ast.Constant) and isinstance(node.value,str):
                self.assertFalse(node.value.lstrip().upper().startswith(('INSERT ','UPDATE ','DELETE ','ALTER ','CREATE ','DROP ','REPLACE ')))
        self.assertNotIn('train_at_date(',source)
        self.assertNotIn('_fit_artifact(',source)


if __name__=='__main__':unittest.main()
