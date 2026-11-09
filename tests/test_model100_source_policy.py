import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from shared.model100_macro import model100_source_policy as p,model100_feature_contract as b
import test_model100_leading_v1 as fixtures

class PolicyTests(unittest.TestCase):
 def policy(self):return p.load_policy(Path(__file__).resolve().parents[1]/'shared/model100_macro/model100_v1_sources.json')
 def raw(self):return fixtures.LeadingTests().row()
 def test_allowlist_exact_11_and_reject_bad_config(self):
  policy=self.policy();self.assertEqual(len(policy['allowed_sources']),11)
  with tempfile.TemporaryDirectory() as d:
   f=Path(d)/'bad.json';f.write_text(json.dumps({**policy,'allowed_sources':['FED_FOMC','FED_FOMC']}))
   with self.assertRaises(ValueError):p.load_policy(f)
 def test_filter_fail_closed_for_unknown_and_excluded(self):
  rs=[{'source_id':s} for s in ['FED_FOMC','ECB_SPF','NYFED_SME','UNKNOWN']]
  self.assertEqual(p.filter_records(rs,self.policy()),[{'source_id':'FED_FOMC'}])
 def test_compile_and_h1_both_filter_excluded_source(self):
  raw=self.raw();raw.update(source_id='ECB_SPF',extra_json={'published_day':'2025-02-05','target_horizon':'LT'})
  rows,_=b.compile_rows([raw],self.policy());self.assertEqual(rows,[])
  compiled=b.compile_rows([self.raw()])[0][0];compiled['source']='ECB_SPF'
  row,_=next(b.build_h1([compiled],[{'window_end_utc':'2025-02-07T13:00:00Z'}],self.policy()))
  self.assertEqual(row['features'],{});self.assertNotIn('ECB_SPF',row['source_missing_masks'])
 def test_actual_day_only_delay_carry_forward_and_revision_exclusion(self):
  r=self.raw();r.update(source_id='ECB_HIST',record_kind='RELEASE_VALUE',event_type='ECB_DEPOSIT_FACILITY_RATE',
    source_published_at_utc=None,asof_class='DAY_ONLY',extra_json={'document_date':'2025-02-05'})
  revision={**r,'record_kind':'REVISION','numeric_value':'99'}
  rows=b.compile_actual_rows([r,revision],self.policy());self.assertEqual(len(rows),1)
  out=list(b.build_h1(rows,[{'window_end_utc':s} for s in ['2025-02-06T11:00:00Z','2025-02-06T12:00:00Z','2025-06-01T12:00:00Z']],self.policy()))
  self.assertEqual(out[0][0]['features'],{})
  f=next(iter(out[-1][0]['features'].values()));self.assertFalse(f['missing']);self.assertTrue(f['stale']);self.assertIsNone(f['source_published_at'])
 def test_bea_second_estimate_and_future_period_not_backdated(self):
  r=self.raw();r.update(source_id='BEA_RELEASES',record_kind='RELEASE_VALUE',event_type='BEA_GDP_REAL_QOQ_ANNUALIZED',
    source_published_at_utc='2025-02-05T13:30:00Z',asof_class='HISTORICAL_DOCUMENTED',release_clock_evidence='DOCUMENTED_CLOCK',
    extra_json={'estimate_stage':'SECOND'})
  self.assertEqual(b.compile_actual_rows([r],self.policy()),[])
  r['extra_json']={'estimate_stage':'ADVANCE'};r['observation_period_start']='2026-01-01'
  self.assertEqual(b.compile_actual_rows([r],self.policy()),[])
 def test_subset_config_removes_mask_and_record_without_mutation(self):
  policy=self.policy();policy['allowed_sources']=['GDP_NOW_ATL'];policy['actual_sources']=[]
  r=self.raw();before=copy.deepcopy(r);rows,_=b.compile_rows([r],policy)
  self.assertEqual(r,before);self.assertTrue(all(x['source']=='GDP_NOW_ATL' for x in rows))
if __name__=='__main__':unittest.main()
