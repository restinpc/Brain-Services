import unittest
from datetime import datetime,timezone
from shared.model100_macro import model100_leading_v1 as m
class LeadingTests(unittest.TestCase):
 def row(self,source='GDP_NOW_ATL',kind='FORECAST_VINTAGE',extra=None):
  return dict(source_id=source,record_key='a',record_kind=kind,event_type='GDP_NOWCAST',numeric_value=2.3,unit='PERCENT',http_status=200,asof_class='DAY_ONLY',release_clock_evidence='DAY_ONLY',source_published_at_utc=None,first_received_at_utc='2026-11-08 00:00:00',observation_period_start='2025-01-01',vintage_month='2025-02-01',canonical_url='https://official.example/doc',content_sha256='a'*64,extra_json=extra or {'forecast_date':'2025-02-05'})
 def test_day_only_not_before_following_day_and_not_source_clock(self):
  rows=m.prepare([self.row()]);self.assertEqual(len(rows),1)
  self.assertEqual(rows[0]['available_at'],'2025-02-06T12:00:00+00:00');self.assertIsNone(rows[0]['source_published_at'])
 def test_current_or_latest_actual_not_forecast(self):
  self.assertEqual(m.prepare([self.row(source='ALFRED',extra={'series_id':'PAYEMS','vintage_date':'2025-02-05'})]),[])
 def test_ifo_situation_excluded_expectations_retained(self):
  row=self.row(source='IFO_VINTAGE',kind='FIRST_ESTIMATE',extra={'series':'Situation','document_date':'2025-02-24'})
  row['record_key']='ifo-original-table:v2025m02:Situation:2025-02-01'
  self.assertEqual(m.prepare([row]),[])
  row['extra_json']['series']='Expectations';row['record_key']=row['record_key'].replace('Situation','Expectations')
  self.assertEqual(m.prepare([row])[0]['role'],'BUSINESS_EXPECTATIONS')
 def test_missing_vintage_never_fills_from_observation_month(self):
  self.assertEqual(m.prepare([self.row(extra={'no_forecast_day':True})]),[])
 def test_cutoff_and_target_release(self):
  rows=m.prepare([self.row()]);r=rows[0]
  self.assertFalse(m.available_before(r,'2025-02-06T11:59:59+00:00'))
  self.assertTrue(m.available_before(r,'2025-02-07T13:30:00+00:00'))
 def test_adp_is_employment_proxy_not_nfp_consensus(self):
  r=self.row(source='ALFRED',kind='FIRST_ESTIMATE',extra={'series_id':'ADPMNUSNERSA','vintage_date':'2025-02-05'})
  x=m.prepare([r])[0];self.assertEqual(x['role'],'PRIVATE_EMPLOYMENT_PROXY');self.assertFalse(x['is_consensus']);self.assertEqual(x['ml_permission'],'ML_TERMS_RECORDED_NOT_COLLECTION_BLOCKER')
 def test_public_archive_context_and_rights_are_independent(self):
  for source in ('NYFED_NOW','CLEVELAND_NOW'):
   x=m.prepare([self.row(source=source)])[0]
   self.assertEqual(x['technical_quality'],'READY_LEADING_CONTEXT')
   self.assertEqual(x['ml_permission'],'RIGHTS_UNVERIFIED')
 def test_weekly_adp_is_not_joined_to_monthly_nfp(self):
  r=m.prepare([self.row(source='ALFRED',kind='FIRST_ESTIMATE',extra={'series_id':'ADPWNUSNERSA','vintage_date':'2025-02-05'})])[0]
  self.assertEqual(m.audit([], [r]),[])
 def test_h1_context_never_admits_tomorrow_or_merges_regions(self):
  rows=m.prepare([self.row()]);r=rows[0];other={**r,'region':'OTHER','value':'9'}
  bars=[{'window_end_utc':'2025-02-06T11:00:00+00:00'},{'window_end_utc':'2025-02-06T13:00:00+00:00'}]
  out=list(m.context_h1([r,other],bars));self.assertEqual(out[0][0]['features'],{})
  self.assertEqual(len(out[1][0]['features']),2);self.assertEqual(out[-1][1]['future_leakage_violations'],0)
 def test_known_target_expires_at_actual_release(self):
  r=m.prepare([self.row()])[0];r['target_release_at']='2025-02-07T13:30:00+00:00'
  out=list(m.context_h1([r],[{'window_end_utc':'2025-02-07T14:00:00+00:00'}]))
  value=next(iter(out[0][0]['features'].values()));self.assertTrue(value['expired_target']);self.assertIsNone(value['value'])
if __name__=='__main__':unittest.main()
