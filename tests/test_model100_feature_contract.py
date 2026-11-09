import unittest
from datetime import datetime,timezone
from shared.model100_macro import model100_feature_contract as m
import test_model100_leading_v1 as fixtures
class ContractTests(unittest.TestCase):
 def raw(self):return fixtures.LeadingTests().row()
 def bar(self,t):return {'window_end_utc':t}
 def compiled(self):return m.compile_rows([self.raw()])[0][0]
 def test_no_auxiliary_or_sme_and_reconciliation(self):
  r=self.raw();a={**r,'source_id':'ALFRED','extra_json':{'series_id':'GDPNOW','vintage_date':'2025-02-05'}}
  sme={**r,'source_id':'NYFED_SME'};rows,s=m.compile_rows([r,a,sme]);self.assertEqual(len(rows),1);self.assertEqual(s['auxiliary_alfred_gdpnow_excluded'],1)
 def test_future_actual_release_clock_is_not_a_countdown_feature(self):
  r=self.compiled();r.update(eligible_feature=True,valid_until_audit='2025-02-07T13:30:00+00:00',expiry_policy='ORIGINAL_TARGET_PUBLICATION')
  out=list(m.build_h1([r],[self.bar('2025-02-06T13:00:00+00:00'),self.bar('2025-02-07T14:00:00+00:00')]))
  first=next(iter(out[0][0]['features'].values()));last=next(iter(out[1][0]['features'].values()))
  self.assertEqual(first['value'],'2.3');self.assertIsNone(first['seconds_to_target_publication']);self.assertIsNone(first['expiry_at']);self.assertTrue(last['expired']);self.assertIsNone(last['value'])
 def test_no_hourly_fake_events_and_actual_value_change_clock(self):
  r=self.compiled();r.update(eligible_feature=True,valid_until_audit='2025-03-01T00:00:00+00:00');second={**r,'event_identity':'b','available_at':'2025-02-06T14:00:00+00:00'}
  out=list(m.build_h1([r,second],[self.bar('2025-02-06T13:00:00+00:00'),self.bar('2025-02-06T14:00:00+00:00'),self.bar('2025-02-06T15:00:00+00:00')]))
  self.assertEqual(out[-1][0]['real_forecast_events_since_previous_cutoff'],[]);self.assertEqual(next(iter(out[-1][0]['features'].values()))['seconds_since_last_value_change'],10800)
 def test_revision_not_first_forecast_input(self):
  r=self.raw();r.update(source_id='ALFRED',record_kind='REVISION',extra_json={'series_id':'ADPMNUSNERSA','vintage_date':'2025-02-05'})
  rows,_=m.compile_rows([r]);self.assertFalse(rows[0]['eligible_feature']);self.assertIn('REVISION',rows[0]['exclusion_reason'])
 def test_day_only_never_admitted_before_policy(self):
  r=self.compiled();r.update(eligible_feature=True,valid_until_audit='2025-03-01T00:00:00+00:00')
  out=list(m.build_h1([r],[self.bar('2025-02-06T11:00:00+00:00')]))
  self.assertEqual(out[0][0]['features'],{})
if __name__=='__main__':unittest.main()
