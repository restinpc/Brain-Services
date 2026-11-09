import unittest
from shared.model100_macro.model100_h1_builder import build, market_coverage


class ReplayTests(unittest.TestCase):
    def test_unknown_first_text_is_not_historical_text_even_without_modified_timestamp(self):
        for flag in ({'current_html_is_not_verified_first_publication_text':True},
                     {'original_first_text_version_verified':False}):
            doc={**self.event(),'record_kind':'ORIGINAL_DOCUMENT','numeric_value':None,'extra_json':flag}
            rows=list(build([doc],[self.bar(18)]))
            self.assertEqual(rows[0][0]['documented_events_in_hour'],[])
            self.assertEqual(rows[0][0]['last_known_document_state'],{})
            self.assertEqual(rows[0][1]['unverified_first_document_versions_unavailable'],1)

    def test_day_only_speech_has_explicit_document_mask_and_no_synthetic_event(self):
        doc={**self.event(),'event_type':'FED_SPEECH','record_kind':'ORIGINAL_DOCUMENT',
             'numeric_value':None,'source_published_at_utc':None,'asof_class':'DAY_ONLY'}
        row=list(build([doc],[self.bar(18)]))[0][0]
        self.assertTrue(row['document_event_type_missing_masks']['FED_FOMC|FED_SPEECH'])
        self.assertEqual(row['last_known_document_type_state'],{})
        self.assertEqual(row['documented_events_in_hour'],[])

    def test_changed_speech_text_cannot_replace_old_text_before_receipt(self):
        old={**self.event(published='2025-01-29 18:30:00',received='2025-01-29 18:30:01'),
             'record_kind':'ORIGINAL_DOCUMENT','numeric_value':None,'extra_json':{'speech_text_sha256':'a'*64}}
        changed={**old,'content_sha256':'b'*64,'first_received_at_utc':'2026-11-07 12:00:00',
                 'extra_json':{'speech_text_sha256':'b'*64}}
        row=list(build([old,changed],[self.bar(18)]))[0][0]
        self.assertEqual(len(row['documented_events_in_hour']),1)
        self.assertEqual(row['documented_events_in_hour'][0]['original_sha256'],old['content_sha256'])

    def test_scheduled_events_are_only_in_explicit_sensitivity_mode(self):
        schedule={**self.event(),'record_kind':'SCHEDULED_RELEASE','numeric_value':None,
                  'source_published_at_utc':None,'asof_class':'SCHEDULED',
                  'extra_json':{'scheduled_at_utc':'2025-01-29T18:30:00Z'}}
        strict=list(build([schedule],[self.bar(18)]))[0][0]
        sensitivity=list(build([schedule],[self.bar(18)],mode='SCHEDULED_SENSITIVITY'))[0][0]
        self.assertEqual(strict['scheduled_sensitivity_events'],[])
        event=sensitivity['scheduled_sensitivity_events'][0]
        self.assertFalse(event['actual_value_available'])
        self.assertFalse(event['historical_schedule_knowledge_at_cutoff_verified'])

    def test_weekend_news_is_carried_with_real_timestamp_not_republished_at_forex_open(self):
        doc={**self.event(published='2026-01-31 13:00:00'),'record_kind':'ORIGINAL_DOCUMENT','numeric_value':None}
        friday={**self.bar(19),'event_time_utc':'2026-01-30 20:00:00','window_end_utc':'2026-01-30 21:00:00'}
        sunday={**self.bar(19),'event_time_utc':'2026-02-01 22:00:00','window_end_utc':'2026-02-01 23:00:00'}
        result=list(build([doc],[friday,sunday]))[1][0]
        self.assertEqual(result['documented_events_in_hour'],[])
        carried=result['documented_events_since_previous_available_cutoff'][0]
        self.assertTrue(carried['carried_across_market_data_gap'])
        self.assertEqual(carried['source_published_at_utc'],'2026-01-31T13:00:00+00:00')
        self.assertIn('FED_FOMC',result['last_known_document_state'])

    def test_sources_without_values_have_explicit_masks(self):
        row=list(build([self.event()],[self.bar(19)]))[0][0]
        self.assertTrue(row['source_numeric_missing_masks']['ZEW'])
        self.assertFalse(row['source_numeric_missing_masks']['FED_FOMC'])
        from parser.model100_primary_sources import SOURCE_PRIORITY,SOURCE_DATA_IDS
        expected={'ECB_MID'}|{SOURCE_DATA_IDS.get(s,s) for s in SOURCE_PRIORITY}
        self.assertEqual(set(row['source_numeric_missing_masks']),expected)
        self.assertTrue(row['source_numeric_missing_masks']['NYFED_SME'])

    def test_certified_shadow_waits_for_actual_receipt_after_candle_close(self):
        row={**self.event(published='2025-01-29 18:59:59',received='2025-01-29 19:00:01'),'asof_class':'CERTIFIED_LIVE'}
        documented=list(build([row],[self.bar(18)]))
        live=list(build([row],[self.bar(18),self.bar(19)],mode='CERTIFIED_LIVE_SHADOW'))
        self.assertFalse(next(iter(documented[0][0]['features'].values()))['missing'])
        self.assertTrue(next(iter(live[0][0]['features'].values()))['missing'])
        self.assertEqual(next(iter(live[1][0]['features'].values()))['quality'],'CERTIFIED_LIVE')

    def test_historical_backfill_never_enters_certified_shadow(self):
        result=list(build([self.event()],[self.bar(19)],mode='CERTIFIED_LIVE_SHADOW'))
        self.assertTrue(next(iter(result[0][0]['features'].values()))['missing'])
        self.assertEqual(result[0][1]['strict_numeric_versions'],0)

    def test_duplicate_hour_is_rejected_and_market_gaps_are_separate_from_releases(self):
        with self.assertRaisesRegex(ValueError,'DUPLICATE_H1_EVENT_TIME'):
            list(build([],[self.bar(18),self.bar(18)]))
        audit=market_coverage([self.bar(17),self.bar(19)])
        self.assertEqual(audit['unavailable_nominal_session_hours'],1)
        self.assertTrue(audit['this_is_not_economic_release_coverage'])

    def test_later_modified_document_text_is_not_original_release_text(self):
        doc={**self.event(),'record_kind':'ORIGINAL_DOCUMENT','numeric_value':None,
             'extra_json':{'source_modified_at_utc':'2026-06-22T15:51:18Z'}}
        result=list(build([doc],[self.bar(18)]))
        self.assertEqual(result[0][0]['documented_events_in_hour'],[])
        self.assertEqual(result[0][1]['modified_document_first_versions_unavailable'],1)

    def test_original_document_is_emitted_only_in_publication_hour_not_every_hour(self):
        doc={**self.event(),'record_kind':'ORIGINAL_DOCUMENT','numeric_value':None,'canonical_url':'https://example.invalid/original'}
        result=list(build([doc],[self.bar(17),self.bar(18),self.bar(19)]))
        self.assertEqual([len(row[0]['documented_events_in_hour']) for row in result],[0,1,0])
        self.assertEqual(result[-1][1]['document_events_emitted'],1)

    def test_wrong_year_actual_cannot_enter_replay(self):
        row={**self.event(),'source_id':'BEA_RELEASES','observation_period_start':'2025-12-01'}
        result=list(build([row],[self.bar(19)]))
        self.assertTrue(next(iter(result[0][0]['features'].values()))['missing'])
        self.assertEqual(result[0][1]['excluded']['ACTUAL_OBSERVATION_AFTER_PUBLICATION'],1)

    def test_release_one_second_after_close_waits_for_next_hour(self):
        result=list(build([self.event(published='2025-01-29 19:00:01')],[self.bar(18),self.bar(19)]))
        self.assertTrue(next(iter(result[0][0]['features'].values()))['missing'])
        self.assertFalse(next(iter(result[1][0]['features'].values()))['missing'])

    def test_staleness_policy_does_not_invent_missing_release_evidence(self):
        result=list(build([self.event()],[self.bar(19)],stale_after_seconds=3599))
        feature=next(iter(result[0][0]['features'].values()))
        self.assertTrue(feature['stale'])
        self.assertFalse(feature['missing'])
        self.assertFalse(feature['stale_policy_is_calendar_gap_evidence'])

    def event(self,**kw):
        return dict(source_id='FED_FOMC',record_key='release',record_kind='RELEASE_VALUE',
            event_type='FED_FUNDS_TARGET_UPPER',region='UNITED_STATES',
            observation_period_start='2025-01-01',numeric_value=kw.get('value','4.5'),unit='PERCENT',
            http_status=200,asof_class='HISTORICAL_DOCUMENTED',source_published_at_utc=kw.get('published','2025-01-29 19:00:00'),
            first_received_at_utc=kw.get('received','2026-11-07 12:00:00'),
            content_sha256=kw.get('sha','a'*64),extra_json={})
    def bar(self,hour):
        return dict(provider='FXCM',instrument='EURUSD',market_type='SPOT_BID_ASK_H1',quality_status='VALIDATED',
                    event_time_utc=f'2025-01-29 {hour:02d}:00:00',window_end_utc=f'2025-01-29 {hour+1:02d}:00:00')
    def test_before_release_missing_at_close_release_available_and_no_retroactive_live_claim(self):
        rows=list(build([self.event()],[self.bar(17),self.bar(18),self.bar(19)]))
        key=next(iter(rows[0][0]['features']))
        self.assertTrue(rows[0][0]['features'][key]['missing'])
        self.assertEqual(rows[1][0]['features'][key]['value'],'4.5')
        self.assertEqual(rows[1][0]['features'][key]['quality'],'DOCUMENTED_REPLAY')
        self.assertEqual(rows[-1][1]['future_leakage_violations'],0)
    def test_day_only_never_gets_synthetic_hour(self):
        row=self.event(published=None)
        result=list(build([row],[self.bar(19)]))
        self.assertTrue(all(v['missing'] for v in result[0][0]['features'].values()))
        self.assertEqual(result[0][1]['strict_numeric_versions'],0)
    def test_later_changed_html_value_is_unavailable_before_receipt(self):
        late=self.event(value='9.9',sha='b'*64,received='2026-11-07 13:00:00')
        result=list(build([self.event(),late],[self.bar(19)]))
        self.assertEqual(next(iter(result[0][0]['features'].values()))['value'],'4.5')
    def test_synthetic_future_publication_does_not_leak(self):
        result=list(build([self.event(published='2025-01-30 19:00:00')],[self.bar(19)]))
        self.assertTrue(next(iter(result[0][0]['features'].values()))['missing'])
    def test_crypto_proxy_is_rejected(self):
        bar={**self.bar(19),'provider':'BINANCE','instrument':'EURUSDT'}
        with self.assertRaisesRegex(ValueError,'FXCM_EURUSD'):
            list(build([self.event()],[bar]))
