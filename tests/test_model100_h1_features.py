import copy
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from model100_h1_features import Config, FeatureBuilder, write_dataset, read_dataset


def fixture():
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return {
        'prices': [dict(date=(base + timedelta(hours=i)).isoformat(),row_updated_at=(base+timedelta(hours=i+1)).isoformat(),open=1+i*.001,
                        close=1.001+i*.001, max=1.002+i*.001, min=.999+i*.001)
                   for i in range(100)],
        'macro': [dict(source='BEA_RELEASES', indicator='GDP', value='2.0',
                       available_at='2025-01-02T15:30:00Z', eligible_feature=True,
                       feature_kind='RELEASED_ACTUAL_CONTEXT', reference_period='2024-10-01',
                       temporal_status='H1_DOCUMENTED', event_identity='gdp-first',
                       original_sha256='a'*64)],
        'news': [dict(press='cnn', source_id=1, date_dt='2025-01-02T15:30:00Z',
                      published_at_utc='2025-01-02T15:30:00Z',
                      first_seen_at_utc='2025-01-02T15:45:00Z',
                      text_observed_at='2025-01-02T15:45:00Z',
                      text_sha='b'*64, nlp_updated_at='2025-01-02T15:46:00Z',
                      topic_id=2, quality_score=.7, novelty=.4, confirmation_count=2,
                      topic_focus=.5, text_version_verified=True)],
        'proxy': [], 'gdelt': [], 'cftc': [],
        'diagnostics': {}, 'nlp_fit_cutoff': '2024-01-01T00:00:00Z',
    }


class H1Tests(unittest.TestCase):
    def test_future_news_macro_and_open_bar(self):
        b = FeatureBuilder(fixture())
        row = b.build_at('2025-01-02T15:00:00Z')
        self.assertEqual(row['features']['news.cnn.count_1h'], 0)
        self.assertIsNone(row['features']['macro.BEA_RELEASES.GDP.value'])
        self.assertEqual(row['provenance']['price']['bar_open'], '2025-01-02T14:00:00+00:00')
        self.assertEqual(b.build_at('2025-01-02T16:00:00Z')['features']['news.cnn.count_1h'], 1)

    def test_revision_does_not_rewrite_past(self):
        a = fixture(); before = FeatureBuilder(a).build_at('2025-01-02T16:00:00Z')
        a['macro'].append(dict(a['macro'][0], value='9', edition='REVISION',
                               available_at='2025-01-03T12:00:00Z'))
        self.assertEqual(before, FeatureBuilder(a).build_at('2025-01-02T16:00:00Z'))

    def test_repeat_deterministic_and_caller_mutation_isolated(self):
        b = FeatureBuilder(fixture()); first = b.build_at('2025-01-02T16:00:00Z')
        first['features']['price.close'] = 999
        self.assertNotEqual(first, b.build_at('2025-01-02T16:00:00Z'))
        self.assertEqual(b.build_at('2025-01-02T16:00:00Z'), b.build_at('2025-01-02T16:00:00Z'))

    def test_late_text_and_research_are_separate(self):
        x = fixture(); x['news'][0]['text_observed_at'] = '2026-01-01T00:00:00Z'
        strict = FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        research = FeatureBuilder(x, Config(mode='RESEARCH')).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(strict['features']['news.cnn.count_1h'], 0)
        self.assertEqual(research['features']['news.cnn.count_1h'], 1)
        self.assertEqual(research['features']['news.cnn.uncertain_1h'], 1)

    def test_missing_and_disabled_groups(self):
        x = fixture(); x['news'] = []
        row = FeatureBuilder(x, Config(groups=('price',))).build_at('2025-01-02T16:00:00Z')
        self.assertIsNone(row['features']['proxy.volume_eur'])
        self.assertEqual(row['features']['news.disabled'], 1)
        self.assertEqual(row['features']['macro.disabled'], 1)

    def test_incremental_full_and_input_invalidation(self):
        b = FeatureBuilder(fixture())
        hours = b.cutoffs('2025-01-02T00:00:00Z', '2025-01-03T00:00:00Z')
        with tempfile.TemporaryDirectory() as full, tempfile.TemporaryDirectory() as inc:
            write_dataset(b, hours, full, chunk_size=5)
            write_dataset(b, hours[:12], inc, chunk_size=5)
            write_dataset(b, hours, inc, chunk_size=5)
            self.assertEqual(list(read_dataset(full)), list(read_dataset(inc)))
            changed = fixture(); changed['prices'][2]['close'] += .0001
            with self.assertRaisesRegex(ValueError, 'INPUT_PREFIX_CHANGED'):
                write_dataset(FeatureBuilder(changed), hours, inc)

    def test_future_prefix_append_is_incremental(self):
        x = fixture(); b = FeatureBuilder(x)
        with tempfile.TemporaryDirectory() as d:
            write_dataset(b, b.cutoffs('2025-01-02T00:00:00Z', '2025-01-02T20:00:00Z'), d)
            x['news'].append(dict(x['news'][0], source_id=2, date_dt='2025-01-03T15:00:00Z',
                                  first_seen_at_utc='2025-01-03T15:01:00Z',
                                  text_observed_at='2025-01-03T15:01:00Z', nlp_updated_at='2025-01-03T15:02:00Z'))
            write_dataset(FeatureBuilder(x), b.cutoffs('2025-01-02T00:00:00Z', '2025-01-03T20:00:00Z'), d)

    def test_forecast_expiry_and_no_t1(self):
        x = fixture(); x['macro'][0].update(feature_kind='FORECAST_OF_TARGET', valid_until_audit='2025-01-02T16:00:00Z')
        x['prices'][0]['t1'] = 999999
        row = FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        self.assertIsNone(row['features']['macro.BEA_RELEASES.GDP.value'])
        self.assertFalse(any('t1' in k for k in row['features']))

    def test_day_only_next_day_noon_and_dst_offsets(self):
        x=fixture();x['macro'][0].update(available_at='2025-01-03T12:00:00Z',temporal_status='H1_CONSERVATIVE_DAY_ONLY')
        b=FeatureBuilder(x)
        self.assertIsNone(b.build_at('2025-01-03T11:00:00Z')['features']['macro.BEA_RELEASES.GDP.value'])
        self.assertEqual(b.build_at('2025-01-03T13:00:00+01:00')['features']['macro.BEA_RELEASES.GDP.value'],2.)
        self.assertEqual(b.build_at('2025-01-03T07:00:00-05:00'),b.build_at('2025-01-03T12:00:00Z'))

    def test_late_old_actual_cannot_replace_new_reference_period(self):
        x=fixture();x['macro'][0]['reference_period']='2024-10-01'
        x['macro'].append(dict(x['macro'][0],reference_period='2024-07-01',value='7',available_at='2025-01-02T15:40:00Z',event_identity='old'))
        self.assertEqual(FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')['features']['macro.BEA_RELEASES.GDP.value'],2.)

    def test_nlp_before_text_version_is_not_evidence(self):
        x=fixture();x['news'][0]['nlp_updated_at']='2025-01-01T00:00:00Z'
        b=FeatureBuilder(x)
        self.assertEqual(b.build_at('2025-01-02T16:00:00Z')['features']['news.cnn.count_1h'],0)
        self.assertEqual(b.diagnostics['nlp_predates_verified_text_cnn'],1)

    def test_unknown_publication_with_proven_receipt_is_not_backdated(self):
        x=fixture();x['news'][0]['published_at_utc']=None
        row=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(row['features']['news.cnn.count_1h'],1)
        self.assertEqual(row['features']['news.cnn.uncertain_1h'],1)

    def test_proxy_receipt_and_cftc_actual_release_gate(self):
        x=fixture();x['proxy']=[dict(event_time_utc='2025-01-02T14:00:00Z',window_end_utc='2025-01-02T15:00:00Z',
                    feature_ready_at_utc='2025-01-02T15:00:01Z',brain_first_received_at_utc='2025-01-02T16:00:01Z',
                    open_price=1,close_price=1.1,base_volume_eur=10)]
        x['cftc']=[dict(report_date='2025-01-01',expected_release_at_utc='2025-01-01T20:30:00Z',
                        source_available_at_utc=None,brain_first_received_at_utc='2025-01-02T14:00:00Z')]
        b=FeatureBuilder(x,Config(groups=('price','proxy','cftc')))
        self.assertEqual(b.build_at('2025-01-02T16:00:00Z')['features']['proxy.missing'],1)
        self.assertEqual(b.build_at('2025-01-02T17:00:00Z')['features']['proxy.missing'],0)
        self.assertEqual(b.build_at('2025-01-02T17:00:00Z')['features']['cftc.missing'],1)

    def test_gdelt_future_bucket_is_not_included(self):
        x=fixture();x['gdelt']=[dict(kind='geo',available_at='2025-01-02T16:00:00Z',count=2,tone_sum=-4,tone_count=2,conflict_count=1,negative_count=2)]
        b=FeatureBuilder(x)
        self.assertEqual(b.build_at('2025-01-02T15:00:00Z')['features']['gdelt.geo.count_1h'],0)
        self.assertEqual(b.build_at('2025-01-02T16:00:00Z')['features']['gdelt.geo.count_1h'],2)

    def test_schema_locked_and_unsupported_source_not_admitted(self):
        x=fixture();b=FeatureBuilder(x);x['macro'].append(dict(x['macro'][0],indicator='NEW_INDICATOR'))
        with self.assertRaisesRegex(ValueError,'NEW_MACRO_SCHEMA_REQUIRED'):FeatureBuilder(x,schema=b.schema)
        x=fixture();x['macro'].append(dict(x['macro'][0],source='NYFED_SME',indicator='forbidden'))
        self.assertFalse(any('NYFED_SME' in k for k in FeatureBuilder(x).schema['macro_keys']))

    def test_framework_ram_adapter_and_missing_table(self):
        from types import SimpleNamespace
        x=fixture();state=SimpleNamespace(global_rates={'sasha_rates_eur_usd':x['prices']})
        self.assertEqual(FeatureBuilder.from_framework_state(state,x).build_at('2025-01-02T16:00:00Z'),FeatureBuilder(x).build_at('2025-01-02T16:00:00Z'))
        with self.assertRaisesRegex(ValueError,'FRAMEWORK_YAHOO_SERIES_REQUIRED'):
            FeatureBuilder.from_framework_state(SimpleNamespace(global_rates={'brain_rates_eur_usd':[]}),x)

    def test_orphan_shard_not_admitted_and_corruption_detected(self):
        from pathlib import Path
        b=FeatureBuilder(fixture())
        with tempfile.TemporaryDirectory() as d:
            write_dataset(b,['2025-01-02T16:00:00Z'],d)
            (Path(d)/'rows-orphan.jsonl.gz').write_bytes(b'not committed')
            self.assertEqual(len(list(read_dataset(d))),1)
            (Path(d)/'rows-00000000.jsonl.gz').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError,'SHARD_SHA_MISMATCH'):list(read_dataset(d))

    def test_regions_are_independent_features(self):
        x=fixture();x['macro'][0]['region']='US'
        x['macro'].append(dict(x['macro'][0],region='DE',value='7',event_identity='gdp-DE'))
        row=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(row['features']['macro.BEA_RELEASES.GDP.US.value'],2.)
        self.assertEqual(row['features']['macro.BEA_RELEASES.GDP.DE.value'],7.)

    def test_future_actual_calendar_does_not_extend_old_forecast(self):
        x=fixture();r=x['macro'][0]
        r.update(source='GDP_NOW_ATL',role='GDP_FORECAST',feature_kind='FORECAST_OF_TARGET',
                 reference_period='2024-10-01',valid_until_audit='2025-01-01T00:00:00Z',eligible_feature=False)
        before=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        r.update(valid_until_audit='2025-01-30T13:30:00Z',eligible_feature=True,target_clock_status='VERIFIED_ORIGINAL_RELEASE')
        after=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(before,after)

    def test_adp_is_actual_proxy_not_future_nfp_consensus(self):
        x=fixture();r=x['macro'][0]
        r.update(source='ALFRED',indicator='ADPMNUSNERSA',role='PRIVATE_EMPLOYMENT_PROXY',
                 feature_kind='FORECAST_OF_TARGET',reference_period='2024-12-01',
                 valid_until_audit='2025-01-02T15:00:00Z',eligible_feature=False)
        row=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(row['features']['macro.ALFRED.ADPMNUSNERSA.value'],2.)

    def test_masks_vector_and_utc_hour_validation(self):
        b=FeatureBuilder(fixture());r=b.build_at('2025-01-02T16:00:00Z')
        self.assertEqual(len(b.vectorize(r)),2*len(r['features']))
        self.assertEqual(r['missing_masks']['proxy.volume_eur'],1)
        with self.assertRaisesRegex(ValueError,'CUTOFF_MUST_BE_UTC_HOUR'):b.build_at('2025-01-02T15:30:00Z')

    def test_bar_clock_closed_but_database_version_still_partial(self):
        x=fixture()
        for r in x['prices']:
            if r['date']=='2025-01-02T15:00:00+00:00':r['row_updated_at']='2025-01-02T15:40:00Z'
        b=FeatureBuilder(x);row=b.build_at('2025-01-02T16:00:00Z')
        self.assertEqual(row['provenance']['price']['bar_open'],'2025-01-02T14:00:00+00:00')
        self.assertEqual(row['features']['price.age_hours'],1)
        self.assertIn('2025-01-02T16:00:00+00:00',b.cutoffs('2025-01-02T15:00:00Z','2025-01-02T16:00:00Z'))

    def test_final_price_version_received_later_not_in_strict_past(self):
        x=fixture()
        for r in x['prices']:
            if r['date']=='2025-01-02T15:00:00+00:00':r['row_updated_at']='2025-01-02T16:40:00Z'
        strict=FeatureBuilder(x).build_at('2025-01-02T16:00:00Z')
        research=FeatureBuilder(x,Config(mode='RESEARCH')).build_at('2025-01-02T16:00:00Z')
        self.assertEqual(strict['provenance']['price']['bar_open'],'2025-01-02T14:00:00+00:00')
        self.assertEqual(research['provenance']['price']['bar_open'],'2025-01-02T15:00:00+00:00')


if __name__ == '__main__':
    unittest.main()
