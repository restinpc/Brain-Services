import unittest, math
from datetime import datetime, timezone
from types import SimpleNamespace
from shared.model100_h1_features import FeatureBuilder,Config,canonical,digest
from shared.model100_rich_context import RichContextBuilder,pack,unpack


class RichTests(unittest.TestCase):
    def base(self,**kwargs):
        return FeatureBuilder(dict(prices=[],macro=[],news=[],proxy=[],gdelt=[],**kwargs))

    def test_lossless(self):
        from decimal import Decimal
        x={'v':[1.0351966619491577,-0.,None,0,Decimal('1.00000000001')]}
        y=unpack(pack(x));self.assertEqual(x,y)
        self.assertEqual(y['v'][1].hex(),'-0x0.0p+0')

    def test_reserved(self):
        with self.assertRaises(ValueError):pack({'$f64':'evil'})
        with self.assertRaises(ValueError):pack(float('nan'))

    def test_empty_deterministic(self):
        b=RichContextBuilder(self.base());t='2026-01-01T15:00:00Z'
        self.assertEqual(digest(b.build_at(t)),digest(b.build_at(t)))
        c=unpack(b.build_at(t));self.assertEqual(len(c['groups']['price']['missing_calendar_hours']),72)
        self.assertIsNone(c['groups']['cftc']['state'])

    def test_no_future_news(self):
        b=self.base();from shared.model100_h1_features import EventIndex,clock
        b.news={'cnn':EventIndex([dict(_at=clock('2026-01-01T15:30:00Z'),_id='cnn:1',source_id=1,press='cnn',_uncertain=True)])}
        r=RichContextBuilder(b)
        self.assertEqual(r.build_at('2026-01-01T15:00:00Z')['groups']['news']['events'],[])
        self.assertEqual(len(r.build_at('2026-01-01T16:00:00Z')['groups']['news']['events']),1)

    def test_open_price(self):
        from shared.model100_h1_features import clock
        b=self.base();b.price_by_open={clock('2026-01-01T15:00:00Z'):dict(_at=clock('2026-01-01T16:00:00Z'),open=1.,close=2.,high=2.,low=1.)}
        r=RichContextBuilder(b)
        self.assertEqual(r.build_at('2026-01-01T15:00:00Z')['groups']['price']['h1_sequence'],[])
        self.assertEqual(len(r.build_at('2026-01-01T16:00:00Z')['groups']['price']['h1_sequence']),1)

    def test_late_price_excluded(self):
        from shared.model100_h1_features import clock
        b=self.base();b.price_by_open={clock('2026-01-01T14:00:00Z'):dict(_at=clock('2026-01-01T16:00:00Z'),open=1.,close=2.,high=2.,low=1.)}
        self.assertEqual(RichContextBuilder(b).build_at('2026-01-01T15:00:00Z')['groups']['price']['h1_sequence'],[])

    def test_future_revision(self):
        r=RichContextBuilder(self.base(),{'macro_revisions':[dict(available_at='2026-01-01T16:00:00Z',event_identity='x',value=9)]})
        self.assertEqual(r.build_at('2026-01-01T15:00:00Z')['groups']['macro']['known_revisions'],[])

    def test_cftc_expected_not_actual(self):
        r=RichContextBuilder(self.base(),{'cftc':[dict(expected_release_at_utc='2025-01-01',report_date='2025-01-01')]})
        self.assertIsNone(r.build_at('2026-01-01T15:00:00Z')['groups']['cftc']['state'])

    def test_group_selection(self):
        r=RichContextBuilder(self.base(),groups=('news',))
        self.assertEqual(list(r.build_at('2026-01-01T15:00:00Z')['groups']),['news'])

    def test_unclosed_cutoff(self):
        with self.assertRaises(ValueError):RichContextBuilder(self.base()).build_at('2026-01-01T15:01:00Z')

    def news_base(self,n=2):
        from shared.model100_h1_features import EventIndex,clock
        b=self.base();b.news={'cnn':EventIndex([dict(_at=clock('2026-01-01T14:00:00Z'),_id='cnn:'+str(i),source_id=i,press='cnn',topic_id=12,_uncertain=True,nlp_updated_at='2026-01-01T14:00:00') for i in range(n)])}
        return b

    def test_news_identity_same_topic(self):
        c=RichContextBuilder(self.news_base()).build_at('2026-01-01T15:00:00Z')['groups']['news']
        self.assertEqual(len(set(r['event_id'] for r in c['events'])),2)

    def test_news_tail_count(self):
        c=RichContextBuilder(self.news_base(80)).build_at('2026-01-01T15:00:00Z')['groups']['news']
        self.assertEqual((len(c['events']),c['omitted_count'],c['total_24h']),(64,16,80))

    def test_later_semantic_row_rejected(self):
        r=RichContextBuilder(self.news_base(1),{'news':[dict(press='cnn',source_id=0,nlp_updated_at='2026-02-01',event_type='future')]})
        self.assertIsNone(r.build_at('2026-01-01T15:00:00Z')['groups']['news']['events'][0]['event_type'])

    def test_gdelt_future(self):
        r=RichContextBuilder(self.base(),{'gdelt':[dict(available_at='2026-01-01T16:00:00Z',row_id='future')]})
        self.assertEqual(r.build_at('2026-01-01T15:00:00Z')['groups']['gdelt']['events'],[])

    def test_fxcm_future(self):
        r=RichContextBuilder(self.base(),{'fxcm':[dict(window_end_utc='2026-01-01T14:00:00Z',feature_ready_at_utc='2026-01-01T16:00:00Z',brain_first_received_at_utc='2026-01-01T14:00:00Z')]})
        self.assertIsNone(r.build_at('2026-01-01T15:00:00Z')['groups']['fxcm']['state'])

    def test_full_incremental_equal(self):
        r=RichContextBuilder(self.news_base());hours=['2026-01-01T15:00:00Z','2026-01-01T16:00:00Z']
        full=[r.build_at(t) for t in hours];inc=[RichContextBuilder(self.news_base()).build_at(t) for t in hours]
        self.assertEqual(full,inc)

    def test_group_hash(self):
        r=RichContextBuilder(self.news_base()).build_at('2026-01-01T15:00:00Z')
        for k,v in r['groups'].items():self.assertEqual(digest(v),r['group_sha256'][k])

    def test_whole_context_hash(self):
        from shared.model100_rich_context import contract
        from shared.model100_rich_store import encode_context
        schema=self.base().schema;schema['rich_contract']=contract()
        b=FeatureBuilder(dict(prices=[],macro=[],news=[],proxy=[],gdelt=[]),schema=schema)
        t='2026-01-01T15:00:00Z';rich=RichContextBuilder(b).build_at(t)
        p=encode_context(b.build_at(t),rich,schema)
        self.assertEqual(p['rich_sha256'],digest(rich))
        self.assertEqual(p['context_sha256'],digest({k:p[k] for k in ('row_hash','payload_sha256','rich_sha256')}))
        rich['groups']['news']['total_24h']=99
        with self.assertRaises(ValueError):encode_context(b.build_at(t),rich,schema)

    def test_future_backfill_receipt_not_input(self):
        from shared.model100_rich_context import public,clock
        x=public(dict(_at=clock('2025-01-01'),first_received_at='2026-01-01'),clock('2025-02-01'),('first_received_at',))
        self.assertIsNone(x['first_received_at'])

    def test_window_sum_independent_of_old_prefix(self):
        from shared.model100_rich_context import StableWindowIndex
        from shared.model100_h1_features import EventIndex,clock
        old=dict(_at=clock('2020-01-01'),_id='old',v=1.e16)
        recent=dict(_at=clock('2026-01-01'),_id='new',v=.5)
        full=StableWindowIndex(EventIndex([old,recent],('v',)))
        short=StableWindowIndex(EventIndex([recent],('v',)))
        self.assertEqual(full.mean('v',1,2),short.mean('v',0,1))
        self.assertEqual(full.total('v',1,2),.5)

    def test_nowcast_error_only_from_pre_release_matched_original(self):
        from shared.model100_h1_features import clock
        b=self.base();b.forecast_vintages=[dict(source='GDP_NOW_ATL',indicator='GDP_NOWCAST',role='GDP_FORECAST',unit='PERCENT_SAAR',
            reference_period='2025-01-01',_at=clock('2025-04-29T12:00:00Z'),target_publication_at_audit='2025-04-30T12:30:00Z',
            target_original_sha256='target',original_sha256='forecast',event_identity='f1',value='2.3')]
        actual=dict(indicator='BEA_GDP_REAL_QOQ_ANNUALIZED',unit='PERCENT_ANNUALIZED',value='2.1',
                    source_published_at='2025-04-30T12:30:00Z',reference_period='2025-01-01',original_sha256='target')
        r=RichContextBuilder(b);x=r._nowcast_comparisons(actual)
        from decimal import Decimal
        self.assertEqual(x[0]['actual_minus_nowcast'],Decimal('-.2'));self.assertFalse(x[0]['is_consensus'])
        from shared.model100_h1_features import EventIndex
        b.macro={'gdp':EventIndex([{**actual,'_at':clock(actual['source_published_at']),'_until':None,'_id':'actual',
                                   'event_identity':'actual','source':'BEA_RELEASES','edition':'FIRST_ESTIMATE'}])}
        r=RichContextBuilder(b)
        self.assertEqual(r.build_at('2025-04-30T12:00:00Z')['groups']['macro']['series_state'],{})
        self.assertEqual(unpack(r.build_at('2025-04-30T13:00:00Z'))['groups']['macro']['series_state']['gdp']['actual_minus_nowcast'],Decimal('-.2'))
        b.forecast_vintages[0]['_at']=clock('2025-04-30T13:00:00Z')
        self.assertEqual(RichContextBuilder(b)._nowcast_comparisons(actual),[])
        b.forecast_vintages[0]['_at']=clock('2025-04-29T12:00:00Z');actual['original_sha256']='different_document'
        self.assertEqual(RichContextBuilder(b)._nowcast_comparisons(actual),[])


if __name__=='__main__':unittest.main()
