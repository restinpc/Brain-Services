"""Bounded event context built from the same admissible inputs as V1.

No database writes, model imports, training, text inference or target prices.
All temporal admission is delegated to the existing FeatureBuilder indexes.
"""
import bisect
import math
from collections import Counter, defaultdict
from datetime import timedelta, datetime
from decimal import Decimal
from .model100_h1_features import canonical, digest, clock, number, HOUR, EventIndex, FeatureBuilder

VERSION = 'model100-rich-v2full-1'
DATASET = 'model100-h1-v2full-20261109'
GROUPS = ('news', 'macro', 'price', 'gdelt', 'binance', 'cftc', 'fxcm')
MAX_BYTES = 524288


def contract():
    return dict(version=VERSION,groups=list(GROUPS),encoding='TAGGED_IEEE754_AND_DECIMAL_V1',
                max_bytes=MAX_BYTES,news_limit=64,macro_history_per_series=6,
                macro_releases_limit=64,macro_revisions_limit=32,
                price_calendar_hours=72,gdelt_limit=32,binance_limit=72,
                cftc_limit=8,fxcm_limit=24,availability='V1_INDEX_ADMISSION_UNCHANGED',
                numeric_aggregation_policy='WINDOW_MATH_FSUM_V2',
                nowcast_join_policy='EXACT_TARGET_SHA_PERIOD_PUBLICATION_CLOCK_INDICATOR_UNIT_V1')


class StableWindowIndex(EventIndex):
    """Reuse admission/proofs; remove dependence on unrelated prefix rounding."""
    def __init__(self,index):
        self.rows=index.rows;self.times=index.times;self.hash_xor=index.hash_xor
        self.topic_times=index.topic_times;self.prefix=index.prefix

    def total(self,key,lo,hi):
        return math.fsum(number(r.get(key)) or 0. for r in self.rows[lo:hi])

    def mean(self,key,lo,hi):
        values=[number(r.get(key)) for r in self.rows[lo:hi]]
        values=[v for v in values if v is not None]
        return math.fsum(values)/len(values) if values else None


class V2FeatureBuilder(FeatureBuilder):
    """V1 names/order/admission, versioned stable window aggregation for V2."""
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.news={k:StableWindowIndex(v) for k,v in self.news.items()}
        self.gdelt={k:StableWindowIndex(v) for k,v in self.gdelt.items()}
        snapshot=args[0] if args else kwargs['snapshot']
        # These are used only to measure a nowcast error AFTER its actual
        # release. They do not relax V1's conservative forward-state expiry.
        self.forecast_vintages=[]
        for r in snapshot.get('macro',[]):
            if r.get('source') not in ('GDP_NOW_ATL','NYFED_NOW','ALFRED','CLEVELAND_NOW'):continue
            at=clock(r.get('available_at'));target=clock(r.get('target_publication_at_audit'))
            if not at or not target or at>=target or not r.get('target_original_sha256'):continue
            if r.get('target_clock_status')!='VERIFIED_ORIGINAL_RELEASE' or number(r.get('value')) is None:continue
            self.forecast_vintages.append({**r,'_at':at})


def pack(value):
    """Tagged float/decimal encoding: no MySQL JSON double conversion loss."""
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('NONFINITE_RICH_NUMBER')
        return {'$f64': value.hex()}
    if isinstance(value, Decimal):
        return {'$decimal': str(value)}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        if any(str(k).startswith('$') for k in value):
            raise ValueError('RESERVED_ENCODING_KEY')
        return {str(k): pack(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [pack(v) for v in value]
    return value


def unpack(value):
    if isinstance(value, dict):
        if set(value) == {'$f64'}:
            return float.fromhex(value['$f64'])
        if set(value) == {'$decimal'}:
            return Decimal(value['$decimal'])
        return {k: unpack(v) for k, v in value.items()}
    return [unpack(v) for v in value] if isinstance(value, list) else value


def select(row, keys):
    return {k: row.get(k) for k in keys}


def age(t, r):
    return (t-r['_at']).total_seconds()/3600


def public(row, t, keys):
    out={**select(row, keys), 'available_at': row['_at'].isoformat(),
         'age_hours': age(t, row)}
    # An archive fetched years later does not expose a future ingestion clock
    # as an input at a historical cutoff. Source ids/SHA retain audit linkage.
    if out.get('first_received_at') and clock(out['first_received_at'])>t:
        out['first_received_at']=None
        out['receipt_missing_reason']='BACKFILL_RECEIPT_AFTER_CUTOFF_NOT_HISTORICAL_BRAIN_RECEIPT'
    for field in ('modified_at_utc','ingested_at_utc'):
        if out.get(field) and clock(out[field])>t:
            out[field]=None;out[field+'_missing_reason']='METADATA_VERSION_AFTER_CUTOFF'
    return out


class RichContextBuilder:
    """One builder per bounded immutable snapshot; no copy of full article text."""
    def __init__(self, base, extras=None, groups=GROUPS):
        self.base = base
        self.extras = extras or {}
        if set(groups)-set(GROUPS):
            raise ValueError('UNKNOWN_RICH_GROUP')
        self.groups = tuple(groups)
        self.news_extra = {(r['press'], int(r['source_id'])): r for r in self.extras.get('news', [])}
        self.proxy_extra={r['event_time_utc']:r for r in self.extras.get('proxy',[])}
        self.forecasts_by_target=defaultdict(list)
        for r in getattr(base,'forecast_vintages',[]):
            self.forecasts_by_target[(r['target_original_sha256'],str(r.get('reference_period'))[:10])].append(r)
        self.geo = sorted(self.extras.get('gdelt', []), key=lambda r: (r['available_at'], r['row_id']))
        self.geo_times = [clock(r['available_at']) for r in self.geo]
        # Preserve revision events separately. Never substitute them into first values.
        self.revisions = sorted((r for r in self.extras.get('macro_revisions', [])
                                 if clock(r.get('available_at'))), key=lambda r: (r['available_at'], r['event_identity']))

    def build_at(self, cutoff):
        t = clock(cutoff)
        if t.minute or t.second or t.microsecond:
            raise ValueError('CUTOFF_MUST_BE_UTC_HOUR')
        methods = {'news': self._news, 'macro': self._macro, 'price': self._price,
                   'gdelt': self._gdelt, 'binance': self._binance,
                   'cftc': self._cftc, 'fxcm': self._fxcm}
        context = {'version': VERSION, 'mode': self.base.config.mode, 'cutoff_utc': t.isoformat(),
                   'availability_contract': 'V1_INDEX_ADMISSION_UNCHANGED',
                   'groups': {k: methods[k](t) for k in self.groups}}
        encoded = pack(context)
        encoded['group_sha256']={k:digest(v) for k,v in encoded['groups'].items()}
        if len(canonical(encoded).encode()) > MAX_BYTES:
            raise ValueError('RICH_CONTEXT_SIZE_LIMIT')
        return encoded

    def _news(self, t):
        rows = []
        for idx in self.base.news.values():
            lo, hi = idx.bounds(t, 24)
            rows.extend(idx.rows[lo:hi])
        rows.sort(key=lambda r: (r['_at'], r['_id']))
        # Most recent first for selection, chronological order for serialization.
        chosen = rows[-64:]
        out = []
        keys = ('press', 'source_id', 'topic_id', 'topic_focus', 'cluster_similarity',
                'quality_score', 'novelty', 'confirmation_count', 'date_dt',
                'published_at_utc', 'first_seen_at_utc', 'text_observed_at',
                'nlp_updated_at', 'text_sha', 'nlp_artifact_sha', 'publication_precision',
                'modified_at_utc','ingested_at_utc')
        for r in chosen:
            extra = self.news_extra.get((r['press'], int(r['source_id'])), {})
            # Metadata of a later NLP row must not decorate an older snapshot.
            same_version = extra.get('nlp_updated_at') == r.get('nlp_updated_at')
            e = public(r, t, keys)
            e.update(event_id=r['_id'], event_type=extra.get('event_type') if same_version else None,
                     cluster_id=extra.get('cluster_id') if same_version else None,
                     semantic_metadata_status='MATCHED_ROW_VERSION' if same_version else 'VERSION_UNAVAILABLE',
                     independent_source_count=None, embedding=None,
                     embedding_missing_reason='NO_PERSISTED_PER_EVENT_VECTOR',
                     temporal_uncertainty=bool(r['_uncertain']))
            e['event_version_sha256']=digest(select(e,('event_id','event_type','cluster_id','topic_id','quality_score','novelty','confirmation_count','text_sha','nlp_artifact_sha','nlp_updated_at')))
            e['version_history_status']='CURRENT_PERSISTED_NLP_VERSION_ONLY'
            out.append(e)
        windows = {}
        for h in (1, 3, 6, 12, 24):
            rr = [r for r in rows if r['_at'] > t-HOUR*h]
            topics = Counter(str(r.get('topic_id')) for r in rr)
            prev = Counter()
            for idx in self.base.news.values():
                lo = bisect.bisect_right(idx.times, t-HOUR*h*2)
                hi = bisect.bisect_right(idx.times, t-HOUR*h)
                prev.update(str(r.get('topic_id')) for r in idx.rows[lo:hi])
            windows[str(h)] = {'count': len(rr), 'providers': dict(sorted(Counter(r['press'] for r in rr).items())),
                               'topics': dict(sorted(topics.items())),
                               'topic_activity_change': {k: topics[k]-prev[k] for k in sorted(topics.keys()|prev.keys())}}
        return {'events': out, 'windows_hours': windows, 'total_24h': len(rows),
                'omitted_count': max(0, len(rows)-len(out)),
                'omitted_topics': dict(sorted(Counter(str(r.get('topic_id')) for r in rows[:-64]).items())),
                'selection': 'LAST_64_AVAILABLE_AT_THEN_SOURCE_ID',
                'event_identity': 'ARTICLE_ID_NOT_PROVEN_CROSS_PROVIDER_EVENT_CLUSTER'}

    def _macro(self, t):
        series = {}; releases = []
        keys = ('source', 'indicator', 'region', 'reference_period', 'edition', 'release_version',
                'event_identity', 'value', 'unit', 'source_published_at', 'first_received_at',
                'original_sha256', 'source_url', 'feature_kind', 'role', 'is_consensus',
                'temporal_status', 'availability_policy', 'expiry_policy')
        for key, idx in sorted(self.base.macro.items()):
            hi = bisect.bisect_right(idx.times, t)
            known = idx.rows[:hi]
            active = [r for r in known if not r['_until'] or t < r['_until']]
            if not active:
                continue
            current = max(active, key=lambda r: (str(r.get('reference_period') or ''), r['_at'], r['_id']))
            previous = [r for r in known if r['_at'] < current['_at'] and r.get('reference_period') != current.get('reference_period')]
            prev = previous[-1] if previous else None
            c = public(current, t, keys)
            c.update(valid_until=current['_until'].isoformat() if current['_until'] else None,
                     previous_known_value=prev.get('value') if prev else None,
                     delta_previous=number(current['value'])-number(prev['value']) if prev else None,
                     consensus=None, actual_minus_consensus=None, actual_minus_nowcast=None,
                     surprise_missing_reason='NO_VERIFIED_PRE_RELEASE_MATCHED_CONSENSUS_OR_NOWCAST')
            c['independent_nowcast']=current['value'] if current.get('feature_kind')=='FORECAST_OF_TARGET' else None
            c['nowcast_comparisons']=self._nowcast_comparisons(current)
            if c['nowcast_comparisons']:
                chosen=max(c['nowcast_comparisons'],key=lambda r:(r['available_at'],r['source'],r['event_identity']))
                c.update(actual_minus_nowcast=chosen['actual_minus_nowcast'],nowcast_source=chosen['source'],
                         nowcast_value=chosen['value'],nowcast_available_at=chosen['available_at'],
                         surprise_missing_reason='CONSENSUS_NOT_AVAILABLE;INDEPENDENT_NOWCAST_ERROR_ONLY')
            # Previous release sequence is deliberately bounded per indicator.
            history_keys=('event_identity','reference_period','edition','release_version','value','original_sha256')
            c['history'] = [public(r, t, history_keys) for r in known[-6:]]
            series[key] = c
            releases.extend(public(r, t, keys) for r in known[max(0,hi-32):] if r['_at'] > t-HOUR*168)
        releases.sort(key=lambda r: (r['available_at'], str(r['event_identity'])))
        revisions = [r for r in self.revisions if clock(r['available_at']) <= t]
        return {'series_state': series, 'releases': releases[-64:], 'release_window_hours': 168,
                'omitted_release_count': max(0, len(releases)-64),
                'known_revisions': [public({**r,'_at':clock(r['available_at'])},t,keys) for r in revisions[-32:]],
                'omitted_revision_count': max(0,len(revisions)-32),
                'revision_policy': 'KNOWN_ONLY_SEPARATE_FROM_ORIGINAL_STATE'}

    def _nowcast_comparisons(self,actual):
        pub=clock(actual.get('source_published_at'))
        if not pub:return []
        target=(actual.get('original_sha256'),str(actual.get('reference_period'))[:10])
        candidates={};indicator=actual.get('indicator')
        for r in self.forecasts_by_target.get(target,[]):
            if r['_at']>=pub or clock(r['target_publication_at_audit'])!=pub:continue
            unit=str(r.get('unit') or '').upper();au=str(actual.get('unit') or '').upper()
            gdp=(indicator=='BEA_GDP_REAL_QOQ_ANNUALIZED' and r.get('role','').startswith('GDP_FORECAST') and
                 unit in ('PERCENT_SAAR','PERCENT_QOQ_SAAR','PERCENT_ANNUALIZED') and au=='PERCENT_ANNUALIZED')
            cpi=(indicator=={'CPI_INFLATION':'CPI_HEADLINE_MOM','CORE_CPI_INFLATION':'CPI_CORE_MOM'}.get(r.get('indicator')) and
                 unit=='PERCENT_MOM_SA' and au=='PERCENT')
            if not (gdp or cpi):continue
            key='ATLANTA_GDPNOW' if gdp and r['source'] in ('GDP_NOW_ATL','ALFRED') else r['source']
            old=candidates.get(key)
            rank=lambda x:(x['_at'],x['source']=='GDP_NOW_ATL',x['event_identity'])
            if old is None or rank(r)>rank(old):candidates[key]=r
        return [dict(source=r['source'],origin_provider=origin,event_identity=r['event_identity'],value=r['value'],
                     actual_minus_nowcast=Decimal(str(actual['value']))-Decimal(str(r['value'])),
                     available_at=r['_at'].isoformat(),original_sha256=r['original_sha256'],
                     target_original_sha256=r['target_original_sha256'],is_consensus=False,
                     unit_relation='MATCHED_TARGET_INDICATOR_MONTHLY_OR_ANNUALIZED_PERCENT')
                for origin,r in sorted(candidates.items())]

    def _price(self, t):
        rows=[]; gaps=[]
        for n in range(72, 0, -1):
            start=t-HOUR*n; r=self.base.price_by_open.get(start)
            if not r or r['_at'] > t:
                gaps.append(start.isoformat());continue
            o,h,l,c=(r[k] for k in ('open','high','low','close')); span=h-l
            prior=self.base.price_by_open.get(start-HOUR)
            pc=prior['close'] if prior and prior['_at']<=t else None
            tr=max(span,abs(h-pc),abs(l-pc)) if pc else span
            hour=start.hour
            rows.append({'bar_open_utc':start.isoformat(),'bar_close_utc':(start+HOUR).isoformat(),
                         'available_at':r['_at'].isoformat(),'open':o,'high':h,'low':l,'close':c,
                         'change':c-o,'range':span,'upper_shadow':h-max(o,c),'lower_shadow':min(o,c)-l,
                         'close_position':(c-l)/span if span else None,'true_range':tr,
                         'log_return':math.log(c/pc) if pc else None,
                         'session_utc': 'ASIA' if hour<7 else 'EUROPE' if hour<13 else 'EUROPE_US' if hour<17 else 'US' if hour<22 else 'ROLLOVER',
                         'session_policy':'FIXED_UTC_ACTIVITY_BUCKET_NOT_EXCHANGE_DST_CLOCK'})
        derived={}
        for w in (24,48,72):
            rr=[r for r in rows if clock(r['bar_open_utc'])>=t-HOUR*w]
            ret=[r['log_return'] for r in rr if r['log_return'] is not None]
            derived[str(w)]={'bars':len(rr),'calendar_hours':w,'missing_hours':w-len(rr),
                             'momentum':math.log(rr[-1]['close']/rr[0]['open']) if rr else None,
                             'realized_volatility':math.sqrt(sum(x*x for x in ret)) if ret else None,
                             'atr':sum(r['true_range'] for r in rr)/len(rr) if rr else None,
                             'high':max((r['high'] for r in rr),default=None),'low':min((r['low'] for r in rr),default=None)}
            if rr:
                move=rr[-1]['close']-rr[0]['open'];atr=derived[str(w)]['atr']
                derived[str(w)]['movement_atr_ratio']=move/atr if atr else None
                derived[str(w)]['regime']='TREND' if atr and abs(move)>3*atr else 'RANGE'
        for i,r in enumerate(rows):
            prev=[x['log_return'] for x in rows[max(0,i-24):i] if x['log_return'] is not None]
            r['preceding_realized_volatility']=math.sqrt(sum(x*x for x in prev)) if prev else None
        if rows:
            direction=1 if rows[-1]['change']>0 else -1 if rows[-1]['change']<0 else 0
            streak=0
            for i in range(len(rows)-1,-1,-1):
                rr=rows[i]
                if (1 if rr['change']>0 else -1 if rr['change']<0 else 0)!=direction:break
                if i<len(rows)-1 and clock(rows[i+1]['bar_open_utc'])-clock(rr['bar_open_utc'])!=HOUR:break
                streak+=1
            derived['directional_streak']=dict(direction=direction,closed_bars=streak)
            derived['last_return_acceleration']=(rows[-1]['log_return']-rows[-2]['log_return']) if len(rows)>1 and rows[-1]['log_return'] is not None and rows[-2]['log_return'] is not None else None
            hi=max(r['high'] for r in rows);lo=min(r['low'] for r in rows)
            derived['position_in_72h_range']=(rows[-1]['close']-lo)/(hi-lo) if hi>lo else None
        return {'provider':'YAHOO_EURUSD','h1_sequence':rows,'missing_calendar_hours':gaps,
                'windows':derived,'window_unit':'CALENDAR_HOURS','no_synthetic_candles':True}

    def _binance(self,t):
        idx=self.base.proxy;lo,hi=idx.bounds(t, 96)
        rows=[r for r in idx.rows[lo:hi] if clock(r['window_end_utc'])>t-HOUR*72]
        keys=('event_time_utc','window_end_utc','open_price','close_price','base_volume_eur',
              'aggressor_buy_volume_eur','signed_trade_delta_eur','trade_count','original_sha256','source_url',
              'feature_ready_at_utc','brain_first_received_at_utc')
        seq=[public(r,t,keys) for r in rows[-72:]]
        for item in seq:
            ex=self.proxy_extra.get(item['event_time_utc'],{})
            matched=ex.get('source_archive_sha256')==item.get('original_sha256') and bool(ex)
            for k in ('high_price','low_price','quote_volume_usdt','aggressor_sell_volume_eur',
                      'aggressor_buy_volume_usdt','aggressor_sell_volume_usdt'):
                item[k]=ex.get(k) if matched else None
            item['extra_fields_status']='MATCHED_ORIGINAL_ARCHIVE' if matched else 'NO_MATCHED_ROW_VERSION'
        derived={}
        for w in (3,6,12,24):
            rr=[r for r in rows if clock(r['window_end_utc'])>t-HOUR*w]
            volume=sum(number(r.get('base_volume_eur')) or 0 for r in rr)
            delta=sum(number(r.get('signed_trade_delta_eur')) or 0 for r in rr)
            derived[str(w)]={'bars':len(rr),'volume_eur':volume if rr else None,
                             'cumulative_delta_eur':delta if rr else None,
                             'imbalance':delta/volume if volume else None}
            older=[r for r in rows if t-HOUR*w*2<clock(r['window_end_utc'])<=t-HOUR*w]
            prev=sum(number(r.get('base_volume_eur')) or 0 for r in older)
            derived[str(w)]['activity_ratio_previous_window']=volume/prev if rr and prev else None
            derived[str(w)]['trade_count']=sum(int(r.get('trade_count') or 0) for r in rr) if rr else None
            change=(number(rr[-1]['close_price'])-number(rr[0]['open_price'])) if rr else None
            derived[str(w)]['price_delta_divergence']=bool(change*delta<0) if change is not None else None
        return {'instrument':'EURUSDT','market_type':'CRYPTO_SPOT_PROXY','h1_sequence':seq,'windows':derived,
                'missing_reason':None if rows else 'NO_RECENT_BAR_AVAILABLE_UNDER_FEATURE_READY_AND_RECEIPT_POLICY'}

    def _gdelt(self,t):
        lo=bisect.bisect_right(self.geo_times,t-HOUR*24);hi=bisect.bisect_right(self.geo_times,t)
        rows=self.geo[lo:hi]
        geo=[r for r in rows if r.get('kind')=='GEO_COUNTRY_CATEGORY_BUCKET']
        gkg=[r for r in rows if r.get('kind')=='GKG_EVENT_SAMPLE']
        gkg_totals={r['available_at']:r['hour_total'] for r in gkg}
        return {'events':rows[-32:],'omitted_count':max(0,len(rows)-32),
                'aggregates':{
                 'geo_all_countries':dict(sorted(sum((Counter(r.get('countries',{})) for r in geo),Counter()).items())),
                 'geo_all_categories':dict(sorted(sum((Counter(r.get('categories',{})) for r in geo),Counter()).items())),
                 'gkg_sample_countries':dict(sorted(sum((Counter(r.get('countries',{})) for r in gkg),Counter()).items())),
                 'gkg_sample_themes':dict(sorted(Counter(x for r in gkg for x in r.get('themes',[])).items())),
                 'gkg_all_event_count':sum(gkg_totals.values()),'gkg_sample_count':len(gkg),
                 'geo_all_event_count':sum(r['count'] for r in geo)},
                'missing_reason':None if rows else 'NO_VERIFIED_RICH_BUCKET',
                'base_aggregate_buckets':{k:len(idx.rows[slice(*idx.bounds(t,24))]) for k,idx in self.base.gdelt.items()},
                'diagnostics':self.extras.get('diagnostics',{})}

    def _cftc(self,t):
        rows=[r for r in self.extras.get('cftc',[]) if r.get('source_available_at_utc') and
              r.get('brain_first_received_at_utc') and max(clock(r['source_available_at_utc']),clock(r['brain_first_received_at_utc']))<=t]
        return {'state':rows[-1] if rows else None,'history':rows[-8:],
                'missing_reason':None if rows else 'ACTUAL_PUBLIC_RELEASE_CLOCK_UNVERIFIED'}

    def _fxcm(self,t):
        rows=[]
        for r in self.extras.get('fxcm',[]):
            clocks=[clock(r.get(k)) for k in ('window_end_utc','feature_ready_at_utc','brain_first_received_at_utc')]
            if all(clocks) and max(clocks)<=t and clocks[0]>t-HOUR*72:
                rows.append(r)
        return {'state':rows[-1] if rows else None,'history':rows[-24:],
                'source_status':'ARCHIVE_ONLY','missing_reason':None if rows else 'NO_RECENT_VERIFIED_ARCHIVE_BAR',
                'not_live_forex_spread':True}
