"""Deterministic read-only H1 feature engine. No model, training or DB writes.

Dates are UTC. A price date is a bar OPEN; its OHLC becomes eligible at +1h.
Research news dates are an explicit assumption, never certified Brain receipt.
"""
from __future__ import annotations

import bisect
import calendar
import copy
import gzip
import hashlib
import json
import math
import os
import statistics
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

VERSION = 'model100-h1-features-v1'
UTC = timezone.utc
HOUR = timedelta(hours=1)
PRESSES = ('cnn', 'nyt', 'wsj', 'twp', 'tgd')
GROUPS = ('price', 'macro', 'news', 'proxy', 'gdelt', 'cftc')
MACRO_SOURCES = ('FED_FOMC','ECB_HIST','BLS_RELEASES','BEA_RELEASES','EUROSTAT_FLASH',
                 'IFO_VINTAGE','GDP_NOW_ATL','ALFRED','ZEW','CLEVELAND_NOW','NYFED_NOW')


def clock(value):
    if not value:
        return None
    d = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False, default=str)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def number(value):
    try:
        f = float(value)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError, OverflowError):
        return None


def macro_key(row):
    # Distinct countries/horizons must not collapse into one ZEW/FRED column.
    return '.'.join(str(x) for x in (row['source'],row['indicator'],row.get('region'),row.get('target_horizon')) if x not in (None,''))


@dataclass(frozen=True)
class Config:
    mode: str = 'STRICT_ASOF'
    groups: tuple = GROUPS[:-1]  # CFTC is experimental and explicitly opt-in.
    news_windows: tuple = (1, 6, 24)
    topic_count: int = 40
    price_windows: tuple = (1, 3, 6, 12, 24)
    stale_price_hours: int = 2

    def __post_init__(self):
        if self.mode not in ('STRICT_ASOF', 'RESEARCH'):
            raise ValueError('INVALID_ASOF_MODE')
        if len(set(self.groups)) != len(self.groups) or not set(self.groups) <= set(GROUPS):
            raise ValueError('INVALID_GROUPS')
        if any(not isinstance(w, int) or w <= 0 for w in (*self.news_windows, *self.price_windows)):
            raise ValueError('INVALID_WINDOWS')
        if not self.news_windows or not self.price_windows or len(set(self.news_windows))!=len(self.news_windows) or len(set(self.price_windows))!=len(self.price_windows):
            raise ValueError('INVALID_WINDOWS')
        if not isinstance(self.topic_count,int) or not 1<=self.topic_count<=512:
            raise ValueError('INVALID_TOPIC_COUNT')


class EventIndex:
    """Sorted availability index and prefix sums; one shared path offline/live."""
    def __init__(self, rows, metrics=()):
        self.rows = sorted(rows, key=lambda r: (r['_at'], str(r.get('_id', '')), digest(r)))
        self.times = [r['_at'] for r in self.rows]
        # Hash source provenance once, not once per historical hour/window.
        self.hash_xor=[0]
        self.topic_times=defaultdict(list)
        for r in self.rows:
            self.hash_xor.append(self.hash_xor[-1]^int(digest(provenance(r)),16))
            if r.get('topic_id') is not None:self.topic_times[int(r['topic_id'])].append(r['_at'])
        self.prefix = {}
        for key in metrics:
            sums, counts = [0.], [0]
            for r in self.rows:
                v = number(r.get(key)); sums.append(sums[-1] + (v or 0.)); counts.append(counts[-1] + (v is not None))
            self.prefix[key] = (sums, counts)

    def bounds(self, t, hours):
        return bisect.bisect_right(self.times, t - HOUR * hours), bisect.bisect_right(self.times, t)

    def mean(self, key, lo, hi):
        sums, counts = self.prefix[key]; n = counts[hi] - counts[lo]
        return (sums[hi] - sums[lo]) / n if n else None

    def total(self, key, lo, hi):
        sums, _ = self.prefix[key]
        return sums[hi] - sums[lo]

    def latest(self, t):
        i = bisect.bisect_right(self.times, t) - 1
        return self.rows[i] if i >= 0 else None

    def range_proof(self,lo,hi):
        return {'count':hi-lo,'first_id':self.rows[lo].get('_id') if hi>lo else None,
                'last_id':self.rows[hi-1].get('_id') if hi>lo else None,
                'provenance_sha256_xor':format(self.hash_xor[hi]^self.hash_xor[lo],'064x')}


def provenance(row):
    proof={k: row[k] for k in ('source', '_id', 'source_url', 'original_sha256', 'text_sha',
                               'release_version', 'reference_period', 'evidence_kind', 'nlp_artifact_sha') if row.get(k) is not None} | {
        'available_at': row['_at'].isoformat(), 'uncertain': bool(row.get('_uncertain')),
    }
    if row.get('topic_id') is not None:
        proof['nlp_values_sha256']=digest({k:row.get(k) for k in ('topic_id','quality_score','novelty','confirmation_count','topic_focus','nlp_updated_at')})
    if 'tone_sum' in row:
        proof['aggregate_payload_sha256']=digest({k:row.get(k) for k in ('count','tone_sum','tone_count','negative_count','conflict_count','min_id','max_id')})
    return proof


class FeatureBuilder:
    def __init__(self, snapshot, config=Config(), schema=None):
        self.config = config
        data = copy.deepcopy(snapshot)
        if data.get('capture_mode',config.mode)!=config.mode:
            raise ValueError('STRICT_RESEARCH_SNAPSHOT_MISMATCH')
        self.contracts={'macro_policy_sha256':data.get('macro_policy_sha256') if 'macro' in config.groups else None,
                        'nlp_artifact_sha256':data.get('nlp_artifact',{}).get('sha256') if 'news' in config.groups else None,
                        'nlp_code_sha256':data.get('nlp_contract_code_sha256') if 'news' in config.groups else None,
                        'nlp_fit_cutoff':data.get('nlp_fit_cutoff') if 'news' in config.groups else None}
        self.diagnostics = Counter()
        self.input_diagnostics = data.get('diagnostics', {})
        self.prices = []
        self.market_cutoffs=[]
        seen = set()
        for r in sorted(data.get('prices', []), key=lambda r: clock(r['date'])):
            d = clock(r['date']); vals = [number(r.get(k)) for k in ('open', 'close', 'max', 'min')]
            if d in seen:
                raise ValueError('DUPLICATE_PRICE_HOUR')
            if d.minute or d.second or any(v is None or v <= 0 for v in vals) or vals[2] < max(vals[:2]) or vals[3] > min(vals[:2]):
                self.diagnostics['invalid_price'] += 1; continue
            seen.add(d)
            self.market_cutoffs.append(d+HOUR)
            updated=clock(r.get('row_updated_at'))
            if updated is None and config.mode=='STRICT_ASOF':
                self.diagnostics['price_version_clock_unverified']+=1;continue
            if updated is not None and updated<d+HOUR:
                self.diagnostics['price_not_refetched_after_close']+=1;continue
            available=max(d+HOUR,updated) if config.mode=='STRICT_ASOF' else d+HOUR
            self.prices.append(dict(zip(('open', 'close', 'high', 'low'), vals),
                                    _at=available, _id=d.isoformat(), source='YAHOO_EURUSD', bar_open=d.isoformat(),
                                    row_completion_checked=updated is not None))
        self.price_index = EventIndex(self.prices)
        self.price_by_open = {clock(r['bar_open']): r for r in self.prices}
        self.price_opens=[clock(r['bar_open']) for r in self.prices]
        self.price_best=[];best=None
        for r in self.price_index.rows:
            best=max((best,r),key=lambda v:clock(v['bar_open']) if v else datetime.min.replace(tzinfo=UTC))
            self.price_best.append(best)
        self.macro = defaultdict(list)
        self.known_macro_keys=set()
        for r in data.get('macro', []):
            if r.get('source') not in MACRO_SOURCES or r.get('edition') == 'REVISION':
                continue
            a = clock(r.get('available_at'))
            if a is None or number(r.get('value')) is None:
                self.diagnostics['invalid_macro'] += 1; continue
            self.known_macro_keys.add(macro_key(r))
            until=clock(r.get('valid_until_audit'));eligible=r.get('eligible_feature',False)
            if r.get('role')=='PRIVATE_EMPLOYMENT_PROXY':
                # ADP is a published private-payroll estimate, not an NFP
                # consensus forecast. Retain it as actual proxy context.
                until=a+timedelta(days=45);eligible=True
                r={**r,'feature_kind':'RELEASED_PROXY_CONTEXT','expiry_policy':'PROXY_CONTEXT_TTL_45D'}
            elif r.get('feature_kind')=='FORECAST_OF_TARGET' and r.get('reference_period'):
                d=clock(str(r['reference_period'])[:10])
                quarter=r.get('role','').startswith('GDP_FORECAST') or 'GDP' in r['indicator']
                month=d.month+2 if quarter else d.month
                bound=d.replace(month=month,day=calendar.monthrange(d.year,month)[1])+timedelta(days=1)
                until=min(until,bound) if until else bound
                eligible=a<until
                r={**r,'expiry_policy':'CONSERVATIVE_PERIOD_END_NO_FUTURE_CALENDAR'}
                if not eligible:self.diagnostics['forecast_after_conservative_period_end']+=1
            if not eligible:continue
            r={**r,'eligible_feature':True}
            key = macro_key(r)
            self.macro[key].append({**r, '_at': a, '_id': r.get('event_identity', digest(r)),
                                    '_until': until,
                                    '_uncertain': r.get('temporal_status') != 'H1_DOCUMENTED'})
        self.macro = {k: EventIndex(v) for k, v in self.macro.items()}
        self.news = {}
        nlp_cutoff = clock(data.get('nlp_fit_cutoff'))
        for press in PRESSES:
            rr = []
            for r in data.get('news', []):
                if r.get('press') != press:
                    continue
                at = clock(r.get('date_dt'))
                if self.config.mode == 'STRICT_ASOF':
                    clocks = [clock(r.get(k)) for k in ('first_seen_at_utc', 'text_observed_at', 'nlp_updated_at')]
                    if not all(clocks) or not r.get('text_version_verified') or not r.get('text_sha'):
                        self.diagnostics['news_unverified_' + press] += 1; continue
                    if clocks[2] < clocks[1]:
                        self.diagnostics['nlp_predates_verified_text_' + press] += 1; continue
                    pub = clock(r.get('published_at_utc'))
                    if pub:
                        clocks.append(pub)
                    elif r.get('publication_precision')=='DAY_ONLY' and r.get('source_day'):
                        clocks.append(clock(str(r['source_day'])[:10])+timedelta(days=1,hours=12))
                    at = max(clocks)
                if at is None:
                    continue
                if nlp_cutoff and at < nlp_cutoff:
                    self.diagnostics['news_before_frozen_fit_cutoff'] += 1; continue
                rr.append({**r, '_at': at, '_id': press + ':' + str(r['source_id']),
                           '_uncertain': self.config.mode == 'RESEARCH' or not r.get('published_at_utc'),
                           'uncertain': int(self.config.mode == 'RESEARCH' or not r.get('published_at_utc')), 'source': 'SERVICE80_NLP_' + press})
            self.news[press] = EventIndex(rr, ('quality_score', 'novelty', 'confirmation_count', 'topic_focus', 'uncertain'))
        self.proxy = self._market_index(data.get('proxy', []))
        self.gdelt = {}
        for kind in ('gkg', 'geo'):
            rr = [{**r, '_at': clock(r['available_at']), '_id': str(r.get('row_id') or digest(r)),
                   'source': 'GDELT_' + kind, '_uncertain': self.config.mode == 'RESEARCH'}
                  for r in data.get('gdelt', []) if r['kind'] == kind and clock(r.get('available_at'))]
            self.gdelt[kind] = EventIndex(rr, ('count', 'tone_sum', 'tone_count', 'conflict_count', 'negative_count'))
        cftc = []
        for r in data.get('cftc', []):
            pub = clock(r.get('source_available_at_utc')); received = clock(r.get('brain_first_received_at_utc'))
            if not pub or not received:
                self.diagnostics['cftc_actual_release_unverified'] += 1; continue
            cftc.append({**r, '_at': max(pub, received), '_id': str(r['report_date']), 'source': 'CFTC'})
        self.cftc = EventIndex(cftc)
        self.schema = schema or self._make_schema()
        if self.schema['version'] != VERSION or self.schema['config'] != json.loads(canonical(asdict(config))):
            raise ValueError('SCHEMA_CONFIG_MISMATCH')
        if self.schema['input_contracts']!=self.contracts:
            raise ValueError('INPUT_CONTRACT_CHANGED_REBUILD_REQUIRED')
        unknown = self.known_macro_keys - set(self.schema['macro_keys'])
        if unknown:
            raise ValueError('NEW_MACRO_SCHEMA_REQUIRED:' + ','.join(sorted(unknown)))
        self.schema_hash = digest(self.schema)
        self._prepare_prefix()

    def _market_index(self, rows):
        out = []
        for r in rows:
            end = clock(r.get('window_end_utc'))
            ready = clock(r.get('feature_ready_at_utc'))
            receipt = clock(r.get('brain_first_received_at_utc'))
            if end is None or ready is None or (self.config.mode == 'STRICT_ASOF' and receipt is None):
                self.diagnostics['proxy_availability_unverified'] += 1; continue
            o,c=number(r.get('open_price')),number(r.get('close_price'))
            if o is None or c is None or min(o,c)<=0:
                self.diagnostics['proxy_invalid_ohlc']+=1;continue
            out.append({**r, '_at': max(x for x in (end, ready, receipt if self.config.mode == 'STRICT_ASOF' else None) if x),
                        '_id': str(r['event_time_utc']), 'source': 'BINANCE_EURUSDT_PROXY',
                        '_uncertain': self.config.mode == 'RESEARCH'})
        return EventIndex(out)

    def _make_schema(self):
        groups = {}
        groups['price'] = ['price.' + k for k in ('open', 'close', 'high', 'low', 'range_pct', 'body_pct', 'age_hours', 'gap_hours', 'stale', 'missing')]
        groups['price'] += ['price.return_' + str(w) + 'h' for w in self.config.price_windows]
        groups['price'] += ['price.volatility_' + str(w) + 'h' for w in (6, 24, 72)]
        groups['price'] += ['price.trend_' + str(w) + 'h' for w in (6, 24)]
        groups['macro'] = ['macro.' + key + '.' + field for key in sorted(self.known_macro_keys)
                           for field in ('value', 'age_hours', 'change_age_hours', 'stale', 'uncertain', 'missing')]
        groups['news'] = ['news.' + press + '.' + field + '_' + str(w) + 'h'
                          for press in PRESSES for w in self.config.news_windows
                          for field in ('count', 'quality', 'novelty', 'confirmation', 'focus', 'uncertain', 'missing')]
        groups['news'] += ['news.topic_' + str(i) + '_24h' for i in range(self.config.topic_count)]
        groups['proxy'] = ['proxy.' + field for field in ('return', 'volume_eur', 'delta_eur', 'trade_count', 'buy_share', 'age_hours', 'stale', 'uncertain', 'missing')]
        groups['gdelt'] = ['gdelt.' + k + '.' + field + '_' + str(w) + 'h' for k in ('gkg', 'geo') for w in (1, 6, 24)
                           for field in ('count', 'tone', 'conflict_count', 'negative_count', 'missing')]
        groups['cftc'] = ['cftc.' + f for f in ('open_interest', 'net_dealer', 'net_asset_manager', 'net_leveraged_money', 'age_hours', 'missing')]
        for group in GROUPS:
            groups[group] += [group + '.disabled', group + '.missing'] if group not in ('price', 'proxy', 'cftc') else [group + '.disabled']
        return {'version': VERSION, 'config': json.loads(canonical(asdict(self.config))),
                'input_contracts':self.contracts,
                'macro_keys': sorted(self.known_macro_keys), 'features_by_group': groups,
                'feature_order': [name for g in GROUPS for name in groups[g]],
                'vector_layout': 'nullable values filled with zero, then one missing mask per value',
                'semantics': {'price': 'Yahoo closed OHLC, no t1', 'news': 'existing Service79/80 NLP; no fit',
                              'macro': 'existing 11-source first-estimate contract', 'cftc': 'actual release required'}}

    def _prepare_prefix(self):
        contributions = []
        indices = [self.price_index]
        if 'proxy' in self.config.groups:indices.append(self.proxy)
        if 'cftc' in self.config.groups:indices.append(self.cftc)
        if 'news' in self.config.groups:indices.extend(self.news.values())
        if 'gdelt' in self.config.groups:indices.extend(self.gdelt.values())
        if 'macro' in self.config.groups:indices.extend(self.macro.values())
        for index in indices:
            for r in index.rows:
                base = {k: v for k, v in r.items() if k not in ('valid_until_audit', 'target_publication_at_audit', 'target_original_url', 'target_original_sha256', 'target_clock_status', 'exclusion_reason', '_until')}
                contributions.append((r['_at'], digest(base)))
                if r.get('_until') and r['_until'].year < 9999:
                    contributions.append((r['_until'], digest({'expiry': r['_id']})))
        contributions.sort()
        self.prefix_times, self.prefix_hashes = [], []
        h = hashlib.sha256()
        for at, sha in contributions:
            h.update(at.isoformat().encode() + sha.encode()); self.prefix_times.append(at); self.prefix_hashes.append(h.hexdigest())

    def prefix_digest(self, t):
        i = bisect.bisect_right(self.prefix_times, clock(t)) - 1
        return self.prefix_hashes[i] if i >= 0 else hashlib.sha256(b'').hexdigest()

    def cutoffs(self, start, end):
        return [t.isoformat() for t in self.market_cutoffs if clock(start) <= t <= clock(end)]

    def build_at(self, cutoff):
        t = clock(cutoff)
        if t is None or t.minute or t.second or t.microsecond:
            raise ValueError('CUTOFF_MUST_BE_UTC_HOUR')
        return copy.deepcopy(self._build(t))

    @lru_cache(maxsize=256)
    def _build(self, t):
        f = {name: None for names in self.schema['features_by_group'].values() for name in names}
        p = {}
        for group in GROUPS:
            f[group + '.disabled'] = int(group not in self.config.groups)
            f[group + '.missing'] = 1
        if 'price' in self.config.groups:
            self._price(t, f, p)
        if 'macro' in self.config.groups:
            self._macro(t, f, p)
        if 'news' in self.config.groups:
            self._news(t, f, p)
        if 'proxy' in self.config.groups:
            self._proxy(t, f, p)
        if 'gdelt' in self.config.groups:
            self._gdelt(t, f, p)
        if 'cftc' in self.config.groups:
            self._cftc(t, f, p)
        row = {'timestamp': t.isoformat(), 'date_dt': t.isoformat(), 'mode': self.config.mode,
               'schema_hash': self.schema_hash, 'features': f,
               'missing_masks': {k:int(v is None) for k,v in f.items()}, 'provenance': p}
        row['row_hash'] = digest(row)
        return row

    def _price(self, t, f, p):
        available_index=bisect.bisect_right(self.price_index.times,t)-1
        if available_index<0:
            return
        r=self.price_best[available_index]
        age = (t-clock(r['bar_open'])-HOUR).total_seconds()/3600
        for k in ('open', 'close', 'high', 'low'):
            f['price.' + k] = r[k]
        f.update({'price.missing': 0, 'price.age_hours': age, 'price.stale': int(age >= self.config.stale_price_hours),
                  'price.range_pct': (r['high']-r['low'])/r['open'], 'price.body_pct': (r['close']-r['open'])/r['open']})
        start = clock(r['bar_open'])
        i = bisect.bisect_left(self.price_opens,start)
        previous=[x for x in self.prices[max(0,i-72):i] if x['_at']<=t]
        f['price.gap_hours'] = (start-clock(previous[-1]['bar_open'])).total_seconds()/3600 if previous else None
        for w in self.config.price_windows:
            old = self.price_by_open.get(start-HOUR*w)
            f['price.return_' + str(w) + 'h'] = math.log(r['close']/old['close']) if old and old['_at']<=t else None
        for w in (6, 24, 72):
            points = [self.price_by_open.get(start-HOUR*k) for k in range(w+1)]
            returns = [math.log(points[k]['close']/points[k+1]['close']) for k in range(w) if points[k] and points[k+1] and points[k]['_at']<=t and points[k+1]['_at']<=t]
            f['price.volatility_' + str(w) + 'h'] = statistics.pstdev(returns) if len(returns) == w else None
        for w in (6, 24):
            points = [self.price_by_open.get(start-HOUR*k) for k in range(w)]
            f['price.trend_' + str(w) + 'h'] = r['close']/(sum(x['close'] for x in points)/w)-1 if all(x and x['_at']<=t for x in points) else None
        history=[x for x in self.prices[max(0,i-72):i+1] if x['_at']<=t]
        p['price'] = provenance(r) | {'bar_open': r['bar_open'], 'provider': 'YAHOO_EURUSD',
             'source_table':'brain.sasha_rates_eur_usd','rowset_sha256':digest(history),
             'row_completion_checked':r['row_completion_checked'],
             'availability_policy':'OBSERVED_COMPLETE_ROW_VERSION' if self.config.mode=='STRICT_ASOF' else 'PUBLIC_CLOSED_MARKET_BAR_RESEARCH_ONLY',
             'assumption': 'closed historical provider OHLC; not certified tick vintage'}

    def _macro(self, t, f, p):
        available_any = False
        for key in self.schema['macro_keys']:
            prefix = 'macro.' + key + '.'; f[prefix+'missing'] = 1
            index = self.macro.get(key)
            if not index:
                continue
            hi = bisect.bisect_right(index.times, t)
            # Latest reference period wins; an old delayed release cannot replace a newer known period.
            candidates = [r for r in index.rows[:hi] if not r['_until'] or t < r['_until']]
            if not candidates:
                continue
            r = max(candidates, key=lambda r: (str(r.get('reference_period') or ''), r['_at'], r['_id']))
            prior = [v for v in candidates if v.get('reference_period') == r.get('reference_period') and v['_at'] <= r['_at']]
            changed = r['_at']
            for old in sorted(prior, key=lambda v: v['_at'], reverse=True):
                if number(old['value']) != number(r['value']):
                    break
                changed = old['_at']
            age = (t-r['_at']).total_seconds()/3600
            f.update({prefix+'value': number(r['value']), prefix+'age_hours': age,
                      prefix+'change_age_hours': (t-changed).total_seconds()/3600,
                      prefix+'stale': int(age > (72 if r['source']=='CLEVELAND_NOW' else 24*7 if r['source'] in ('GDP_NOW_ATL','NYFED_NOW') else 24*45)),
                      prefix+'uncertain': int(r['_uncertain']), prefix+'missing': 0})
            p['macro.'+key] = provenance(r)
            available_any = True
        f['macro.missing'] = int(not available_any)

    def _news(self, t, f, p):
        any_news = False; topics = Counter(); proof = []; latest=[]; providers=[]
        for press, index in self.news.items():
            for w in self.config.news_windows:
                lo, hi = index.bounds(t, w); n = hi-lo
                prefix = 'news.' + press + '.'
                f[prefix+'count_' + str(w) + 'h'] = n
                f[prefix+'missing_' + str(w) + 'h'] = int(n==0)
                for dest, src in [('quality','quality_score'),('novelty','novelty'),('confirmation','confirmation_count'),('focus','topic_focus'),('uncertain','uncertain')]:
                    f[prefix+dest+'_' + str(w) + 'h'] = index.mean(src, lo, hi)
                any_news |= bool(n)
            lo, hi = index.bounds(t, 24)
            if hi>lo:
                proof.append({'provider':press,**index.range_proof(lo,hi)})
                latest.append(index.times[hi-1]);providers.append('SERVICE80_NLP_'+press)
            for topic,times in index.topic_times.items():
                topics[topic]+=bisect.bisect_right(times,t)-bisect.bisect_right(times,t-HOUR*24)
        for i in range(self.config.topic_count):
            f['news.topic_' + str(i) + '_24h'] = topics[i]
        f['news.missing'] = int(not any_news)
        if proof:
            p['news'] = {'input_count':sum(x['count'] for x in proof), 'rowset_sha256':digest(proof),
                         'latest_available_at':max(latest).isoformat(), 'range_proofs':proof,
                         'providers':providers, 'evidence_kind':'EXISTING_SERVICE79_80_NLP_RANGE_DIGEST',
                         'availability_policy':'BRAIN_RECEIPT_TEXT_AND_NLP_VERSION' if self.config.mode=='STRICT_ASOF' else 'LEGACY_DATE_RESEARCH_ONLY'}

    def _proxy(self, t, f, p):
        r = self.proxy.latest(t)
        if not r:
            return
        o,c = number(r.get('open_price')),number(r.get('close_price'))
        age = (t-clock(r['window_end_utc'])).total_seconds()/3600
        if age > 72:
            return
        vol = number(r.get('base_volume_eur')); buy=number(r.get('aggressor_buy_volume_eur'))
        f.update({'proxy.return':math.log(c/o) if o and c else None,'proxy.volume_eur':vol,
                  'proxy.delta_eur':number(r.get('signed_trade_delta_eur')),'proxy.trade_count':number(r.get('trade_count')),
                  'proxy.buy_share':buy/vol if vol and buy is not None else None,'proxy.age_hours':age,
                  'proxy.stale':int(age>2),'proxy.uncertain':int(r['_uncertain']),'proxy.missing':0})
        p['proxy']=provenance(r)|{'instrument':'EURUSDT','market_type':'CRYPTO_SPOT_PROXY','bar_end':str(r['window_end_utc'])}

    def _gdelt(self, t, f, p):
        total=0; proofs=[]
        for kind,index in self.gdelt.items():
            for w in (1,6,24):
                lo,hi=index.bounds(t,w);prefix='gdelt.'+kind+'.'
                n=index.total('count',lo,hi);tc=index.total('tone_count',lo,hi)
                f.update({prefix+'count_'+str(w)+'h':n,prefix+'tone_'+str(w)+'h':index.total('tone_sum',lo,hi)/tc if tc else None,
                          prefix+'missing_'+str(w)+'h':int(n==0),
                          prefix+'conflict_count_'+str(w)+'h':index.total('conflict_count',lo,hi),
                          prefix+'negative_count_'+str(w)+'h':index.total('negative_count',lo,hi)})
                total+=n
                if w==24:proofs.extend(provenance(r) for r in index.rows[lo:hi])
        f['gdelt.missing']=int(total==0)
        if proofs:p['gdelt']={'rowset_sha256':digest(proofs),'latest_available_at':max(x['available_at'] for x in proofs),
                              'evidence_kind':'DB_SOURCE_AGGREGATE_NOT_ORIGINAL_DOCUMENT_SHA','input_buckets':len(proofs)}

    def _cftc(self,t,f,p):
        r=self.cftc.latest(t)
        if not r:return
        for key in ('open_interest','net_dealer','net_asset_manager','net_leveraged_money'):f['cftc.'+key]=number(r.get(key))
        f['cftc.age_hours']=(t-r['_at']).total_seconds()/3600;f['cftc.missing']=0;p['cftc']=provenance(r)

    @classmethod
    def from_framework_state(cls,state,snapshot,config=Config(),schema=None):
        """Read existing Framework RAM without mutating it or calling model code."""
        if 'sasha_rates_eur_usd' not in state.global_rates:
            raise ValueError('FRAMEWORK_YAHOO_SERIES_REQUIRED')
        if config.mode=='STRICT_ASOF' and any(not r.get('row_updated_at') for r in state.global_rates['sasha_rates_eur_usd']):
            raise ValueError('FRAMEWORK_PRICE_VERSION_CLOCK_REQUIRED')
        data={**snapshot,'prices':copy.deepcopy(state.global_rates['sasha_rates_eur_usd'])}
        return cls(data,config,schema)

    def vectorize(self,row):
        if row['schema_hash']!=self.schema_hash:
            raise ValueError('SCHEMA_HASH_MISMATCH')
        keys=self.schema['feature_order']
        return [float(row['features'][k]) if row['features'][k] is not None else 0. for k in keys]+[float(row['missing_masks'][k]) for k in keys]


def _atomic(path, data):
    path=Path(path);fd,name=tempfile.mkstemp(dir=path.parent,prefix='.part-')
    try:
        with os.fdopen(fd,'wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(name,path)
        if os.name=='posix':
            parent=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(parent)
            finally:os.close(parent)
    finally:
        if os.path.exists(name):os.unlink(name)


def read_dataset(directory):
    p=Path(directory);manifest=json.loads((p/'manifest.json').read_text())
    for shard in manifest['shards']:
        b=(p/shard['file']).read_bytes()
        if hashlib.sha256(b).hexdigest()!=shard['sha256']:raise ValueError('SHARD_SHA_MISMATCH')
        for line in gzip.decompress(b).decode().splitlines():yield json.loads(line)


def write_dataset(builder,cutoffs,directory,chunk_size=128,input_reference=None):
    """Atomic chunks + checkpoint, flock prevents a concurrent output writer.

    No source/DB writes. Changed committed input prefix requires an explicit new
    dataset, rather than silently replacing historical features.
    """
    if not isinstance(chunk_size,int) or chunk_size<=0:raise ValueError('INVALID_CHUNK_SIZE')
    p=Path(directory);p.mkdir(parents=True,exist_ok=True)
    with (p/'writer.lock').open('a+b') as lock:
        if os.name=='posix':
            import fcntl
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        else:
            import msvcrt
            lock.write(b'0');lock.flush();lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        mf=p/'manifest.json'
        state=json.loads(mf.read_text()) if mf.exists() else {'schema_hash':builder.schema_hash,'shards':[],'rows':0,'last_cutoff':None}
        if state['schema_hash']!=builder.schema_hash:raise ValueError('SCHEMA_HASH_MISMATCH')
        if state['last_cutoff'] and state['prefix_digest']!=builder.prefix_digest(state['last_cutoff']):raise ValueError('INPUT_PREFIX_CHANGED_REBUILD_REQUIRED')
        if input_reference is not None:state['input_reference']=input_reference
        # Validate existing files on resume; orphan shards are never admitted.
        for shard in state['shards']:
            if hashlib.sha256((p/shard['file']).read_bytes()).hexdigest()!=shard['sha256']:raise ValueError('SHARD_SHA_MISMATCH')
        hours=sorted({clock(t) for t in cutoffs})
        hours=[t for t in hours if not state['last_cutoff'] or t>clock(state['last_cutoff'])]
        _atomic(p/'schema.json',canonical(builder.schema).encode())
        if not mf.exists():_atomic(mf,canonical(state).encode())
        for start in range(0,len(hours),chunk_size):
            batch=[builder.build_at(t) for t in hours[start:start+chunk_size]]
            raw=''.join(canonical(r)+'\n' for r in batch).encode();body=gzip.compress(raw,mtime=0)
            name='rows-'+str(state['rows']).zfill(8)+'.jsonl.gz';_atomic(p/name,body)
            state['shards'].append({'file':name,'sha256':hashlib.sha256(body).hexdigest(),'rows':len(batch)})
            state['rows']+=len(batch);state['last_cutoff']=batch[-1]['timestamp'];state['prefix_digest']=builder.prefix_digest(state['last_cutoff'])
            _atomic(mf,canonical(state).encode())
        if input_reference is not None:_atomic(mf,canonical(state).encode())
        return state
