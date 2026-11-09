"""Official FRED/ALFRED vintage API; protected key stays on B1.

Vintage dates are days when FRED/ALFRED changed a value, not documented
intraday publication clocks of the upstream economic release.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    import model100_primary_sources as common
except ImportError:
    from work import model100_primary_sources as common

VERSION = 'model100-alfred-api-v2'
START = date(2025, 1, 1)
OBSERVATION_START = '2024-01-01'
SERIES = ('CPIAUCSL', 'CPILFESL', 'PAYEMS', 'PPIFIS', 'PCEPI',
          'PCEPILFE', 'GDPC1', 'ADPMNUSNERSA', 'ADPWNUSNERSA', 'RSAFS','GDPNOW')
ADP_SERIES = ('ADPMNUSNERSA', 'ADPWNUSNERSA')


def upstream_provider(series):
    if series=='GDPNOW':return 'ATLANTA_FED'
    if series in ADP_SERIES:return 'ADP'
    if series=='RSAFS':return 'CENSUS'
    if series in ('CPIAUCSL','CPILFESL','PAYEMS','PPIFIS'):return 'BLS'
    if series in ('PCEPI','PCEPILFE','GDPC1'):return 'BEA'
    raise ValueError('UPSTREAM_PROVIDER_UNMAPPED')


def value_kind(series,initial_match,vintage,value):
    if series=='GDPNOW':return 'FORECAST_VINTAGE'
    return 'FIRST_ESTIMATE' if initial_match==(vintage.isoformat(),value) else 'REVISION'


def _repair_source_metadata(db):
    cur=db.cursor()
    cur.execute(f"SELECT * FROM `{common.TABLE}` WHERE source_id='ALFRED' AND numeric_value IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(extra_json,'$.upstream_provider'))='BLS_OR_BEA'")
    fields=[field[0] for field in cur.description];rows=cur.fetchall();cur.close()
    for values in rows:
        row=dict(zip(fields,values));ex=_extra(row['extra_json'])
        row['extra_json']=json.dumps({**ex,'upstream_provider':upstream_provider(row['event_type']),
                                     'upstream_provider_metadata_revision':1})
        common.store(db,row)
    return len(rows)


def protected_key(name='FRED_API_KEY', pattern=r'[a-fA-F0-9]{32}'):
    key = os.environ.get(name, '').strip()
    if not key:
        path = Path('/brain/Brain-Services/.env')
        if path.is_file():
            for line in path.read_text(encoding='utf-8').splitlines():
                match = re.match(r'^\s*(?:export\s+)?' + re.escape(name) + r'\s*=\s*(.*?)\s*$', line)
                if match:
                    key = match.group(1).strip().strip('"').strip("'")
                    break
    return key if re.fullmatch(pattern, key) else None


def _extra(value):
    if isinstance(value, dict):
        return value
    return json.loads(value or '{}')


def _prior(db, key):
    cur = db.cursor()
    try:
        cur.execute(f'SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',
                    ('ALFRED', key))
        row = cur.fetchone()
        return _extra(row[0]) if row else {}
    finally:
        cur.close()


def api_fetch(db, endpoint, params, record_key, token, now):
    public_params = {'file_type': 'json', **params}
    public_url = 'https://api.stlouisfed.org/fred/' + endpoint + '?' + urllib.parse.urlencode(public_params)
    request_url = public_url + '&api_key=' + urllib.parse.quote(token)
    # Credentials never enter canonical URLs, metadata, exception strings or
    # request logs. Responses containing an echoed credential are not stored.
    request = urllib.request.Request(request_url, headers={'User-Agent': 'Brain-Model100-PrimarySources/1.0'})
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            body = response.read(12_000_001)
            if len(body) > 12_000_000:
                raise ValueError('API_RESPONSE_TOO_LARGE')
            received = datetime.now(timezone.utc).replace(tzinfo=None)
            meta = {'status': response.status, 'url': public_url,
                    'content_type': response.headers.get('Content-Type'),
                    'etag': response.headers.get('ETag'),
                    'last_modified': response.headers.get('Last-Modified'),
                    'network_response_bytes': len(body), 'received_at_utc': received.isoformat() + 'Z'}
    except urllib.error.HTTPError as exc:
        retry = exc.headers.get('Retry-After') if exc.headers else None
        raise ApiFailure('RATE_LIMITED' if exc.code == 429 else ('BLOCKED_ACCESS' if exc.code in (401,403) else 'HTTP_ERROR'),
                         exc.code, retry) from None
    except (TimeoutError, urllib.error.URLError, OSError):
        raise ApiFailure('NETWORK_TIMEOUT', 0, None) from None
    if token.encode() in body:
        raise ApiFailure('CREDENTIAL_ECHO_REJECTED', 0, None)
    payload = json.loads(body)
    if 'error_code' in payload:
        raise ApiFailure('API_ERROR', int(payload['error_code']), None)
    row = common.base_row('ALFRED', record_key, 'ALFRED_API_RESPONSE', public_url,
                          body, meta, now, None, 'DAY_ONLY', 'DAY_ONLY',
                          title='Official ALFRED ' + endpoint, event_type='VINTAGE_API_ARTIFACT',
                          region='UNITED_STATES', extra_json={'endpoint': endpoint,
                          'parameters': public_params, 'api_key_source': 'protected_existing_config'})
    row['parser_version'] = VERSION
    common.store(db, row)
    time.sleep(.5)
    return payload, body, meta


class ApiFailure(Exception):
    def __init__(self, classification, status, retry_after):
        super().__init__(classification)
        self.classification, self.status, self.retry_after = classification, status, retry_after


def parse_changes(series, payload, today):
    if payload.get('output_type') != 3:
        raise ValueError('ALFRED_OUTPUT_TYPE_MISMATCH')
    out = []
    for item in payload.get('observations', []):
        observation = date.fromisoformat(item['date'])
        for name, raw in item.items():
            match = re.fullmatch(re.escape(series) + r'_(\d{8})', name)
            if not match or raw == '.':
                continue
            vintage = datetime.strptime(match.group(1), '%Y%m%d').date()
            if not START <= vintage <= today:
                continue
            try:
                value = Decimal(str(raw))
            except InvalidOperation:
                raise ValueError('ALFRED_NONNUMERIC_VALUE') from None
            if not value.is_finite():
                raise ValueError('ALFRED_NONFINITE_VALUE')
            out.append({'series_id': series, 'observation': observation,
                        'vintage': vintage, 'value': value})
    return sorted(out, key=lambda x: (x['observation'], x['vintage']))


def initial_values(payload):
    if payload.get('output_type') != 4:
        raise ValueError('ALFRED_INITIAL_OUTPUT_TYPE_MISMATCH')
    result = {}
    for row in payload.get('observations', []):
        if row.get('value') == '.':
            continue
        result[row['date']] = (row['realtime_start'], Decimal(row['value']))
    return result


def _checkpoint(db, series, now, **extra):
    payload = json.dumps({'series_id': series, 'checkpoint_schema': 1}, sort_keys=True).encode()
    row = common.base_row('ALFRED', 'api-checkpoint:' + series, 'RUN_CONTROL',
                         'brain://model100/alfred-api-checkpoint', payload,
                         {'status': 0, 'content_type': 'application/json'}, now, None,
                         'OBSERVED', 'RUN_CONTROL', event_type='BACKFILL_CHECKPOINT',
                         extra_json=extra)
    row['parser_version'] = VERSION
    common.store(db, row)


def _existing_values(db, series):
    cur = db.cursor()
    try:
        cur.execute(f'SELECT record_key,numeric_value FROM `{common.TABLE}` WHERE source_id=%s AND record_key LIKE %s AND numeric_value IS NOT NULL',
                    ('ALFRED', 'api:' + series + ':%'))
        return {(key, Decimal(str(value))) for key, value in cur.fetchall()}
    finally:
        cur.close()


def _retry_at(value, now):
    try:
        return now + timedelta(seconds=max(0, int(value)))
    except (ValueError, TypeError):
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc).replace(tzinfo=None)
        except (ValueError, TypeError, OverflowError):
            return now + timedelta(minutes=5)


def collect(db, now, adp_only=False, max_backfills=2):
    # Information collection is active. Future ML terms are a separate review.
    if db is not None:
        notice={'status':'COLLECTION_ACTIVE','collection_scope':'INFORMATION_ONLY_NO_TRAINING',
                'supersedes_collection_block':True,'future_ml_terms_review_separate':True,
                'policy_url':'https://fred.stlouisfed.org/legal/','history_preserved':True}
        body=json.dumps(notice,sort_keys=True).encode()
        common.store(db,common.base_row('ADP_NER' if adp_only else 'ALFRED',
            'control:fred-ml-license','RUN_CONTROL',notice['policy_url'],body,
            {'status':0,'content_type':'application/json'},now,None,'RUN_CONTROL','RUN_CONTROL',extra_json=notice))
    token = protected_key()
    result = {'source': 'ADP_NER' if adp_only else 'ALFRED', 'provider': 'ALFRED',
              'status': 'SUCCESS_UNCHANGED', 'series': {}, 'errors': [],
              'initial_values': 0, 'revision_values': 0, 'backfilled_series': 0,
              'new_semantic_values': 0,
              'publication_time_quality': 'DAY_ONLY', 'history_start': START.isoformat()}
    if not token:
        result['status'] = 'REQUIRES_API_KEY'
        result['classification'] = 'REQUIRES_API_KEY'
        return result
    result['upstream_metadata_rows_corrected']=_repair_source_metadata(db)
    requested = ADP_SERIES if adp_only else SERIES
    incomplete = []
    for series in requested:
        checkpoint = _prior(db, 'api-checkpoint:' + series)
        retry_at = checkpoint.get('next_retry_at_utc')
        if retry_at and now < datetime.fromisoformat(retry_at):
            result['errors'].append({'series_id':series, 'status':'RATE_LIMITED', 'next_retry_at_utc':retry_at})
            result['status'] = 'RATE_LIMITED'
            break
        try:
            dates, _, _ = api_fetch(db, 'series/vintagedates', {
                'series_id': series, 'realtime_start': START.isoformat(),
                'realtime_end': now.date().isoformat(), 'limit': 10000},
                'api-vintagedates:' + series, token, now)
            vintages = dates.get('vintage_dates', [])
            if dates.get('count', 0) > len(vintages):
                raise ValueError('VINTAGE_DATES_PAGINATION_INCOMPLETE')
            digest = hashlib.sha256(json.dumps(vintages).encode()).hexdigest()
            if checkpoint.get('vintages_sha256') == digest and checkpoint.get('state') == 'COMPLETE':
                result['series'][series] = {'status': 'SUCCESS_UNCHANGED', 'vintages': len(vintages)}
                continue
            if result['backfilled_series'] >= max_backfills:
                incomplete.append(series)
                continue
            if not vintages:
                _checkpoint(db, series, now, state='NO_PUBLISHED_VINTAGES', vintages_sha256=digest)
                result['series'][series] = {'status': 'PARTIAL_COVERAGE', 'vintages': 0}
                continue
            metadata, _, _ = api_fetch(db, 'series', {'series_id': series},
                                       'api-series-meta:' + series, token, now)
            info = metadata.get('seriess', [{}])[0]
            params = {'series_id': series, 'observation_start': OBSERVATION_START,
                      'observation_end': now.date().isoformat(), 'limit': 100000}
            initial, _, _ = api_fetch(db, 'series/observations', {
                **params, 'output_type': 4, 'realtime_start': '1776-07-04',
                'realtime_end': now.date().isoformat()}, 'api-initial:' + series, token, now)
            first = initial_values(initial)
            parsed=[]
            # Bound public URL length under the existing 1024-byte schema.
            # Each parsed cell keeps the bytes/SHA of its own source response.
            for offset in range(0,len(vintages),50):
                changes,body,meta=api_fetch(db,'series/observations',{
                    **params,'output_type':3,'vintage_dates':','.join(vintages[offset:offset+50])},
                    'api-changes:'+series+':'+str(offset),token,now)
                if changes.get('count',0)>len(changes.get('observations',[])):
                    raise ValueError('OBSERVATION_PAGINATION_INCOMPLETE')
                parsed.extend((item,body,meta) for item in parse_changes(series,changes,now.date()))
            counts = {'FIRST_ESTIMATE': 0, 'REVISION': 0,'FORECAST_VINTAGE':0}
            existing = _existing_values(db, series)
            new_values = 0
            for item,body,meta in parsed:
                obs, vintage, value = item['observation'], item['vintage'], item['value']
                initial_match = first.get(obs.isoformat())
                kind = value_kind(series,initial_match,vintage,value)
                if initial_match and initial_match[0] == vintage.isoformat() and initial_match[1] != value:
                    raise ValueError('INITIAL_VINTAGE_VALUE_CONFLICT')
                record_key = f'api:{series}:{obs}:{vintage}'
                row = common.base_row('ALFRED', record_key, kind,
                    meta['url'], body, meta, now, None, 'DAY_ONLY', 'DAY_ONLY',
                    title=info.get('title', series), event_type=series,
                    region='UNITED_STATES', observation_period_start=obs.isoformat(),
                    vintage_month=vintage.replace(day=1).isoformat(), numeric_value=value,
                    unit=(info.get('units_short') or 'SOURCE_LEVEL')[:48],
                    reference_timezone='America/Chicago', extra_json={
                        'series_id': series, 'vintage_date': vintage.isoformat(),
                        'upstream_provider': upstream_provider(series),
                        'source_units': info.get('units'), 'seasonal_adjustment': info.get('seasonal_adjustment'),
                        'value_class': kind, 'fred_output_type': 3,
                        'initial_verified_against_output_type_4': kind == 'FIRST_ESTIMATE',
                        'vintage_day_is_not_upstream_publication_hour': True,
                        'source_published_at_unknown': True,
                        'is_forecast_not_observed_gdp':series=='GDPNOW'})
                row['parser_version'] = VERSION
                common.store(db, row)
                if (record_key, value) not in existing:
                    new_values += 1
                    existing.add((record_key, value))
                counts[kind] += 1
            _checkpoint(db, series, now, state='COMPLETE', vintages_sha256=digest,
                        vintage_count=len(vintages), observation_start=OBSERVATION_START,
                        first_vintage=min(vintages), last_vintage=max(vintages),
                        first_estimate_values=counts['FIRST_ESTIMATE'], revision_values=counts['REVISION'],
                        forecast_vintage_values=counts['FORECAST_VINTAGE'])
            result['series'][series] = {'status': 'SUCCESS_NEW_DATA' if new_values else 'SUCCESS_UNCHANGED', **counts,
                                      'new_semantic_values': new_values,
                                      'vintage_start': min(vintages), 'vintage_end': max(vintages)}
            result['initial_values'] += counts['FIRST_ESTIMATE']
            result['revision_values'] += counts['REVISION']
            result['forecast_values']=result.get('forecast_values',0)+counts['FORECAST_VINTAGE']
            result['backfilled_series'] += 1
            result['new_semantic_values'] += new_values
        except ApiFailure as exc:
            result['errors'].append({'series_id': series, 'status': exc.classification,
                                     'http_status': exc.status, 'retry_after': exc.retry_after})
            _checkpoint(db, series, now, **{**checkpoint, 'last_error': exc.classification,
                        'http_status': exc.status, 'retry_after': exc.retry_after,
                        'next_retry_at_utc': _retry_at(exc.retry_after, now).isoformat() if exc.status == 429 else None})
            if exc.classification in ('RATE_LIMITED', 'BLOCKED_ACCESS'):
                result['status'] = exc.classification
                break
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            result['errors'].append({'series_id': series, 'status': 'PARSE_ERROR',
                                     'error_type': type(exc).__name__})
    result['remaining_backfill_series'] = incomplete
    if result['errors'] and result['status'] == 'SUCCESS_UNCHANGED':
        result['status'] = 'PARTIAL_COVERAGE'
    elif incomplete:
        result['status'] = 'BACKFILL_INCOMPLETE'
    elif result['new_semantic_values']:
        result['status'] = 'SUCCESS_NEW_DATA'
    return result
