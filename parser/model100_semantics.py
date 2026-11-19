"""Economic identity independent of transport bytes; no database mutation."""
import hashlib
import json
import re
from decimal import Decimal
from datetime import datetime


def _text(value):
    if isinstance(value, str) and len(value)>10 and value[10:11] in (' ', 'T'):
        try:
            return datetime.fromisoformat(value.replace('Z','+00:00')).isoformat()
        except ValueError:
            pass
    return value.isoformat() if hasattr(value, 'isoformat') else str(value) if value is not None else None


def economic_identity(row):
    extra = row.get('extra_json') or {}
    if isinstance(extra, str):
        extra = json.loads(extra)
    status = int(row.get('http_status') or 0)
    if status not in (200, 304) or row.get('asof_class') in ('UNUSABLE','FETCH_STATUS','RUN_CONTROL'):
        return None
    kind = row.get('record_kind')
    numeric = row.get('numeric_value') is not None
    if not numeric and kind not in ('ORIGINAL_DOCUMENT', 'RELEASE'):
        return None
    edition = extra.get('estimate_stage') or extra.get('value_kind') or extra.get('value_class') or kind
    if row.get('source_id')=='BEA_RELEASES' and edition=='UNSPECIFIED':
        match=re.search(r'\b(Initial|Advance|Second|Third|Updated) Estimate\b',row.get('title') or '',re.I)
        if match:edition=match.group(1).upper()
    vintage = (extra.get('vintage_date') or extra.get('forecast_date') or extra.get('vintage_code') or
               extra.get('publication_date') or extra.get('release_document_date') or
               extra.get('document_date') or extra.get('updated_at_raw') or
               _text(row.get('source_published_at_utc')) or _text(row.get('vintage_month')))
    if kind == 'CURRENT_PAGE_ESTIMATE':
        vintage = 'CURRENT_UNDATED_SUMMARY'
    dimensions = {'provider': row.get('source_id'), 'indicator': row.get('event_type'),
                  'region': row.get('region'), 'reference_period': _text(row.get('observation_period_start')),
                  'release_edition': edition, 'vintage': vintage}
    series = extra.get('series_id') or extra.get('series') or extra.get('metric')
    if series:
        dimensions['series_id'] = series
    if extra.get('dimensions'):
        dimensions['series_dimensions'] = extra['dimensions']
        dimensions['dataflow'] = extra.get('dataflow')
    if not numeric:
        dimensions['original_document_url'] = row.get('canonical_url', '').split('#',1)[0]
    if vintage is None and numeric:
        # Missing vintage evidence must never merge unrelated releases.
        dimensions['unresolved_document_url'] = row.get('canonical_url')
    payload = json.dumps(dimensions, sort_keys=True, ensure_ascii=False, separators=(',',':'))
    event_id = hashlib.sha256(payload.encode()).hexdigest()
    value = format(Decimal(str(row['numeric_value'])).normalize(), 'f') if numeric else None
    value_payload = json.dumps({'event_identity': event_id, 'value': value,
                                'unit': row.get('unit')}, sort_keys=True, separators=(',',':'))
    text_sha=extra.get('speech_text_sha256') if not numeric else None
    if text_sha:
        value_payload=json.dumps({'event_identity':event_id,'speech_text_sha256':text_sha},sort_keys=True,separators=(',',':'))
    return {'event_identity': event_id, 'event_dimensions': dimensions,
            'semantic_value_sha256': hashlib.sha256(value_payload.encode()).hexdigest() if numeric or text_sha else None,
            'identity_version': 2}


def semantic_versions(rows):
    """Collapse dynamic HTML copies; retain changed values and their SHA evidence.

    A changed value under an existing event ID is an amendment. If the
    original release's SHA first arrived later, documented replay cannot
    assume its changed value had been present at the original publication.
    """
    groups = {}
    for row in sorted(rows, key=lambda r: (_text(r.get('first_received_at_utc')) or '',r.get('content_sha256',''))):
        identity = economic_identity(row)
        if not identity:
            continue
        event_id = identity['event_identity']
        fingerprint = identity['semantic_value_sha256'] or event_id
        key = (event_id, fingerprint)
        if key not in groups:
            groups[key] = {**row, **identity, 'original_sha256_versions': [], 'semantic_amendment': False}
        groups[key]['original_sha256_versions'].append(row['content_sha256'])
    seen = set()
    result = []
    for row in groups.values():
        row['semantic_amendment'] = row['event_identity'] in seen
        seen.add(row['event_identity'])
        result.append(row)
    return result
