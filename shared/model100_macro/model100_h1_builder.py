"""Read-only EUR/USD H1 documented replay and live-shadow builder.

Inputs are consistent JSONL exports; this program never connects to or writes
the database. DAY_ONLY/API update times cannot supply strict historical hours.
"""
import argparse
import gzip
import json
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from parser.model100_semantics import semantic_versions
    from parser.model100_primary_sources import SOURCE_PRIORITY,SOURCE_DATA_IDS
except ImportError:
    from parser.model100_semantics import semantic_versions
    from parser.model100_primary_sources import SOURCE_PRIORITY,SOURCE_DATA_IDS


def clock(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def family(row):
    extra=row.get('extra_json') or '{}'
    extra=json.loads(extra) if isinstance(extra,str) else extra
    series=extra.get('series_id') or extra.get('series') or extra.get('metric') or extra.get('dataflow') or ''
    return '|'.join(str(row.get(k) or '') for k in ('source_id','event_type','record_kind'))+'|'+str(series)


def prepare_events(records):
    numeric = [r for r in records if r.get('numeric_value') is not None]
    semantic = semantic_versions(numeric)
    families = sorted({family(r) for r in semantic})
    events, excluded = [], Counter()
    for row in semantic:
        published = clock(row.get('source_published_at_utc'))
        received = clock(row['first_received_at_utc'])
        if published is None:
            excluded['DAY_ONLY_OR_API_UPDATE_ONLY'] += 1
            continue
        if row.get('source_id') == 'BEA_RELEASES' and row.get('observation_period_start') and str(row['observation_period_start'])[:10] > published.date().isoformat():
            excluded['ACTUAL_OBSERVATION_AFTER_PUBLICATION'] += 1
            continue
        if row.get('release_clock_evidence') in ('SCHEDULED','SCHEDULE_ONLY'):
            excluded['SCHEDULE_ONLY'] += 1
            continue
        if row.get('asof_class') not in ('HISTORICAL_DOCUMENTED','HISTORICAL_ARCHIVE','HISTORICAL_REPLAY','CERTIFIED_LIVE'):
            excluded['UNSUPPORTED_QUALITY'] += 1
            continue
        available = max(published, received) if row['semantic_amendment'] else published
        events.append({**row, '_published':published, '_received':received, '_available':available})
    events.sort(key=lambda r:(r['_available'],str(r.get('observation_period_start') or ''),r['event_identity']))
    return families, events, dict(excluded)


def build(records, bars, derived_at=None, stale_after_seconds=45*86400,mode='DOCUMENTED_REPLAY'):
    if mode not in ('DOCUMENTED_REPLAY','SCHEDULED_SENSITIVITY','CERTIFIED_LIVE_SHADOW'):
        raise ValueError('UNKNOWN_ASOF_MODE')
    derived_at = derived_at or datetime.now(timezone.utc)
    families, events, excluded = prepare_events(records)
    if mode=='CERTIFIED_LIVE_SHADOW':
        certified=[]
        for row in events:
            if row.get('asof_class')=='CERTIFIED_LIVE' and 0<=(row['_received']-row['_published']).total_seconds()<=300:
                row={**row,'_available':max(row['_published'],row['_received'])};certified.append(row)
            else:excluded['NOT_CERTIFIED_LIVE']=excluded.get('NOT_CERTIFIED_LIVE',0)+1
        events=sorted(certified,key=lambda r:r['_available'])
    documents=[];modified_documents_excluded=0
    document_records=semantic_versions([r for r in records if r.get('record_kind') in ('ORIGINAL_DOCUMENT','RELEASE')])
    document_families=sorted({r['source_id']+'|'+str(r.get('event_type') or '') for r in document_records})
    unverified_documents_excluded=0
    for row in document_records:
        pub=clock(row.get('source_published_at_utc'))
        ex=row.get('extra_json') or '{}';ex=json.loads(ex) if isinstance(ex,str) else ex
        modified=clock(ex.get('source_modified_at_utc'))
        if pub and modified and modified>pub:
            modified_documents_excluded+=1
            continue
        if ex.get('current_html_is_not_verified_first_publication_text') or ex.get('original_first_text_version_verified') is False:
            # A trustworthy datePublished does not authenticate the old text.
            # Keep the original in the manifest; exclude its title/text/SHA
            # from historical features until a first-version proof exists.
            unverified_documents_excluded+=1
            continue
        if pub and row.get('asof_class') in ('HISTORICAL_DOCUMENTED','HISTORICAL_ARCHIVE','HISTORICAL_REPLAY','CERTIFIED_LIVE') and row.get('release_clock_evidence') not in ('SCHEDULED','SCHEDULE_ONLY'):
            available=max(pub,clock(row['first_received_at_utc'])) if row['semantic_amendment'] else pub
            if mode=='CERTIFIED_LIVE_SHADOW':
                received=clock(row['first_received_at_utc'])
                if row.get('asof_class')!='CERTIFIED_LIVE' or not 0<=(received-pub).total_seconds()<=300:continue
                available=max(pub,received)
            documents.append((available,row))
    documents.sort(key=lambda item:(item[0],item[1]['event_identity']))
    document_cursor=0
    state, cursor = {}, 0
    previous_cutoff=None;document_state={};document_type_state={}
    stats = {'bars':0,'future_leakage_violations':0,'negative_age_violations':0,
             'day_only_values_used_in_strict':0,'certified_live_feature_rows':0,
             'semantic_numeric_versions':len(semantic_versions([r for r in records if r.get('numeric_value') is not None])),
             'strict_numeric_versions':len(events), 'excluded':excluded,
             'feature_families':len(families), 'amendments_conservatively_delayed':sum(r['semantic_amendment'] for r in events)}
    stats['document_events_emitted']=0
    stats['modified_document_first_versions_unavailable']=modified_documents_excluded
    stats['unverified_first_document_versions_unavailable']=unverified_documents_excluded
    stats['day_only_document_versions_excluded']=sum(clock(r.get('source_published_at_utc')) is None for r in document_records)
    schedule_clocks=[]
    if mode=='SCHEDULED_SENSITIVITY':
        for row in records:
            if row.get('record_kind')!='SCHEDULED_RELEASE':continue
            ex=row.get('extra_json') or '{}';ex=json.loads(ex) if isinstance(ex,str) else ex
            scheduled=clock(ex.get('scheduled_at_utc'))
            if scheduled:schedule_clocks.append((scheduled,row))
    seen_bars=set()
    for bar in sorted(bars,key=lambda r:r['event_time_utc']):
        if bar.get('provider') != 'FXCM' or bar.get('instrument') != 'EURUSD' or bar.get('quality_status') != 'VALIDATED' or bar.get('market_type')!='SPOT_BID_ASK_H1':
            raise ValueError('REQUIRES_VALIDATED_FXCM_EURUSD_H1')
        event_clock=clock(bar['event_time_utc'])
        if event_clock in seen_bars:raise ValueError('DUPLICATE_H1_EVENT_TIME')
        seen_bars.add(event_clock)
        cutoff = clock(bar['window_end_utc'])
        lower_cutoff=previous_cutoff or event_clock
        if (cutoff-clock(bar['event_time_utc'])).total_seconds()!=3600:
            raise ValueError('INVALID_H1_WINDOW')
        numeric_events=[]
        while cursor<len(events) and events[cursor]['_available']<=cutoff:
            event=events[cursor]
            state[family(event)]=event
            if event['_available']>lower_cutoff:
                numeric_events.append({'source_id':event['source_id'],'indicator':event['event_type'],
                    'region':event.get('region'),
                    'reference_period':str(event.get('observation_period_start')),
                    'value':str(event['numeric_value']),'unit':event.get('unit'),
                    'release_edition':event['event_dimensions']['release_edition'],
                    'source_published_at_utc':event['_published'].isoformat(),
                    'available_at_utc':event['_available'].isoformat(),
                    'carried_across_market_data_gap':event['_available']<=event_clock,
                    'original_sha256':event['content_sha256'],'source_url':event.get('canonical_url')})
            cursor+=1
        features={}
        for key in families:
            row=state.get(key)
            if row is None:
                features[key]={'value':None,'missing':True,'quality':'UNAVAILABLE_AT_CUTOFF'}
                continue
            available,published,received=row['_available'],row['_published'],row['_received']
            stats['future_leakage_violations']+=int(available>cutoff or published>cutoff)
            age=(cutoff-published).total_seconds()
            stats['negative_age_violations']+=int(age<0)
            certified=(row.get('asof_class')=='CERTIFIED_LIVE' and received<=cutoff and 0<=(received-published).total_seconds()<=300)
            stats['certified_live_feature_rows']+=int(certified)
            features[key]={'value':str(row['numeric_value']),'missing':False,'unit':row.get('unit'),
                'region':row.get('region'),'event_type':row.get('event_type'),
                'reference_period':str(row.get('observation_period_start')),
                'release_edition':row['event_dimensions']['release_edition'],
                'available_at_utc':available.isoformat(),'source_published_at_utc':published.isoformat(),
                'first_received_at_utc':received.isoformat(),'age_seconds':age,
                'stale':age > stale_after_seconds,
                'stale_policy_seconds':stale_after_seconds,
                'stale_policy_is_calendar_gap_evidence':False,
                'quality':'CERTIFIED_LIVE' if certified else 'DOCUMENTED_REPLAY',
                'event_identity':row['event_identity'],'original_sha256':row['content_sha256'],
                'source_url':row.get('canonical_url'),
                'amendment':row['semantic_amendment']}
        document_events=[];document_since_previous=[]
        while document_cursor<len(documents) and documents[document_cursor][0]<=cutoff:
            available,row=documents[document_cursor];document_cursor+=1
            published=clock(row['source_published_at_utc'])
            ex=row.get('extra_json') or '{}';ex=json.loads(ex) if isinstance(ex,str) else ex
            event={'source_id':row['source_id'],'event_type':row.get('event_type'),
                'region':row.get('region'),'speaker':ex.get('speaker'),
                'title':row.get('title'),'source_published_at_utc':published.isoformat(),
                'available_at_utc':available.isoformat(),
                'first_received_at_utc':row['first_received_at_utc'],
                'quality':'CERTIFIED_LIVE' if mode=='CERTIFIED_LIVE_SHADOW' else 'DOCUMENTED_REPLAY',
                'first_publication_text_verified':False if ex.get('current_html_is_not_verified_first_publication_text') else ex.get('original_first_text_version_verified'),
                'source_url':row.get('canonical_url'),
                'original_sha256':row['content_sha256'],'event_identity':row['event_identity']}
            document_state[row['source_id']]=event
            document_type_state[row['source_id']+'|'+str(row.get('event_type') or '')]=event
            if available>lower_cutoff:
                document_since_previous.append({**event,'carried_across_market_data_gap':available<=event_clock})
            if available>event_clock:document_events.append(event)
        stats['document_events_emitted']+=len(document_events)
        schedules=[]
        for scheduled,r in schedule_clocks:
            if event_clock<scheduled<=cutoff:
                schedules.append({'source_id':r['source_id'],'record_key':r['record_key'],
                                  'quality':'SCHEDULE_ONLY','actual_value_available':False,
                                  'historical_schedule_knowledge_at_cutoff_verified':False})
        stats['bars']+=1
        previous_cutoff=cutoff
        source_masks={source:not any(key.split('|',1)[0]==source and not value['missing'] for key,value in features.items())
                      for source in sorted({'ECB_MID'}|{SOURCE_DATA_IDS.get(s,s) for s in SOURCE_PRIORITY})}
        yield {'provider':'FXCM','instrument':'EURUSD','market_type':bar.get('market_type'),
               'event_time_utc':bar['event_time_utc'],'replay_cutoff_utc':cutoff.isoformat(),
               'derived_at_utc':derived_at.isoformat(),'feature_ready_at_utc':derived_at.isoformat(),
               'asof_mode':mode,'features':features,
               'source_numeric_missing_masks':source_masks,
               'documented_events_in_hour':document_events,
               'documented_events_since_previous_available_cutoff':document_since_previous,
               'numeric_events_since_previous_available_cutoff':numeric_events,
               'last_known_document_state':{source:{**event,'age_seconds':(cutoff-clock(event['source_published_at_utc'])).total_seconds()} for source,event in document_state.items()},
               'last_known_document_type_state':{kind:{**event,'age_seconds':(cutoff-clock(event['source_published_at_utc'])).total_seconds()} for kind,event in document_type_state.items()},
               'document_event_type_missing_masks':{kind:kind not in document_type_state for kind in document_families},
               'scheduled_sensitivity_events':schedules}, stats


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--records',required=True)
    parser.add_argument('--bars',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--stale-after-seconds',type=int,default=45*86400,
                        help='Explicit feature-age policy, not proof of a missed scheduled release')
    parser.add_argument('--mode',choices=('DOCUMENTED_REPLAY','SCHEDULED_SENSITIVITY','CERTIFIED_LIVE_SHADOW'),default='DOCUMENTED_REPLAY')
    args=parser.parse_args(argv)
    records=[json.loads(line) for line in Path(args.records).open(encoding='utf-8')]
    bars=[json.loads(line) for line in Path(args.bars).open(encoding='utf-8')]
    stats={}
    with gzip.open(args.output,'wt',encoding='utf-8') as stream:
        for row,stats in build(records,bars,stale_after_seconds=args.stale_after_seconds,mode=args.mode):
            stream.write(json.dumps(row,separators=(',',':'))+'\n')
    Path(args.output+'.audit.json').write_text(json.dumps(stats,indent=2),encoding='utf-8')
    coverage=market_coverage(bars)
    Path(args.output+'.market_coverage.json').write_text(json.dumps(coverage,indent=2),encoding='utf-8')
    print(json.dumps(stats))


def market_coverage(bars):
    stamps=sorted({clock(b['event_time_utc']) for b in bars})
    if not stamps:return {'candles':0,'status':'NO_VALIDATED_CANDLES'}
    seen=set(stamps);missing=[];expected=0;point=stamps[0]
    zone=ZoneInfo('America/New_York')
    while point<=stamps[-1]:
        local=point.astimezone(zone)
        nominal_open=(local.weekday()<4 or local.weekday()==4 and local.hour<17 or
                      local.weekday()==6 and local.hour>=17)
        if nominal_open:
            expected+=1
            if point not in seen:missing.append(point.isoformat())
        point+=timedelta(hours=1)
    return {'provider':'FXCM','instrument':'EURUSD','candles':len(bars),'unique_hours':len(seen),
            'source_start_utc':stamps[0].isoformat(),'source_end_utc':(stamps[-1]+timedelta(hours=1)).isoformat(),
            'requested_history_start_utc':'2025-01-01T00:00:00+00:00',
            'requested_start_covered':stamps[0]<=datetime(2025,1,1,tzinfo=timezone.utc),
            'nominal_forex_session_hours_between_available_endpoints':expected,
            'unavailable_nominal_session_hours':len(missing),
            'unavailable_hours':missing,'missing_reason':'UNRESOLVED_MARKET_GAP_OR_HOLIDAY',
            'calendar_rule':'Sunday 17:00 to Friday 17:00 America/New_York; holiday closures not certified',
            'this_is_not_economic_release_coverage':True}


if __name__=='__main__': main()
