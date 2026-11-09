"""Read-only Model100FeatureBuilder input contract v1. No DB/model writes."""
import argparse,calendar,collections,gzip,hashlib,json
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path
try:
 from . import model100_leading_v1 as leading
 from .model100_h1_builder import clock
except ImportError:
 from . import model100_leading_v1 as leading
 from .model100_h1_builder import clock
VERSION='model100-leading-input-v1'
SOURCES=('ALFRED','IFO_VINTAGE','GDP_NOW_ATL','ECB_SPF','NYFED_NOW','CLEVELAND_NOW','ZEW','NYFED_SME')

def period_end(value,quarter=False):
 d=clock(str(value)[:10]);month=d.month+2 if quarter else d.month
 return d.replace(month=month,day=calendar.monthrange(d.year,month)[1])+timedelta(days=1)

def target_key(r):
 role=r['role'];indicator=r['indicator'];period=str(r['reference_period'])[:10]
 if role.startswith('GDP_FORECAST'):return ('GDP_FORECAST',period)
 if indicator=='ADPMNUSNERSA':return ('PRIVATE_EMPLOYMENT_PROXY',period)
 if role=='INFLATION_FORECAST':
  metric={'CPI_INFLATION':'CPI_HEADLINE_MOM','CORE_CPI_INFLATION':'CPI_CORE_MOM','PCE_INFLATION':'PCE_PUBLICATION','CORE_PCE_INFLATION':'PCE_PUBLICATION'}.get(indicator)
  if metric:return ('INFLATION_FORECAST_'+metric,period)
 return None

def target_clocks(records):
 targets=leading.target_releases(records)
 for r in records:
  if r['source_id']!='BEA_RELEASES' or r['event_type']!='BEA_PCE_PRICE_YOY' or r.get('asof_class')=='UNUSABLE':continue
  x=leading.metadata(r);pub=clock(r.get('source_published_at_utc'))
  if not pub or x.get('estimate_stage')!='MONTHLY_RELEASE' or r.get('release_clock_evidence') in ('SCHEDULE_ONLY','SCHEDULED'):continue
  k=('INFLATION_FORECAST_PCE_PUBLICATION',str(r.get('observation_period_start'))[:10]);old=targets.get(k)
  if old is None or pub<old['published']:targets[k]={'published':pub,'url':r['canonical_url'],'sha':r['content_sha256']}
 return targets

def compile_rows(records,source_policy=None):
 if source_policy is not None:
  try:from .model100_source_policy import filter_records
  except ImportError:from .model100_source_policy import filter_records
  records=filter_records(records,source_policy)
 candidates=leading.prepare(records);targets=target_clocks(records);out=[];aux=0
 try:from .model100_leading_matrix import inventory
 except ImportError:from .model100_leading_matrix import inventory
 inventory_rows=inventory(records)
 for r in candidates:
  if r['role']=='GDP_FORECAST_ALFRED':aux+=1;continue
  k=target_key(r);t=targets.get(k);available=clock(r['available_at']);is_revision=r['edition']=='REVISION'
  expiry=None;policy='';kind='FORECAST_OF_TARGET' if k else 'EXPECTATIONS_CONTEXT'
  if t:expiry=t['published'];policy='ORIGINAL_TARGET_PUBLICATION'
  elif k:
   expiry=period_end(r['reference_period'],r['role'].startswith('GDP_FORECAST'));policy='CONSERVATIVE_PERIOD_END_IF_TARGET_CLOCK_UNRESOLVED'
  else:
   days=120 if r['source']=='ECB_SPF' else 14 if r['indicator']=='ADPWNUSNERSA' else 45
   expiry=available+timedelta(days=days);policy='CONTEXT_TTL_POLICY_'+str(days)+'D_NOT_PROVIDER_PUBLICATION'
  # Survey round is not a target economic period (notably SPF LT/P9M/P21M).
  target_period=r['reference_period'] if k else None
  group='|'.join(str(x or '') for x in (r['source'],r['indicator'],r['region'],target_period,r['target_horizon']))
  r={**r,'value':format(Decimal(r['value']).normalize(),'f')}
  out.append({**r,'contract_version':VERSION,'feature_group':group,'target_economic_period':target_period,
   'survey_reference_period':None if k else r['reference_period'],'feature_kind':kind,
   'valid_until_audit':expiry.isoformat(),'expiry_policy':policy,
   'target_publication_at_audit':t['published'].isoformat() if t else None,
   'target_original_url':t['url'] if t else None,'target_original_sha256':t['sha'] if t else None,
   'target_clock_status':'VERIFIED_ORIGINAL_RELEASE' if t else 'TARGET_CLOCK_UNRESOLVED' if k else 'NO_SINGLE_TARGET_RELEASE',
   'eligible_feature':not is_revision and available<expiry,
   'exclusion_reason':'REVISION_RETAINED_SEPARATE_NOT_FIRST_ESTIMATE_INPUT' if is_revision else 'AVAILABLE_AT_OR_AFTER_TARGET_OR_POLICY_EXPIRY' if available>=expiry else None,
   'release_version':r['edition']+':'+str(r['forecast_vintage_day']),
   'temporal_status':'H1_DOCUMENTED' if r['technical_quality']=='READY_LEADING_H1' else 'H1_CONSERVATIVE_DAY_ONLY'})
 out.sort(key=lambda r:(r['available_at'],r['feature_group'],r['event_identity']))
 stats={'collection_primary_values':sum(r['historical_values'] for r in inventory_rows),'temporal_primary_candidates':len(out),'auxiliary_alfred_gdpnow_excluded':aux,
        'legacy_candidate_count_including_auxiliary':len(candidates),'excluded_sme_public_results_date':next(r['historical_values'] for r in inventory_rows if r['source']=='NY_FED_SME'),
        'excluded_spf_later_api_updates':sum(r['source_id']=='ECB_SPF' and r.get('numeric_value') is not None and leading.metadata(r).get('later_update_not_original_round',False) for r in records),'feature_eligible_versions':sum(r['eligible_feature'] for r in out),
        'feature_exclusions':dict(collections.Counter(r['exclusion_reason'] for r in out if not r['eligible_feature'])),
        'reconciliation_by_source':[{k:r[k] for k in ('source','historical_values','temporal_candidates')} for r in inventory_rows],
        'primary_candidates_by_source':dict(collections.Counter(r['source'] for r in out))}
 if source_policy is not None:
  out=[r for r in out if (r['source']!='ALFRED' or r['indicator'] in source_policy['alfred_series']) and (r['source']!='IFO_VINTAGE' or r['indicator'] in source_policy['ifo_series'])]
  actuals=compile_actual_rows(records,source_policy);out.extend(actuals)
  out.sort(key=lambda r:(r['available_at'],r['feature_group'],r['event_identity']))
  stats.update(source_policy_version=source_policy['version'],allowed_sources=source_policy['allowed_sources'],
   excluded_sources=source_policy.get('excluded_sources',sorted(set(SOURCES)-set(source_policy['allowed_sources']))),
   actual_versions=len(actuals),feature_eligible_versions=sum(r['eligible_feature'] for r in out),
   output_versions_by_source=dict(collections.Counter(r['source'] for r in out if r['eligible_feature'])))
 return out,stats

def compile_actual_rows(records,policy):
 """Reuse documented release validation; allow dated ECB PDF with explicit delay."""
 try:
  from .model100_h1_builder import prepare_events
  from parser.model100_semantics import semantic_versions
 except ImportError:
  from .model100_h1_builder import prepare_events
  from parser.model100_semantics import semantic_versions
 actuals=[]
 for r in records:
  if r['source_id'] not in policy['actual_sources'] or r.get('numeric_value') is None or r.get('asof_class')=='UNUSABLE':continue
  if r['record_kind'] not in ('FIRST_ESTIMATE','RELEASE_VALUE'):continue
  x=leading.metadata(r)
  if r['source_id']=='BEA_RELEASES' and r['event_type']=='BEA_GDP_REAL_QOQ_ANNUALIZED' and x.get('estimate_stage') not in ('ADVANCE','INITIAL'):continue
  actuals.append(r)
 _,events,_=prepare_events(actuals)
 for r in semantic_versions([r for r in actuals if r['source_id']=='ECB_HIST' and not r.get('source_published_at_utc')]):
  day=leading.metadata(r).get('document_date')
  if not day:continue
  available=clock(day)+timedelta(days=1,hours=12)
  if r['semantic_amendment']:available=max(available,clock(r['first_received_at_utc']))
  events.append({**r,'_available':available,'_published':None})
 out=[]
 for r in events:
  documented=r['_published'] is not None
  out.append({'source':r['source_id'],'indicator':r['event_type'],'region':r.get('region'),
   'reference_period':r.get('observation_period_start'),'target_economic_period':None,'target_horizon':None,
   'value':format(Decimal(str(r['numeric_value'])).normalize(),'f'),'unit':r.get('unit'),
   'release_version':r['record_kind']+':'+str(r.get('observation_period_start')),'edition':r['record_kind'],
   'available_at':r['_available'].isoformat(),'source_published_at':r['_published'].isoformat() if documented else None,
   'forecast_vintage_day':str(r['_published'].date()) if documented else leading.metadata(r)['document_date'],
   'first_received_at':r['first_received_at_utc'],'source_url':r['canonical_url'],'original_sha256':r['content_sha256'],
   'event_identity':r['event_identity'],'contract_version':VERSION,'feature_kind':'RELEASED_ACTUAL_CONTEXT',
   'feature_group':'|'.join(('ACTUAL',r['source_id'],r['event_type'],str(r.get('region') or ''))),
   'temporal_status':'H1_DOCUMENTED' if documented else 'H1_CONSERVATIVE_DAY_ONLY',
   'availability_policy':'DOCUMENTED_PUBLICATION' if documented else 'CONSERVATIVE_NEXT_DAY_12UTC_CONTEXT_ONLY',
   'eligible_feature':True,'exclusion_reason':None,'valid_until_audit':'9999-12-31T00:00:00+00:00',
   'expiry_policy':'ACTUAL_CARRY_FORWARD_UNTIL_REPLACED_WITH_STALE_MASK','target_publication_at_audit':None})
 return out

def build_h1(rows,bars,source_policy=None):
 if source_policy is not None:
  allowed=set(source_policy['allowed_sources']);rows=[r for r in rows if r['source'] in allowed]
  mask_sources=source_policy['allowed_sources']
 else:mask_sources=SOURCES
 ordered=sorted([r for r in rows if r['eligible_feature']],key=lambda r:(r['available_at'],r['feature_group'],r['event_identity']))
 cursor=0;state={};change={};previous_cutoff=None;stats={'bars':0,'future_availability_violations':0,'expired_values_exposed':0,'negative_change_age':0,'future_target_clock_exposed_as_feature':0,'new_forecast_events':0,'bootstrap_versions':0}
 for bar in sorted(bars,key=lambda b:clock(b['window_end_utc'])):
  cutoff=clock(bar['window_end_utc']);events=[]
  while cursor<len(ordered) and clock(ordered[cursor]['available_at'])<=cutoff:
   r=ordered[cursor];g=r['feature_group'];previous=state.get(g)
   if previous is None or previous['value']!=r['value']:change[g]=clock(r['available_at'])
   state[g]=r;cursor+=1
   if previous_cutoff is None and clock(r['available_at'])<=clock(bar.get('event_time_utc') or (cutoff-timedelta(hours=1)).isoformat()):stats['bootstrap_versions']+=1
   else:events.append(r['event_identity'])
  features={}
  for g,r in state.items():
   available=clock(r['available_at']);expiry=clock(r['valid_until_audit']);expired=cutoff>=expiry;age=(cutoff-available).total_seconds();changed=(cutoff-change[g]).total_seconds()
   stats['future_availability_violations']+=int(available>cutoff);stats['negative_change_age']+=int(changed<0)
   # Future ACTUAL release clocks are audit metadata, never a feature countdown.
   f={'source':r['source'],'indicator':r['indicator'],'region':r['region'],'target_economic_period':r['target_economic_period'],'target_horizon':r['target_horizon'],
      'value':None if expired else r['value'],'unit':r['unit'],'release_version':r['release_version'],'reference_period':r['reference_period'],
      'available_at':r['available_at'],'release_day':r['forecast_vintage_day'],'source_published_at':r['source_published_at'],
      'missing':expired,'expired':expired,'stale':age>({'CLEVELAND_NOW':3,'NYFED_NOW':7,'GDP_NOW_ATL':7,'ECB_SPF':120}.get(r['source'],14 if r['indicator']=='ADPWNUSNERSA' else 45))*86400,'temporal_uncertainty':r['temporal_status']!='H1_DOCUMENTED',
      'temporal_status':r['temporal_status'],'feature_kind':r['feature_kind'],'seconds_since_available':age,
      'seconds_since_last_value_change':changed,'seconds_to_target_publication':None,'countdown_missing_reason':'NO_ASOF_VERIFIED_CALENDAR',
      'expiry_at':expiry.isoformat() if expiry<=cutoff or r['expiry_policy']!='ORIGINAL_TARGET_PUBLICATION' else None,
      'expiry_policy':r['expiry_policy'],'source_url':r['source_url'],'original_sha256':r['original_sha256'],'event_identity':r['event_identity']}
   stats['expired_values_exposed']+=int(expired and f['value'] is not None);features[g]=f
  stats['bars']+=1;stats['new_forecast_events']+=len(events);previous_cutoff=cutoff
  yield {'contract_version':VERSION,'provider':'FXCM','instrument':'EURUSD','market_type':'FOREX','cutoff_utc':cutoff.isoformat(),
         'features':features,'source_missing_masks':{s:not any(f['source']==s and not f['missing'] for f in features.values()) for s in mask_sources},
         'real_forecast_events_since_previous_cutoff':events},stats

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--records',required=True);p.add_argument('--bars',required=True);p.add_argument('--outdir',required=True);p.add_argument('--mode',choices=('CONSERVATIVE','DOCUMENTED'),default='CONSERVATIVE');p.add_argument('--source-config',default=str(Path(__file__).with_name('model100_v1_sources.json')));a=p.parse_args()
 policy=None
 if a.source_config:
  try:from .model100_source_policy import load_policy
  except ImportError:from .model100_source_policy import load_policy
  policy=load_policy(a.source_config)
 records=[json.loads(s) for s in Path(a.records).open()];bars=[json.loads(s) for s in Path(a.bars).open()];rows,stats=compile_rows(records,policy);out=Path(a.outdir);out.mkdir(exist_ok=True,parents=True)
 stats['source_config_sha256']=hashlib.sha256(Path(a.source_config).read_bytes()).hexdigest() if a.source_config else None
 with (out/'FEATUREBUILDER_INPUT.jsonl').open('w') as f:
  for r in rows:f.write(json.dumps(r)+'\n')
 example=[];audit={}
 with gzip.open(out/'FEATUREBUILDER_H1.jsonl.gz','wt') as f:
  for row,audit in build_h1([r for r in rows if a.mode=='CONSERVATIVE' or r['temporal_status']=='H1_DOCUMENTED'],bars,policy):
   f.write(json.dumps(row,separators=(',',':'))+'\n')
   if row['real_forecast_events_since_previous_cutoff'] and len(example)<3:example.append(row)
 (out/'FEATUREBUILDER_EXAMPLE.json').write_text(json.dumps(example,indent=2));stats['h1_audit']=audit;stats['mode']=a.mode;stats['records_sha256']=hashlib.sha256(Path(a.records).read_bytes()).hexdigest();stats['bars_sha256']=hashlib.sha256(Path(a.bars).read_bytes()).hexdigest();(out/'FEATUREBUILDER_AUDIT.json').write_text(json.dumps(stats,indent=2));print(json.dumps(stats))
