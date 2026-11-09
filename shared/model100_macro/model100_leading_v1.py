"""Read-only leading candidates; no DB writes, training, or implicit ML licence.

DAY_ONLY context policy is next day 12:00UTC, later than the end of relevant
European/US source civil days. This is a conservative policy, NOT a source hour.
Strict H1 contains only original documented publication clocks.
"""
import argparse,json,gzip
from datetime import datetime,date,time,timedelta,timezone
from pathlib import Path
try:
 from parser.model100_semantics import semantic_versions
 from .model100_h1_builder import clock
except ImportError:
 from parser.model100_semantics import semantic_versions
 from .model100_h1_builder import clock

def metadata(r):
 x=r.get('extra_json') or {};return json.loads(x) if isinstance(x,str) else x

def available_before(row,cutoff):return clock(row['available_at'])<clock(cutoff)

def prepare(records):
 candidates=[]
 for r in records:
  if r.get('numeric_value') is None or r.get('asof_class')=='UNUSABLE':continue
  x=metadata(r);s=r['source_id'];kind=r['record_kind'];role=None;day=None
  if s=='IFO_VINTAGE' and r['record_key'].startswith('ifo-original-table:') and x.get('series')=='Expectations' and kind=='FIRST_ESTIMATE':
   role='BUSINESS_EXPECTATIONS';day=x.get('document_date')
  elif s=='GDP_NOW_ATL' and kind=='FORECAST_VINTAGE':
   role='GDP_FORECAST';day=x.get('forecast_date')
  elif s=='ALFRED' and x.get('series_id')=='GDPNOW' and kind=='FORECAST_VINTAGE':
   role='GDP_FORECAST_ALFRED';day=x.get('vintage_date')
  elif s=='ALFRED' and x.get('series_id') in ('ADPMNUSNERSA','ADPWNUSNERSA') and kind in ('FIRST_ESTIMATE','REVISION'):
   role='PRIVATE_EMPLOYMENT_PROXY';day=x.get('vintage_date')
  elif s in ('NYFED_NOW','CLEVELAND_NOW') and kind=='FORECAST_VINTAGE':
   role='GDP_FORECAST_NYFED' if s=='NYFED_NOW' else 'INFLATION_FORECAST';day=x.get('forecast_date')
  elif s=='ZEW' and kind=='FORECAST_VINTAGE' and x.get('published_day'):
   role='ANALYST_EXPECTATIONS';day=x['published_day']
  elif s=='ECB_SPF' and kind=='FORECAST_VINTAGE' and x.get('published_day') and not x.get('later_update_not_original_round'):
   role='PROFESSIONAL_FORECAST';day=x['published_day']
  if role:candidates.append({**r,'_role':role,'_day':day})
 result=[]
 for r in semantic_versions(candidates):
  pub=clock(r.get('source_published_at_utc'));quality=r.get('asof_class')
  strict=pub is not None and quality in ('HISTORICAL_DOCUMENTED','HISTORICAL_REPLAY','CERTIFIED_LIVE') and r.get('release_clock_evidence') not in ('SCHEDULED','SCHEDULE_ONLY')
  if strict:available=pub;policy='DOCUMENTED_PUBLICATION'
  else:
   try:day=date.fromisoformat(str(r['_day'])[:10])
   except ValueError:continue
   available=datetime.combine(day+timedelta(days=1),time(12),tzinfo=timezone.utc);policy='CONSERVATIVE_NEXT_DAY_12UTC_CONTEXT_ONLY'
  if r['semantic_amendment']:
   available=max(available,clock(r['first_received_at_utc']));policy+=';AMENDMENT_NOT_BACKDATED'
  x=metadata(r)
  result.append({'source':r['source_id'],'indicator':x.get('series_id') or x.get('series') or r['event_type'],
   'role':r['_role'],'region':r.get('region'),'reference_period':r.get('observation_period_start'),'forecast_vintage_day':r['_day'],
   'value':str(r['numeric_value']),'unit':r.get('unit'),'edition':r['record_kind'],
   'source_published_at':pub.isoformat() if pub else None,'first_received_at':r['first_received_at_utc'],
   'available_at':available.isoformat(),'availability_policy':policy,
   'technical_quality':'READY_LEADING_H1' if strict else 'READY_LEADING_CONTEXT',
   'is_consensus':False,'ml_permission':'ML_TERMS_RECORDED_NOT_COLLECTION_BLOCKER' if r['source_id']=='ALFRED' else 'ECB_FREE_REUSE_WITH_ATTRIBUTION' if r['source_id']=='ECB_SPF' else 'RIGHTS_UNVERIFIED',
   'target_horizon':x.get('target_horizon'),
   'source_url':r['canonical_url'],'original_sha256':r['content_sha256'],'event_identity':r['event_identity']})
 return sorted(result,key=lambda r:(r['available_at'],r['source'],str(r['reference_period'])))

def target_releases(records):
 targets={}
 for r in records:
  if r.get('asof_class')=='UNUSABLE' or r.get('numeric_value') is None:continue
  x=metadata(r);pub=clock(r.get('source_published_at_utc'))
  if not pub or r.get('release_clock_evidence') in ('SCHEDULED','SCHEDULE_ONLY'):continue
  role=None
  if r['source_id']=='BLS_RELEASES' and r['event_type']=='NFP_CHANGE' and r['record_kind']=='FIRST_ESTIMATE':role='PRIVATE_EMPLOYMENT_PROXY'
  if r['source_id']=='BEA_RELEASES' and r['event_type']=='BEA_GDP_REAL_QOQ_ANNUALIZED' and str(x.get('estimate_stage','')).upper() in ('ADVANCE','INITIAL'):role='GDP_FORECAST'
  if r['source_id']=='BLS_RELEASES' and r['event_type'] in ('CPI_HEADLINE_MOM','CPI_CORE_MOM') and r['record_kind']=='FIRST_ESTIMATE':role='INFLATION_FORECAST_'+r['event_type']
  if role:
   key=(role,str(r.get('observation_period_start'))[:10]);prior=targets.get(key)
   if prior is None or pub<prior['published']:targets[key]={'published':pub,'url':r['canonical_url'],'sha':r['content_sha256']}
 return targets

def audit(records,rows):
 targets=target_releases(records);pairs=[]
 for r in rows:
  role='GDP_FORECAST' if r['role'].startswith('GDP_FORECAST') else r['role']
  if r['indicator']=='ADPWNUSNERSA':continue  # Weekly period is not a monthly NFP target.
  if role=='INFLATION_FORECAST':
   metric={'CPI_INFLATION':'CPI_HEADLINE_MOM','CORE_CPI_INFLATION':'CPI_CORE_MOM'}.get(r['indicator'])
   if not metric:continue  # PCE target clocks require independent original-release proof.
   role+='_'+metric
  target=targets.get((role,str(r['reference_period'])[:10]))
  if target:
   lead=(target['published']-clock(r['available_at'])).total_seconds()
   pairs.append({**r,'target_release_at':target['published'].isoformat(),'target_url':target['url'],'target_sha256':target['sha'],'lead_seconds':lead,'eligible_before_target':lead>0})
 return pairs

def context_h1(rows,bars):
 """Explicit context replay; never mixes a DAY_ONLY policy with strict evidence."""
 ordered=sorted(rows,key=lambda r:clock(r['available_at']));cursor=0;state={}
 stats={'bars':0,'future_leakage_violations':0,'mode':'CONSERVATIVE_LEADING_CONTEXT','not_certified_live':True}
 for bar in sorted(bars,key=lambda b:clock(b['window_end_utc'])):
  cutoff=clock(bar['window_end_utc'])
  while cursor<len(ordered) and clock(ordered[cursor]['available_at'])<=cutoff:
   r=ordered[cursor];key='|'.join(str(r.get(k) or '') for k in ('source','indicator','region','reference_period','target_horizon'))
   state[key]=r;cursor+=1
  features={}
  for key,r in state.items():
   available=clock(r['available_at']);stats['future_leakage_violations']+=int(available>cutoff)
   expired=bool(r.get('target_release_at') and clock(r['target_release_at'])<=cutoff)
   features[key]={**r,'value':None if expired else r['value'],'missing':expired,'expired_target':expired,'age_seconds':(cutoff-available).total_seconds(),'stale_after_45d':(cutoff-available).total_seconds()>45*86400}
  stats['bars']+=1
  yield {'provider':'FXCM','instrument':'EURUSD','cutoff':cutoff.isoformat(),'mode':stats['mode'],'features':features},stats

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--records',required=True);p.add_argument('--outdir',required=True);p.add_argument('--bars');a=p.parse_args()
 records=[json.loads(s) for s in Path(a.records).open(encoding='utf-8')];rows=prepare(records);pairs=audit(records,rows)
 pairmap={r['event_identity']:r for r in pairs}
 rows=[{**r,'target_join_status':'VERIFIED_BEFORE_TARGET' if pairmap[r['event_identity']]['eligible_before_target'] else 'AFTER_TARGET','target_release_at':pairmap[r['event_identity']]['target_release_at']} if r['event_identity'] in pairmap else {**r,'target_join_status':'TARGET_NOT_VERIFIED'} for r in rows]
 out=Path(a.outdir);out.mkdir(parents=True,exist_ok=True)
 for name,data in [('LEADING_CANDIDATES',rows),('LEADING_TARGET_JOIN',pairs)]:
  with (out/(name+'.jsonl')).open('w',encoding='utf-8') as f:
   for r in data:f.write(json.dumps(r,default=str)+'\n')
 summary={'candidates':len(rows),'target_pairs':len(pairs),'before_target':sum(r['eligible_before_target'] for r in pairs),'technical_sources':sorted({r['source'] for r in rows}),'rights_cleared_v1_sources':len({r['source'] for r in rows if r['ml_permission']=='ECB_FREE_REUSE_WITH_ATTRIBUTION'}),'training_not_performed':True,'rights_assessed_per_source':True,'collection_scope':'INFORMATION_ONLY_NO_TRAINING'}
 if a.bars:
  bars=[json.loads(s) for s in Path(a.bars).open(encoding='utf-8')];stats={}
  with gzip.open(out/'LEADING_H1_CONTEXT.jsonl.gz','wt',encoding='utf-8') as f:
   for row,stats in context_h1([r for r in rows if r.get('target_join_status')!='AFTER_TARGET'],bars):f.write(json.dumps(row,default=str,separators=(',',':'))+'\n')
  summary['h1_context_audit']=stats
 (out/'LEADING_SUMMARY.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary))
