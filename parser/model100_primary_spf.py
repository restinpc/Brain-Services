"""ECB SPF quarterly forecast history; native ID166 only, no inferred source hour."""
import json,hashlib,re,time
import xml.etree.ElementTree as ET
from datetime import datetime,date,timedelta
try:
 import model100_primary_sources as common
 import model100_primary_archive as archive
except ImportError:
 from work import model100_primary_sources as common,model100_primary_archive as archive
SOURCE='ECB_SPF';VERSION='model100-spf-v1';KEY='control:spf-quarterly'
# Results-published column, not survey/questionnaire dates, from official ECB PDF.
CALENDAR_URL='https://www.ecb.europa.eu/stats/prices/indic/forecast/shared/files/SPF_rounds_dates.pdf'
DAYS=dict(zip(('2025-Q1','2025-Q2','2025-Q3','2025-Q4','2026-Q1','2026-Q2','2026-Q3','2026-Q4'),('2025-01-31','2025-04-22','2025-07-25','2025-10-31','2026-02-06','2026-05-04','2026-07-24','2026-10-30')))
SERIES=('Q.U2.HICP.POINT.LT.Q.AVG','Q.U2.CORE.POINT.LT.Q.AVG','Q.U2.RGDP.POINT.LT.Q.AVG','Q.U2.RGDP.POINT.P9M.Q.AVG','Q.U2.RGDP.POINT.P21M.Q.AVG')

def tag(e):return e.tag.split('}')[-1]
def children(e,name):return [x for x in e if tag(x)==name]
def parse(body):
 root=ET.fromstring(body);result=[]
 for ds in root.iter():
  if tag(ds)!='DataSet':continue
  update=ds.get('validFromDate');clock=common.parse_explicit_datetime(update)
  for series in children(ds,'Series'):
   dims={v.get('id'):v.get('value') for k in children(series,'SeriesKey') for v in k}
   for obs in children(series,'Obs'):
    dim=children(obs,'ObsDimension');val=children(obs,'ObsValue')
    if not dim or not val:continue
    rnd=dim[0].get('value','')
    if not re.fullmatch(r'20\d{2}-Q[1-4]',rnd) or int(rnd[:4])<2025:continue
    value=float(val[0].get('value'));expected=DAYS.get(rnd)
    # API history date must agree with independently documented round release DAY.
    original=expected is not None and clock is not None and clock.date().isoformat()==expected
    result.append({'round':rnd,'value':value,'dimensions':dims,'published_day':expected if original else None,
     'round_results_published_day':expected,'updated_at_raw':update,'publication_hour_unknown':True,
     'later_update_not_original_round':expected is not None and not original,'publication_day_unverified':expected is None})
 return result

def state(db):
 cur=db.cursor()
 try:
  cur.execute(f'SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',(SOURCE,KEY));r=cur.fetchone();return json.loads(r[0]) if r else {}
 finally:cur.close()
def save_state(db,s,now):
 common.store(db,common.base_row(SOURCE,KEY,'RUN_CONTROL','brain://model100/spf-quarterly',b'{"spf_checkpoint":1}',{'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',event_type='BACKFILL_CHECKPOINT',extra_json=s))
def collect(db,now):
 s=state(db);s.setdefault('done',[]);s.setdefault('failed',{});result={'source':SOURCE,'documents':0,'values':0,'errors':[]}
 pending=[key for key in SERIES if key not in s['done']]
 if not pending and (not s.get('last_poll_cycle_utc') or now-datetime.fromisoformat(s['last_poll_cycle_utc'])>=timedelta(hours=6)):
  s['done']=[];pending=list(SERIES);s['last_poll_cycle_utc']=now.isoformat()
 for key in pending[:2]:
  retry=s['failed'].get(key,{}).get('retry_at')
  if retry and now<datetime.fromisoformat(retry):continue
  url='https://data-api.ecb.europa.eu/service/data/SPF/'+key+'?startPeriod=2025&includeHistory=true'
  try:
   cur=db.cursor()
   try:
    cur.execute(f'SELECT content_sha256 FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',(SOURCE,'spf:'+key));previous=cur.fetchone()
   finally:cur.close()
   body,meta=common.conditional_fetch(db,SOURCE,'spf:'+key,url,timeout=8,cap=1000000)
   values=parse(body)
   if not values:raise ValueError('SPF_QUARTERLY_FORECAST_HISTORY_EMPTY')
   archive._save_index(db,SOURCE,'spf:'+key,url,body,meta,now)
   if previous and previous[0]==hashlib.sha256(body).hexdigest():
    s['done'].append(key);s['failed'].pop(key,None);result['documents']+=1;save_state(db,s,now);continue
   for v in values:
    rnd=v['round'];d=v['dimensions'];y,q=map(int,re.fullmatch(r'(\d{4})-Q(\d)',rnd).groups());period=date(y,(q-1)*3+1,1)
    ex={**v,'series_id':key,'vintage_code':rnd,'forecast_not_actual':True,'value_class':'SURVEY_FORECAST',
        'publication_day_evidence_url':CALENDAR_URL,'calendar_evidence_method':'Results published column independently inspected; calendar PDF B1 fetch currently TLS timeout',
        'api_valid_from_is_not_source_publication_hour':True,'reference_period_is_survey_round_not_target_period':True,
        'target_horizon':d.get('FCT_HORIZON'),'ml_permission':'ECB statistical reuse policy; attribution required; verify applicable dataset exceptions',
        'published_day':v['published_day']}
    r=common.base_row(SOURCE,'spf:'+key+':'+rnd+':'+str(v['updated_at_raw']),'FORECAST_VINTAGE',url,body,meta,now,None,'DAY_ONLY','DAY_ONLY',
       title='ECB SPF '+key+' '+rnd,event_type='SPF_'+d.get('FCT_TOPIC','UNKNOWN'),region='EURO_AREA',observation_period_start=period.isoformat(),vintage_month=period.isoformat(),numeric_value=v['value'],unit='PERCENT',reference_timezone='Europe/Frankfurt',extra_json=ex)
    r['parser_version']=VERSION;common.store(db,r);result['values']+=1
   s['done'].append(key);s['failed'].pop(key,None);result['documents']+=1
  except Exception as e:
   try:from model100_primary_ifo_releases import retry_info
   except ImportError:from work.model100_primary_ifo_releases import retry_info
   s['failed'][key]=retry_info(e,now);archive._record_fetch_status(db,SOURCE,'spf:'+key,url,now,getattr(e,'code',None),type(e).__name__+':'+str(e)[:150]);result['errors'].append({'series':key,'status':s['failed'][key]['status']})
  save_state(db,s,now);time.sleep(.5)
 s['checked_day']=now.date().isoformat();save_state(db,s,now)
 result['remaining_items']=len(set(SERIES)-set(s['done']));result['status']=('PARTIAL_COVERAGE' if any(e['status'] not in ('NETWORK_TIMEOUT','RATE_LIMITED') for e in result['errors']) else 'BACKFILL_INCOMPLETE') if result['remaining_items'] else 'SUCCESS_NEW_DATA' if result['values'] else 'SUCCESS_UNCHANGED'
 return result
