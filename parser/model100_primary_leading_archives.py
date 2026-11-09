"""Official NY Fed workbook/Cleveland daily chart archives, native ID166 writes.
Local HTTP relay is explicit provenance, never represented as a B1 HTTP fetch.
"""
import gzip,hashlib,io,json,math,re,zipfile
import xml.etree.ElementTree as ET
from datetime import date,datetime
from pathlib import Path
try:
 import model100_primary_sources as c
 import model100_primary_archive as archive
except ImportError:
 from work import model100_primary_sources as c,model100_primary_archive as archive
VERSION='model100-leading-archives-v1'
CONFIG={
 'CLEVELAND_NOW':('https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json','/tmp/model100-cleveland-original.json','9253b45d839c5fe1e5fd1e2d583f9f7aa48bedeb02b4a4060a04041d598c17cd'),
 'NYFED_NOW':('https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/NowCast/downloads/New-York-Fed-Staff-Nowcast_download_data','/tmp/model100-nyfed-original.xlsx','70a1feea10f1d45c53ea1e89960be044d9289da9d43e81506df042962cfc4124')}
START=date(2025,1,1)
def parse_cleveland(body,today):
 out=[];keys=set()
 for chart in json.loads(body):
  match=re.fullmatch(r'(20\d{2})-(\d{1,2})',chart.get('chart',{}).get('subcaption',''))
  if not match:continue
  y,month=map(int,match.groups());target=date(y,month,1)
  if target<date(2024,12,1) or target>today:continue
  labels=[x['label'] for x in chart['categories'][0]['category'] if not x.get('vline')]
  days=[];year=y;last=0
  for label in labels:
   m=re.fullmatch(r'(\d{2})/(\d{2})',label)
   if not m:raise ValueError('CLEVELAND_UNDATED_LABEL')
   mon,day=map(int,m.groups())
   if last and mon<last:year+=1
   stamp=date(year,mon,day);days.append(stamp);last=mon
  for series in chart['dataset']:
   metric=series['seriesname']
   if metric not in ('CPI Inflation','Core CPI Inflation','PCE Inflation','Core PCE Inflation'):continue
   if len(series['data'])!=len(days):raise ValueError('CLEVELAND_CHART_ALIGNMENT')
   for day,cell in zip(days,series['data']):
    if not START<=day<=today or str(cell.get('value','')).strip()=='':continue
    value=float(cell['value'])
    if not math.isfinite(value) or abs(value)>30:raise ValueError('CLEVELAND_VALUE_RANGE')
    key=(metric,target,day)
    if key in keys:raise ValueError('CLEVELAND_DUPLICATE_FORECAST')
    keys.add(key);out.append({'metric':metric,'target':target.isoformat(),'forecast_day':day.isoformat(),'value':value,'unit':'PERCENT_MOM_SA'})
 return sorted(out,key=lambda r:(r['forecast_day'],r['target'],r['metric']))

def parse_nyfed(body,today):
 z=zipfile.ZipFile(io.BytesIO(body));strings=c.excel_shared_strings(z);sheets=dict(c.excel_sheets(z))
 if 'Forecasts By Quarter' not in sheets:raise ValueError('NYFED_FORECAST_WORKSHEET_MISSING')
 rows=ET.fromstring(z.read(sheets['Forecasts By Quarter'])).findall('m:sheetData/m:row',c.NS)
 headers={};out=[];seen=set()
 for row in rows:
  cells={c.col_index(x.get('r','A1')):c.excel_cell_value(x,strings) for x in row.findall('m:c',c.NS)}
  if cells.get(0)=='Forecast Date':headers={k:v for k,v in cells.items() if re.fullmatch(r'20\d{2}Q[1-4]',v or '')};continue
  if not headers or not cells.get(0):continue
  raw=cells[0]
  try:day=c.excel_serial_date(raw)
  except (ValueError,OverflowError):continue
  if not START<=day<=today:continue
  for col,q in headers.items():
   raw=cells.get(col,'')
   if not raw or raw in ('#N/A','NA','N/A'):continue
   value=float(raw)
   if not math.isfinite(value) or abs(value)>100:raise ValueError('NYFED_VALUE_RANGE')
   y,quarter=int(q[:4]),int(q[-1]);target=date(y,3*quarter-2,1)
   key=(day,target)
   if key in seen:raise ValueError('NYFED_DUPLICATE_FORECAST')
   seen.add(key);out.append({'metric':'GDP_NOWCAST','target':target.isoformat(),'forecast_day':day.isoformat(),'value':value,'unit':'PERCENT_QOQ_SAAR'})
 return sorted(out,key=lambda r:(r['forecast_day'],r['target']))

def state(db,source):
 cur=db.cursor()
 try:
  cur.execute(f'SELECT extra_json FROM `{c.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',(source,'control:leading-chart-archive'));r=cur.fetchone();return json.loads(r[0]) if r else {}
 finally:cur.close()
def checkpoint(db,source,s,now):
 c.store(db,c.base_row(source,'control:leading-chart-archive','RUN_CONTROL','brain://model100/leading-chart-archive',b'{"leading_checkpoint":1}',{'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',event_type='BACKFILL_CHECKPOINT',extra_json=s))
def collect(db,source,now):
 url,spool,expected=CONFIG[source];s=state(db,source)
 try:
  if not s.get('complete') and Path(spool).exists() and s.get('sha256',expected)==expected:
   body=Path(spool).read_bytes();digest=hashlib.sha256(body).hexdigest()
   if digest!=expected:raise ValueError('RELAY_ORIGINAL_SHA_MISMATCH')
   meta={'status':200,'content_type':'application/json' if source=='CLEVELAND_NOW' else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','url':url}
   relay=True
  else:
   body,meta=c.conditional_fetch(db,source,'leading-archive:original',url,timeout=8,cap=15000000);digest=hashlib.sha256(body).hexdigest();relay=False
  items=(parse_cleveland if source=='CLEVELAND_NOW' else parse_nyfed)(body,now.date())
  if not items:raise ValueError('NO_HISTORICAL_FORECASTS')
  history_values=len(items)
  if s.get('sha256')!=digest:
   cur=db.cursor()
   try:
    cur.execute(f'SELECT record_key,numeric_value FROM `{c.TABLE}` WHERE source_id=%s AND record_kind=%s ORDER BY first_received_at_utc',(source,'FORECAST_VINTAGE'));known={k:float(v) for k,v in cur.fetchall()}
   finally:cur.close()
   pending=[]
   for v in items:
    key='leading:'+v['metric']+':'+v['target']+':'+v['forecast_day']
    if key not in known or not math.isclose(known[key],v['value'],abs_tol=1e-10,rel_tol=0):pending.append(key)
   s={'sha256':digest,'cursor':0,'pending_keys':pending}
  if 'pending_keys' in s:
   keys=set(s['pending_keys']);items=[v for v in items if 'leading:'+v['metric']+':'+v['target']+':'+v['forecast_day'] in keys]
  proof={'official_archive':True,'relay_http_fetch':relay,'transport_path':'authorized local workstation HTTP200 -> SHA-verified B1 spool' if relay else 'B1 HTTP',
    'original_http_fetch_at_utc':None,
    'relay_fetch_clock_is_approximate':relay,'first_received_is_actual_B1_ingestion':True,
    'archive_update_clock_not_forecast_publication_clock':True,'original_first_snapshot_bytes_not_claimed':True,
    'history_is_provider_published_forecast_sequence':True,'rights_status':'RIGHTS_UNVERIFIED'}
  archive._store_document(db,source,'leading-archive:original',url,body,meta,now,'Official historical forecast sequence','FORECAST_ARCHIVE',extra=proof)
  start=s['cursor'];stop=min(start+400,len(items))
  for v in items[start:stop]:
   key='leading:'+v['metric']+':'+v['target']+':'+v['forecast_day']
   r=c.base_row(source,key,'FORECAST_VINTAGE',url,body,meta,now,None,'DAY_ONLY','DAY_ONLY',title='Historical '+v['metric']+' forecast',event_type=v['metric'].upper().replace(' ','_'),region='UNITED_STATES',observation_period_start=v['target'],vintage_month=v['forecast_day'][:7]+'-01',numeric_value=v['value'],unit=v['unit'],reference_timezone='America/New_York',extra_json={**proof,'metric':v['metric'],'forecast_date':v['forecast_day'],'target_period':v['target'],'vintage_date':v['forecast_day'],'value_class':'FORECAST_NOT_ACTUAL','is_consensus':False,'source_published_at_unknown':True})
   r['parser_version']=VERSION;c.store(db,r)
  s.update(cursor=stop,total=len(items),complete=stop==len(items),checked_day=now.date().isoformat());checkpoint(db,source,s,now)
  return {'source':source,'status':'BACKFILL_INCOMPLETE' if stop<len(items) else 'SUCCESS_NEW_DATA' if stop>start else 'SUCCESS_UNCHANGED','new_values':stop-start,'history_values':history_values,'remaining_items':len(items)-stop,'sha256':digest,'relay':relay}
 except Exception as e:
  archive._record_fetch_status(db,source,'leading-archive',url,now,getattr(e,'code',None),type(e).__name__+':'+str(e)[:150])
  return {'source':source,'status':c._exception_outcome(e),'remaining_items':max(0,s.get('total',0)-s.get('cursor',0)),'error':type(e).__name__+':'+str(e)[:120]}
