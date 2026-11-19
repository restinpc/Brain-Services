"""Official NY Fed SME rate-path medians. Questionnaire release != results release."""
import base64,gzip,hashlib,io,json,math,re,zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
try:
 import model100_primary_sources as c
 import model100_primary_archive as archive
 import model100_primary_leading_archives as leading
except ImportError:
 from work import model100_primary_sources as c,model100_primary_archive as archive,model100_primary_leading_archives as leading
SPOOL='/tmp/model100-sme-official-pack.json.gz'
def parse(body):
 z=zipfile.ZipFile(io.BytesIO(body));ss=c.excel_shared_strings(z);sheets=c.excel_sheets(z);out=[];seen=set()
 for name,path in sheets:
  if path.startswith('xl/xl/'):path=path[3:]
  rows=ET.fromstring(z.read(path)).findall('m:sheetData/m:row',c.NS);headers={}
  for row in rows:
   cells={c.col_index(x.get('r')):''.join(x.itertext()) if x.get('t')=='inlineStr' else c.excel_cell_value(x,ss) for x in row.findall('m:c',c.NS)}
   if cells.get(0)=='survey_release_date':headers=cells;continue
   if not headers:continue
   r={headers[k]:v for k,v in cells.items() if k in headers}
   if r.get('panel_type')!='Combined' or r.get('question_tag')!='fftr_pathofmodes' or r.get('aggregation')!='pctl50':continue
   # Public workbook stores rate levels as fractions; retain original and unit conversion.
   raw=float(r['aggregation_value']);v=raw*100
   if not math.isfinite(v) or not -5<=v<=30:raise ValueError('SME_RATE_RANGE')
   day=r.get('horizon_date','')
   if not re.fullmatch(r'20\d{2}-\d{2}-\d{2}',day):continue
   key=(r['survey_release_date'],r['value_tag'],day)
   if key in seen:raise ValueError('SME_DUPLICATE_RATE_HORIZON')
   seen.add(key);out.append({**r,'value_percent':v,'raw_fraction':raw})
 if not out:raise ValueError('SME_NO_COMBINED_RATE_MEDIANS')
 return out

def store_item(db,now,item,pub=None):
 source='NYFED_SME'
 count=0
 body=base64.b64decode(item['body']);digest=hashlib.sha256(body).hexdigest()
 if digest!=item['sha256']:raise ValueError('SME_ORIGINAL_SHA_MISMATCH')
 values=parse(body);url=item['url'];meta={'status':200,'content_type':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
 proof={'rights_status':'RIGHTS_UNVERIFIED','relay_http_fetch':item.get('relay_http_fetch',True),'original_http_fetch_clock':None,'questionnaire_distribution_is_not_results_publication':True,'results_published_after_corresponding_FOMC':True,'results_publication_day_unknown':True,'excluded_from_historical_leading_replay':True,'publication_policy_evidence_url':'https://www.newyorkfed.org/markets/market-intelligence/survey-of-market-expectations'}
 archive._store_document(db,source,'sme-original:'+item['file'],url,body,meta,now,'NY Fed SME aggregate data','SURVEY_RATE_EXPECTATIONS',extra=proof)
 for v in values:
  key='sme-rate:'+v['survey_release_date']+':'+v['value_tag']+':'+v['horizon_date']
  r=c.base_row(source,key,'FORECAST_VINTAGE',url,body,meta,now,None,'DAY_ONLY','DAY_ONLY',event_type='SURVEY_FED_FUNDS_PATH_MEDIAN',region='UNITED_STATES',observation_period_start=v['horizon_date'],numeric_value=v['value_percent'],unit='PERCENT_RATE',extra_json={**proof,'series_id':'SME_FFTR_COMBINED_MEDIAN','questionnaire_distribution_date':v['survey_release_date'],'survey_due_date':v['survey_due_date'],'target_horizon':v['horizon_date'],'raw_fraction':v['raw_fraction'],'aggregation':'pctl50','panel_type':'Combined','value_tag':v['value_tag'],'value_class':'SURVEY_EXPECTATION','is_consensus':False})
  c.store(db,r);count+=1
 return count

def collect(db,now):
 source='NYFED_SME';s=leading.state(db,source)
 if s.get('complete'):
  try:import model100_primary_leading_live as live
  except ImportError:from work import model100_primary_leading_live as live
  return live.collect(db,source,now,s,store_item)
 try:
  items=json.loads(gzip.decompress(Path(SPOOL).read_bytes()))['documents'];start=s.get('cursor',0);count=0
  for item in items[start:start+1]:
   count+=store_item(db,now,item)
   start+=1
  s.update(cursor=start,total=len(items),complete=start==len(items));leading.checkpoint(db,source,s,now)
  return {'source':source,'status':'SUCCESS_NEW_DATA','new_values':count,'remaining_items':len(items)-start,'originals_processed':start,'results_publication_clock':'UNVERIFIED'}
 except Exception as e:return {'source':source,'status':c._exception_outcome(e),'error':str(e)[:150],'remaining_items':max(0,s.get('total',14)-s.get('cursor',0))}
