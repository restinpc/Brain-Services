"""ZEW official monthly expectation tables; unknown release clocks stay unknown."""
import base64,gzip,hashlib,io,json,re
from pathlib import Path
try:
 import model100_primary_sources as c
 import model100_primary_archive as archive
 import model100_primary_leading_archives as leading
except ImportError:
 from work import model100_primary_sources as c,model100_primary_archive as archive,model100_primary_leading_archives as leading
VERSION='model100-zew-tables-v1'
SPOOL='/tmp/model100-zew-official-pack.json.gz'
PUBLICATION_SPOOL='/tmp/model100-zew-publication-pack.json.gz'
HEADINGS={'Economic expectations':'ECONOMIC_EXPECTATIONS','Inflation rate':'INFLATION_EXPECTATIONS','Short-term interest rates':'SHORT_RATE_EXPECTATIONS','Long-term interest rates':'LONG_RATE_EXPECTATIONS','Exchange rates (vs. Euro)':'FX_EXPECTATIONS','Sectors':None,'Current economic situation':None,'Stock market indices':None}
REGIONS={'Euro area':'EURO_AREA','Germany':'GERMANY','USA':'UNITED_STATES','China':'CHINA','Dollar':'USD_VS_EUR','Yuan':'CNY_VS_EUR'}
def parse_text(text,period):
 section=None;out=[];seen=set()
 for line in text.splitlines():
  compact=''.join(line.lower().split())
  matched=False
  for heading,metric in HEADINGS.items():
   h=''.join(heading.lower().split())
   if compact.startswith(h) or compact.startswith(''.join(ch*2 for ch in h)):
    section=metric;matched=True;break
  if matched or not section:continue
  for label,region in REGIONS.items():
   prefix=re.match(''.join(re.escape(ch)+r'\s*' for ch in label.replace(' ','')),line)
   if not prefix:continue
   tail=re.sub(r'\([^)]*\)','',line[prefix.end():])
   nums=re.findall(r'[-+]?\d+\.\d+',tail)
   if len(nums)!=4:raise ValueError('ZEW_EXPECTATION_COLUMNS')
   a,b,d,balance=map(float,nums)
   if abs(a+b+d-100)>.31 or abs(a-d-balance)>.21:raise ValueError('ZEW_EXPECTATION_BALANCE')
   key=(section,region)
   if key in seen:raise ValueError('ZEW_DUPLICATE_EXPECTATION')
   seen.add(key);out.append({'metric':section,'region':region,'value':balance,'period':period,'positive':a,'neutral':b,'negative':d})
 if len(out)!=16:raise ValueError('ZEW_EXPECTED_16_BALANCES:'+str(len(out)))
 return out

def store_item(db,now,item,pub=None):
 from pypdf import PdfReader
 source='ZEW'
 body=base64.b64decode(item['body']);digest=hashlib.sha256(body).hexdigest()
 if digest!=item['sha256']:raise ValueError('ZEW_ORIGINAL_SHA_MISMATCH')
 text='\n'.join(p.extract_text() or '' for p in PdfReader(io.BytesIO(body)).pages)
 values=parse_text(text,item['period']);url=item['url'];meta={'status':200,'content_type':'application/pdf'}
 if pub:
  html=base64.b64decode(pub['body'])
  if hashlib.sha256(html).hexdigest()!=pub['sha256'] or not(pub.get('dateline_confirmed') and pub.get('table_link_confirmed')):raise ValueError('ZEW_PUBLICATION_PROOF_INVALID')
  archive._store_document(db,source,'zew-publication:'+item['period'],pub['url'],html,{'status':200,'content_type':'text/html'},now,'ZEW original dated release','ZEW_EXPECTATIONS_RELEASE',extra={'published_day':pub['published_day'],'relay_http_fetch':item.get('relay_http_fetch',True),'source_published_at_unknown':True,'table_sha256':digest})
  item={**item,'published_day':pub['published_day'],'publication_day_evidence_url':pub['url']}
 proof={'relay_http_fetch':item.get('relay_http_fetch',True),'transport_path':'authorized local official HTTP200 -> B1 SHA-verified spool' if item.get('relay_http_fetch',True) else 'B1 direct HTTP','rights_status':'RIGHTS_UNVERIFIED','published_day':item.get('published_day'),'publication_day_evidence_url':item.get('publication_day_evidence_url'),'source_published_at_unknown':True,'survey_horizon':'NEXT_SIX_MONTHS','survey_month_is_not_publication_day':True,'publication_html_sha256':pub['sha256'] if pub else None,'publication_day_proof':'ORIGINAL_DATED_RELEASE_AND_MONTHLY_TABLE_ASSOCIATION' if pub else 'JANUARY_ORIGINAL_HTML'}
 archive._store_document(db,source,'zew-original:'+item['period'],url,body,meta,now,'ZEW monthly survey','ZEW_EXPECTATIONS',extra=proof)
 for v in values:
  row=c.base_row(source,'zew-expectations:'+v['metric']+':'+v['region']+':'+item['period'],'FORECAST_VINTAGE',url,body,meta,now,None,'DAY_ONLY','DAY_ONLY',event_type=v['metric'],region=v['region'],observation_period_start=item['period'],numeric_value=v['value'],unit='SURVEY_BALANCE_PERCENTAGE_POINTS',extra_json={**proof,**v,'value_class':'EXPECTATION_NOT_ACTUAL','is_consensus':False,'forecast_date':item.get('published_day')})
  row['parser_version']=VERSION;c.store(db,row)
 return 16

def collect(db,now):
 from pypdf import PdfReader
 source='ZEW';s=leading.state(db,source)
 publications={}
 if Path(PUBLICATION_SPOOL).exists():
  publications={r['period']:r for r in json.loads(gzip.decompress(Path(PUBLICATION_SPOOL).read_bytes()))['documents']}
  if s.get('publication_revision')!=2:s={'cursor':0,'publication_revision':2}
 if s.get('complete'):
  try:import model100_primary_leading_live as live
  except ImportError:from work import model100_primary_leading_live as live
  return live.collect(db,source,now,s,store_item)
 try:
  pack=json.loads(gzip.decompress(Path(SPOOL).read_bytes()));items=pack['documents'];start=s.get('cursor',0)
  # A bounded single original per fenced run; preserve each PDF independently.
  for item in items[start:start+1]:
   store_item(db,now,item,publications.get(item['period']))
   start+=1
  s.update(cursor=start,total=len(items),complete=start==len(items),checked_day=now.date().isoformat());leading.checkpoint(db,source,s,now)
  return {'source':source,'status':'SUCCESS_NEW_DATA' if start>s.get('previous_cursor',-1) else 'SUCCESS_UNCHANGED','originals_processed':start,'remaining_items':len(items)-start,'history_values':start*16}
 except Exception as e:return {'source':source,'status':c._exception_outcome(e),'error':str(e)[:180],'remaining_items':max(0,s.get('total',22)-s.get('cursor',0))}
