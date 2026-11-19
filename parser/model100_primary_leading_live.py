"""Bounded official discovery for existing ZEW/SME collectors. Native ID166 only."""
import base64,hashlib,json,re,html
from datetime import datetime,timedelta,date
from urllib.parse import urljoin
try:
 import model100_primary_sources as c
 import model100_primary_archive as a
 import model100_primary_leading_archives as leading
 import model100_primary_ifo_releases as retry
except ImportError:
 from work import model100_primary_sources as c,model100_primary_archive as a,model100_primary_leading_archives as leading,model100_primary_ifo_releases as retry
VERSION='model100-leading-live-v2'
URLS={'ZEW':'https://www.zew.de/en/press/latest-press-releases','NYFED_SME':'https://www.newyorkfed.org/markets/market-intelligence/survey-of-market-expectations'}

def discover(source,body,today):
 text=body.decode('utf-8','replace');out=[]
 if source=='ZEW':
  pat=r'<span class="c-category-label">(.*?)</span>\s*<h3[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>'
  for cat,url,title in re.findall(pat,text,re.S):
   m=re.search(r'(\d{2})\.(\d{2})\.(20\d{2})',cat)
   if not m or 'ZEW Indicator of Economic Sentiment' not in cat or 'release dates' in html.unescape(title).lower():continue
   d,mon,y=m.groups();day=date(int(y),int(mon),int(d))
   if date(2025,1,1)<=day<=today:out.append({'period':f'{y}-{mon}-01','published_day':day.isoformat(),'url':urljoin(URLS[source],url),'title':html.unescape(title)})
 else:
  for path in sorted(set(re.findall(r'href=["\']([^"\']*/(20\d{2})/[^"\']*data\.xlsx)["\']',text))):
   url,y=path
   if 2025<=int(y)<=today.year:out.append({'url':urljoin(URLS[source],url),'file':url.rsplit('/',1)[-1]})
 return out

def table_link_confirmed(body, expected):
 parser=a.LinkParser();parser.feed(body.decode('utf-8','replace'))
 target=c.urllib.parse.urlparse(expected)
 for href,_,_ in parser.links:
  candidate=c.urllib.parse.urlparse(urljoin(URLS['ZEW'],href))
  if (candidate.scheme in ('http','https') and not candidate.username and not candidate.password
      and candidate.hostname==target.hostname and candidate.path==target.path
      and candidate.query==target.query and candidate.port in (None,80,443)):
   return True
 return False


def fetch(db,source,key,url,now):
 body,meta=c.conditional_fetch(db,source,key,url,timeout=6,cap=4000000)
 a._save_index(db,source,key,url,body,meta,now)
 return body,meta

def collect(db,source,now,s,store_item):
 next_retry=s.get('live_next_retry_at_utc')
 if s.get('live_failure')=='PARSE_ERROR' and s.get('live_parser_version')!=VERSION:next_retry=None
 if next_retry and now<datetime.fromisoformat(next_retry):return {'source':source,'status':'RETRY_DEFERRED','next_retry_at_utc':next_retry,'http_attempted':False,'live_status':s.get('live_status','LIVE_PARTIAL')}
 url=URLS[source]
 try:
  body,meta=fetch(db,source,'live-discovery:index',url,now)
  items=discover(source,body,now.date())
  if not items:raise ValueError('LIVE_INDEX_CONTAINS_NO_CONFIRMED_RESULT_LINKS')
  cur=db.cursor()
  try:
   cur.execute(f'SELECT DISTINCT canonical_url FROM `{c.TABLE}` WHERE source_id=%s AND record_kind=%s',(source,'FORECAST_VINTAGE'));known={r[0] for r in cur.fetchall()}
  finally:cur.close()
  def original_url(i):return ('https://download.zew.de/e_'+i['period'][5:7]+'_'+i['period'][:4]+'_Tabelle.pdf') if source=='ZEW' else i['url']
  pending=[i for i in items if original_url(i) not in known]
  # Review the latest original for amendments even when there are no new links.
  item=(sorted(pending,key=lambda i:i.get('published_day',i['url']))[0] if pending else sorted(items,key=lambda i:i.get('published_day',i['url']))[-1])
  if source=='ZEW':
   page,pm=fetch(db,source,'live-release:'+item['period'],item['url'],now)
   day=item['published_day'];dateline=day[8:10]+'.'+day[5:7]+'.'+day[:4]
   if dateline not in html.unescape(page.decode(errors='replace')):raise ValueError('ZEW_LIVE_DATELINE_NOT_CONFIRMED')
   pu=original_url(item);pdf,dm=fetch(db,source,'live-table:'+item['period'],pu,now)
   pub={**item,'body':base64.b64encode(page).decode(),'sha256':hashlib.sha256(page).hexdigest(),'dateline_confirmed':True,'table_link_confirmed':table_link_confirmed(page,pu)}
   if not pub['table_link_confirmed']:raise ValueError('ZEW_LIVE_TABLE_ASSOCIATION_UNCONFIRMED')
   doc={**item,'url':pu,'body':base64.b64encode(pdf).decode(),'sha256':hashlib.sha256(pdf).hexdigest(),'publication_day_evidence_url':item['url']}
  else:
   pdf,dm=fetch(db,source,'live-workbook:'+item['file'],item['url'],now);doc={**item,'body':base64.b64encode(pdf).decode(),'sha256':hashlib.sha256(pdf).hexdigest()};pub=None
  # Helper preserves the existing event keys and original byte versions.
  doc['relay_http_fetch']=False
  count=store_item(db,now,doc,pub)
  changed=bool(pending) or bool(s.get('live_last_original_sha256') and s['live_last_original_sha256']!=doc['sha256'])
  s.update(live_status='LIVE_VERIFIED',live_parser_version=VERSION,live_failure=None,last_live_check_utc=now.isoformat(),live_next_retry_at_utc=None,live_http_status=meta['status'],live_last_original_sha256=doc['sha256'])
  leading.checkpoint(db,source,s,now)
  return {'source':source,'status':'SUCCESS_NEW_DATA' if changed else 'SUCCESS_UNCHANGED','new_document_discovered':bool(pending),'values_seen':count,'live_status':'LIVE_VERIFIED','http_attempted':True,'index_http_status':meta['status'],'remaining_items':max(0,len(pending)-1),'results_publication_clock':'UNVERIFIED' if source=='NYFED_SME' else 'DAY_ONLY'}
 except Exception as e:
  info=retry.retry_info(e,now);s.update(live_parser_version=VERSION,live_status='LIVE_BLOCKED' if info['status'] in ('NETWORK_TIMEOUT','BLOCKED_ACCESS') else 'LIVE_PARTIAL',live_failure=info['status'],live_next_retry_at_utc=info['retry_at'],last_live_check_utc=now.isoformat())
  leading.checkpoint(db,source,s,now);a._record_fetch_status(db,source,'live-discovery',url,now,getattr(e,'code',None),type(e).__name__+':'+str(e)[:120])
  return {'source':source,'status':info['status'],'live_status':s['live_status'],'http_attempted':True,'error':str(e)[:140],'next_retry_at_utc':info['retry_at'],'history_preserved':True}
