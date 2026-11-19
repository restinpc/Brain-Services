"""Official ifo release PDFs and their original monthly table vintages."""
import hashlib
import html
import io
import json
import re
import time
import urllib.request
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

try:
    import model100_primary_sources as common
    import model100_primary_archive as archive
except ImportError:
    from work import model100_primary_sources as common, model100_primary_archive as archive

SOURCE='IFO_VINTAGE'
VERSION='model100-ifo-originals-v2'
SERIES_URL='https://www.ifo.de/en/survey/ifo-business-climate-index-germany'
SEARCH_URL='https://www.ifo.de/api-proxy/search/prod-ifo-content-node-ifo-en/_search'
MONTHS='January February March April May June July August September October November December'.split()
MONTH_RE='|'.join(MONTHS)
KEY='control:ifo-originals'


class Links(HTMLParser):
    def __init__(self):
        super().__init__();self.links=[];self.times=[]
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag in ('a','qz-link') and a.get('href'):self.links.append(a['href'])
        if tag=='time' and a.get('datetime'):self.times.append(a['datetime'])


def release_item(url,title=''):
    url=urljoin('https://www.ifo.de',url)
    match=re.fullmatch(r'https://www\.ifo\.de/en/facts/(20\d\d-\d\d-\d\d)/([^/?#]+)',url)
    if not match or 'business-climate' not in match[2] or any(k in match[2] for k in ('eastern','saxony','manufacturing','industry')):return None
    month=re.search(r'('+MONTH_RE+r')-(20\d\d)$',match[2],re.I)
    if not month:return None
    day=date.fromisoformat(match[1]);period=date(int(month[2]),MONTHS.index(month[1].capitalize())+1,1)
    if (day.year,day.month)!=(period.year,period.month):return None
    return {'url':url,'day':day.isoformat(),'period':period.isoformat(),'title':title}


def parse_search(body,now):
    data=json.loads(body)
    if data.get('error') or 'hits' not in data:raise ValueError('IFO_SEARCH_RESPONSE_INVALID')
    total=data['hits']['total'];total=total['value'] if isinstance(total,dict) else total
    result={}
    for hit in data['hits'].get('hits',[]):
        row=hit['_source'];item=release_item(row.get('url_alias',''),row.get('label',''))
        if item and '2025-01-01'<=item['day']<=now.date().isoformat():result[item['url']]=item
    return sorted(result.values(),key=lambda i:i['day']),int(total)


def parse_page(body,url):
    item=release_item(url)
    if not item:raise ValueError('IFO_RELEASE_URL_INVALID')
    p=Links();p.feed(body.decode('utf-8','replace'))
    clocks=[common.parse_explicit_datetime(t) for t in p.times]
    clocks=[t for t in clocks if t is not None]
    matching={t for t in clocks if t.date().isoformat()==item['day']}
    if clocks and any(t.year!=int(item['day'][:4]) for t in clocks):raise ValueError('IFO_PAGE_CLOCK_DAY_MISMATCH')
    if not matching:
        # URL dates are identifiers. A nearby machine clock is accepted only
        # after the linked original PDF independently confirms its day.
        matching={t for t in clocks if abs((t.date()-date.fromisoformat(item['day'])).days)<=1}
    pdfs=[]
    for href in p.links:
        u=urljoin(url,href);parts=urlparse(u)
        if parts.scheme=='https' and parts.hostname=='www.ifo.de' and re.fullmatch(r'/en/media/\d+/download',parts.path) and u not in pdfs:pdfs.append(u)
    if not pdfs:raise ValueError('IFO_ORIGINAL_PDF_LINK_MISSING')
    return {**item,'published':next(iter(matching)) if len(matching)==1 else None,'pdf_urls':pdfs,
            'page_clock_candidates':[t.isoformat() for t in clocks]}


def parse_pdf_text(text,period,day,html_evidence=None):
    text=' '.join(text.replace('\u2212','-').split())
    if not re.search(r'ifo Business Climate Germany',text[:160],re.I):raise ValueError('IFO_WRONG_REGIONAL_SURVEY')
    heading=re.search(r'Results of (?:the )?ifo Business Survey for ('+MONTH_RE+r') (20\d\d)',text,re.I)
    dateline=re.search(r'Munich,? ('+MONTH_RE+r') (\d{1,2}),? (20\d\d)',text,re.I)
    if not heading or not dateline:raise ValueError('IFO_ORIGINAL_EDITION_MISSING')
    actual_period=date(int(heading[2]),MONTHS.index(heading[1].capitalize())+1,1).isoformat()
    actual_day=date(int(dateline[3]),MONTHS.index(dateline[1].capitalize())+1,int(dateline[2])).isoformat()
    if actual_period!=period or abs((date.fromisoformat(actual_day)-date.fromisoformat(day)).days)>1:raise ValueError('IFO_ORIGINAL_EDITION_MISMATCH')
    table=re.search(r'Month/year (.*?) Climate (.*?) Situation (.*?) Expectations (.*?) Source',text,re.I)
    if not table:raise ValueError('IFO_ORIGINAL_TABLE_MISSING')
    labels=re.findall(r'(\d{2})/(\d{2})',table[1])
    periods=[date(2000+int(y),int(m),1).isoformat() for m,y in labels]
    header_reconciliation=False
    if len(periods)==13 and periods[-1]==periods[-2] and periods[:-1]==sorted(set(periods[:-1])) and html_evidence:
        # Repair only this proven source-header typo, and only if every value
        # in the original PDF matches the independently headed official HTML.
        for raw_table in re.findall(r'<table\b[^>]*>.*?</table>',html_evidence.decode('utf-8','replace'),re.S|re.I):
            plain=' '.join(html.unescape(re.sub(r'<[^>]+>',' ',raw_table)).split())
            ht=re.search(r'^(.*?) Climate (.*?) Situation (.*?) Expectations (.*)$',plain,re.I)
            if not ht:continue
            hp=[date(2000+int(y),int(m),1).isoformat() for m,y in re.findall(r'(\d{2})/(\d{2})',ht[1])]
            equal=all(re.findall(r'[+-]?\d+(?:\.\d+)?',ht[g])==re.findall(r'[+-]?\d+(?:\.\d+)?',table[g]) for g in (2,3,4))
            if hp[:-1]==periods[:-1] and hp[-1:]==[period] and len(hp)==13 and hp==sorted(set(hp)) and equal:
                periods=hp;header_reconciliation=True;break
    if len(periods)!=13 or periods!=sorted(set(periods)) or periods[-1]!=period:raise ValueError('IFO_ORIGINAL_TABLE_CHRONOLOGY')
    out=[]
    for name,group in zip(('Climate','Situation','Expectations'),(2,3,4)):
        values=[float(v) for v in re.findall(r'[+-]?\d+(?:\.\d+)?',table[group])]
        if len(values)!=len(periods) or any(not 0<=v<=200 for v in values):raise ValueError('IFO_ORIGINAL_TABLE_SHAPE')
        for obs,value in zip(periods,values):out.append({'series':name,'period':obs,'value':value,'kind':'FIRST_ESTIMATE' if obs==period else 'VINTAGE_VALUE',
            'pdf_document_date':actual_day,'header_reconciliation':header_reconciliation})
    return out


def state(db):
    cur=db.cursor()
    try:
        cur.execute(f'SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',(SOURCE,KEY))
        row=cur.fetchone();return json.loads(row[0] or '{}') if row else {}
    finally:cur.close()


def checkpoint(db,value,now):
    body=b'{"checkpoint":"ifo-originals-v1"}'
    row=common.base_row(SOURCE,KEY,'RUN_CONTROL','brain://model100/ifo-originals',body,
        {'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',
        event_type='BACKFILL_CHECKPOINT',extra_json=value)
    common.store(db,row)


def needs_parse(saved,url):
    target=VERSION if saved['items'][url]['period'] in ('2025-12-01','2026-04-01','2026-05-01') else 'model100-ifo-originals-v1'
    return url not in saved['done'] or saved.get('parsed_versions',{}).get(url)!=target


def retry_due(saved,url,now):
    info=saved['failed'].get(url,{})
    if info.get('status')=='PARSE_ERROR' and info.get('parser_version')!=VERSION:return True
    return not info.get('retry_at') or now>=datetime.fromisoformat(info['retry_at'])


def retry_info(exc,now):
    status=common._exception_outcome(exc)
    delay=604800 if status=='BLOCKED_ACCESS' else 86400 if status=='PARSE_ERROR' else 900
    headers={k.lower():v for k,v in exc.headers.items()} if getattr(exc,'headers',None) else {}
    retry=headers.get('retry-after','')
    if retry.isdigit():delay=max(1,int(retry))
    elif retry:
        clock=common.parse_explicit_datetime(retry)
        if clock:delay=max(1,int((clock-now).total_seconds())+1)
    return {'status':status,'retry_at':(now+timedelta(seconds=delay)).isoformat(),'parser_version':VERSION}


def record_failure(db,key,url,exc,now):
    info=retry_info(exc,now)
    headers={k.lower():v for k,v in exc.headers.items()} if getattr(exc,'headers',None) else {}
    if getattr(exc,'code',None) and hasattr(exc,'read'):
        body=exc.read(1_000_001)
        if body:
            common.store(db,common.base_row(SOURCE,'ifo-http-error:'+hashlib.sha256(url.encode()).hexdigest(),
                'HTTP_ERROR',getattr(exc,'url',url),body[:1_000_000],
                {'status':exc.code,'content_type':headers.get('content-type')},now,None,'OBSERVED','FETCH_STATUS',
                extra_json={'response_body_truncated':len(body)>1_000_000,'error_class':info['status']}))
    archive._record_fetch_status(db,SOURCE,key,url,now,getattr(exc,'code',None),type(exc).__name__+':'+str(exc)[:150],headers)
    return info


def discover(db,now,offset=0):
    query={'size':100,'from':offset,'query':{'term':{'field_typ.label.keyword':'ifo Business Climate Index for Germany'}},'_source':['label','url_alias']}
    req=urllib.request.Request(SEARCH_URL,data=json.dumps(query).encode(),headers={'User-Agent':'Brain-Model100-PrimarySources/1.0','Content-Type':'application/json'},method='POST')
    with urllib.request.urlopen(req,timeout=12) as response:
        body=response.read(2_000_001)
        if len(body)>2_000_000:raise ValueError('IFO_SEARCH_SIZE_LIMIT')
        meta={'status':response.status,'url':SEARCH_URL,'content_type':response.headers.get('Content-Type'),
              'received_at_utc':common.utcnow().replace(tzinfo=common.UTC).isoformat()}
    items,total=parse_search(body,now)
    row=common.base_row(SOURCE,'original-search:'+str(offset),'ORIGINAL_INDEX',SEARCH_URL,body,meta,now,None,'OBSERVED','OBSERVED',
        extra_json={'request_method':'POST','request_query':query,'public_search_endpoint_from_official_site_javascript':True})
    common.store(db,row)
    return items,total


def extract_values(pdf,item,html_evidence=None):
    from pypdf import PdfReader
    if not pdf.startswith(b'%PDF'):raise ValueError('IFO_ORIGINAL_NOT_PDF')
    reader=PdfReader(io.BytesIO(pdf))
    if reader.is_encrypted or len(reader.pages)>20:raise ValueError('IFO_ORIGINAL_PDF_LIMIT')
    texts=[]
    for p in reader.pages[:2]:
        content=p.get_contents()
        if content and len(content.get_data())>5_000_000:raise ValueError('IFO_ORIGINAL_PDF_PAGE_LIMIT')
        texts.append(p.extract_text() or '')
    return parse_pdf_text(' '.join(texts),item['period'],item['day'],html_evidence)


def fetch_release(db,item,now):
    item={**item,'title':item.get('title') or 'ifo Business Climate Germany '+item['period'][:7]}
    key='original:'+hashlib.sha256(item['url'].encode()).hexdigest()
    body,meta=common.conditional_fetch(db,SOURCE,key+':html',item['url'],timeout=12,cap=2_000_000)
    try:page=parse_page(body,item['url'])
    except Exception as exc:
        common.store(db,common.base_row(SOURCE,key+':invalid-html','VALIDATION_FAILURE',item['url'],body,meta,now,None,
            'DAY_ONLY','UNUSABLE',event_type='PARSE_ERROR',extra_json={'error':type(exc).__name__+':'+str(exc)[:120]}))
        raise
    pub=page['published'];html_sha=hashlib.sha256(body).hexdigest()
    if pub and pub>now:raise ValueError('IFO_FUTURE_PUBLICATION')
    ex={'document_date':pub.date().isoformat() if pub else item['day'],'url_encoded_day':item['day'],
        'page_clock_candidates':page['page_clock_candidates'],'vintage_code':'v'+item['period'][:4]+'m'+item['period'][5:7],
        'publication_clock_basis':'Official release page time datetime with explicit UTC offset; not a default release hour',
        'publication_evidence_html_sha256':html_sha,'release_page_url':item['url'],
        'historical_receipt_is_not_live_receipt':True}
    common.store(db,common.base_row(SOURCE,key+':html','ORIGINAL_DOCUMENT',item['url'],body,meta,now,pub,
        'DOCUMENTED_PAGE_CLOCK' if pub else 'DAY_ONLY','HISTORICAL_DOCUMENTED' if pub else 'DAY_ONLY',
        title=item['title'],event_type='IFO_RELEASE_PAGE',region='DE',extra_json={**ex,'original_first_text_version_verified':False}))
    original=page['pdf_urls'][0];time.sleep(.5)
    pdf,pdf_meta=common.conditional_fetch(db,SOURCE,key+':pdf',original,timeout=15,cap=3_000_000)
    # Preserve original bytes before extraction so failed parsing is recoverable.
    digest,path=common.preserve(SOURCE,pdf)
    try:values=extract_values(pdf,item,body)
    except Exception as exc:
        common.store(db,common.base_row(SOURCE,key+':invalid-pdf','VALIDATION_FAILURE',original,pdf,pdf_meta,now,None,
            'DAY_ONLY','UNUSABLE',event_type='PARSE_ERROR',extra_json={**ex,'error':type(exc).__name__+':'+str(exc)[:120]}))
        raise
    document_day=values[0]['pdf_document_date']
    if pub and pub.date().isoformat()!=document_day:pub=None
    extra={**ex,'document_date':document_day,'source_pdf_header_reconciled_with_html':values[0]['header_reconciliation'],
           'original_first_text_version_verified':True,'original_pdf_sha256':digest,
           'table_version_is_release_vintage_not_latest_series':True}
    received=common.parse_explicit_datetime(pdf_meta.get('received_at_utc')) or now
    certified=pub is not None and 0<=(received-pub).total_seconds()<=300
    quality='CERTIFIED_LIVE' if certified else 'HISTORICAL_DOCUMENTED' if pub else 'DAY_ONLY'
    extra['historical_receipt_is_not_live_receipt']=not certified
    common.store(db,common.base_row(SOURCE,key+':pdf','ORIGINAL_DOCUMENT',original,pdf,pdf_meta,now,pub,
        'DOCUMENTED_PAGE_CLOCK_LINKED_PDF' if pub else 'DAY_ONLY',quality,title=item['title'],
        event_type='IFO_BUSINESS_CLIMATE_RELEASE',region='DE',extra_json=extra))
    cur=db.cursor()
    try:
        cur.execute(f'SELECT COUNT(*) FROM `{common.TABLE}` WHERE source_id=%s AND record_key LIKE %s AND content_sha256=%s',(SOURCE,'ifo-original-table:'+extra['vintage_code']+':%',digest))
        prior_count=cur.fetchone()[0]
    finally:cur.close()
    for v in values:
        row=common.base_row(SOURCE,'ifo-original-table:'+extra['vintage_code']+':'+v['series']+':'+v['period'],v['kind'],original,pdf,pdf_meta,now,pub,
            'DOCUMENTED_PAGE_CLOCK_LINKED_PDF' if pub else 'DAY_ONLY',quality,title=item['title'],
            event_type='BUSINESS_CLIMATE',region='DE',observation_period_start=v['period'],vintage_month=item['period'],
            numeric_value=v['value'],unit='INDEX',reference_timezone='UTC' if pub else 'Europe/Berlin',
            extra_json={**extra,'series':v['series'],'value_class':'FIRST_ESTIMATE' if v['kind']=='FIRST_ESTIMATE' else 'ORIGINAL_RELEASE_TABLE_VINTAGE',
                        'prior_period_table_cell_is_not_automatically_a_revision':True})
        row['parser_version']=VERSION;common.store(db,row)
    return {'url':item['url'],'original_pdf_url':original,'sha256':digest,'value_rows':len(values),'new_value_rows':max(0,len(values)-prior_count)}


def collect(db,now):
    saved=state(db);saved.setdefault('items',{});saved.setdefault('done',[]);saved.setdefault('failed',{});saved.setdefault('parsed_versions',{})
    result={'source':SOURCE,'lane':'official-ifo-originals','documents':0,'new_value_rows':0,'errors':[]}
    if saved.get('host_retry_at') and now<datetime.fromisoformat(saved['host_retry_at']):
        return {**result,'status':saved.get('host_retry_status','RATE_LIMITED'),'remaining_items':saved.get('remaining_items'),
                'next_attempt_utc':saved['host_retry_at'],'http_attempted':False}
    discovery_url=SEARCH_URL
    try:
        if not saved.get('discovery_complete') or not saved.get('last_discovery_utc') or now-datetime.fromisoformat(saved['last_discovery_utc'])>=timedelta(days=1):
            offset=int(saved.get('discovery_offset',0));items,total=discover(db,now,offset)
            saved['items'].update({i['url']:i for i in items});saved['search_total_items']=total
            saved['discovery_complete']=offset+100>=total;saved['discovery_offset']=0 if saved['discovery_complete'] else offset+100
            saved['last_discovery_utc']=now.isoformat();checkpoint(db,saved,now)
        discovery_url=SERIES_URL
        body,meta=common.conditional_fetch(db,SOURCE,'originals-live-index',SERIES_URL,timeout=12,cap=2_000_000)
        archive._save_index(db,SOURCE,'originals-live-index',SERIES_URL,body,meta,now)
        links=Links();links.feed(body.decode('utf-8','replace'))
        for link in links.links:
            item=release_item(link)
            if item and '2025-01-01'<=item['day']<=now.date().isoformat():
                prior=saved['items'].get(item['url'],{})
                saved['items'][item['url']]={**item,'title':prior.get('title') or item['title']}
    except Exception as exc:
        info=record_failure(db,'ifo-discovery',discovery_url,exc,now)
        result['errors'].append({'phase':'DISCOVERY','status':info['status'],'url':discovery_url})
        if info['status'] in ('RATE_LIMITED','BLOCKED_ACCESS','NETWORK_TIMEOUT'):
            saved.update(host_retry_at=info['retry_at'],host_retry_status=info['status']);checkpoint(db,saved,now)
            return {**result,'status':info['status'],'remaining_items':saved.get('remaining_items'),'next_attempt_utc':info['retry_at']}
    pending=[i for u,i in saved['items'].items() if needs_parse(saved,u) and retry_due(saved,u,now)]
    pending.sort(key=lambda i:(i['day']!=now.date().isoformat(),i['day']))
    review=False
    if not pending and (not saved.get('last_revision_check_utc') or now-datetime.fromisoformat(saved['last_revision_check_utc'])>=timedelta(days=7)):
        pending=sorted((i for u,i in saved['items'].items() if u in saved['done'] and u not in saved['failed']),key=lambda i:i['day'],reverse=True)[:2];review=bool(pending)
    for item in pending[:2]:
        try:
            doc=fetch_release(db,item,now);result.setdefault('originals',[]).append(doc)
            result['documents']+=1;result['new_value_rows']+=doc['new_value_rows']
            if item['url'] not in saved['done']:saved['done'].append(item['url'])
            saved['parsed_versions'][item['url']]=VERSION if item['period'] in ('2025-12-01','2026-04-01','2026-05-01') else 'model100-ifo-originals-v1'
            saved['failed'].pop(item['url'],None)
        except Exception as exc:
            info=record_failure(db,'ifo-original:'+hashlib.sha256(item['url'].encode()).hexdigest(),item['url'],exc,now)
            saved['failed'][item['url']]=info
            result['errors'].append({'url':item['url'],'status':info['status'],'detail':str(exc)[:100]})
            if info['status']=='RATE_LIMITED':
                saved.update(host_retry_at=info['retry_at'],host_retry_status=info['status']);checkpoint(db,saved,now)
                return {**result,'status':'RATE_LIMITED','remaining_items':saved.get('remaining_items'),'next_attempt_utc':info['retry_at']}
        checkpoint(db,saved,now);time.sleep(.5)
    if review and not result['errors']:saved['last_revision_check_utc']=now.isoformat()
    saved['discovered_items']=len(saved['items']);saved['remaining_items']=sum(needs_parse(saved,u) for u in saved['items'])
    saved['last_checked_utc']=now.isoformat();checkpoint(db,saved,now)
    result['remaining_items']=saved['remaining_items'];result['expected_original_release_items']=saved['discovered_items']
    immediate=any(needs_parse(saved,u) and retry_due(saved,u,now) for u in saved['items'])
    result['status']='BACKFILL_INCOMPLETE' if immediate or not saved.get('discovery_complete') else 'PARTIAL_COVERAGE' if saved['remaining_items'] or result['errors'] else 'SUCCESS_NEW_DATA' if result['new_value_rows'] else 'SUCCESS_UNCHANGED'
    return result
