"""BLS original releases preserved by the Federal Reserve's public FRASER archive."""
import hashlib
import html
import io
import json
import re
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal

try:
    import model100_primary_sources as common
    import model100_primary_archive as archive
except ImportError:
    from work import model100_primary_sources as common, model100_primary_archive as archive

VERSION='model100-bls-fraser-20261108-v8'
PARSE_VERSIONS={'CPI':'model100-bls-fraser-20261108-v5','NFP':'model100-bls-fraser-20261108-v7','PPI':'model100-bls-fraser-20261108-v4'}
INDICES={'CPI':'consumer-price-index-6838','NFP':'employment-situation-144','PPI':'producer-price-indexes-6837'}
MONTHS='January February March April May June July August September October November December'.split()
MONTH_RE='|'.join(MONTHS)


def parse_version(item):
    return VERSION if item['family']=='PPI' and item['period']=='2025-11-01' else PARSE_VERSIONS[item['family']]


def unparsed_items(items, versions):
    return [item for item in items if versions.get(item['url'])!=parse_version(item)]


def parser_error_backup(prior, corrected):
    if prior.get('asof_class')=='UNUSABLE' or any(prior.get(k)!=corrected.get(k)
            for k in ('source_id','record_key','content_sha256')):
        return None
    if Decimal(str(prior['numeric_value']))==Decimal(str(corrected['numeric_value'])):
        return None
    identity=json.dumps([prior['source_id'],prior['record_key'],prior['content_sha256'],str(prior['numeric_value'])])
    ex=prior.get('extra_json') or '{}';ex=json.loads(ex) if isinstance(ex,str) else dict(ex)
    return {**prior,'record_key':'quarantine:bls-parser-v6:'+hashlib.sha256(identity.encode()).hexdigest(),
            'record_kind':'VALIDATION_FAILURE','asof_class':'UNUSABLE',
            'extra_json':json.dumps({**ex,'original_record_key':prior['record_key'],
                'original_record_kind':prior['record_kind'],'quarantine_reason':'PROVEN_PARSER_HEADLINE_CAPTURE_ERROR',
                'corrected_value':str(corrected['numeric_value']),'correction_parser_version':VERSION})}


def persist_validated_value(db, record, prior_rows):
    corrected=False;backups=0
    for prior in prior_rows:
        if any(prior.get(k)!=record.get(k) for k in ('source_id','record_key','content_sha256')):
            continue
        if Decimal(str(prior['numeric_value']))==Decimal(str(record['numeric_value'])):
            continue
        corrected=True
        backup=parser_error_backup(prior,record)
        if backup:
            common.store(db,backup)
            common.store(db,{**prior,'asof_class':'UNUSABLE','extra_json':backup['extra_json']})
            backups+=1
    if corrected:
        # The shared writer deliberately preserves numeric_value on duplicate
        # byte keys. Keep it intact: invalidate the proven error and append a
        # stable corrected extraction key, preserving the same original SHA.
        fingerprint=hashlib.sha256(str(Decimal(str(record['numeric_value'])).normalize()).encode()).hexdigest()[:16]
        ex=record.get('extra_json') or '{}';ex=json.loads(ex) if isinstance(ex,str) else dict(ex)
        record={**record,'record_key':record['record_key']+':parser-corrected:'+fingerprint,
                'extra_json':json.dumps({**ex,'corrects_parser_record_key':record['record_key'],
                    'parser_correction_is_not_source_revision':True})}
    common.store(db,record)
    return backups


def parse_index(body,family,now):
    parser=archive.LinkParser();parser.feed(body.decode('utf-8','replace'))
    out={}
    for href,title,_ in parser.links:
        url=urllib.parse.urljoin('https://fraser.stlouisfed.org',href)
        p=urllib.parse.urlparse(url)
        if p.hostname!='fraser.stlouisfed.org' or not p.path.startswith('/title/'+INDICES[family]+'/'):continue
        m=re.search(r'('+MONTH_RE+r')-(20\d{2})-\d+$',p.path,re.I)
        if not m:continue
        period=date(int(m[2]),MONTHS.index(m[1].capitalize())+1,1)
        if date(2024,12,1)<=period<=now.date():out[url]={'url':url,'period':period.isoformat(),'family':family,'title':title}
    return sorted(out.values(),key=lambda x:x['period'])


def pdf_url(body):
    # FRASER's citation is the archive's original-file URL, including legacy //docs paths.
    m=re.search(r'<meta\s+name=["\']citation_pdf_url["\']\s+content=["\']([^"\']+)',body.decode('utf-8','replace'),re.I)
    if not m:raise ValueError('FRASER_ORIGINAL_PDF_LINK_MISSING')
    url=html.unescape(m[1]);p=urllib.parse.urlparse(url)
    if p.scheme!='https' or p.hostname!='fraser.stlouisfed.org' or not p.path.lower().endswith('.pdf'):
        raise ValueError('FRASER_ORIGINAL_PDF_URL_NOT_ALLOWED')
    return url


def parse_release(text,family,expected_period,now):
    text=' '.join(text.replace('\u2212','-').replace('\u2013','-').replace('\u2014','-').split())
    heading={'CPI':'CONSUMER PRICE INDEX','NFP':'(?:THE )?EMPLOYMENT SITUATION','PPI':'PRODUCER PRICE INDEXES?'}[family]
    m=re.search(heading+r'\s*-\s*('+MONTH_RE+r')\s+(20\d{2})',text,re.I)
    if not m:raise ValueError('BLS_ORIGINAL_HEADING_MISSING')
    period=date(int(m[2]),MONTHS.index(m[1].capitalize())+1,1)
    if period!=date.fromisoformat(expected_period):raise ValueError('BLS_WRONG_OBSERVATION_PERIOD')
    clock=re.search(r'(\d{1,2}):(\d{2})\s*([ap])\.?m\.?\s*\((ET|EST|EDT)\)\s*(?:Monday|Tuesday|Wednesday|Thursday|Friday),?\s*('+MONTH_RE+r')\s+(\d{1,2}),?\s+(20\d{2})',text[:1200],re.I)
    if not clock:raise ValueError('BLS_ORIGINAL_PUBLICATION_CLOCK_MISSING')
    hour=int(clock[1])%12+(12 if clock[3].lower()=='p' else 0)
    day=date(int(clock[7]),MONTHS.index(clock[5].capitalize())+1,int(clock[6]))
    zone=ZoneInfo('America/New_York')
    if clock[4].upper() in ('EST','EDT'):
        zone=timezone(timedelta(hours=-5 if clock[4].upper()=='EST' else -4))
    published=datetime(day.year,day.month,day.day,hour,int(clock[2]),tzinfo=zone).astimezone(timezone.utc).replace(tzinfo=None)
    if published>now or published.date()<date(2025,1,1):raise ValueError('BLS_PUBLICATION_OUT_OF_SCOPE')
    if period>published.date():raise ValueError('BLS_OBSERVATION_AFTER_PUBLICATION')
    values=[]
    def add(name,value,unit='percent',obs=period,kind='FIRST_ESTIMATE'):
        values.append({'indicator':name,'value':float(value),'unit':unit,'period':obs.isoformat(),'kind':kind})
    def movement(match):
        value=float(match[2]);return -value if re.search(r'decreas|declin|fell|down',match[1],re.I) else value
    month=MONTHS[period.month-1]
    if family=='NFP':
        m=re.search(r'Total nonfarm payroll employment\s+((?:edged|moved) (?:up|down)|increased|rose|declined|decreased|fell|grew)\s+(?:by\s+)?([\d,]+)',text,re.I)
        if m:add('NFP_CHANGE',movement((None,m[1],m[2].replace(',',''))),'persons')
        else:
            m=re.search(r'Total nonfarm payroll employment\s+(?:was |is )?(?:little changed|changed little)(?: in '+month+r')?\s*\(([+-]?[\d,]+)\)',text,re.I)
            if m:add('NFP_CHANGE',m[1].replace(',',''),'persons')
        if not any(v['indicator']=='NFP_CHANGE' for v in values) and re.search(r'Total nonfarm payroll employment (?:was |is )?(?:little changed|changed little)',text,re.I):
            # "little changed" is not an exact zero. Do not infer a headline number.
            pass
        m=re.search(r'unemployment rate.{0,65}?\b(?:at|to)\s+(\d+(?:\.\d+)?)\s+percent',text,re.I)
        if m:add('UNEMPLOYMENT_RATE',m[1])
        for m in re.finditer(r'(?:change in total nonfarm payroll employment|change) for ('+MONTH_RE+r') was revised (?:up|down) by [\d,]+,? from [+-]?[\d,]+ to ([+-]?[\d,]+)',text,re.I):
            month_number=MONTHS.index(m[1].capitalize())+1
            year=period.year-(month_number>=period.month)
            add('NFP_CHANGE',m[2].replace(',',''),'persons',date(year,month_number,1),'REVISION')
        if period==date(2025,11,1) and re.search(r'first publication of October data',text,re.I):
            table=re.search(r'Table B-1\..*?Not seasonally adjusted Seasonally adjusted.*?Total nonfarm\.?\s*([\d.,+\-\s]+?)Total private',text,re.I)
            cells=re.findall(r'[+-]?\d[\d,]*(?:\.\d+)?',table[1]) if table else []
            numbers=[float(v.replace(',','')) for v in cells]
            headline=next(v['value'] for v in values if v['indicator']=='NFP_CHANGE' and v['period']==period.isoformat())
            if len(numbers)!=9 or numbers[8]*1000!=headline or (numbers[7]-numbers[6])*1000!=headline:
                raise ValueError('BLS_SHUTDOWN_CATCHUP_TABLE_INVALID')
            add('NFP_CHANGE',(numbers[6]-numbers[5])*1000,'persons',date(2025,10,1))
            values[-1]['extraction_evidence']='First October publication; Table B-1 seasonally adjusted October minus September levels, thousands converted to persons'
    elif family=='CPI':
        m=re.search(r'CPI-U\).{0,35}?(increased|rose|decreased|declined|fell)\s+(\d+(?:\.\d+)?)\s+percent.{0,75}?\bin '+month+r'\b',text,re.I)
        if m:add('CPI_HEADLINE_MOM',movement(m))
        elif re.search(r'CPI-U\).{0,25}?(?:was |remained )?unchanged.{0,65}?\bin '+month+r'\b',text,re.I):add('CPI_HEADLINE_MOM',0)
        m=re.search(r'(?:Over the last 12 months,? the all items index|all items index.{0,30}?12 months).{0,12}?(increased|rose|decreased|declined|fell)\s+(\d+(?:\.\d+)?)\s+percent',text,re.I)
        if not m:m=re.search(r'all items index (rose|increased|decreased|declined|fell) (\d+(?:\.\d+)?) percent for the 12 months',text,re.I)
        if m:add('CPI_HEADLINE_YOY',movement(m))
        m=re.search(r'index for all items less food and energy (rose|increased|decreased|declined|fell) (\d+(?:\.\d+)?) percent in '+month,text,re.I)
        if m:add('CPI_CORE_MOM',movement(m))
        elif re.search(r'index for all items less food and energy (?:was |remained )?unchanged in '+month,text,re.I):add('CPI_CORE_MOM',0)
        m=re.search(r'all items less food and energy index (?:also )?(rose|increased|decreased|declined|fell) (\d+(?:\.\d+)?) percent over the (?:last 12 months|year)',text,re.I)
        if m:add('CPI_CORE_YOY',movement(m))
    else:
        m=re.search(r'Producer Price Index for final demand (increased|rose|advanced|climbed|moved up|moved down|edged up|edged down|decreased|declined|fell) (\d+(?:\.\d+)?) percent in '+month,text,re.I)
        if m:add('PPI_FINAL_DEMAND_MOM',movement(m))
        elif re.search(r'Producer Price Index for final demand (?:was |remained )?unchanged in '+month,text,re.I):
            add('PPI_FINAL_DEMAND_MOM',0)
        if period==date(2025,11,1) and published.date()==date(2026,1,14):
            # Standalone October release was cancelled; this is the actual
            # catch-up original. ALFRED PPIFIS first vintage independently
            # confirms January14, never an invented October release clock.
            m=re.search(r'Final demand prices (edged up|edged down|increased|rose|advanced|decreased|declined|fell) (\d+(?:\.\d+)?) percent in October',text,re.I)
            if m:
                add('PPI_FINAL_DEMAND_MOM',movement(m),'percent',date(2025,10,1))
                values[-1]['extraction_evidence']='Delayed October first value in January14 original; cancelled standalone release and ALFRED PPIFIS initial vintage corroborate availability'
        m=re.search(r'(?:final demand index|index for final demand) (increased|rose|advanced|climbed|moved up|moved down|decreased|declined|fell) (\d+(?:\.\d+)?) percent for the 12 months',text,re.I)
        if m:add('PPI_FINAL_DEMAND_YOY',movement(m))
        elif period.month==12:
            m=re.search(r'index for final demand (increased|rose|advanced|decreased|declined|fell) (\d+(?:\.\d+)?) percent in '+str(period.year),text,re.I)
            if m:add('PPI_FINAL_DEMAND_YOY',movement(m))
    if not values or family=='NFP' and not any(v['indicator']=='NFP_CHANGE' for v in values):
        raise ValueError('BLS_REQUIRED_HEADLINE_VALUES_MISSING')
    return published,values


def extract_pdf(body):
    from pypdf import PdfReader
    if not body.startswith(b'%PDF'):raise ValueError('FRASER_NOT_PDF')
    reader=PdfReader(io.BytesIO(body))
    if reader.is_encrypted:raise ValueError('BLS_ENCRYPTED_PDF_NOT_SUPPORTED')
    texts=[]
    for page in reader.pages[:5]:
        content=page.get_contents()
        if content and len(content.get_data())>5_000_000:raise ValueError('BLS_PDF_PAGE_TOO_LARGE')
        texts.append(page.extract_text() or '')
    if re.search(r'first publication of October data', ' '.join(texts), re.I):
        # Only the shutdown catch-up edition needs the original establishment
        # table beyond the usual first five pages. Never substitute latest API.
        for page in reader.pages[5:60]:
            content=page.get_contents()
            if content and len(content.get_data())>5_000_000:raise ValueError('BLS_PDF_PAGE_TOO_LARGE')
            text=page.extract_text() or ''
            if re.search(r'Table B-1\. Employees on nonfarm payrolls',text):
                texts.append(text)
                break
    return '\n'.join(texts)


def collect(db,now):
    source='BLS_RELEASES';key='control:bls-fraser'
    cur=db.cursor();cur.execute(f"SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",(source,key))
    row=cur.fetchone();cur.close();state=json.loads(row[0]) if row and row[0] else {}
    state.setdefault('done',[]);state.setdefault('failed',{});state.setdefault('parsed_versions',{})
    result={'source':source,'lane':'official-fraser-archive','documents':0,'values':0,'errors':[]}
    pending=[]
    for family,title in INDICES.items():
        url='https://fraser.stlouisfed.org/title/'+title+'?browse=2020s'
        try:
            body,meta=common.conditional_fetch(db,source,'fraser-index:'+family,url,timeout=12,cap=2_000_000)
            archive._save_index(db,source,'fraser-index:'+family,url,body,meta,now)
            notice=' '.join(html.unescape(re.sub(r'<[^>]+>',' ',body.decode('utf-8','replace'))).split())
            if re.search(r'October 2025.{0,80}?not published because of 2025 lapse',notice,re.I):
                series={'CPI':'CPI','NFP':'EMPLOYMENT_SITUATION','PPI':'PPI'}[family]
                record=common.base_row(source,'not-published:'+series+':2025-10-01','RELEASE_STATUS',url,body,meta,
                    now,None,'NOT_PUBLISHED','NOT_PUBLISHED',title=series+' October 2025 not published',
                    event_type=series,region='UNITED_STATES',observation_period_start='2025-10-01',
                    extra_json={'status':'NOT_PUBLISHED','reason':'2025 lapse in federal government appropriations',
                                'basis':'OFFICIAL_FRASER_TITLE_NOTICE','original_publisher':'BLS','archival_host':'FRASER'})
                common.store(db,record)
            items=parse_index(body,family,now)
            result.setdefault('discovered',{})[family]=len(items)
            pending.extend(items)
        except Exception as exc:
            archive._record_fetch_status(db,source,'fraser-index:'+family,url,now,getattr(exc,'code',None),type(exc).__name__+':'+str(exc)[:150])
            result['errors'].append({'url':url,'status':common._exception_outcome(exc)})
    def checkpoint():
        payload=json.dumps({'schema':1,'archive':'FRASER'},sort_keys=True).encode()
        record=common.base_row(source,key,'RUN_CONTROL','brain://model100/bls-fraser',payload,
            {'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',extra_json=state)
        common.store(db,record)
    candidates=unparsed_items(pending,state['parsed_versions'])
    revalidation=False
    if not candidates and (not state.get('last_revalidation_utc') or
            now-datetime.fromisoformat(state['last_revalidation_utc'])>=timedelta(days=7)):
        candidates=[max((i for i in pending if i['family']==family),key=lambda i:i['period'])
                    for family in INDICES if any(i['family']==family for i in pending)]
        revalidation=True
    eligible=[i for i in candidates if not state['failed'].get(i['url'],{}).get('retry_at')
              or now>=datetime.fromisoformat(state['failed'][i['url']]['retry_at'])]
    for item in eligible[:3]:
        url=item['url']
        try:
            body,meta=common.conditional_fetch(db,source,'fraser-item:'+url.rsplit('-',1)[-1],url,timeout=12,cap=2_000_000)
            archive._save_index(db,source,'fraser-item:'+url.rsplit('-',1)[-1],url,body,meta,now)
            original=pdf_url(body)
            cur=db.cursor();cur.execute(f"SELECT content_sha256 FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",
                (source,'fraser-pdf:'+url.rsplit('-',1)[-1]))
            prior=cur.fetchone();cur.close()
            pdf,meta=common.conditional_fetch(db,source,'fraser-pdf:'+url.rsplit('-',1)[-1],original,timeout=15,cap=8_000_000)
            if state['parsed_versions'].get(url)==parse_version(item) and prior and prior[0]==hashlib.sha256(pdf).hexdigest():
                result['unchanged_originals']=result.get('unchanged_originals',0)+1
                continue
            # Save the genuine PDF even when extraction/validation fails.
            title=item['family']+' original '+item['period']
            archive._store_document(db,source,'fraser-pdf:'+url.rsplit('-',1)[-1],original,pdf,meta,now,title,
                'BLS_'+item['family'],evidence='DAY_ONLY',asof='DAY_ONLY',region='UNITED_STATES',
                extra={'original_publisher':'BLS','archival_host':'FRASER','archive_item_url':url})
            published,values=parse_release(extract_pdf(pdf),item['family'],item['period'],now)
            cur=db.cursor();cur.execute(f"SELECT * FROM `{common.TABLE}` WHERE source_id=%s AND canonical_url IN (%s,%s) AND numeric_value IS NOT NULL",
                (source,original,meta.get('url',original)))
            fields=[d[0] for d in cur.description];prior_rows=[dict(zip(fields,r)) for r in cur.fetchall()];cur.close()
            known_values={(r['event_type'],str(r['observation_period_start']),datetime.fromisoformat(str(r['source_published_at_utc'])).isoformat(),float(r['numeric_value'])) for r in prior_rows if r['source_published_at_utc'] is not None and r['asof_class']!='UNUSABLE'}
            extra={'original_publisher':'BLS','archival_host':'FRASER','archive_item_url':url,
                   'document_date':published.date().isoformat(),'timestamp_source':'BLS original PDF embargo release clock',
                   'source_timezone':'America/New_York','archival_receipt_is_not_original_live_receipt':True}
            archive._store_document(db,source,'fraser-pdf:'+url.rsplit('-',1)[-1],original,pdf,meta,now,title,
                'BLS_'+item['family'],pub_at=published,evidence='DOCUMENTED_CLOCK',asof='HISTORICAL_DOCUMENTED',region='UNITED_STATES',extra=extra)
            for value in values:
                record=common.base_row(source,'fraser:'+item['family']+':'+value['indicator']+':'+value['period']+':'+published.isoformat(),
                    value['kind'],original,pdf,meta,now,published,'DOCUMENTED_CLOCK','HISTORICAL_DOCUMENTED',
                    title=title,event_type=value['indicator'],region='UNITED_STATES',observation_period_start=value['period'],
                    vintage_month=published.date().replace(day=1).isoformat(),numeric_value=value['value'],unit=value['unit'],
                    reference_timezone='America/New_York',extra_json={**extra,'release_edition':value['kind'],
                    'vintage_date':published.date().isoformat(),'provider':'BLS','indicator':value['indicator'],
                    'extraction_evidence':value.get('extraction_evidence')})
                record['parser_version']=VERSION
                backups=persist_validated_value(db,record,prior_rows)
                result['quarantined_parser_errors']=result.get('quarantined_parser_errors',0)+backups
                value_key=(value['indicator'],value['period'],published.isoformat(),float(value['value']))
                if value_key not in known_values:
                    result['new_numeric_values']=result.get('new_numeric_values',0)+1
                    known_values.add(value_key)
            result['documents']+=1;result['values']+=len(values)
            if url not in state['done']:state['done'].append(url)
            state['parsed_versions'][url]=parse_version(item)
            state['failed'].pop(url,None)
        except Exception as exc:
            status=common._exception_outcome(exc)
            headers={k.lower():v for k,v in getattr(exc,'headers',{}).items()} if getattr(exc,'headers',None) else {}
            retry=86400 if status in ('BLOCKED_ACCESS','PARSE_ERROR') else 900
            if headers.get('retry-after','').isdigit():retry=max(1,int(headers['retry-after']))
            state['failed'][url]={'status':status,'retry_at':(now+timedelta(seconds=retry)).isoformat()}
            archive._record_fetch_status(db,source,'fraser:'+url.rsplit('-',1)[-1],url,now,getattr(exc,'code',None),type(exc).__name__+':'+str(exc)[:150],headers)
            result['errors'].append({'url':url,'status':status,'detail':str(exc)[:150]})
        checkpoint();time.sleep(0.3)
    state['last_checked_utc']=now.isoformat();state['discovered_items']=len(pending)
    if revalidation and not result['errors']:state['last_revalidation_utc']=now.isoformat()
    state['remaining_items']=len(unparsed_items(pending,state['parsed_versions']))
    state['state']='INCOMPLETE' if state['remaining_items'] or result['errors'] else 'COMPLETE_ARCHIVE_ITEMS'
    checkpoint();result['remaining_items']=state['remaining_items']
    result['status']='BACKFILL_INCOMPLETE' if state['remaining_items'] else 'PARTIAL_COVERAGE' if result['errors'] else 'SUCCESS_NEW_DATA' if result.get('new_numeric_values') else 'SUCCESS_UNCHANGED'
    return result
