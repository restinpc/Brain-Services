"""ECB originals republished by the Eurosystem's Banca d'Italia; B1 native writer only."""
import html
import hashlib
import io
import json
import re
import time
import urllib.parse
from datetime import date, datetime, timedelta
try:
    import model100_primary_sources as common
    import model100_primary_archive as archive
    from model100_primary_ifo_releases import retry_info
except ImportError:
    from work import model100_primary_sources as common
    from work import model100_primary_archive as archive
    from work.model100_primary_ifo_releases import retry_info

SOURCE = 'ECB_HIST'
KEY = 'control:ecb-eurosystem-originals'
VERSION = 'model100-ecb-eurosystem-v8'
ROOT = 'https://www.bancaditalia.it'
INDEX = ROOT + '/media/bce-comunicati/ricerca/ricerca.html'
START = date(2025, 1, 1)
TITLES = r'monetary policy decisions|monetary policy statement|account of the monetary policy|decisions taken by the governing council'


def parse_index(body, year, page):
    parser = archive.LinkParser()
    parser.feed(body.decode('utf-8', 'replace'))
    items, last = {}, page
    for href, title, _ in parser.links:
        target = urllib.parse.urljoin(ROOT, href.strip())
        parsed = urllib.parse.urlparse(target)
        if parsed.hostname != 'www.bancaditalia.it':
            continue
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == '/media/bce-comunicati/ricerca/ricerca.html':
            if query.get('min_anno_pubblicazione') == [str(year)] and query.get('max_anno_pubblicazione') == [str(year)]:
                last = max(last, int(query.get('page', ['1'])[0]))
        if (parsed.path.startswith('/media/bce-comunicati/documenti/' + str(year) + '/')
                and parsed.path.lower().endswith('.pdf') and re.search(TITLES, title, re.I)):
            # dotCMS needs its public language selector even for the .en PDF.
            url = urllib.parse.urlunparse(parsed._replace(query='language_id=1', fragment=''))
            items[url] = {'url': url, 'title': title, 'index_year': year}
    return items, last


BDE_ACCOUNTS='https://www.bde.es/wbe/en/noticias-eventos/actualidad-bce/resenas-las-reuniones-politica-monetaria/'
BDE_STATEMENTS=(
 'https://www.bde.es/f/webbe/GAP/Secciones/SalaPrensa/ComunicadosBCE/NotasInformativasBCE/25/mps250605en.pdf',
 'https://www.bde.es/f/webbe/GAP/Secciones/SalaPrensa/ComunicadosBCE/DecisionesPoliticaMonetaria/25/mps250911en.pdf',
 'https://www.bde.es/f/webbe/GAP/Secciones/SalaPrensa/ComunicadosBCE/DecisionesPoliticaMonetaria/25/mps251218en-1.pdf')


def parse_bde_index(body,today):
    items={}
    for card in body.decode('utf-8','replace').split('<div class="block-search-result">')[1:]:
        m=re.search(r'class="block-search-result__date"[^>]*>\s*(\d{2}/\d{2}/20\d{2})',card)
        if not m:continue
        day=datetime.strptime(m[1],'%d/%m/%Y').date()
        if not START<=day<=today:continue
        for href in re.findall(r'href="([^"]+\.pdf)"',card):
            url=urllib.parse.urljoin('https://www.bde.es',html.unescape(href))
            part=urllib.parse.urlparse(url)
            if part.hostname!='www.bde.es' or '/ComunicadosBCE/ResenasReunionesPoliticaMonetaria/' not in part.path:continue
            items[url]={'url':url,'title':'Account of the monetary policy meeting','index_year':day.year,
                'expected_publication_day':day.isoformat(),'official_republisher':'Banco de Espana, Eurosystem'}
    return items


def parse_text(text, item, today):
    text = ' '.join(text.split())
    if not re.search(r'European Central Bank', text, re.I):
        raise ValueError('ECB_ISSUER_NOT_VERIFIED')
    match = re.search(r'Press release\s*/?\s*(\d{1,2}\s+[A-Za-z]+\s+20\d{2})', text, re.I)
    if not match:
        match = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+20\d{2})\s+(?:Decisions taken by|Account of the monetary policy|Monetary policy statement)',text[:1000],re.I)
    if 'Banco de Espana' in item.get('official_republisher',''):
        match=re.search(r'(\d{1,2}\s+[A-Za-z]+\s+20\d{2})\s+(?:1\.\s+Review of financial|Conclusion of the ECB)',text[:1000],re.I)
        if not match:
            match=re.search(r'^(\d{1,2}\s+[A-Za-z]+\s+20\d{2})\s+Page',text[:1000],re.I)
        if not match:
            match=re.search(r'MONETARY POLICY STATEMENT.*?(?:Frankfurt am Main|Florence|Berlin),\s*(\d{1,2}\s+[A-Za-z]+\s+20\d{2})',text[:1000],re.I)
    if not match:
        raise ValueError('ECB_PUBLICATION_DAY_NOT_IN_ORIGINAL')
    day = datetime.strptime(match.group(1), '%d %B %Y').date()
    if not START <= day <= today or day.year != item['index_year']:
        raise ValueError('ECB_WRONG_YEAR_OR_FUTURE_DOCUMENT')
    if item.get('expected_publication_day') and item['expected_publication_day']!=day.isoformat():
        raise ValueError('ECB_MIRROR_PUBLICATION_DAY_CONFLICT')
    decision = bool(re.fullmatch(r'Monetary policy decisions', item['title'].strip(), re.I))
    rates = {}
    if decision:
        url_day = re.search(r'(?:ecb[._])?mp(\d{6})', item['url'])
        if url_day and datetime.strptime(url_day.group(1), '%y%m%d').date() != day:
            raise ValueError('ECB_FILENAME_DOCUMENT_DATE_CONFLICT')
        m = re.search(r'deposit facility,\s*the main refinancing operations and\s*the marginal lending facility.*?(?:at|to)\s+([+-]?\d+(?:\.\d+)?)\s*%\s*,\s*([+-]?\d+(?:\.\d+)?)\s*%\s*,?\s*and\s*([+-]?\d+(?:\.\d+)?)\s*%\s+respectively', text, re.I)
        if not m:
            raise ValueError('ECB_THREE_POLICY_RATES_MISSING')
        rates = dict(zip(('ECB_DEPOSIT_FACILITY_RATE','ECB_MAIN_REFINANCING_RATE','ECB_MARGINAL_LENDING_RATE'), map(float, m.groups())))
        if any(not -2 <= value <= 20 for value in rates.values()):
            raise ValueError('ECB_RATE_RANGE')
    return day, rates


def state(db):
    cur = db.cursor()
    try:
        cur.execute(f'SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1', (SOURCE, KEY))
        prior = cur.fetchone()
        return json.loads(prior[0]) if prior else {}
    finally:
        cur.close()


def checkpoint(db, value, now):
    row = common.base_row(SOURCE, KEY, 'RUN_CONTROL', 'brain://model100/ecb-eurosystem-originals',
        b'{"checkpoint":"ecb-eurosystem-originals-v1"}', {'status':0,'content_type':'application/json'},
        now, None, 'OBSERVED', 'RUN_CONTROL', event_type='BACKFILL_CHECKPOINT', extra_json=value)
    row['parser_version'] = VERSION
    common.store(db, row)


def original_storage_key(db,key):
    cur=db.cursor()
    try:
        cur.execute(f'SELECT record_kind FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',(SOURCE,key))
        prior=cur.fetchone()
        return key+':validated' if prior and prior[0]=='VALIDATION_FAILURE' else key
    finally:
        cur.close()


def fetch_document(db, item, now):
    key = 'eurosystem-pdf:' + hashlib.sha256(item['url'].encode()).hexdigest()
    body, meta = common.conditional_fetch(db, SOURCE, key, item['url'], timeout=12, cap=4_000_000)
    if not body.startswith(b'%PDF'):
        raise ValueError('ECB_ORIGINAL_NOT_PDF')
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(body))
    if reader.is_encrypted or len(reader.pages) > 60:
        raise ValueError('ECB_PDF_LIMIT')
    text = '\n'.join(page.extract_text() or '' for page in reader.pages)
    try:
        day, rates = parse_text(text, item, now.date())
    except ValueError as error:
        common.store(db,common.base_row(SOURCE,'validation:'+key+':'+VERSION,'VALIDATION_FAILURE',item['url'],body,meta,now,None,
                     'OBSERVED','UNUSABLE',extra_json={'error':str(error),'parser_version':VERSION}))
        raise
    proof = {'document_date':day.isoformat(), 'release_document_date':day.isoformat(),
             'issuer':'European Central Bank', 'official_republisher':item.get('official_republisher','Banca d Italia, Eurosystem'),
             'original_pdf_sha256':hashlib.sha256(body).hexdigest(),
             'publication_hour_unknown':True, 'scheduled_1415_is_not_publication_evidence':True,
             'pdf_creation_and_http_last_modified_are_not_publication_times':True,
             'scope':'official Eurosystem mirror; not a retrospective annual report'}
    key = original_storage_key(db,key)
    cur = db.cursor()
    try:
        cur.execute(f'SELECT 1 FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s AND content_sha256=%s LIMIT 1', (SOURCE,key,hashlib.sha256(body).hexdigest()))
        is_new = cur.fetchone() is None
    finally:
        cur.close()
    archive._store_document(db, SOURCE, key, item['url'], body, meta, now, item['title'],
                            'ECB_POLICY_DECISION' if rates else 'ECB_POLICY_DOCUMENT',
                            evidence='DAY_ONLY', asof='DAY_ONLY', region='EURO_AREA', extra=proof)
    for indicator, value in rates.items():
        row = common.base_row(SOURCE, 'eurosystem-rate:' + day.isoformat() + ':' + indicator,
            'RELEASE_VALUE', item['url'], body, meta, now, None, 'DAY_ONLY', 'DAY_ONLY',
            title=item['title'], event_type=indicator, region='EURO_AREA',
            observation_period_start=day.isoformat(), vintage_month=day.replace(day=1).isoformat(),
            numeric_value=value, unit='PERCENT', reference_timezone='Europe/Frankfurt',
            extra_json={**proof, 'value_class':'POLICY_DECISION_RELEASE', 'effective_date_is_not_publication_date':True})
        row['parser_version'] = VERSION
        common.store(db, row)
    return len(rates), is_new


def collect(db, now):
    saved = state(db)
    for name, default in [('items',{}),('done',[]),('failed',{}),('year',2025),('page',1),('reviewed_at',{})]:
        saved.setdefault(name, default)
    if saved.get('validation_recovery_fix')!='v7':
        # Earlier keys could remain UNUSABLE because the shared writer preserves kind/asof.
        saved['done']=[url for url in saved['done'] if urllib.parse.urlparse(url).hostname!='www.bde.es']
        saved['validation_recovery_fix']='v7'
    for old_url in list(saved['items']):
        if urllib.parse.urlparse(old_url).hostname=='www.bancaditalia.it' and not urllib.parse.urlparse(old_url).query:
            item = saved['items'].pop(old_url)
            new_url = old_url + '?language_id=1'
            saved['items'][new_url] = {**item,'url':new_url}
            saved['failed'].pop(old_url,None)
    for url,info in list(saved['failed'].items()):
        if info.get('status')=='PARSE_ERROR' and info.get('parser_version')!=VERSION:
            saved['failed'].pop(url)
    result = {'source':SOURCE, 'lane':'official-eurosystem-originals', 'documents':0, 'values':0, 'new_document_versions':0, 'errors':[]}
    if saved.get('retry_at') and now < datetime.fromisoformat(saved['retry_at']):
        return {**result, 'status':saved.get('retry_status','NETWORK_TIMEOUT'), 'next_retry_at_utc':saved['retry_at']}
    try:
        if not saved.get('bde_checked_at') or now-datetime.fromisoformat(saved['bde_checked_at']) >= timedelta(hours=6):
            page=saved.get('bde_page',1)
            url=BDE_ACCOUNTS+'?page='+str(page)
            body,meta=common.conditional_fetch(db,SOURCE,'bde-accounts-index:'+str(page),url,timeout=12,cap=3_000_000)
            archive._save_index(db,SOURCE,'bde-accounts-index:'+str(page),url,body,meta,now)
            saved['items'].update(parse_bde_index(body,now.date()))
            saved['bde_page']=2 if page==1 else 1
            if page==2:saved['bde_checked_at']=now.isoformat()
            for url in BDE_STATEMENTS:
                saved['items'][url]={'url':url,'title':'Monetary policy statement','index_year':2025,'official_republisher':'Banco de Espana, Eurosystem'}
            checkpoint(db,saved,now)
        if saved.get('discovery_complete') and (not saved.get('last_full_review') or now-datetime.fromisoformat(saved['last_full_review']) >= timedelta(days=7)):
            saved.update(discovery_complete=False,year=2025,page=1,last_full_review=now.isoformat())
        review = bool(saved.get('discovery_complete'))
        if not review or not saved.get('last_index_check') or now-datetime.fromisoformat(saved['last_index_check']) >= timedelta(hours=6):
            year, page = (now.year,1) if review else (saved['year'],saved['page'])
            params = {'min_anno_pubblicazione':year,'max_anno_pubblicazione':year,'page':page,'com.dotmarketing.htmlpage.language':1}
            url = INDEX + '?' + urllib.parse.urlencode(params)
            body, meta = common.conditional_fetch(db,SOURCE,f'eurosystem-index:{year}:{page}',url,timeout=12,cap=4_000_000)
            archive._save_index(db,SOURCE,f'eurosystem-index:{year}:{page}',url,body,meta,now)
            found, last = parse_index(body,year,page)
            saved['items'].update(found)
            if not review:
                if page < last:
                    saved['page'] = page+1
                elif year < now.year:
                    saved.update(year=year+1,page=1)
                else:
                    saved['discovery_complete'] = True
            saved['last_index_check'] = now.isoformat()
            checkpoint(db,saved,now)
        pending = [(url,item) for url,item in saved['items'].items() if url not in saved['done']
                   and (not saved['failed'].get(url,{}).get('retry_at') or now>=datetime.fromisoformat(saved['failed'][url]['retry_at']))]
        # Rotate a bounded pair of completed originals for amendments; retries keep their backoff.
        if not pending:
            pending = sorted([(url,item) for url,item in saved['items'].items() if url in saved['done']
                and (not saved['failed'].get(url,{}).get('retry_at') or now>=datetime.fromisoformat(saved['failed'][url]['retry_at']))
                and (not saved['reviewed_at'].get(url) or now-datetime.fromisoformat(saved['reviewed_at'][url]) >= timedelta(days=7))],
                key=lambda pair:saved['reviewed_at'].get(pair[0],''))[:2]
        for url,item in pending[:3]:
            try:
                count, is_new = fetch_document(db,item,now)
                if url not in saved['done']:
                    saved['done'].append(url)
                saved['reviewed_at'][url] = now.isoformat()
                result['new_document_versions'] += int(is_new)
                saved['failed'].pop(url,None)
                result['documents'] += 1
                result['values'] += count
            except Exception as exc:
                info = retry_info(exc,now)
                info['parser_version'] = VERSION
                saved['failed'][url] = info
                archive._record_fetch_status(db,SOURCE,'eurosystem-document',url,now,getattr(exc,'code',None),str(exc)[:180])
                result['errors'].append({'url':url,'status':info['status'],'error':str(exc)[:120]})
                if info['status'] in ('BLOCKED_ACCESS','RATE_LIMITED','NETWORK_TIMEOUT'):
                    saved.update(retry_at=info['retry_at'],retry_status=info['status'])
                    break
            checkpoint(db,saved,now)
            time.sleep(.5)
    except Exception as exc:
        info = retry_info(exc,now)
        saved.update(retry_at=info['retry_at'],retry_status=info['status'])
        result['errors'].append({'phase':'INDEX','status':info['status'],'error':str(exc)[:150]})
    saved['remaining'] = len(set(saved['items'])-set(saved['done']))
    saved['checked_at'] = now.isoformat()
    checkpoint(db,saved,now)
    result.update(discovered=len(saved['items']),remaining=saved['remaining'],discovery_complete=saved.get('discovery_complete',False) and bool(saved.get('bde_checked_at')))
    result['remaining_items'] = saved['remaining']
    result['status'] = saved.get('retry_status') if saved.get('retry_at') and now<datetime.fromisoformat(saved['retry_at']) else 'BACKFILL_INCOMPLETE' if not saved.get('discovery_complete') or not saved.get('bde_checked_at') or saved['remaining'] else 'SUCCESS_NEW_DATA' if result['new_document_versions'] else 'SUCCESS_UNCHANGED'
    return result



def collect_decisions_live(db, now):
    """Current-year decisions only; preserve the existing original/rate keys."""
    result = {'source': SOURCE, 'lane': 'decision-live-index', 'documents': 0,
              'values': 0, 'new_document_versions': 0}
    params = {'min_anno_pubblicazione': now.year, 'max_anno_pubblicazione': now.year,
              'page': 1, 'com.dotmarketing.htmlpage.language': 1}
    url = INDEX + '?' + urllib.parse.urlencode(params)
    try:
        body, meta = common.conditional_fetch(db, SOURCE, 'live-decisions-index', url,
                                               timeout=12, cap=4_000_000)
        items, _ = parse_index(body, now.year, 1)
        decisions = [item for item in items.values()
                     if re.search(r'monetary policy decisions', item['title'], re.I)]
        if not decisions:
            raise ValueError('ECB_LIVE_INDEX_NO_DECISION_LINKS')
        archive._save_index(db, SOURCE, 'live-decisions-index', url, body, meta, now)
        # The latest listed decision is reviewed for amendments, without
        # replaying speeches/accounts or resetting the historical cursor.
        item = max(decisions, key=lambda i: i["url"])
        count, is_new = fetch_document(db, item, now)
        if count != 3:
            raise ValueError('ECB_LIVE_REQUIRED_THREE_RATES')
        result.update(documents=1, values=count, new_document_versions=int(is_new),
                      index_http_status=meta['status'],
                      status='SUCCESS_NEW_DATA' if is_new else 'SUCCESS_UNCHANGED')
    except Exception as error:
        archive._record_fetch_status(db, SOURCE, 'live-decisions-index', url, now,
                                     getattr(error, 'code', None),
                                     type(error).__name__ + ':' + str(error)[:160])
        result.update(status=common._exception_outcome(error), error=str(error)[:160])
    return result
