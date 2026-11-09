"""Independent bounded Fed speech archive and official RSS lane under ID166."""
import hashlib
import html
import json
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date,datetime,timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

try:
    import model100_primary_sources as common
    import model100_primary_archive as archive
except ImportError:
    from work import model100_primary_sources as common,model100_primary_archive as archive

SOURCE='FED_FOMC'
RSS='https://www.federalreserve.gov/feeds/speeches.xml'
VERSION='model100-fed-speeches-20261108-v2'
ACCESS_BLOCKED_URLS=('https://www.federalreserve.gov/newsevents/speech/waller20261008a.htm',)


def speech_day(url):
    p=urllib.parse.urlparse(url)
    if p.scheme!='https' or p.hostname!='www.federalreserve.gov':return None
    m=re.fullmatch(r'/newsevents/speech/[a-z]+(20\d{2})(\d{2})(\d{2})[a-z]\.htm',p.path)
    try:return date(*map(int,m.groups())) if m else None
    except ValueError:return None


class SpeechText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth=0;self.article=[];self.capture=None;self.fields={};self.pieces=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='div':
            if attrs.get('id')=='article' and not self.depth:self.depth=1
            elif self.depth:self.depth+=1
        if self.depth and tag in ('p','h3'):
            cls=attrs.get('class','')
            if cls in ('speaker','article__time','title'):
                self.capture=(tag,cls);self.pieces=[]
    def handle_data(self,data):
        if self.depth:self.article.append(data)
        if self.capture:self.pieces.append(data)
    def handle_endtag(self,tag):
        if self.capture and tag==self.capture[0]:
            self.fields[self.capture[1]]=' '.join(' '.join(self.pieces).split());self.capture=None
        if tag=='div' and self.depth:self.depth-=1


def parse_speech(body,url):
    expected=speech_day(url)
    p=SpeechText();p.feed(body.decode('utf-8','replace'))
    if not expected or not p.fields.get('article__time'):raise ValueError('FED_SPEECH_DATE_MISSING')
    actual=datetime.strptime(p.fields['article__time'],'%B %d, %Y').date()
    if abs((actual-expected).days)>1:raise ValueError('FED_SPEECH_WRONG_YEAR_OR_DATE')
    text=' '.join(' '.join(p.article).split())
    if not p.fields.get('speaker') or not p.fields.get('title') or len(text)<150:
        raise ValueError('FED_SPEECH_REQUIRED_TEXT_MISSING')
    return {'day':actual,'url_encoded_day':expected,'url_date_discrepancy_days':(actual-expected).days,
            'speaker':p.fields['speaker'],'title':p.fields['title'],
            'text':text,'speech_text_sha256':hashlib.sha256(text.encode()).hexdigest()}


def parse_rss(body,now):
    root=ET.fromstring(body);items={}
    for item in root.findall('./channel/item'):
        url=(item.findtext('link') or '').strip();day=speech_day(url)
        if not day or not date(2025,1,1)<=day<=now.date():continue
        try:published=parsedate_to_datetime(item.findtext('pubDate') or '')
        except (TypeError,ValueError):continue
        if published.tzinfo is None:continue
        published=published.astimezone(common.UTC).replace(tzinfo=None)
        if published>now or abs((published.date()-day).days)>7:continue
        items[url]={'url':url,'published':published,'day':day,'title':item.findtext('title')}
    return sorted(items.values(),key=lambda r:(r['published'],r['url']))


def state(db,key):
    cur=db.cursor();cur.execute(f"SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",(SOURCE,key))
    row=cur.fetchone();cur.close()
    return json.loads(row[0]) if row and row[0] else {}


def save_state(db,key,value,now):
    body=json.dumps({'state_key':key,'schema':1},sort_keys=True).encode()
    row=common.base_row(SOURCE,key,'RUN_CONTROL','brain://model100/fed-speech-state',body,
        {'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',
        event_type='BACKFILL_CHECKPOINT',extra_json=value)
    common.store(db,row)


def failure(db,key,url,exc,now):
    status=common._exception_outcome(exc)
    headers={k.lower():v for k,v in exc.headers.items()} if getattr(exc,'headers',None) else {}
    seconds=604800 if status=='BLOCKED_ACCESS' else 86400 if status=='PARSE_ERROR' else 900
    retry=headers.get('retry-after','')
    if retry.isdigit():seconds=max(1,int(retry))
    else:
        clock=common.parse_explicit_datetime(retry)
        if clock:seconds=max(1,int((clock-now).total_seconds())+1)
    if getattr(exc,'code',None) and hasattr(exc,'read'):
        error_body=exc.read(1_000_001)
        if error_body:
            row=common.base_row(SOURCE,'speech-http-error:'+key,'HTTP_ERROR',url,error_body[:1_000_000],
                {'status':exc.code,'content_type':headers.get('content-type'),'received_at_utc':common.utcnow().replace(tzinfo=common.UTC).isoformat()},
                now,None,'OBSERVED','FETCH_STATUS',event_type='FETCH_STATUS',
                extra_json={'response_body_truncated':len(error_body)>1_000_000,'http_error_class':status})
            common.store(db,row)
    archive._record_fetch_status(db,SOURCE,key,url,now,getattr(exc,'code',None),type(exc).__name__+':'+str(exc)[:150],headers)
    return {'status':status,'retry_at':(now+timedelta(seconds=seconds)).isoformat(),'parser_version':VERSION}


def retry_due(info,now):
    if info.get('status')=='PARSE_ERROR' and info.get('parser_version')!=VERSION:return True
    return not info.get('retry_at') or now>=datetime.fromisoformat(info['retry_at'])


def seed_blocked(checkpoint,now):
    checkpoint.setdefault('failed',{})
    for url in ACCESS_BLOCKED_URLS:
        checkpoint['failed'].setdefault(url,{'status':'BLOCKED_ACCESS','http_status':403,
            'basis':'confirmed B1 HTTP403 probe before adapter deployment; no retry in this session',
            'observed_day':'2026-11-07','retry_at':(now+timedelta(days=7)).isoformat()})


def fetch_speech(db,item,now,live=False):
    url=item['url'];key='speech:'+url.rsplit('/',1)[-1]
    body,meta=common.conditional_fetch(db,SOURCE,key,url,timeout=12,cap=4_000_000)
    try:
        data=parse_speech(body,url)
        if data['day']>now.date():raise ValueError('FED_SPEECH_FUTURE_DAY_EXCLUDED')
    except ValueError as exc:
        raw=common.base_row(SOURCE,'validation-failed:'+key,'VALIDATION_FAILURE',url,body,meta,
            now,None,'DAY_ONLY','UNUSABLE',event_type='PARSE_ERROR',extra_json={'error':str(exc),'parser_version':VERSION})
        common.store(db,raw)
        raise
    text_sha,text_path=common.preserve(SOURCE,data['text'].encode())
    pub=item.get('published')
    cur=db.cursor();cur.execute(f"SELECT source_published_at_utc,extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",(SOURCE,key))
    prior=cur.fetchone();cur.close()
    prior_extra=(json.loads(prior[1] or '{}') if isinstance(prior[1],str) else prior[1] or {}) if prior else {}
    if pub is None and prior and prior[0] is not None:pub=prior[0]
    changed=prior_extra.get('speech_text_sha256')!=text_sha
    receipt=common.parse_explicit_datetime(meta.get('received_at_utc')) or now
    quality=('CERTIFIED_LIVE' if live and pub and 0<=(receipt-pub).total_seconds()<=300 else
             'HISTORICAL_DOCUMENTED' if pub else 'DAY_ONLY')
    extra={**prior_extra,'document_date':data['day'].isoformat(),'speaker':data['speaker'],
        'document_day_evidence':'official article__time; URL date is only an identifier',
        'url_encoded_day':data['url_encoded_day'].isoformat(),'url_date_discrepancy_days':data['url_date_discrepancy_days'],
        'speech_text_sha256':text_sha,'speech_text_raw_path':text_path,'speech_text_characters':len(data['text']),
        'speech_delivery_day_is_not_publication_hour':True,'original_first_text_version_verified':quality=='CERTIFIED_LIVE' or not changed and prior_extra.get('original_first_text_version_verified',False),
        'timestamp_source':prior_extra.get('timestamp_source') or ('official Fed speeches RSS explicit pubDate' if pub else 'official speech page day only'),
        'source_timezone':'America/New_York','is_policy_decision':False}
    archive._store_document(db,SOURCE,key,url,body,meta,now,data['title'],'FED_SPEECH',pub_at=pub,
        evidence='DOCUMENTED_CLOCK' if pub else 'DAY_ONLY',asof=quality,region='UNITED_STATES',extra=extra)
    return {'url':url,'original_sha256':hashlib.sha256(body).hexdigest(),'speech_text_sha256':text_sha,
            'speaker':data['speaker'],'quality':quality,'http_status':meta.get('status'),'semantic_text_changed':changed}


def collect_archive(db,now,policy=False):
    key='control:fed-policy-archive' if policy else 'control:fed-speech-archive';checkpoint=state(db,key)
    checkpoint.setdefault('done',[]);checkpoint.setdefault('failed',{})
    if not policy:seed_blocked(checkpoint,now)
    result={'source':SOURCE,'lane':'policy-archive-chunk' if policy else 'speech-archive-chunk','documents':0,'errors':[]}
    candidates={}
    for year in (2025,2026):
        url=(f'https://www.federalreserve.gov/newsevents/pressreleases/{year}-press.htm' if policy else
             f'https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm')
        try:
            body,meta,links=archive._index_links(db,SOURCE,('policy-index:' if policy else 'speech-index:')+str(year),url,now,timeout=12)
            for href,title,_ in links:
                target=urllib.parse.urljoin(url,href)
                if policy:
                    parsed=urllib.parse.urlparse(target)
                    if parsed.hostname!='www.federalreserve.gov' or not re.fullmatch(r'/newsevents/pressreleases/monetary20\d{6}[a-z]\.htm',parsed.path):continue
                    day=archive._fed_release_date(parsed.path)
                else:day=speech_day(target)
                if day and date(2025,1,1)<=day<=now.date():candidates[target]={'url':target,'day':day,'title':title}
        except Exception as exc:
            info=failure(db,('policy-index:' if policy else 'speech-index:')+str(year),url,exc,now)
            result['errors'].append({'url':url,'status':info['status']})
    pending=[item for url,item in sorted(candidates.items(),key=lambda x:(x[1]['day'],x[0])) if url not in checkpoint['done']]
    eligible=[i for i in pending if retry_due(checkpoint['failed'].get(i['url'],{}),now)]
    known_urls=set()
    if policy:
        cur=db.cursor();cur.execute(f"SELECT DISTINCT canonical_url FROM `{common.TABLE}` WHERE source_id=%s AND record_kind='ORIGINAL_DOCUMENT'",(SOURCE,))
        known_urls={r[0] for r in cur.fetchall()};cur.close()
    review=False
    if not eligible and (not checkpoint.get('last_revision_check_utc') or now-datetime.fromisoformat(checkpoint['last_revision_check_utc'])>=timedelta(days=7)):
        eligible=sorted((i for u,i in candidates.items() if u in checkpoint['done'] and u not in checkpoint['failed']),key=lambda i:i['day'],reverse=True)[:3]
        review=bool(eligible)
    for item in eligible[:3]:
        try:
            if policy:
                outcome=archive._download_original(db,SOURCE,item['url'],item['title'],'FOMC_RELEASE',item['day'],now,'UNITED_STATES','America/New_York',{'discovery_lane':'official general press archive monetary-policy releases'})
                if outcome not in ('STORED','UNCHANGED_304'):
                    status='BLOCKED_ACCESS' if outcome=='BLOCKED_ACCESS' else 'NETWORK_TIMEOUT' if 'timeout' in outcome.lower() else 'PARSE_ERROR'
                    checkpoint['failed'][item['url']]={'status':status,'retry_at':(now+timedelta(days=7 if status=='BLOCKED_ACCESS' else 1)).isoformat()}
                    result['errors'].append({'url':item['url'],'status':status})
                    save_state(db,key,checkpoint,now);continue
                original={'url':item['url'],'status':outcome,'semantic_text_changed':item['url'] not in known_urls}
                known_urls.add(item['url'])
            else:original=fetch_speech(db,item,now)
            result.setdefault('originals',[]).append(original);result['documents']+=1
            result['new_semantic_documents']=result.get('new_semantic_documents',0)+int(original['semantic_text_changed'])
            if item['url'] not in checkpoint['done']:checkpoint['done'].append(item['url'])
            checkpoint['failed'].pop(item['url'],None)
        except Exception as exc:
            info=failure(db,'speech:'+item['url'].rsplit('/',1)[-1],item['url'],exc,now)
            checkpoint['failed'][item['url']]=info
            result['errors'].append({'url':item['url'],'status':info['status']})
        save_state(db,key,checkpoint,now);time.sleep(0.3)
    remaining=len([u for u in candidates if u not in checkpoint['done']])
    if review and not result['errors']:checkpoint['last_revision_check_utc']=now.isoformat()
    immediate=any(u not in checkpoint['done'] and retry_due(checkpoint['failed'].get(u,{}),now) for u in candidates)
    checkpoint.update(discovered_items=len(candidates),remaining_items=remaining,
                      last_checked_utc=now.isoformat(),state=('INCOMPLETE' if immediate else 'BLOCKED_ITEMS_REMAIN') if remaining else 'COMPLETE_INDEX_ITEMS')
    save_state(db,key,checkpoint,now);result['remaining_items']=checkpoint['remaining_items']
    result['status']='BACKFILL_INCOMPLETE' if immediate else 'PARTIAL_COVERAGE' if remaining or result['errors'] else 'SUCCESS_NEW_DATA' if result.get('new_semantic_documents') else 'SUCCESS_UNCHANGED'
    return result


def collect_live(db,now):
    key='control:fed-speech-live';prior=state(db,key)
    if prior.get('next_attempt_utc') and now<datetime.fromisoformat(prior['next_attempt_utc']):
        return {'source':SOURCE,'lane':'speech-live-rss','status':'NOT_DUE'}
    result={'source':SOURCE,'lane':'speech-live-rss','documents':0,'errors':[]}
    next_attempt=(now+timedelta(seconds=300)).isoformat()
    try:
        body,meta=common.conditional_fetch(db,SOURCE,'speech-rss',RSS,timeout=10,cap=2_000_000)
        archive._save_index(db,SOURCE,'speech-rss',RSS,body,meta,now)
        items=parse_rss(body,now)
        cur=db.cursor();cur.execute(f"SELECT canonical_url,source_published_at_utc FROM `{common.TABLE}` WHERE source_id=%s AND event_type='FED_SPEECH' AND record_kind='ORIGINAL_DOCUMENT'",(SOURCE,))
        known={r[0] for r in cur.fetchall() if r[1] is not None};cur.close()
        archive_state=state(db,'control:fed-speech-archive');saved_failed=dict(archive_state.get('failed',{}));seed_blocked(archive_state,now)
        blocked=dict(archive_state.get('failed',{}))
        for item in items:
            failure=blocked.get(item['url'],{})
            if item['url'] in known or not retry_due(failure,now):continue
            if result['documents']>=3:break
            try:
                result.setdefault('originals',[]).append(fetch_speech(db,item,now,live=True));result['documents']+=1
            except Exception as exc:
                info=failure(db,'speech:'+item['url'].rsplit('/',1)[-1],item['url'],exc,now)
                result['errors'].append({'url':item['url'],'status':info['status']});blocked[item['url']]=info
        if blocked!=saved_failed:
            archive_state['failed']=blocked;save_state(db,'control:fed-speech-archive',archive_state,now)
        result.update(feed_http_status=meta.get('status'),feed_bytes=meta.get('network_response_bytes'),rss_items=len(items))
        result['status']='PARTIAL_COVERAGE' if result['errors'] else 'SUCCESS_NEW_DATA' if result['documents'] else 'SUCCESS_UNCHANGED'
    except Exception as exc:
        info=failure(db,'speech-rss',RSS,exc,now);result['status']=info['status'];next_attempt=info['retry_at']
    save_state(db,key,{'last_outcome':result['status'],'last_checked_utc':now.isoformat(),
        'next_attempt_utc':next_attempt},now)
    return result
