"""Official MID SOAP recovery for the recent repository, never a 2025 archive."""
import base64
import hashlib
import html
import json
import re
import ssl
import subprocess
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import model100_primary_sources as common
except ImportError:
    from work import model100_primary_sources as common

URL='https://mid.ecb.europa.eu/searchReleases'
KEY='control:mid-soap-recovery'


def unsigned_message(body):
    # The provider signs the original XML stream without its footer, including
    # the XML declaration and original whitespace. Do not reserialize it.
    result,count=re.subn(rb'\s*<escb:footer>.*?</escb:footer>',b'',body,flags=re.S)
    if count!=1:raise ValueError('MID_FOOTER_NOT_UNIQUE')
    return result


def verify_signature(body):
    root=ET.fromstring(body)
    raw=common.xml_text(root,'digest')
    if not raw:raise ValueError('MID_SIGNATURE_ABSENT')
    der=base64.b64decode(''.join(raw.split()),validate=True)
    ca=ssl.get_default_verify_paths().cafile
    if not ca:raise ValueError('SYSTEM_CA_BUNDLE_NOT_FOUND')
    with tempfile.TemporaryDirectory(prefix='model100-mid-signature-') as folder:
        folder=Path(folder);sig=folder/'signature.der';payload=folder/'payload';signer=folder/'signer.pem'
        sig.write_bytes(der)
        checked=subprocess.run(['openssl','cms','-verify','-inform','DER','-in',str(sig),
            '-CAfile',ca,'-purpose','any','-signer',str(signer),'-out',str(payload)],capture_output=True,timeout=10)
        if checked.returncode:raise ValueError('MID_CMS_CHAIN_OR_SIGNATURE_INVALID')
        hostname=subprocess.run(['openssl','x509','-in',str(signer),'-noout','-checkhost',
            'signature.mid.ecb.europa.eu'],capture_output=True,timeout=5)
        if hostname.returncode:raise ValueError('MID_SIGNER_HOSTNAME_INVALID')
        signed=payload.read_bytes().strip().decode('ascii')
    digest=hashlib.md5(unsigned_message(body)).hexdigest()
    if signed!=digest:raise ValueError('MID_MESSAGE_DIGEST_MISMATCH')
    return {'signature_verified':True,'certificate_chain_verified':True,
            'signer_hostname':'signature.mid.ecb.europa.eu','provider_signed_md5':signed,
            'signature_der_sha256':hashlib.sha256(der).hexdigest()}


def parse_decision(body):
    root=ET.fromstring(body)
    if common.xml_text(root,'releaseType')!='PressReleaseMonPolDecision':
        raise ValueError('MID_DECISION_TYPE_MISMATCH')
    raw=common.xml_text(root,'publicationDateTime')
    content=common.xml_text(root,'content') or ''
    text=' '.join(html.unescape(re.sub('<[^>]+>',' ',content.replace('<![CDATA[','').replace(']]>',''))).split())
    published=common.parse_explicit_datetime(raw)
    evidence='MID_EXPLICIT_OFFSET'
    if published is None:
        # A raw naive timestamp is never silently UTC. This decision's own
        # text must name the applicable clock, independently of its schedule.
        local=datetime.fromisoformat(raw)
        zone=ZoneInfo('Europe/Luxembourg')
        zoned=local.replace(tzinfo=zone)
        token='CEST' if zoned.utcoffset()==timedelta(hours=2) else 'CET'
        if re.search(r'\b'+token+r'\b',text):
            published=zoned.astimezone(timezone.utc).replace(tzinfo=None)
            evidence='MID_DOCUMENT_'+token
        else:evidence='DAY_ONLY'
    rates=re.search(r'deposit facility,\s*the main refinancing operations and\s*the marginal lending facility.*?(?:at|to)\s+([+-]?\d+(?:\.\d+)?)%\s*,\s*([+-]?\d+(?:\.\d+)?)%\s*,?\s*and\s*([+-]?\d+(?:\.\d+)?)%\s+respectively',text.replace('\u2212','-'),re.I)
    return {'published':published,'clock_evidence':evidence,'publication_raw':raw,
            'release_raw':common.xml_text(root,'releaseDateTime'),'text':text,
            'title':common.xml_text(root,'releaseTitle'),
            'rates':dict(zip(('ECB_DEPOSIT_FACILITY_RATE','ECB_MAIN_REFINANCING_RATE','ECB_MARGINAL_LENDING_RATE'),map(float,rates.groups()))) if rates else {}}


def store_decision(db,url,original,metadata,now,publication_hint=None,asof='HISTORICAL_DOCUMENTED'):
    try:
        verified=verify_signature(original);parsed=parse_decision(original)
        pub=publication_hint or parsed['published']
        received=common.parse_explicit_datetime(metadata.get('received_at_utc')) or now
        if pub and pub>received:raise ValueError('MID_FUTURE_PUBLICATION')
    except (ValueError,ET.ParseError,subprocess.SubprocessError) as error:
        common.store(db,common.base_row('ECB_MID','mid:'+url.rsplit('/',1)[-1],'RELEASE',url,
            original,metadata,now,None,'UNKNOWN','UNUSABLE',event_type='PressReleaseMonPolDecision',
            extra_json={'signature_verified':False,'validation_error':type(error).__name__+':'+str(error)[:120]}))
        raise
    evidence='MID_RSS_EXPLICIT_OFFSET' if publication_hint else parsed['clock_evidence']
    key='mid:'+url.rsplit('/',1)[-1]
    ex={**verified,'publicationDateTime_raw':parsed['publication_raw'],
        'releaseDateTime_raw':parsed['release_raw'],'decision_event':True,
        'recovery_parser_revision':1,'source_clock_basis':evidence,
        'recovered_from_recent_repository':publication_hint is None,
        'first_received_is_not_original_publication':asof!='CERTIFIED_LIVE'}
    quality=asof if pub else 'DAY_ONLY'
    common.store(db,common.base_row('ECB_MID',key,'RELEASE',url,original,metadata,now,pub,evidence,quality,
        title=parsed['title'],event_type='PressReleaseMonPolDecision',region='EA',
        reference_timezone='Europe/Luxembourg' if pub else None,extra_json=ex))
    observation=pub.date() if pub else datetime.fromisoformat(parsed['publication_raw']).date()
    for metric,value in parsed['rates'].items():
        common.store(db,common.base_row('ECB_MID',key+':'+metric,'RELEASE_VALUE',url,original,metadata,now,pub,
            evidence,quality,title=parsed['title'],event_type=metric,region='EA',
            observation_period_start=observation.isoformat(),vintage_month=observation.replace(day=1).isoformat(),
            numeric_value=value,unit='PERCENT',reference_timezone='Europe/Luxembourg' if pub else None,extra_json=ex))
    return len(parsed['rates'])


def recover(db,now):
    cur=db.cursor();cur.execute(f'SELECT extra_json FROM `{common.TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1',('ECB_MID',KEY))
    prior=cur.fetchone();cur.close();state=json.loads(prior[0]) if prior else {}
    if state.get('next_attempt_utc') and now<datetime.fromisoformat(state['next_attempt_utc']):
        return {'source':'ECB_MID','lane':'recent-soap-recovery','status':'NOT_DUE'}
    result={'source':'ECB_MID','lane':'recent-soap-recovery','documents':0,'rate_values':0,'errors':[]}
    retry=3600
    try:
        start=(now-timedelta(days=29)).strftime('%d/%m/%Y');end=now.strftime('%d/%m/%Y')
        request_body=(f'<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:sear="searchReleases"><soapenv:Header/><soapenv:Body><sear:searchReleases><releaseType>PressReleaseMonPolDecision</releaseType><startPublicationDate>{start}</startPublicationDate><endPublicationDate>{end}</endPublicationDate><language>EN</language><sortBy>publicationDate</sortBy><sortFlag>DESC</sortFlag></sear:searchReleases></soapenv:Body></soapenv:Envelope>').encode()
        req=urllib.request.Request(URL,data=request_body,headers={'Content-Type':'text/xml; charset=utf-8','SOAPAction':'"searchReleases"','User-Agent':'Brain-Model100/1.0 research collector'})
        with urllib.request.urlopen(req,timeout=12) as response:
            body=response.read(2_000_001)
            if len(body)>2_000_000:raise ValueError('MID_QUERY_RESPONSE_TOO_LARGE')
            meta={'status':response.status,'url':URL,'content_type':response.headers.get('Content-Type'),
                  'received_at_utc':datetime.now(timezone.utc).isoformat()}
        root=ET.fromstring(body)
        if common.xml_text(root,'errorOutput'):raise ValueError('MID_SOAP_ERROR')
        if common.xml_text(root,'resultsCount') is None:raise ValueError('MID_SOAP_RESPONSE_REQUIRED')
        common.store(db,common.base_row('ECB_MID','soap:recent-decisions','REPOSITORY_QUERY',URL,body,meta,now,None,'DAY_ONLY','DAY_ONLY',extra_json={'query_start':start,'query_end':end,'response_bytes':len(body),'http_method':'POST_READ_QUERY'}))
        result['http_status']=meta['status'];result['matches']=int(common.xml_text(root,'resultsCount'))
        for node in root.iter():
            if node.tag.rsplit('}',1)[-1]!='results':continue
            url=common.xml_text(node,'linkToPublication') or ''
            if not re.fullmatch(r'https://mid\.ecb\.europa\.eu/rel/[a-f0-9]{32}\.xml',url):
                raise ValueError('MID_RESULT_URL_NOT_ALLOWLISTED')
            key='mid:'+url.rsplit('/',1)[-1]
            cur=db.cursor();cur.execute(f"SELECT COUNT(DISTINCT event_type) FROM `{common.TABLE}` WHERE source_id='ECB_MID' AND record_key LIKE %s AND numeric_value IS NOT NULL AND JSON_EXTRACT(extra_json,'$.signature_verified')=true AND JSON_EXTRACT(extra_json,'$.recovery_parser_revision')=1",(key+':ECB_%',))
            exists=cur.fetchone();cur.close()
            if exists and exists[0]>=3:continue
            time.sleep(1.1)
            original,metadata=common.conditional_fetch(db,'ECB_MID',key,url,timeout=12,cap=3_000_000)
            original_day=datetime.fromisoformat(parse_decision(original)['publication_raw']).date()
            if not (now.date()-timedelta(days=30)<=original_day<=now.date()):
                raise ValueError('MID_RESULT_OUTSIDE_RECENT_REPOSITORY_WINDOW')
            count=store_decision(db,url,original,metadata,now)
            result['documents']+=1
            result['rate_values']+=count
            if count!=3:result['errors'].append({'url':url,'reason':'REQUIRED_RATE_FIELDS_NOT_PARSED'})
        result['status']='PARTIAL_COVERAGE' if result['errors'] else 'SUCCESS_NEW_DATA' if result['documents'] else 'SUCCESS_UNCHANGED'
    except Exception as error:
        result['status']=common._exception_outcome(error)
        retry=86400 if result['status']=='BLOCKED_ACCESS' else 900 if result['status']=='NETWORK_TIMEOUT' else 3600
        if result['status']=='RATE_LIMITED':
            value=error.headers.get('Retry-After') if error.headers else None
            try:retry=max(300,int(value))
            except (ValueError,TypeError):
                clock=common.parse_explicit_datetime(value)
                retry=max(300,int((clock-now).total_seconds())) if clock else 900
        result['errors'].append({'type':type(error).__name__,'status':getattr(error,'code',None),'reason':str(error)[:150]})
        payload=json.dumps(result['errors'][-1],sort_keys=True).encode()
        common.store(db,common.base_row('ECB_MID','fetch-status:soap-recovery','FETCH_STATUS',URL,payload,
            {'status':getattr(error,'code',0),'content_type':'application/json'},now,None,'OBSERVED','FETCH_STATUS',
            extra_json={'classification':result['status'],'retry_seconds':retry}))
    payload=b'{"source":"ECB_MID","control":"recent-soap-recovery"}'
    common.store(db,common.base_row('ECB_MID',KEY,'RUN_CONTROL','brain://model100/mid-soap-recovery',payload,{'status':0},now,None,'OBSERVED','RUN_CONTROL',event_type='BACKFILL_CHECKPOINT',extra_json={'next_attempt_utc':(now+timedelta(seconds=retry)).isoformat(),'last_outcome':result['status']}))
    return result
