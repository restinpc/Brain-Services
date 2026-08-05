#!/usr/bin/env python3
from __future__ import annotations
import ast, json, re, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta
from pathlib import Path

ROOT=Path('/brain/Brain-Services')
SERVICES=[35,42,44,46,47,49,50,53,56,57,58,62,63,65,66,67,68,70,72,73,74,75,76]
PAIRS=[1,3,4]; DAYS=[0,1]; LOOKBACK_DAYS=10; TARGET_END=datetime(2026,8,4)
OUT_JSON=Path('/brain/Brain-Server/logs/framework_audit_23.json')
OUT_TXT=Path('/brain/Brain-Server/logs/framework_audit_23.txt')

def request_json(url,timeout=180):
    req=urllib.request.Request(url,headers={'User-Agent':'Brain-23-Audit/1.0'})
    try:
        with urllib.request.urlopen(req,timeout=timeout) as r:
            body=r.read().decode('utf-8',errors='replace'); code=int(r.getcode())
    except urllib.error.HTTPError as e:
        code=int(e.code); body=e.read().decode('utf-8',errors='replace')
    except Exception as e:
        return None,None,str(e)
    try:return code,json.loads(body),body
    except Exception:return code,None,body

def normalize_range(v):
    if isinstance(v,list):
        out=[]
        for x in v:
            try: out.append(int(x))
            except: pass
        return out
    if isinstance(v,int): return list(range(max(0,v)))
    if isinstance(v,str): return [int(x) for x in re.findall(r'-?\d+',v)]
    return []

def find_recursive(obj,names):
    if isinstance(obj,dict):
        for n in names:
            if n in obj:return obj[n]
        for v in obj.values():
            f=find_recursive(v,names)
            if f is not None:return f
    return None

def read_ranges(sid):
    d=ROOT/str(sid); types=[]; vars_=[]; cfg=d/'config.toml'
    if cfg.exists():
        try:
            try: import tomllib
            except ImportError: import tomli as tomllib
            with cfg.open('rb') as f:data=tomllib.load(f)
            types=normalize_range(find_recursive(data,('types_range','type_range','types','TYPES_RANGE','TYPE_RANGE')))
            vars_=normalize_range(find_recursive(data,('var_range','vars_range','vars','VAR_RANGE','VARS_RANGE')))
        except: pass
    for fn in ('config.py','server.py','model.py'):
        p=d/fn
        if not p.exists():continue
        text=p.read_text(errors='replace')
        if not types:
            for key in ('TYPES_RANGE','TYPE_RANGE','types_range','type_range'):
                m=re.search(rf'(?m)^\s*{re.escape(key)}\s*=\s*(\[[^\n]+\]|\([^\n]+\)|range\([^\n]+\))',text)
                if m:
                    raw=m.group(1)
                    try:
                        if raw.startswith('range('):
                            nums=[int(x) for x in re.findall(r'-?\d+',raw)]; types=list(range(*nums))
                        else: types=normalize_range(ast.literal_eval(raw))
                    except: pass
                    if types:break
        if not vars_:
            for key in ('VAR_RANGE','VARS_RANGE','var_range','vars_range'):
                m=re.search(rf'(?m)^\s*{re.escape(key)}\s*=\s*(\[[^\n]+\]|\([^\n]+\)|range\([^\n]+\))',text)
                if m:
                    raw=m.group(1)
                    try:
                        if raw.startswith('range('):
                            nums=[int(x) for x in re.findall(r'-?\d+',raw)]; vars_=list(range(*nums))
                        else: vars_=normalize_range(ast.literal_eval(raw))
                    except: pass
                    if vars_:break
    overrides={58:([0],[2]),67:([0,1,2],[0,1,2,3])}
    if sid in overrides:
        ft,fv=overrides[sid]
        if not types or types==[0]:types=ft
        if not vars_ or vars_==[0]:vars_=fv
    return sorted(set(types or [0]))[:64],sorted(set(vars_ or [0]))[:128]

def endpoint_limit(openapi,path,default):
    try:
        for p in openapi['paths'][path]['get'].get('parameters',[]):
            if p.get('name')=='samples':
                m=p.get('schema',{}).get('maximum')
                if m is not None:return max(1,min(default,int(m)))
    except:pass
    return default

def frame_stats(payload,frame):
    try:checks=payload['payLoad']['frames'][frame]['checks']
    except:return {'valid':False,'nonempty':False,'checks':0,'keys':0,'nonzero':0}
    keys=sum(int(x.get('keys',0) or 0) for x in checks); nonzero=sum(int(x.get('nonzero',0) or 0) for x in checks)
    return {'valid':True,'nonempty':keys>0 or nonzero>0,'checks':len(checks),'keys':keys,'nonzero':nonzero}

def causal_status(payload):
    if not isinstance(payload,dict) or payload.get('status')!='ok':return 'INVALID'
    text=json.dumps(payload.get('payLoad',{}),ensure_ascii=False).upper()
    if '"STATUS": "FAIL"' in text:return 'FAIL'
    if '"STATUS": "PASS"' in text or ('"PASS"' in text and '"FAIL"' not in text):return 'PASS'
    return 'INCONCLUSIVE'

def values_check(port,pair,day,typ,var):
    rows=[]
    for off in range(LOOKBACK_DAYS-1,-1,-1):
        dt=TARGET_END-timedelta(days=off)
        q=urllib.parse.urlencode({'pair':pair,'day':day,'date':dt.strftime('%Y-%m-%d 00:00:00'),'type':typ,'var':var,'param':''})
        code,payload,_=request_json(f'http://127.0.0.1:{port}/values?{q}',180)
        count=0
        if isinstance(payload,dict) and isinstance(payload.get('payLoad'),dict):count=len(payload['payLoad'])
        rows.append({'date':dt.strftime('%Y-%m-%d'),'http':code,'count':count,'details':payload.get('details') if isinstance(payload,dict) else None})
    return rows

def main():
    report={'created_at':datetime.now().isoformat(),'services':[],'summary':{}}
    lines=[]; totals={'services':0,'http_ok':0,'diagnostics_ok':0,'direct_all_empty':0,'values_all_empty':0,'causal_pass':0,'failed':0}
    for sid in SERVICES:
        totals['services']+=1; port=8862+sid
        rec={'id':sid,'port':port,'metadata':{},'ranges':{},'direct':[],'values':[],'causal':[],'classification':[]}
        lines.append(f'\n===== SERVICE {sid} port={port} =====')
        code,root,_=request_json(f'http://127.0.0.1:{port}/',30)
        if code!=200 or not isinstance(root,dict):
            rec['classification'].append('HTTP_FAILED');totals['failed']+=1;report['services'].append(rec);lines.append(f'HTTP FAILED code={code}');continue
        totals['http_ok']+=1; md=root.get('metadata',{})
        rec['metadata']={k:(root.get('name') if k=='name' else md.get(k)) for k in ('name','dataset','ctx_index','weight_codes','simple_rates','enriched_table','cache_role','cache_table','last_reload')}
        lines.append('META '+json.dumps(rec['metadata'],ensure_ascii=False))
        types,vars_=read_ranges(sid);rec['ranges']={'types':types,'vars':vars_};lines.append(f'RANGES types={types} vars={vars_}')
        code,openapi,_=request_json(f'http://127.0.0.1:{port}/openapi.json',30);paths=openapi.get('paths',{}) if isinstance(openapi,dict) else {}
        has_tf='/diagnostics/timeframes' in paths;has_causal='/diagnostics/future_leak' in paths
        if not has_tf:
            rec['classification'].append('DIAGNOSTICS_MISSING');totals['failed']+=1;report['services'].append(rec);lines.append('DIAGNOSTICS MISSING');continue
        totals['diagnostics_ok']+=1;samples=endpoint_limit(openapi,'/diagnostics/timeframes',24);csamples=endpoint_limit(openapi,'/diagnostics/future_leak',5)
        any_direct=False;first={}
        for pair in PAIRS:
            for typ in types:
                for var in vars_:
                    q=urllib.parse.urlencode({'pair':pair,'type':typ,'var':var,'samples':samples})
                    code,payload,_=request_json(f'http://127.0.0.1:{port}/diagnostics/timeframes?{q}',300)
                    h=frame_stats(payload,'hour');d=frame_stats(payload,'day');ok=code==200 and isinstance(payload,dict) and payload.get('status')=='ok'
                    rec['direct'].append({'pair':pair,'type':typ,'var':var,'http':code,'ok':ok,'hour':h,'day':d})
                    if h['nonempty'] or d['nonempty']:
                        any_direct=True;first.setdefault(pair,(typ,var))
                    lines.append(f'DIRECT pair={pair} t={typ} v={var} H={int(h["nonempty"])} D={int(d["nonempty"])} http={code}')
        if not any_direct:rec['classification'].append('DIRECT_MODEL_ALL_EMPTY');totals['direct_all_empty']+=1
        any_values=False
        for pair in PAIRS:
            typ,var=first.get(pair,(types[0],vars_[0]))
            for day in DAYS:
                rows=values_check(port,pair,day,typ,var);nonempty=sum(1 for x in rows if x['count']>0)
                rec['values'].append({'pair':pair,'day':day,'type':typ,'var':var,'nonempty_dates':nonempty,'rows':rows})
                any_values=any_values or nonempty>0;lines.append(f'VALUES pair={pair} day={day} t={typ} v={var} nonempty={nonempty}/{LOOKBACK_DAYS}')
        if not any_values:rec['classification'].append('VALUES_LAST10_ALL_EMPTY');totals['values_all_empty']+=1
        if has_causal:
            service_pass=False
            for pair,(typ,var) in first.items():
                q=urllib.parse.urlencode({'pair':pair,'type':typ,'var':var,'samples':csamples})
                code,payload,_=request_json(f'http://127.0.0.1:{port}/diagnostics/future_leak?{q}',420);status=causal_status(payload)
                rec['causal'].append({'pair':pair,'type':typ,'var':var,'http':code,'status':status});lines.append(f'CAUSAL pair={pair} t={typ} v={var} status={status}')
                service_pass=service_pass or status=='PASS'
            if service_pass:totals['causal_pass']+=1
            elif first:rec['classification'].append('CAUSAL_NO_PASS')
        if any_direct and not any_values:rec['classification'].append('DIRECT_NONEMPTY_VALUES_EMPTY')
        elif not any_direct and not any_values:rec['classification'].append('MODEL_OR_INPUT_DATA_EMPTY')
        elif any_direct and any_values:rec['classification'].append('BASIC_PATH_OK')
        report['services'].append(rec)
    report['summary']=totals;OUT_JSON.parent.mkdir(parents=True,exist_ok=True);OUT_JSON.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    lines.append('\n===== SUMMARY =====');lines += [f'{k}={v}' for k,v in totals.items()];OUT_TXT.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('\n'.join(lines[-20:]));print(f'JSON: {OUT_JSON}');print(f'TEXT: {OUT_TXT}')
if __name__=='__main__':main()
