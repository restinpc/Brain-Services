#!/usr/bin/env python3
import argparse, json, urllib.parse, urllib.request, concurrent.futures, sys

DEFAULT_IDS = [46,47,50,53,56,57,58,62,63,64,65,66,67,68,69,70,76]
PAIRS = (1,3,4)
DAYS = (0,1)

def get_json(url, timeout=120):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))

def base_url(model_id):
    return f"http://127.0.0.1:{8862 + model_id}"

def slots_for(mid):
    root = get_json(base_url(mid) + "/", 30)
    md = root.get("metadata", {})
    types = md.get("types_range") or [0]
    vars_ = md.get("var_range") or [0]
    params = md.get("param_range") or [""]
    return [(mid,p,d,t,v,str(pa)) for p in PAIRS for d in DAYS for t in types for v in vars_ for pa in params]

def run_slot(slot):
    mid,p,d,t,v,param = slot
    q=urllib.parse.urlencode({"pair":p,"day":d,"type":t,"var":v,"param":param})
    url=base_url(mid)+"/audit/future_leak?"+q
    try:
        out=get_json(url, 600)
        payload=out.get("payLoad", out)
        passed=bool(payload.get("passed")) if out.get("status")=="ok" else False
        return {"model":mid,"pair":p,"day":d,"type":t,"var":v,"param":param,"passed":passed,"response":out}
    except Exception as e:
        return {"model":mid,"pair":p,"day":d,"type":t,"var":v,"param":param,"passed":False,"error":str(e)}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--models", default=",".join(map(str,DEFAULT_IDS)))
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--output", default="future_leak_audit_results.json")
    args=ap.parse_args()
    ids=[int(x) for x in args.models.split(',') if x.strip()]
    slots=[]
    for mid in ids:
        try: slots.extend(slots_for(mid))
        except Exception as e:
            print(f"model {mid}: metadata error: {e}", file=sys.stderr)
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i,r in enumerate(ex.map(run_slot, slots),1):
            results.append(r)
            print(f"[{i}/{len(slots)}] model={r['model']} pair={r['pair']} day={r['day']} type={r['type']} var={r['var']} => {'PASS' if r['passed'] else 'FAIL'}")
    summary={"total":len(results),"passed":sum(x['passed'] for x in results),"failed":sum(not x['passed'] for x in results)}
    with open(args.output,'w',encoding='utf-8') as f: json.dump({"summary":summary,"results":results},f,ensure_ascii=False,indent=2)
    print(json.dumps(summary,ensure_ascii=False))
    print(args.output)
    return 0 if summary['failed']==0 else 2
if __name__=='__main__': raise SystemExit(main())
