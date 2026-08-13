"""Source-only runtime model for Brain news services 79."""
from __future__ import annotations

import bisect
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any
import numpy as np

RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
PRETEST_ALLOW_EMPTY = True
SHIFT_WINDOW = 12
VAR_RANGE = list(range(8))
TYPES_RANGE = [0,1,2,3,4]
USE_ML_VALUES = False
TOPIC_FOCUS_MIN = 0.36
CLUSTER_SIM_MIN = 0.70
NOVELTY_MIN = 0.35
MIN_EVENT_SUPPORT = 3

def _dt(row: dict[str,Any]):
    v=row.get("date") or row.get("date_dt")
    if isinstance(v,datetime): return v
    if isinstance(v,str):
        try: return datetime.fromisoformat(v[:19])
        except ValueError: return None
    return None

def _var_allows_row(row,var):
    try:
        focus=float(row.get("topic_focus") or 0.0); fit=float(row.get("cluster_similarity") or 0.0)
        novelty=float(row.get("novelty") or 0.0); confirm=int(row.get("confirmation_count") or 1)
    except (TypeError,ValueError): return var in (0,7)
    if var==0: return True
    if var==1: return focus>=TOPIC_FOCUS_MIN
    if var==2: return fit>=CLUSTER_SIM_MIN
    if var==3: return novelty>=NOVELTY_MIN
    if var==4: return confirm>=2
    if var==5: return confirm>=3
    if var==6: return novelty>=NOVELTY_MIN and confirm>=2
    if var==7: return True
    return False

def _ctx_reverse(ctx_index):
    out={}
    for info in (ctx_index or {}).values():
        try: cid=int(info.get("id"))
        except (TypeError,ValueError): continue
        et=str(info.get("event_type") or "").strip().lower()
        if et: out[et]=(cid,info)
    return out

def _frame_date(dt,is_daily):
    return dt.replace(hour=0,minute=0,second=0,microsecond=0) if is_daily else dt.replace(minute=0,second=0,microsecond=0)

def _previous_direction(np_rates,target):
    if not np_rates: return False,"ext_min"
    dns=np_rates.get("dates_ns"); opens=np_rates.get("open"); closes=np_rates.get("close")
    if dns is None or opens is None or closes is None or len(dns)==0: return False,"ext_min"
    cut=int(np.searchsorted(dns,int(target.timestamp()),side="left"))
    if cut<=0: return False,"ext_min"
    i=cut-1; pm=float(closes[i])>float(opens[i])
    return pm, "ext_max" if pm else "ext_min"

def _current_rows(source,target,di,is_daily):
    unit=timedelta(days=1) if is_daily else timedelta(hours=1); start=target-unit*SHIFT_WINDOW
    ts=di.get("dataset_timestamps")
    if ts is not None and len(ts)==len(source):
        l=int(np.searchsorted(ts,int(start.timestamp()),side="left")); r=int(np.searchsorted(ts,int(target.timestamp()),side="right")); return source[l:r]
    dates=di.get("dates") or []
    if dates and len(dates)==len(source): return source[bisect.bisect_left(dates,start):bisect.bisect_right(dates,target)]
    return [x for x in source if (d:=_dt(x)) is not None and start<=d<=target]

def _rate_index(np_rates,frame):
    dns=np_rates.get("dates_ns")
    if dns is None: return None
    ts=int(frame.timestamp()); i=int(np.searchsorted(dns,ts,side="left"))
    return i if i<len(dns) and int(dns[i])==ts else None

def _quality(row):
    try: q=float(row.get("quality_score") or row.get("pct_change") or 1.0)
    except (TypeError,ValueError): q=1.0
    return min(max(q,0.25),2.5)

def _mode1_score(hits,total,predict_max):
    if total<=0: return 0.0
    s=(hits/total)*2.0-1.0
    return -s if predict_max else s

def _add(target,code,value):
    if value and np.isfinite(value): target[code]=target.get(code,0.0)+float(value)

def _slot_for_date(dataset,target_date,*,calc_type,var,dataset_index):
    source=dataset_index.get("full_dataset") or dataset
    by_key=dataset_index.get("by_key") or {}; key_dates=dataset_index.get("key_dates") or {}
    ctxr=_ctx_reverse(dataset_index.get("ctx_index")); npr=dataset_index.get("np_rates")
    if not source or not by_key or not key_dates or not ctxr or not npr: return {}
    daily=bool(dataset_index.get("is_daily")); unit=timedelta(days=1) if daily else timedelta(hours=1); secs=86400 if daily else 3600
    predict_max, ext_name=_previous_direction(npr,target_date); ext=npr.get(ext_name); t1=npr.get("t1")
    if ext is None or t1 is None: return {}
    out={}
    for row in _current_rows(source,target_date,dataset_index,daily):
        edt=_dt(row)
        if edt is None or edt>target_date or not _var_allows_row(row,var): continue
        et_raw=str(row.get("event_type") or "").strip(); et=et_raw.lower(); ctx=ctxr.get(et)
        if not ctx: continue
        cid,_=ctx; shift=int(max(0.0,(target_date-edt).total_seconds())//secs)
        if shift<0 or shift>SHIFT_WINDOW: continue
        ars=by_key.get(et_raw) or by_key.get(et) or []; ads=key_dates.get(et_raw) or key_dates.get(et) or []
        if not ars or not ads: continue
        end=bisect.bisect_left(ads,target_date); start=max(0,end-2000)
        total=0; sumt=0.0; hits=0.0; qsum=0.0
        for j in range(start,end):
            a=ars[j]
            if not _var_allows_row(a,var): continue
            ad=ads[j]
            if ad==edt: continue
            frame=_frame_date(ad+unit*shift,daily)
            if frame+unit>target_date: continue
            idx=_rate_index(npr,frame)
            if idx is None: continue
            tv=float(t1[idx])
            if not np.isfinite(tv): continue
            q=_quality(a); total+=1; sumt+=tv*q; qsum+=q; hits+=q if bool(ext[idx]) else 0.0
        if total<MIN_EVENT_SUPPORT or qsum<=0: continue
        m0=sumt/qsum; m1=_mode1_score(hits,qsum,predict_max); cq=_quality(row)
        if calc_type in (0,1): _add(out,f"{cid}_0_{shift}",m0)
        if calc_type in (0,2): _add(out,f"{cid}_1_{shift}",m1)
        if calc_type==3: _add(out,f"{cid}_0_{shift}",m0*cq)
        if calc_type==4:
            _add(out,f"{cid}_0_{shift}",m0*cq); _add(out,f"{cid}_1_{shift}",m1)
    return {k:round(v,6) for k,v in out.items() if abs(v)>1e-12}


# ──────────────────────────────────────────────────────────────────────────────
# FAST BATCH CACHE
# ──────────────────────────────────────────────────────────────────────────────
#
# fill_cache asks the model for the same candle chunk 40 times:
#   5 types × 8 vars.
# The expensive historical analogue scan does not depend on type and can compute
# all 8 vars in one pass.  Keep a small LRU of per-chunk base results so the
# remaining 39 calls only reshape already computed dictionaries.
_BATCH_CACHE_LOCK = threading.RLock()
_BATCH_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_BATCH_CACHE_MAX = 10

def _var_mask(row):
    """Return eligibility for all eight vars with semantics identical to _var_allows_row."""
    try:
        focus=float(row.get("topic_focus") or 0.0)
        fit=float(row.get("cluster_similarity") or 0.0)
        novelty=float(row.get("novelty") or 0.0)
        confirm=int(row.get("confirmation_count") or 1)
        parsed=True
    except (TypeError,ValueError):
        parsed=False
        focus=fit=novelty=0.0
        confirm=1
    if not parsed:
        return (True,False,False,False,False,False,False,True)
    return (
        True,
        focus>=TOPIC_FOCUS_MIN,
        fit>=CLUSTER_SIM_MIN,
        novelty>=NOVELTY_MIN,
        confirm>=2,
        confirm>=3,
        novelty>=NOVELTY_MIN and confirm>=2,
        True,
    )

def _batch_cache_key(dataset, dates, di):
    if not dates:
        return None
    by_key=di.get("by_key")
    npr=di.get("np_rates") or {}
    dns=npr.get("dates_ns")
    # by_key and dates_ns are framework-owned stable objects across the 40 slot
    # calls for one pair/timeframe; avoid depending on temporary list/view ids.
    return (
        id(by_key), id(dns), bool(di.get("is_daily")),
        len(dates), dates[0], dates[-1],
    )

def _base_add(target, code, value):
    if value and np.isfinite(value):
        target[code]=target.get(code,0.0)+float(value)

def _compute_batch_base(dataset, dates, di):
    """
    Compute all vars and all type-independent components in one pass.

    Returns:
      {date: {var: (mode0_plain, mode1_plain, mode0_quality_weighted)}}
    """
    source=di.get("full_dataset") or dataset
    by_key=di.get("by_key") or {}
    key_dates=di.get("key_dates") or {}
    ctxr=_ctx_reverse(di.get("ctx_index"))
    npr=di.get("np_rates")
    if not source or not by_key or not key_dates or not ctxr or not npr:
        return {d:{v:({}, {}, {}) for v in VAR_RANGE} for d in dates}

    daily=bool(di.get("is_daily"))
    unit=timedelta(days=1) if daily else timedelta(hours=1)
    secs=86400 if daily else 3600
    t1=npr.get("t1")
    ext_min=npr.get("ext_min")
    ext_max=npr.get("ext_max")
    if t1 is None or ext_min is None or ext_max is None:
        return {d:{v:({}, {}, {}) for v in VAR_RANGE} for d in dates}

    result={}
    for target_date in dates:
        predict_max,_=_previous_direction(npr,target_date)
        ext=ext_max if predict_max else ext_min

        # Three independently accumulated result maps for every var:
        # m0, m1, and quality-weighted m0.
        maps=[({}, {}, {}) for _ in VAR_RANGE]

        for row in _current_rows(source,target_date,di,daily):
            edt=_dt(row)
            if edt is None or edt>target_date:
                continue

            current_mask=_var_mask(row)
            if not any(current_mask):
                continue

            et_raw=str(row.get("event_type") or "").strip()
            et=et_raw.lower()
            ctx=ctxr.get(et)
            if not ctx:
                continue
            cid,_=ctx

            shift=int(max(0.0,(target_date-edt).total_seconds())//secs)
            if shift<0 or shift>SHIFT_WINDOW:
                continue

            ars=by_key.get(et_raw) or by_key.get(et) or []
            ads=key_dates.get(et_raw) or key_dates.get(et) or []
            if not ars or not ads:
                continue

            end=bisect.bisect_left(ads,target_date)
            start=max(0,end-2000)

            totals=[0]*8
            sumt=[0.0]*8
            hits=[0.0]*8
            qsum=[0.0]*8

            for j in range(start,end):
                a=ars[j]
                ad=ads[j]
                if ad==edt:
                    continue

                frame=_frame_date(ad+unit*shift,daily)
                if frame+unit>target_date:
                    continue
                idx=_rate_index(npr,frame)
                if idx is None:
                    continue

                tv=float(t1[idx])
                if not np.isfinite(tv):
                    continue

                amask=_var_mask(a)
                if not any(amask):
                    continue
                q=_quality(a)
                hit_q=q if bool(ext[idx]) else 0.0

                for v,allowed in enumerate(amask):
                    if not allowed:
                        continue
                    totals[v]+=1
                    sumt[v]+=tv*q
                    qsum[v]+=q
                    hits[v]+=hit_q

            cq=_quality(row)
            for v,allowed in enumerate(current_mask):
                if not allowed or totals[v]<MIN_EVENT_SUPPORT or qsum[v]<=0:
                    continue
                m0=sumt[v]/qsum[v]
                m1=_mode1_score(hits[v],qsum[v],predict_max)
                code0=f"{cid}_0_{shift}"
                code1=f"{cid}_1_{shift}"
                o0,o1,o3=maps[v]
                _base_add(o0,code0,m0)
                _base_add(o1,code1,m1)
                _base_add(o3,code0,m0*cq)

        # Match the old function's final 6-decimal normalization before caching.
        result[target_date]={
            v:(
                {k:round(x,6) for k,x in m0.items() if abs(x)>1e-12},
                {k:round(x,6) for k,x in m1.items() if abs(x)>1e-12},
                {k:round(x,6) for k,x in m3.items() if abs(x)>1e-12},
            )
            for v,(m0,m1,m3) in enumerate(maps)
        }
    return result

def _get_batch_base(dataset, dates, di):
    key=_batch_cache_key(dataset,dates,di)
    if key is None:
        return {}
    with _BATCH_CACHE_LOCK:
        cached=_BATCH_CACHE.get(key)
        if cached is not None:
            _BATCH_CACHE.move_to_end(key)
            return cached

    computed=_compute_batch_base(dataset,dates,di)

    with _BATCH_CACHE_LOCK:
        # Another worker may have completed the same chunk while we calculated.
        existing=_BATCH_CACHE.get(key)
        if existing is not None:
            _BATCH_CACHE.move_to_end(key)
            return existing
        _BATCH_CACHE[key]=computed
        while len(_BATCH_CACHE)>_BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)
    return computed

def _compose_type(base_for_var, calc_type):
    m0,m1,m3=base_for_var
    if calc_type==1:
        return dict(m0)
    if calc_type==2:
        return dict(m1)
    if calc_type==3:
        return dict(m3)
    if calc_type==0:
        out=dict(m0); out.update(m1); return out
    if calc_type==4:
        out=dict(m3); out.update(m1); return out
    return {}


def model(rates,dataset,date,*,type=0,var=0,param="",dataset_index=None):
    if not dataset or not date or dataset_index is None: return {}
    t=int(type); v=int(var)
    if t not in TYPES_RANGE or v not in VAR_RANGE: return {}
    di=dict(dataset_index); di.setdefault("full_dataset",dataset)
    return _slot_for_date(dataset,date,calc_type=t,var=v,dataset_index=di)

def batch_model(rates,dataset,dates,*,type=0,var=0,param="",dataset_index=None):
    if not dates or not dataset or dataset_index is None: return {d:{} for d in dates}
    t=int(type); v=int(var)
    if t not in TYPES_RANGE or v not in VAR_RANGE: return {d:{} for d in dates}
    di=dict(dataset_index); di.setdefault("full_dataset",dataset)
    base=_get_batch_base(dataset,dates,di)
    return {d:_compose_type(base.get(d,{}).get(v,({}, {}, {})),t) for d in dates}

async def enrich_dataset(engine_vlad,engine_brain):
    return {"mode":"noop","source":"vlad_news_algo_events","source_only_runtime":True}
