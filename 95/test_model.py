from __future__ import annotations

from datetime import datetime, timedelta
import importlib.util
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SHARED = os.path.abspath(os.path.join(HERE, '..', 'shared'))
if SHARED not in sys.path:
    sys.path.insert(0, SHARED)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import model

CFG = {
    'min_history': 3,
    'history_window': 60,
    'big_quantile': 0.75,
    'strict_next_hour': True,
    'zero_epsilon': 1e-12,
}


def row(i, dt, country='US', title='CPI MoM', actual=0.3, forecast=0.2, previous=0.1, importance=1):
    return {
        'id': i, 'datetime': dt, 'Country': country, 'Title': title,
        'Actual': actual, 'Forecast': forecast, 'Previous': previous,
        'Importance': importance,
    }


def test_strict_release_timing():
    src = [
        row(1, datetime(2026,1,1,13,30), actual=.1, forecast=0),
        row(2, datetime(2026,2,1,13,30), actual=.1, forecast=0),
        row(3, datetime(2026,3,1,13,30), actual=.1, forecast=0),
        row(4, datetime(2026,4,1,13,30), actual=.2, forecast=0),
        row(5, datetime(2026,5,1,15,0), actual=.2, forecast=0),
    ]
    out, _ = model.build_enriched_rows(src, cfg=CFG)
    ind = [x for x in out if x.get('event_scope') == 'INDICATOR']
    assert ind[0]['date_dt'] == datetime(2026,4,1,14,0)
    assert ind[1]['date_dt'] == datetime(2026,5,1,16,0)


def test_duplicate_release_is_one_event():
    dt0 = datetime(2026,1,1,13,30)
    src = [
        row(1, dt0, actual=.1, forecast=0),
        row(2, dt0, actual=None, forecast=0),
        row(3, datetime(2026,2,1,13,30), actual=.1, forecast=0),
        row(4, datetime(2026,3,1,13,30), actual=.1, forecast=0),
        row(5, datetime(2026,4,1,13,30), actual=.2, forecast=0),
    ]
    out, st = model.build_enriched_rows(src, cfg=CFG)
    assert st['deduped_releases'] == 4
    assert len([x for x in out if x.get('event_scope') == 'INDICATOR']) == 1


def test_missing_actual_or_forecast_never_emits():
    src = [
        row(1, datetime(2026,1,1), actual=.1, forecast=0),
        row(2, datetime(2026,2,1), actual=.1, forecast=0),
        row(3, datetime(2026,3,1), actual=.1, forecast=0),
        row(4, datetime(2026,4,1), actual=None, forecast=.2),
        row(5, datetime(2026,5,1), actual=.3, forecast=None),
    ]
    out, _ = model.build_enriched_rows(src, cfg=CFG)
    assert out == []


def test_previous_is_ignored():
    base = [
        row(1, datetime(2026,1,1), actual=.1, forecast=0, previous=1),
        row(2, datetime(2026,2,1), actual=.2, forecast=0, previous=2),
        row(3, datetime(2026,3,1), actual=.1, forecast=0, previous=3),
        row(4, datetime(2026,4,1), actual=-.2, forecast=0, previous=4),
    ]
    alt = [dict(x, Previous=999999) for x in base]
    a, _ = model.build_enriched_rows(base, cfg=CFG)
    b, _ = model.build_enriched_rows(alt, cfg=CFG)
    assert a == b


def test_future_rows_cannot_change_past():
    base = []
    for i in range(1, 10):
        base.append(row(i, datetime(2025, i, 1, 13,30), actual=(i % 3 - 1) * .1, forecast=0))
    old, _ = model.build_enriched_rows(base, cfg=CFG)
    future = base + [
        row(100, datetime(2026,1,1,13,30), actual=1000, forecast=-1000),
        row(101, datetime(2026,2,1,13,30), actual=-1000, forecast=1000),
    ]
    full, _ = model.build_enriched_rows(future, cfg=CFG)
    cutoff = datetime(2026,1,1)
    full_old = [x for x in full if x['source_datetime'] < cutoff]
    assert old == full_old


def test_different_indicators_never_share_scale():
    src=[]
    # CPI typical miss 0.1
    for i in range(1,5):
        src.append(row(i, datetime(2026,i,1,13,30), title='CPI MoM', actual=.1, forecast=0))
    # Payroll typical miss 100, must not contaminate CPI threshold
    for i in range(1,5):
        src.append(row(100+i, datetime(2026,i,2,13,30), title='Non Farm Payrolls', actual=100, forecast=0))
    out,_=model.build_enriched_rows(src,cfg=CFG)
    cpi=[x for x in out if x['title']=='CPI MoM' and x.get('event_scope') == 'INDICATOR']
    nfp=[x for x in out if x['title']=='Non Farm Payrolls' and x.get('event_scope') == 'INDICATOR']
    assert len(cpi)==1 and len(nfp)==1
    assert abs(cpi[0]['surprise_score']-1.0)<1e-9
    assert abs(nfp[0]['surprise_score']-1.0)<1e-9


def test_same_timestamp_does_not_cross_contaminate():
    # The same title twice at same timestamp collapses before history; a separate
    # release at same time with different title cannot change CPI history.
    base=[]
    for i in range(1,4):
        base.append(row(i, datetime(2026,i,1,13,30), title='CPI MoM', actual=.1, forecast=0))
    target=row(10, datetime(2026,4,1,13,30), title='CPI MoM', actual=.4, forecast=0)
    other=row(11, datetime(2026,4,1,13,30), title='GDP Growth Rate QoQ', actual=9999, forecast=-9999)
    a,_=model.build_enriched_rows(base+[target],cfg=CFG)
    b,_=model.build_enriched_rows(base+[target,other],cfg=CFG)
    ac=[x for x in a if x['title']=='CPI MoM']
    bc=[x for x in b if x['title']=='CPI MoM']
    assert ac==bc


def test_sign_is_literal_actual_minus_forecast_not_economic_direction():
    src=[]
    for i in range(1,4):
        src.append(row(i, datetime(2026,i,1), title='Initial Jobless Claims', actual=200, forecast=190))
    src.append(row(4, datetime(2026,4,1), title='Initial Jobless Claims', actual=230, forecast=200))
    out,_=model.build_enriched_rows(src,cfg=CFG)
    ind = [x for x in out if x.get('event_scope') == 'INDICATOR']
    assert len(ind)==1
    assert ind[0]['sign_state']=='POS'
    # No LONG/SHORT/BULL/BEAR is hand-coded into the event identity.
    assert all(tok not in ind[0]['event_type'] for tok in ('LONG','SHORT','BULL','BEAR'))


def test_big_surprise_emits_dense_family_companion():
    src=[]
    for i,val in enumerate([0.1,0.1,0.1,0.5],1):
        src.append(row(i, datetime(2026,i,1,13,30), title='CPI MoM', actual=val, forecast=0))
    out,_=model.build_enriched_rows(src,cfg=CFG)
    family=[x for x in out if x.get('event_scope')=='INDICATOR_BIG']
    assert len(family)==1
    assert family[0]['event_type']=='MCX.US.CPI_MOM.POS.BIG'
