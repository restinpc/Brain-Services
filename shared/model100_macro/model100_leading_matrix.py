"""Read-only three-axis leading-source inventory; no single all-or-nothing readiness count."""
import json,argparse,csv
from pathlib import Path
try:
 from .model100_leading_v1 import prepare,metadata
except ImportError:
 from .model100_leading_v1 import prepare,metadata
CATALOG=[('ADP_ALFRED','ALFRED'),('IFO_EXPECTATIONS','IFO_VINTAGE'),('GDPNow','GDP_NOW_ATL'),('ECB_SPF','ECB_SPF'),('NY_FED_NOWCAST','NYFED_NOW'),('CLEVELAND_NOWCAST','CLEVELAND_NOW'),('ZEW','ZEW'),('NY_FED_SME','NYFED_SME'),('CME_FEDWATCH',None),('TRUFLATION',None)]
def inventory(records):
 candidates=prepare(records);out=[]
 for label,source in CATALOG:
  rr=[r for r in records if source and r['source_id']==source and r.get('numeric_value') is not None and r.get('asof_class')!='UNUSABLE']
  cc=[r for r in candidates if r['source']==source]
  if label=='ADP_ALFRED':rr=[r for r in rr if metadata(r).get('series_id') in ('ADPMNUSNERSA','ADPWNUSNERSA')];cc=[r for r in cc if r['role']=='PRIVATE_EMPLOYMENT_PROXY']
  if label=='IFO_EXPECTATIONS':rr=[r for r in rr if r['record_key'].startswith('ifo-original-table:') and metadata(r).get('series')=='Expectations' and r['record_kind']=='FIRST_ESTIMATE']
  if label=='ECB_SPF':rr=[r for r in rr if r['record_kind']=='FORECAST_VINTAGE']
  days=[metadata(r).get('forecast_date') or metadata(r).get('vintage_date') or metadata(r).get('document_date') or metadata(r).get('published_day') or metadata(r).get('questionnaire_distribution_date') for r in rr];days=[str(d)[:10] for d in days if d]
  rights='ML_TERMS_RECORDED_NOT_COLLECTION_BLOCKER' if label=='ADP_ALFRED' else 'ECB_FREE_REUSE_WITH_ATTRIBUTION' if label=='ECB_SPF' else 'REQUIRES_LICENSE' if label=='CME_FEDWATCH' else 'RIGHTS_UNVERIFIED' if label=='TRUFLATION' else 'RIGHTS_UNVERIFIED'
  note={'ADP_ALFRED':'History and collection active. Future ML terms recorded separately; no training performed. Direct ADP rights remain unverified.', 'ZEW':'352 balances/22 months; all22 publication days confirmed by original dated releases; all DAY_ONLY.', 'NY_FED_SME':'227 medians/14 workbooks; questionnaire distribution is not public results time; all excluded from historical replay.', 'ECB_SPF':'25 original-round candidates; 15 later API updates excluded.', 'NY_FED_NOWCAST':'175 forecasts; latest workbook ends2026-10-30; original daily snapshot bytes not claimed.', 'CLEVELAND_NOWCAST':'3344 forecast values; excludes Actual series; provider historical chart, not original daily snapshot bytes.', 'CME_FEDWATCH':'Official historical API requires paid subscription; no entitlement.', 'TRUFLATION':'Official API returned401 on B1; no Truflation key names in protected env or74parser/config files. Historical extraction awaits API access.'}.get(label,'Existing history retained; rights independent of temporal quality.')
  out.append(dict(source=label,history='PRESENT' if rr else 'NOT_LOADED',historical_values=len(rr),first_day=min(days) if days else None,last_day=max(days) if days else None,temporal_candidates=len(cc),documented_hour_values=sum(r['technical_quality']=='READY_LEADING_H1' for r in cc),temporal_status='CONSERVATIVE_DAY_CONTEXT' if label=='ZEW' else 'UNVERIFIED_RESULTS_PUBLICATION' if label=='NY_FED_SME' else 'DOCUMENTED_AND_CONTEXT' if label=='IFO_EXPECTATIONS' else 'CONSERVATIVE_DAY_CONTEXT' if cc else 'UNAVAILABLE',rights=rights,access='REQUIRES_API_KEY_HTTP401' if label=='TRUFLATION' else 'REQUIRES_SUBSCRIPTION' if label=='CME_FEDWATCH' else 'HISTORY_RECEIVED',live='B1_NETWORK_TIMEOUT_RELAY_HISTORY_LOADED' if label in ('NY_FED_NOWCAST','CLEVELAND_NOWCAST') else 'NOT_VERIFIED' if label in ('ZEW','NY_FED_SME') else 'REGISTERED_COLLECTOR' if rr else 'NO_ENTITLEMENT',note=note))
 return out
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--records',required=True);p.add_argument('--outdir',required=True);a=p.parse_args();records=[json.loads(s) for s in Path(a.records).open()];rows=inventory(records);d=Path(a.outdir);d.mkdir(exist_ok=True,parents=True)
 (d/'LEADING_THREE_AXES.json').write_text(json.dumps(rows,indent=2))
 with (d/'LEADING_THREE_AXES.csv').open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=rows[0].keys());w.writeheader();w.writerows(rows)
 summary={'planned_sources':10,'sources_with_history':sum(bool(r['historical_values']) for r in rows),'sources_with_some_temporal_candidates':sum(bool(r['temporal_candidates']) for r in rows),'sources_with_documented_hours':sum(bool(r['documented_hour_values']) for r in rows),'rights_statuses':{v:sum(r['rights']==v for r in rows) for v in sorted({r['rights'] for r in rows})},'not_full_pipeline_completion':True};(d/'LEADING_THREE_AXES_SUMMARY.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary))
