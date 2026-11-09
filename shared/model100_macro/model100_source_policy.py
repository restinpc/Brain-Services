"""Feature-only allowlist. Never changes collectors or database state."""
import json
from pathlib import Path

def load_policy(path):
    policy=json.loads(Path(path).read_text(encoding='utf-8'))
    allowed=policy.get('allowed_sources')
    if not isinstance(allowed,list) or not allowed or any(not isinstance(s,str) or not s for s in allowed) or len(set(allowed))!=len(allowed):
        raise ValueError('INVALID_SOURCE_ALLOWLIST')
    if not set(policy.get('actual_sources',[]))<=set(allowed):raise ValueError('ACTUAL_SOURCE_NOT_ALLOWED')
    if set(policy.get('excluded_sources',[]))&set(allowed):raise ValueError('ALLOW_EXCLUDE_OVERLAP')
    if policy.get('include_revisions') is not False or policy.get('day_only_policy')!='NEXT_DAY_12UTC':
        raise ValueError('UNSUPPORTED_V1_TEMPORAL_POLICY')
    return policy

def filter_records(records,policy):
    allowed=set(policy['allowed_sources'])
    return [r for r in records if r.get('source_id') in allowed]
