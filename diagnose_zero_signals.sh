#!/usr/bin/env bash
set -u

cd /brain/Brain-Services || exit 1
set -a
source <(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' .env | sed 's/\r$//')
set +a

mysql_conn() {
    local prefix="$1" db="$2" sql="$3"
    local host_var="${prefix}_HOST" port_var="${prefix}_PORT" user_var="${prefix}_USER" pass_var="${prefix}_PASSWORD"
    local host="${!host_var:-127.0.0.1}" port="${!port_var:-3306}" user="${!user_var:-root}" pass="${!pass_var:-}"
    MYSQL_PWD="$pass" mysql --connect-timeout=10 -h "$host" -P "$port" -u "$user" -D "$db" -N -B -e "$sql"
}

mysql_brain() { mysql_conn MASTER "${MASTER_NAME:-brain}" "$1"; }
mysql_super() { mysql_conn SUPER  "${SUPER_NAME:-brain}"  "$1"; }
mysql_vlad()  { mysql_conn DB     "${DB_NAME:-vlad}"       "$1"; }

json_pretty() {
python3 -c '
import json, sys
raw=sys.stdin.read().strip()
if not raw:
    print("<EMPTY HTTP BODY>")
    raise SystemExit
try:
    obj=json.loads(raw)
except Exception:
    print(raw)
    raise SystemExit
print(json.dumps(obj, ensure_ascii=False, indent=2))
'
}

payload_summary() {
python3 -c '
import json, math, sys
raw=sys.stdin.read().strip()
if not raw:
    print("EMPTY_HTTP_BODY")
    raise SystemExit
try:
    obj=json.loads(raw)
except Exception:
    print("NON_JSON:", raw[:1000])
    raise SystemExit
payload=obj.get("payLoad") if isinstance(obj,dict) else None
if isinstance(payload,dict):
    vals=[]
    for value in payload.values():
        if isinstance(value,(int,float)) and math.isfinite(float(value)):
            vals.append(float(value))
    print(json.dumps({
        "status": obj.get("status"),
        "error": obj.get("error"),
        "payload_keys": len(payload),
        "payload_sum": round(sum(vals),8),
        "details": obj.get("details"),
    }, ensure_ascii=False, indent=2))
else:
    print(json.dumps(obj, ensure_ascii=False, indent=2))
'
}

# model_id signal_id pair day
TARGETS=(
  "58 11 1 0"
  "61 63 1 1"
  "33 65 3 0"
  "61 73 3 1"
  "58 196 4 0"
  "61 136 4 1"
)

for target in "${TARGETS[@]}"; do
    read -r model_id signal_id expected_pair expected_day <<< "$target"
    signal_table="brain_signal${model_id}"

    row="$(mysql_brain "
        SELECT CONCAT_WS(CHAR(31),
            COALESCE(\`pair\`,''), COALESCE(\`is_day\`,''),
            COALESCE(\`type\`,0), COALESCE(\`var\`,0),
            HEX(COALESCE(\`param\`,'')), HEX(COALESCE(\`params\`,'')),
            COALESCE(\`buy\`,0), COALESCE(\`sell\`,0),
            COALESCE(\`active\`,0),
            DATE_FORMAT(\`actual_date\`,'%Y-%m-%d %H:%i:%s')
        )
        FROM \`${signal_table}\` WHERE \`id\`=${signal_id} LIMIT 1;
    ")"

    if [[ -z "$row" ]]; then
        echo "MODEL ${model_id} SIGNAL ${signal_id}: signal row not found"
        continue
    fi

    IFS=$'\x1f' read -r pair day calc_type calc_var param_hex params_hex buy sell active actual_date <<< "$row"
    param="$(python3 -c 'import sys; print(bytes.fromhex(sys.argv[1]).decode("utf-8"))' "$param_hex")"
    params="$(python3 -c 'import sys; print(bytes.fromhex(sys.argv[1]).decode("utf-8"))' "$params_hex")"

    case "$pair" in
      1) postfix="eur_usd" ;;
      3) postfix="btc_usd" ;;
      4) postfix="eth_usd" ;;
      *) echo "Unknown pair=$pair"; continue ;;
    esac
    rate_table="brain_rates_${postfix}"
    [[ "$day" == "1" ]] && rate_table+="_day"

    rate_row="$(mysql_brain "
        SELECT CONCAT_WS(CHAR(31), \`id\`, DATE_FORMAT(\`date\`,'%Y-%m-%d %H:%i:%s'))
        FROM \`${rate_table}\` ORDER BY \`date\` DESC LIMIT 1;
    ")"
    IFS=$'\x1f' read -r rate_id rate_date <<< "$rate_row"

    url="$(mysql_super "SELECT COALESCE(\`url\`,'') FROM \`brain_service\` WHERE \`id\`=${model_id} LIMIT 1;")"
    url="${url%/}"
    if [[ -z "$url" ]]; then
        case "$model_id" in
          33) url="http://127.0.0.1:8896" ;;
          58) url="http://127.0.0.1:8920" ;;
          61) url="http://127.0.0.1:8923" ;;
        esac
    fi

    echo
    echo "================================================================================"
    echo "MODEL=${model_id} SIGNAL=${signal_id} pair=${pair} day=${day} type=${calc_type} var=${calc_var}"
    echo "URL=${url} rate_table=${rate_table} rate_id=${rate_id} rate_date=${rate_date}"
    echo "active=${active} actual_date=${actual_date} buy=${buy} sell=${sell}"
    echo "param=${param}"
    echo "params=${params}"
    echo "================================================================================"

    echo "[1] SERVICE METADATA"
    curl -sS --max-time 60 "$url/" | json_pretty

    echo "[2] DIRECT /values FOR LATEST RATE"
    if [[ "$model_id" == "61" ]]; then
        curl -sS --max-time 600 -G "$url/values" \
          --data-urlencode "pair=${pair}" \
          --data-urlencode "day=${day}" \
          --data-urlencode "date=${rate_date}" \
          --data-urlencode "params=${params}" | payload_summary
    else
        curl -sS --max-time 600 -G "$url/values" \
          --data-urlencode "pair=${pair}" \
          --data-urlencode "day=${day}" \
          --data-urlencode "date=${rate_date}" \
          --data-urlencode "type=${calc_type}" \
          --data-urlencode "var=${calc_var}" \
          --data-urlencode "param=${param}" | payload_summary
    fi

    echo "[3] PHP CACHE FOR LATEST RATE"
    php_cache="$(mysql_brain "
        SELECT COALESCE(\`json\`,'<NO_ROW>')
        FROM \`brain_neuronet${model_id}\`
        WHERE \`signal_id\`=${signal_id} AND \`rate_id\`=${rate_id}
        LIMIT 1;
    " 2>/dev/null || true)"
    echo "$php_cache"

    echo "[4] PRECOMPUTED FINAL SIGNAL TABLE FOR LATEST RATE"
    final_table="brain_signal${model_id}_${signal_id}"
    exists="$(mysql_brain "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='${final_table}';")"
    if [[ "$exists" == "1" ]]; then
        mysql_brain "SELECT \`rate_id\`, \`value\` FROM \`${final_table}\` WHERE \`rate_id\`=${rate_id} LIMIT 1;"
    else
        echo "${final_table}: TABLE_MISSING"
    fi

    echo "[5] SEARCH BACKWARDS FOR THE LATEST NONEMPTY SERVICE PAYLOAD"
    MODEL_ID="$model_id" URL="$url" PAIR="$pair" DAY_FLAG="$day" TYPE_VAL="$calc_type" VAR_VAL="$calc_var" \
    PARAM_VAL="$param" PARAMS_HEX="$params_hex" RATE_DATE="$rate_date" python3 - <<'PY'
import json, os
from datetime import datetime, timedelta
import requests

model=int(os.environ['MODEL_ID'])
url=os.environ['URL'].rstrip('/')
pair=int(os.environ['PAIR'])
day=int(os.environ['DAY_FLAG'])
type_val=int(os.environ['TYPE_VAL'])
var_val=int(os.environ['VAR_VAL'])
param=os.environ.get('PARAM_VAL','')
params=bytes.fromhex(os.environ.get('PARAMS_HEX','')).decode('utf-8')
start=datetime.strptime(os.environ['RATE_DATE'],'%Y-%m-%d %H:%M:%S')
step=timedelta(days=1)
limit=45

for i in range(limit):
    dt=start-step*i
    q={'pair':pair,'day':day,'date':dt.strftime('%Y-%m-%d %H:%M:%S')}
    if model==61:
        q['params']=params
    else:
        q.update({'type':type_val,'var':var_val,'param':param})
    try:
        r=requests.get(url+'/values',params=q,timeout=60)
        obj=r.json()
        payload=obj.get('payLoad') if isinstance(obj,dict) else None
        if isinstance(payload,dict) and payload:
            print(json.dumps({'latest_nonempty_date':q['date'],'keys':len(payload),'sample':dict(list(payload.items())[:3])},ensure_ascii=False,indent=2))
            break
        if isinstance(obj,dict) and obj.get('status')!='ok':
            print(json.dumps({'date':q['date'],'service_error':obj},ensure_ascii=False,indent=2))
            break
    except Exception as exc:
        print(json.dumps({'date':q['date'],'request_error':str(exc)},ensure_ascii=False,indent=2))
        break
else:
    print(json.dumps({'latest_nonempty_date':None,'checked_days':limit},ensure_ascii=False,indent=2))
PY

    if [[ "$model_id" == "58" ]]; then
        echo "[6] MODEL 58 DIRECT COMPUTE_BATCH, BYPASSING VALUES CACHE"
        dates_json="$(python3 -c 'import json,sys; print(json.dumps([sys.argv[1]]))' "$rate_date")"
        curl -sS --max-time 1800 -X POST -H 'Content-Type: application/json' \
          "$url/compute_batch?pair=${pair}&day=${day}&type=${calc_type}&var=${calc_var}&param=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' "$param")" \
          --data "$dates_json" | json_pretty
    fi

    if [[ "$model_id" == "61" ]]; then
        echo "[6] MODEL 61 CHILD BREAKDOWN: MODELS 47 AND 53"
        URL="$url" PAIR="$pair" DAY_FLAG="$day" RATE_DATE="$rate_date" PARAMS_HEX="$params_hex" python3 - <<'PY'
import json, os
import requests

url=os.environ['URL'].rstrip('/')
pair=int(os.environ['PAIR'])
day=int(os.environ['DAY_FLAG'])
date=os.environ['RATE_DATE']
params=json.loads(bytes.fromhex(os.environ['PARAMS_HEX']).decode('utf-8'))
meta=requests.get(url+'/',timeout=30).json()
child_urls=meta.get('child_urls') or {}
combos=[params] if isinstance(params,dict) else [x for x in params if isinstance(x,dict)]

unique={}
for combo in combos:
    for mid, sub in combo.items():
        if not isinstance(sub,dict):
            continue
        clean={k:v for k,v in sub.items() if k!='k'}
        key=(str(mid),json.dumps(clean,sort_keys=True,ensure_ascii=False))
        unique[key]=clean

out=[]
for (mid,_), sub in unique.items():
    child=child_urls.get(mid)
    row={'model':int(mid),'url':child,'params':sub}
    if not child:
        row['error']='child URL missing'
        out.append(row)
        continue
    q=dict(sub)
    q.update({'pair':pair,'day':day,'date':date})
    try:
        obj=requests.get(child.rstrip('/')+'/values',params=q,timeout=120).json()
        payload=obj.get('payLoad') if isinstance(obj,dict) else None
        row.update({'status':obj.get('status') if isinstance(obj,dict) else None,
                    'error':obj.get('error') if isinstance(obj,dict) else None,
                    'payload_keys':len(payload) if isinstance(payload,dict) else None,
                    'sample':dict(list(payload.items())[:3]) if isinstance(payload,dict) else None})
    except Exception as exc:
        row['error']=str(exc)
    out.append(row)
print(json.dumps(out,ensure_ascii=False,indent=2))
PY
    fi

done
