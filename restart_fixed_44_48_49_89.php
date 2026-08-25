<?php
/**
 * restart_fixed_44_48_49_89.php
 *
 * Последовательность:
 *   1. Проверяет SHA256 shared/brain_framework.py на всех master_nodes.
 *   2. Перезапускает исправленные сервисы 44, 48, 49 и 89 на всех нодах.
 *   3. На Brain 1 в фоне очищает values-cache (H1 + D1), не трогая backtest.
 *   4. Параллельно пересчитывает H1 + D1 для этих четырёх сервисов.
 *   5. Для моделей с ID <= 81 сразу после успешного fill_cache запускает
 *      Reinit на всех master_nodes. Для моделей с ID > 81 выполняется только
 *      пересчёт кеша, без Reinit.
 *
 * Совместимость: PHP 5.3.
 *
 * Установка:
 *   cp /brain/Brain-Services/restart_fixed_44_48_49_89.php \
 *      /brain/Brain-Server/public_html/engine/code/restart_and_check_all_services.php
 *
 * Запуск:
 *   cd /brain/Brain-Server
 *   bash centos/php.sh restart_and_check_all_services.php
 */

function _rrcr_service_ids()
{
    return array(44, 48, 49, 89);
}

function _rrcr_reinit_max_id()
{
    return 81;
}

function _rrcr_framework_sha256()
{
    return '2054bd152afc6584681ac807a004ed59f86b7c02ed4632417de2f0ae266a8dbd';
}

function _rrcr_get_nodes()
{
    $nodes = array();
    $res = master::mysql(
        'SELECT `id`, `node` FROM `master_nodes` ORDER BY `id` ASC'
    );
    while ($row = mysqli_fetch_array($res)) {
        $nodes[] = array(
            'id' => intval($row['id']),
            'node' => (string)$row['node']
        );
    }
    if (!$nodes) {
        throw new Exception('Таблица master_nodes пуста');
    }
    return $nodes;
}

function _rrcr_get_services()
{
    $wanted = _rrcr_service_ids();
    $idList = implode(',', $wanted);
    $services = array();

    $res = master::mysql(
        'SELECT `id`, `name`, `url` FROM `brain_service` '
        .'WHERE `id` IN ('.$idList.') ORDER BY `id` ASC'
    );

    while ($row = mysqli_fetch_array($res)) {
        $id = intval($row['id']);
        $url = trim((string)(isset($row['url']) ? $row['url'] : ''));
        $port = 8862 + $id;
        if ($url !== '') {
            $parts = @parse_url($url);
            if (is_array($parts) && isset($parts['port'])) {
                $port = intval($parts['port']);
            }
        }
        $services[$id] = array(
            'id' => $id,
            'name' => isset($row['name']) ? (string)$row['name'] : '',
            'port' => $port
        );
    }

    $missing = array();
    foreach ($wanted as $id) {
        if (!isset($services[$id])) {
            $missing[] = $id;
        }
    }
    if ($missing) {
        throw new Exception(
            'В brain_service отсутствуют модели: '.implode(',', $missing)
        );
    }

    $ordered = array();
    foreach ($wanted as $id) {
        $ordered[] = $services[$id];
    }
    return $ordered;
}

function _rrcr_escape_command($command)
{
    master::mysql('SELECT 1');
    if (!isset($_SERVER['master_connection'])) {
        throw new Exception('Нет master_connection');
    }
    return mysqli_real_escape_string(
        $_SERVER['master_connection'],
        $command
    );
}

function _rrcr_queue_for_nodes($nodes, $command, $userId)
{
    $escaped = _rrcr_escape_command($command);
    $timestamp = time();
    $taskIds = array();

    foreach ($nodes as $node) {
        $query = 'INSERT INTO `brain_shell` '
            .'(`user_id`, `node_id`, `type`, `command`, `timestamp`, `status`) VALUES ('
            .'"'.intval($userId).'", '
            .'"'.intval($node['id']).'", '
            .'"0", '
            .'"'.$escaped.'", '
            .'"'.$timestamp.'", '
            .'0)';
        master::mysql($query);
        $taskIds[$node['id']] = intval(
            mysqli_insert_id($_SERVER['master_connection'])
        );
        echo 'Queued: '.$node['node']
            .' task='.$taskIds[$node['id']].PHP_EOL;
    }

    return $taskIds;
}

function _rrcr_wait_tasks($nodes, $taskIds, $timeoutSeconds)
{
    $deadline = time() + intval($timeoutSeconds);
    $finished = array();

    while (time() < $deadline && count($finished) < count($taskIds)) {
        foreach ($taskIds as $nodeId => $taskId) {
            if (isset($finished[$nodeId])) {
                continue;
            }
            $res = master::mysql(
                'SELECT `status`, `output` FROM `brain_shell` '
                .'WHERE `id` = "'.intval($taskId).'"'
            );
            $row = mysqli_fetch_array($res);
            if ($row && intval($row['status']) >= 2) {
                $finished[$nodeId] = (string)(
                    isset($row['output']) ? $row['output'] : ''
                );
                echo 'Finished node_id='.$nodeId
                    .' task='.$taskId.PHP_EOL;
            }
        }
        if (count($finished) < count($taskIds)) {
            sleep(3);
        }
    }

    $failed = false;
    echo PHP_EOL.str_repeat('=', 90).PHP_EOL;
    foreach ($nodes as $node) {
        echo 'NODE: '.$node['node'].' (id='.$node['id'].')'.PHP_EOL;
        echo str_repeat('-', 90).PHP_EOL;

        if (!isset($finished[$node['id']])) {
            echo 'TIMEOUT'.PHP_EOL;
            $failed = true;
        } else {
            $output = $finished[$node['id']];
            echo $output.PHP_EOL;
            if (strpos($output, 'RESULT=FAILED') !== false
                || strpos($output, 'FRAMEWORK SHA FAILED') !== false
                || strpos($output, 'HTTP FAILED') !== false
                || strpos($output, 'REINIT FAILED') !== false) {
                $failed = true;
            }
        }
        echo str_repeat('=', 90).PHP_EOL;
    }

    return !$failed;
}

function _rrcr_reinit_stage()
{
    $nodes = _rrcr_get_nodes();
    $allIds = _rrcr_service_ids();
    $ids = $allIds;

    if (isset($_REQUEST['model_id'])) {
        $modelId = intval($_REQUEST['model_id']);
        if (!in_array($modelId, $allIds)) {
            throw new Exception('Недопустимый model_id='.$modelId);
        }
        if ($modelId > _rrcr_reinit_max_id()) {
            echo 'MODEL '.$modelId.' REINIT SKIPPED: ID > '
                ._rrcr_reinit_max_id().PHP_EOL;
            exit(0);
        }
        $ids = array($modelId);
    } else {
        $ids = array_values(array_filter(
            $allIds,
            function ($id) {
                return intval($id) <= _rrcr_reinit_max_id();
            }
        ));
    }

    $idSpec = implode(' ', $ids);
    $userId = 1;

    $command = <<<'BASH'
set -u
ROOT=/brain/Brain-Server
IDS='__IDS__'
FAIL=0

printf 'NODE=%s REINIT START=%s\n' "$(hostname)" "$(date '+%F %T')"

for id in $IDS; do
    printf 'MODEL %s REINIT START\n' "$id"

    bash "$ROOT/centos/php.sh" \
        "kill.php?target=neuronet_id=$id" \
        >/dev/null 2>&1 || true

    output=$(cd "$ROOT" && bash centos/php.sh \
        "reinit_model.php?neuronet_id=$id" 2>&1)
    rc=$?

    printf '%s\n' "$output"

    if [ "$rc" -eq 0 ] && printf '%s' "$output" | grep -q 'reinit done'; then
        printf 'MODEL %s REINIT OK\n' "$id"
    else
        printf 'MODEL %s REINIT FAILED rc=%s\n' "$id" "$rc"
        FAIL=1
    fi
done

printf 'NODE=%s REINIT FINISH=%s RESULT=%s\n' \
    "$(hostname)" "$(date '+%F %T')" \
    "$([ "$FAIL" -eq 0 ] && echo OK || echo FAILED)"

exit "$FAIL"
BASH;

    $command = str_replace('__IDS__', $idSpec, $command);
    echo 'REINIT STAGE: queuing model(s) '.implode(',', $ids)
        .' on '.count($nodes).' nodes'.PHP_EOL;

    $taskIds = _rrcr_queue_for_nodes($nodes, $command, $userId);
    $ok = _rrcr_wait_tasks($nodes, $taskIds, 21600);

    echo 'REINIT FINAL RESULT: '.($ok ? 'OK' : 'FAILED').PHP_EOL;
    exit($ok ? 0 : 1);
}

function restart_and_check_all_services()
{
    engine::log('restart_and_check_all_services()');

    try {
        if (isset($_REQUEST['stage'])
            && (string)$_REQUEST['stage'] === 'reinit') {
            _rrcr_reinit_stage();
            return;
        }

        $services = _rrcr_get_services();
        $nodes = _rrcr_get_nodes();
        $sha = _rrcr_framework_sha256();
        $userId = 1;

        $spec = array();
        foreach ($services as $service) {
            $spec[] = $service['id'].':'.$service['port'];
        }
        $serviceSpec = implode(' ', $spec);

        $restartCommand = <<<'BASH'
set -u
ROOT=/brain/Brain-Server
SERVICES=/brain/Brain-Services
SPEC='__SERVICE_SPEC__'
EXPECTED_SHA='__FRAMEWORK_SHA__'
FAIL=0

printf 'NODE=%s START=%s\n' "$(hostname)" "$(date '+%F %T')"
printf 'SERVICES=%s\n' "$SPEC"

FRAMEWORK="$SERVICES/shared/brain_framework.py"
if [ ! -f "$FRAMEWORK" ]; then
    printf 'FRAMEWORK SHA FAILED: file not found %s\n' "$FRAMEWORK"
    exit 31
fi

ACTUAL_SHA=$(sha256sum "$FRAMEWORK" | awk '{print $1}')
printf 'FRAMEWORK SHA expected=%s actual=%s\n' "$EXPECTED_SHA" "$ACTUAL_SHA"
if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
    printf 'FRAMEWORK SHA FAILED\n'
    exit 32
fi

for item in $SPEC; do
    id=${item%%:*}
    if [ -f "$SERVICES/$id/server.py" ]; then
        bash "$ROOT/shell/stop.sh" "$id" >/dev/null 2>&1 || true
    else
        printf 'SERVICE %s SKIP: server.py отсутствует\n' "$id"
        FAIL=1
    fi
done

sleep 2

for item in $SPEC; do
    id=${item%%:*}
    [ -f "$SERVICES/$id/server.py" ] || continue
    printf 'SERVICE %s START... ' "$id"
    if bash "$ROOT/shell/start.sh" "$id" >/dev/null 2>&1; then
        printf 'OK\n'
    else
        printf 'ERROR\n'
        FAIL=1
    fi
done

for item in $SPEC; do
    id=${item%%:*}
    port=${item##*:}
    [ -f "$SERVICES/$id/server.py" ] || continue

    ready=0
    code=000
    for n in $(seq 1 120); do
        code=$(curl -sS -o /tmp/brain_health_${id}.out \
            -w '%{http_code}' --connect-timeout 2 --max-time 5 \
            "http://127.0.0.1:${port}/" 2>/dev/null || true)
        if [ "$code" != '000' ]; then
            ready=1
            break
        fi
        sleep 2
    done

    if [ "$ready" -eq 1 ]; then
        printf 'SERVICE %s HTTP OK port=%s code=%s\n' \
            "$id" "$port" "$code"
    else
        printf 'SERVICE %s HTTP FAILED port=%s\n' "$id" "$port"
        FAIL=1
    fi
done

printf 'NODE=%s FINISH=%s RESULT=%s\n' \
    "$(hostname)" "$(date '+%F %T')" \
    "$([ "$FAIL" -eq 0 ] && echo OK || echo FAILED)"
exit "$FAIL"
BASH;

        $restartCommand = str_replace(
            array('__SERVICE_SPEC__', '__FRAMEWORK_SHA__'),
            array($serviceSpec, $sha),
            $restartCommand
        );

        echo 'STEP 1: verify framework and restart services 44,48,49,89 on all nodes'.PHP_EOL;
        $taskIds = _rrcr_queue_for_nodes($nodes, $restartCommand, $userId);
        $restartOk = _rrcr_wait_tasks($nodes, $taskIds, 3600);

        if (!$restartOk) {
            throw new Exception(
                'Перезапуск завершился с ошибкой. Кеш и reinit не запущены.'
            );
        }

        $runId = date('Ymd_His').'_'.getmypid();
        $tmpFile = '/tmp/brain_causal_cache_44_48_49_89_'.$runId.'.py';
        $logDir = '/brain/Brain-Server/logs';
        $logFile = $logDir.'/causal_cache_44_48_49_89_'.$runId.'.log';
        $pidFile = '/tmp/brain_causal_cache_44_48_49_89.pid';

        if (!is_dir($logDir)) {
            @mkdir($logDir, 0775, true);
        }

        if (is_file($pidFile)) {
            $oldPid = intval(trim((string)@file_get_contents($pidFile)));
            if ($oldPid > 0 && @file_exists('/proc/'.$oldPid)) {
                throw new Exception(
                    'Уже запущен full-cache job pid='.$oldPid
                );
            }
            @unlink($pidFile);
        }

        $pythonCode = <<<'PYCODE'
from __future__ import print_function

import concurrent.futures
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

POLL_SECONDS = 10
START_TIMEOUT = 180
STOP_TIMEOUT = 300
FILL_START_GRACE = 180
SERVICE_FILL_TIMEOUT = 43200
MAX_WORKERS = 4

SPEC = os.environ.get('BRAIN_SERVICE_SPEC', '')
PID_FILE = os.environ.get(
    'BRAIN_PID_FILE',
    '/tmp/brain_causal_cache_44_48_49_89.pid'
)
SELF_FILE = os.environ.get('BRAIN_PYTHON_FILE', '')
ROOT = '/brain/Brain-Server'


def out(message):
    print('%s %s' % (
        time.strftime('%Y-%m-%d %H:%M:%S'),
        message
    ), flush=True)


def compact(value, limit=1800):
    text = str(value or '').replace('\r', ' ').replace('\n', ' ')
    return text[:limit]


def request_json(url, timeout=60):
    req = urllib.request.Request(
        url,
        headers={'User-Agent': 'Brain-Causal-Cache-4/1.0'}
    )
    try:
        response = urllib.request.urlopen(req, timeout=timeout)
        body = response.read().decode('utf-8', errors='replace')
        code = int(response.getcode())
        response.close()
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        try:
            body = exc.read().decode('utf-8', errors='replace')
        except Exception:
            body = str(exc)
    except Exception as exc:
        return None, None, str(exc)

    try:
        return code, json.loads(body), body
    except Exception:
        return code, None, body


def status_ok(data):
    return isinstance(data, dict) and data.get('status') == 'ok'


def payload_of(data):
    if isinstance(data, dict):
        return data.get('payLoad', {})
    return {}


def wait_http(port, sid):
    deadline = time.time() + START_TIMEOUT
    last = ''
    while time.time() < deadline:
        code, data, body = request_json(
            'http://127.0.0.1:%d/' % port,
            10
        )
        if code is not None and 200 <= code < 500:
            out('SERVICE %d HTTP OK port=%d code=%d' % (
                sid, port, code
            ))
            return True
        last = body
        time.sleep(3)
    out('SERVICE %d HTTP FAILED port=%d error=%s' % (
        sid, port, compact(last)
    ))
    return False


def get_paths(port):
    code, data, body = request_json(
        'http://127.0.0.1:%d/openapi.json' % port,
        30
    )
    if code != 200 or not isinstance(data, dict):
        return None, code, body
    paths = data.get('paths', {})
    return paths if isinstance(paths, dict) else {}, code, body


def get_fill_status(port):
    code, data, body = request_json(
        'http://127.0.0.1:%d/fill_status' % port,
        30
    )
    if code != 200 or not status_ok(data):
        return None, code, body
    return payload_of(data), code, body


def stop_running_fill(port, sid):
    pay, code, body = get_fill_status(port)
    if pay is None:
        out('SERVICE %d FILL STATUS unavailable before clear' % sid)
        return True

    if str(pay.get('state', '')).lower() != 'running':
        return True

    out('SERVICE %d stopping previous fill' % sid)
    code, data, body = request_json(
        'http://127.0.0.1:%d/fill_stop' % port,
        60
    )
    if code != 200 or not status_ok(data):
        out('SERVICE %d FILL_STOP FAILED http=%s body=%s' % (
            sid, code, compact(body)
        ))
        return False

    deadline = time.time() + STOP_TIMEOUT
    while time.time() < deadline:
        pay, code, body = get_fill_status(port)
        if pay is not None:
            state = str(pay.get('state', '')).lower()
            if state != 'running':
                out('SERVICE %d previous fill stopped state=%s' % (
                    sid, state or 'unknown'
                ))
                return True
        time.sleep(3)

    out('SERVICE %d FILL_STOP TIMEOUT' % sid)
    return False


def clear_full_cache(port, sid):
    query = urllib.parse.urlencode({
        'pairs': '1,3,4',
        'days': '0,1',
        'also_backtest': 'false',
        'stop_fill': 'true',
    })
    code, data, body = request_json(
        'http://127.0.0.1:%d/clear_cache?%s' % (
            port, query
        ),
        1200
    )
    if code != 200 or not status_ok(data):
        out('SERVICE %d CLEAR CACHE FAILED http=%s body=%s' % (
            sid, code, compact(body)
        ))
        return False

    pay = payload_of(data)
    out(
        'SERVICE %d CLEAR CACHE OK deleted_cache=%s '
        'deleted_backtest=%s' % (
            sid,
            pay.get('deleted_cache', '?'),
            pay.get('deleted_backtest', '?')
        )
    )
    return True


def start_full_fill(port, sid):
    query = urllib.parse.urlencode({
        'pairs': '1,3,4',
        'days': '0,1',
        'batch_size': '300',
    })
    code, data, body = request_json(
        'http://127.0.0.1:%d/fill_cache?%s' % (
            port, query
        ),
        300
    )
    if code != 200 or not status_ok(data):
        out('SERVICE %d FILL START FAILED http=%s body=%s' % (
            sid, code, compact(body)
        ))
        return False

    pay = payload_of(data)
    if not pay.get('started', False):
        out('SERVICE %d FILL NOT STARTED payload=%s' % (
            sid,
            compact(json.dumps(pay, ensure_ascii=False))
        ))
        return False

    out(
        'SERVICE %d FILL STARTED pairs=%s days=%s '
        'slots_total=%s date_from=%s date_to=%s' % (
            sid,
            pay.get('pairs'),
            pay.get('days'),
            pay.get('slots_total'),
            pay.get('date_from'),
            pay.get('date_to')
        )
    )
    return True


def wait_fill(port, sid):
    deadline = time.time() + SERVICE_FILL_TIMEOUT
    start_deadline = time.time() + FILL_START_GRACE
    seen_running = False
    last_signature = None
    last_print = 0
    time.sleep(3)

    while time.time() < deadline:
        pay, code, body = get_fill_status(port)
        if pay is None:
            out('SERVICE %d FILL STATUS ERROR http=%s body=%s' % (
                sid, code, compact(body)
            ))
            time.sleep(POLL_SECONDS)
            continue

        state = str(pay.get('state', '')).lower()
        if state == 'running':
            seen_running = True

        signature = (
            state,
            pay.get('done'),
            pay.get('skipped'),
            pay.get('errors'),
            pay.get('slots_done'),
            pay.get('slots_total')
        )
        now = time.time()
        if signature != last_signature or now - last_print >= 60:
            out(
                'SERVICE %d FILL STATUS state=%s done=%s '
                'skipped=%s errors=%s slots=%s/%s' % (
                    sid,
                    state or 'unknown',
                    pay.get('done', '?'),
                    pay.get('skipped', '?'),
                    pay.get('errors', '?'),
                    pay.get('slots_done', '?'),
                    pay.get('slots_total', '?')
                )
            )
            last_signature = signature
            last_print = now

        if state in ('done', 'completed', 'success', 'ok'):
            errors = int(pay.get('errors', 0) or 0)
            if errors > 0:
                out('SERVICE %d FILL FINISHED WITH ERRORS=%d' % (
                    sid, errors
                ))
                return False
            out('SERVICE %d FILL DONE' % sid)
            return True

        if state in (
            'failed', 'error', 'cancelled',
            'canceled', 'stopped'
        ):
            out('SERVICE %d FILL FAILED final=%s' % (
                sid,
                compact(json.dumps(pay, ensure_ascii=False), 3000)
            ))
            return False

        if state == 'idle' and not seen_running:
            if time.time() >= start_deadline:
                out('SERVICE %d FILL START TIMEOUT' % sid)
                return False

        if state == 'idle' and seen_running:
            errors = int(pay.get('errors', 0) or 0)
            done = int(pay.get('done', 0) or 0)
            skipped = int(pay.get('skipped', 0) or 0)
            if errors == 0 and (done > 0 or skipped > 0):
                out(
                    'SERVICE %d FILL DONE state=idle '
                    'done=%d skipped=%d' % (
                        sid, done, skipped
                    )
                )
                return True
            out('SERVICE %d FILL STOPPED UNEXPECTEDLY' % sid)
            return False

        time.sleep(POLL_SECONDS)

    out('SERVICE %d FILL TIMEOUT after=%ds' % (
        sid, SERVICE_FILL_TIMEOUT
    ))
    return False


def process_service(item):
    sid, port = item
    out('=' * 90)
    out('SERVICE %d BEGIN port=%d' % (sid, port))

    if not wait_http(port, sid):
        return sid, 'FAILED'

    paths, code, body = get_paths(port)
    if paths is None:
        out('SERVICE %d OPENAPI FAILED http=%s body=%s' % (
            sid, code, compact(body)
        ))
        return sid, 'FAILED'

    required = (
        '/clear_cache',
        '/fill_cache',
        '/fill_status',
        '/fill_stop'
    )
    missing = [path for path in required if path not in paths]
    if missing:
        out('SERVICE %d FAILED unsupported endpoints=%s' % (
            sid, ','.join(missing)
        ))
        return sid, 'FAILED'

    if not stop_running_fill(port, sid):
        return sid, 'FAILED'
    if not clear_full_cache(port, sid):
        return sid, 'FAILED'
    if not start_full_fill(port, sid):
        return sid, 'FAILED'
    if not wait_fill(port, sid):
        return sid, 'FAILED'

    if sid > 81:
        out(
            'SERVICE %d RESULT=OK CACHE ONLY; '
            'REINIT SKIPPED (ID > 81)' % sid
        )
        return sid, 'OK'

    out('SERVICE %d CACHE RESULT=OK; STARTING REINIT ON ALL NODES' % sid)
    command = [
        'bash',
        os.path.join(ROOT, 'centos/php.sh'),
        'restart_and_check_all_services.php?stage=reinit&model_id=%d' % sid
    ]
    proc = subprocess.Popen(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    for line in iter(proc.stdout.readline, ''):
        if not line:
            break
        out('SERVICE %d REINIT: %s' % (sid, line.rstrip()))
    proc.wait()

    if proc.returncode != 0:
        out('SERVICE %d REINIT FAILED rc=%d' % (
            sid, proc.returncode
        ))
        return sid, 'FAILED'

    out('SERVICE %d RESULT=OK CACHE+REINIT' % sid)
    return sid, 'OK'


items = []
for raw in SPEC.split():
    sid_text, port_text = raw.split(':', 1)
    items.append((int(sid_text), int(port_text)))

try:
    with open(PID_FILE, 'w') as fh:
        fh.write(str(os.getpid()))
except Exception as exc:
    out('WARNING cannot write pid file: %s' % exc)

out('NODE=%s START=%s' % (
    os.uname().nodename,
    time.strftime('%Y-%m-%d %H:%M:%S')
))
out(
        'MODE=CAUSAL_FIX_CACHE_CLEAR_REFILL_44_48_49_89 '
    'days=0,1 backtest=NOT_TOUCHED max_workers=%d' % MAX_WORKERS
)
out('SERVICES=%d' % len(items))

ok_count = 0
fail_count = 0

try:
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as pool:
        futures = [
            pool.submit(process_service, item)
            for item in items
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                sid, result = future.result()
            except Exception as exc:
                out('WORKER FAILED exception=%s' % compact(exc))
                fail_count += 1
                continue

            if result == 'OK':
                ok_count += 1
            else:
                fail_count += 1

    out('=' * 90)
    out(
        'CACHE SUMMARY OK=%d FAILED=%d TOTAL=%d' % (
            ok_count, fail_count, len(items)
        )
    )

    if fail_count != 0:
        out('FULL PIPELINE RESULT=FAILED')
        sys.exit(1)

    out(
        'FULL PIPELINE RESULT=OK; MODELS 44,48,49,89 CACHE COMPLETED; '
        'REINIT COMPLETED ONLY FOR ID <= 81'
    )
finally:
    try:
        os.unlink(PID_FILE)
    except Exception:
        pass
    if SELF_FILE:
        try:
            os.unlink(SELF_FILE)
        except Exception:
            pass
PYCODE;

        if (@file_put_contents($tmpFile, $pythonCode) === false) {
            throw new Exception(
                'Не удалось создать временный Python-контроллер'
            );
        }
        @chmod($tmpFile, 0700);

        $env = 'BRAIN_SERVICE_SPEC='.escapeshellarg($serviceSpec)
            .' BRAIN_PID_FILE='.escapeshellarg($pidFile)
            .' BRAIN_PYTHON_FILE='.escapeshellarg($tmpFile);

        $command = $env
            .' nohup python3 '.escapeshellarg($tmpFile)
            .' >> '.escapeshellarg($logFile)
            .' 2>&1 < /dev/null & echo $!';

        $pid = trim((string)@shell_exec($command));
        if ($pid === '' || !ctype_digit($pid)) {
            @unlink($tmpFile);
            throw new Exception(
                'Не удалось запустить фоновый cache-controller'
            );
        }

        echo 'STEP 1 OK: services 44,48,49,89 restarted on all nodes'.PHP_EOL;
        echo 'STEP 2 STARTED: full H1+D1 cache rebuild on Brain 1'.PHP_EOL;
        echo 'STEP 3 AUTO: Reinit after cache only for model ID <= 81; ID > 81 cache only'.PHP_EOL;
        echo 'Background PID: '.$pid.PHP_EOL;
        echo 'Log: '.$logFile.PHP_EOL;
        echo 'PID file: '.$pidFile.PHP_EOL;
        echo 'Просмотр: tail -f '.escapeshellarg($logFile).PHP_EOL;
        echo 'Процесс: ps -fp '.$pid.PHP_EOL;
        echo 'Backtest cache: NOT TOUCHED'.PHP_EOL;
        return true;
    } catch (Exception $e) {
        echo 'FATAL: '.$e->getMessage().PHP_EOL;
        engine::throw('restart_and_check_all_services()', $e);
        exit(1);
    }
}

restart_and_check_all_services();
