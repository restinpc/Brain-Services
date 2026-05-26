#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Orderbook Parser
========================
Fetches one depth snapshot from Binance REST API and inserts one row into MySQL.

Table schema (vlad_binance_ethusdt_orderbook / vlad_binance_btcusdt_orderbook):
    id             BIGINT        AUTO_INCREMENT PK
    timestamp      DATETIME(6)
    last_update_id BIGINT
    spread         DECIMAL(20,10)
    best_bid       DECIMAL(20,10)
    best_ask       DECIMAL(20,10)
    bids_json      LONGTEXT
    asks_json      LONGTEXT

Called by parser.php (blocking exec, no &):
    python binance_orderbook.py <table> <host> <port> <user> <pass> <db> [symbol]

brain_parser settings:
    script   = binance_orderbook.py
    interval = 0   (hourly)
    argument = ETHUSDT  (or BTCUSDT)

@path /brain/Brain-Services/parser/binance_orderbook.py
@name    Brain Server    @version 1.5.9
"""

import sys
import json
import ssl
import traceback
import urllib.request
import urllib.error
from decimal import Decimal
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
DEPTH_LIMIT     = 500
API_URL         = 'https://api.binance.com/api/v3/depth'
REQUEST_TIMEOUT = 15

# ---------------------------------------------------------------------------
def log(msg):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f'[{now}] {msg}', flush=True)

def err(msg):
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f'[{now}] ERROR: {msg}', file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
def symbol_from_table(table: str) -> str:
    """vlad_binance_ethusdt_orderbook -> ETHUSDT"""
    parts = table.lower().split('_')
    try:
        idx = parts.index('binance')
        return parts[idx + 1].upper()
    except (ValueError, IndexError):
        return ''

# ---------------------------------------------------------------------------
def fetch_orderbook(symbol: str) -> dict:
    url = f'{API_URL}?symbol={symbol}&limit={DEPTH_LIMIT}'
    req = urllib.request.Request(url, headers={
        'User-Agent': 'BrainServer/1.5.9',
        'Accept':     'application/json',
    })
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
        return json.loads(resp.read().decode('utf-8'))

# ---------------------------------------------------------------------------
def get_connection(host, port, user, password, db):
    try:
        import pymysql
        return pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=db, autocommit=False, charset='utf8mb4',
            connect_timeout=30,
        )
    except ImportError:
        pass
    import mysql.connector
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        database=db, autocommit=False, charset='utf8mb4',
        connection_timeout=30,
    )

# ---------------------------------------------------------------------------
def save_snapshot(conn, table: str, symbol: str, ob: dict):
    bids = ob.get('bids', [])
    asks = ob.get('asks', [])

    if not bids or not asks:
        raise RuntimeError(f'Empty bids or asks for {symbol}')

    best_bid       = Decimal(bids[0][0])
    best_ask       = Decimal(asks[0][0])
    spread         = best_ask - best_bid
    last_update_id = int(ob['lastUpdateId'])
    timestamp      = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    bids_json      = json.dumps(bids, separators=(',', ':'))
    asks_json      = json.dumps(asks, separators=(',', ':'))

    sql = (
        f'INSERT INTO `{table}` '
        f'(`timestamp`, `last_update_id`, `spread`, `best_bid`, `best_ask`, `bids_json`, `asks_json`) '
        f'VALUES (%s, %s, %s, %s, %s, %s, %s)'
    )
    cur = conn.cursor()
    try:
        cur.execute(sql, (timestamp, last_update_id, spread, best_bid, best_ask, bids_json, asks_json))
        conn.commit()
        log(f'OK | symbol={symbol} | best_bid={best_bid} | best_ask={best_ask} | spread={spread} | levels={len(bids)}+{len(asks)}')
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 7:
        print(__doc__)
        sys.exit(1)

    table    = sys.argv[1]
    host     = sys.argv[2]
    port     = int(sys.argv[3])
    user     = sys.argv[4]
    password = sys.argv[5]
    db       = sys.argv[6]
    symbol   = sys.argv[7].upper().strip() if len(sys.argv) > 7 else symbol_from_table(table)

    if not symbol:
        err(f'Cannot determine symbol from table "{table}". Pass it as 7th argument.')
        sys.exit(1)

    log(f'Start | symbol={symbol} | table={table} | depth={DEPTH_LIMIT}')

    try:
        ob = fetch_orderbook(symbol)
    except Exception as e:
        err(f'Fetch error: {e}')
        err(traceback.format_exc())
        sys.exit(1)

    try:
        conn = get_connection(host, port, user, password, db)
        save_snapshot(conn, table, symbol, ob)
        conn.close()
    except Exception as e:
        err(f'DB error: {e}')
        err(traceback.format_exc())
        sys.exit(1)

    log('Done.')

if __name__ == '__main__':
    main()
