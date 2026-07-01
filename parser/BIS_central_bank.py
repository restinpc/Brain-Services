"""
Таблицы:
  vlad_bis_policy_rates     — ставки центральных банков (полная история)
  vlad_bis_credit_gdp       — credit-to-GDP ratio

Запуск:
  python BIS_central_bank.py vlad_bis_policy_rates [host] [port] [user] [password] [database]
"""

import os, sys, argparse, traceback, csv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "bis_central_bank")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="BIS_central_bank.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="BIS Central Bank Data → MySQL")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не все параметры БД"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {
    "vlad_bis_policy_rates": {"description": "BIS central bank policy rates", "dataflow": "WS_CBPOL"},
    "vlad_bis_credit_gdp":   {"description": "BIS credit-to-GDP ratio",     "dataflow": "WS_CREDIT_GAP"},
}

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "BrainProject/1.0", "Accept": "text/csv"})
    return s

class BISCollector:
    def __init__(self, table_name, dataflow):
        self.table_name = table_name
        self.dataflow = dataflow
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_code VARCHAR(10) NOT NULL,
                country_name VARCHAR(100),
                period VARCHAR(20) NOT NULL COMMENT 'YYYY or YYYY-MM or YYYY-QQ or YYYY-MM-DD',
                value DECIMAL(10,4),
                unit VARCHAR(50),
                frequency VARCHAR(10) COMMENT 'M/Q/A/D',
                series_key VARCHAR(200) COMMENT 'BIS series key',
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_series_period (series_key(100), period),
                INDEX idx_country (country_code),
                INDEX idx_period (period)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='BIS SDMX data ({self.dataflow})';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()
        url = f"https://stats.bis.org/api/v1/data/{self.dataflow}/all"
        params = {"lastNObservations": "24", "format": "csv"}

        print(f"    BIS API v1 CSV: {self.dataflow}...")
        resp = self.session.get(url, params=params, timeout=90)

        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}"); return

        text = resp.text.strip()
        if not text or len(text) < 100:
            print("    Пустой ответ"); return

        rows = []
        try:
            csv_reader = csv.reader(text.splitlines(), delimiter=',')
            headers = next(csv_reader)
            header_idx = {h.strip(): i for i, h in enumerate(headers)}

            # === ИНДЕКСЫ КОЛОНОК ===
            country_idx = None
            for col in ['REF_AREA', 'BORROWERS_CTY']:
                if col in header_idx:
                    country_idx = header_idx[col]
                    break
            if country_idx is None:
                print("    Нет колонки страны"); return

            freq_idx    = header_idx.get('FREQ')
            period_idx  = header_idx.get('TIME_PERIOD')
            value_idx   = header_idx.get('OBS_VALUE')
            if None in (freq_idx, period_idx, value_idx):
                print("    Отсутствуют обязательные колонки"); return

            cg_dtype_idx = header_idx.get('CG_DTYPE')

            for row in csv_reader:
                if len(row) <= value_idx: continue

                # значение
                val_str = row[value_idx].strip() if value_idx < len(row) else ''
                if not val_str or val_str.upper() in ('NAN', 'NA', 'NULL', ''):
                    continue
                try:
                    val = float(val_str)
                except (ValueError, TypeError):
                    continue

                country_code = row[country_idx].strip() if country_idx < len(row) else ""
                period       = row[period_idx].strip() if period_idx < len(row) else ""
                freq         = row[freq_idx].strip()    if freq_idx < len(row) else ""

                # уникальный ключ серии
                series_key = f"{self.dataflow}:{freq}:{country_code}"
                if cg_dtype_idx is not None and cg_dtype_idx < len(row):
                    dtype = row[cg_dtype_idx].strip()
                    if dtype:
                        series_key += f":{dtype}"

                rows.append((
                    country_code[:10],
                    "",                     # country_name — в CSV нет названий
                    period[:20],
                    round(val, 4),
                    "",
                    freq,
                    series_key[:200],
                ))

        except Exception as e:
            print(f"    Parse error: {e}")
            return

        if not rows:
            print("    Нет данных после парсинга")
            return

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (country_code, country_name, period, value, unit, frequency, series_key)
            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        c.executemany(sql, rows)
        conn.commit()
        n = c.rowcount
        c.close(); conn.close()

        print(f"    Записано {n} новых observations из {len(rows)} total")
        countries = set(r[0] for r in rows if r[0])
        print(f"      Стран: {len(countries)}: {', '.join(sorted(countries)[:20])}...")

def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица. Допустимые:"); [print(f"  - {n}") for n in DATASETS]; sys.exit(1)
    ds = DATASETS[args.table_name]
    print(f" BIS Central Bank Collector ({ds['dataflow']})")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}"); print("=" * 60)
    BISCollector(args.table_name, ds["dataflow"]).process()
    print("=" * 60); print(" ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n Прервано"); sys.exit(1)
    except Exception as e: print(f"\n {e!r}"); send_error_trace(e); sys.exit(1)