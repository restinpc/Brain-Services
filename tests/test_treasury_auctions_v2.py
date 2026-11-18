import runpy
import sys
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class TreasuryAuctionV2Tests(unittest.TestCase):
    def load_parser(self):
        argv = ['treasury_gov.py', 'vlad_tr_auctions_query_updated', 'host', '3306', 'user', 'pass', 'db']
        with patch.object(sys, 'argv', argv):
            return runpy.run_path(str(ROOT / 'parser' / 'treasury_gov.py'), run_name='treasury_gov_test')

    def test_updated_auction_endpoint_is_separate_from_legacy(self):
        ns = self.load_parser()
        self.assertEqual(ns['DATASETS']['vlad_tr_auctions_query_updated'],
                         'v1/accounting/od/treasury_securities_auctions_v2')
        self.assertEqual(ns['DATASETS']['vlad_tr_auctions_query'],
                         'v1/accounting/od/auctions_query')

    def test_v2_fetch_is_capped_at_current_source_date(self):
        ns = self.load_parser()
        collector = ns['TreasuryCollector']('vlad_tr_auctions_query_updated')
        seen = []
        class Response:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {'data': [{'cusip': '123456789', 'issue_date': '2026-11-01', 'record_date': '2026-11-02'}]}
        class Session:
            def get(self, url, params, timeout): seen.append(params); return Response()
        collector.session = Session()
        collector.detect_date_column = lambda endpoint: 'record_date'
        self.assertEqual(len(collector.fetch_all_pages('v2/test')), 1)
        self.assertEqual(seen[0]['filter'], 'record_date:lte:' + ns['datetime'].now(ns['timezone'].utc).date().isoformat())
        self.assertEqual(seen[0]['sort'], '-record_date')

    def test_v2_table_has_official_unique_key(self):
        ns = self.load_parser()
        collector = ns['TreasuryCollector']('vlad_tr_auctions_query_updated')
        statements = []
        class Cursor:
            def execute(self, sql, params=None): statements.append(sql)
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def cursor(self): return Cursor()
            def commit(self): pass
        collector.get_db_connection = lambda: Conn()
        collector.create_table_from_sample({'cusip': '123456789', 'issue_date': '2026-11-01', 'record_date': '2026-11-02'})
        self.assertIn('uq_treasury_cusip_issue_date', statements[0])

    def test_v2_upsert_preserves_history_and_uses_official_key(self):
        ns = self.load_parser()
        collector = ns['TreasuryCollector']('vlad_tr_auctions_query_updated')
        statements = []

        class Cursor:
            rowcount = 1
            def execute(self, sql, params=None): statements.append((sql, params))
            def executemany(self, sql, rows): statements.append((sql, rows))
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def cursor(self): return Cursor()
            def commit(self): pass

        collector.get_db_connection = lambda: Conn()
        collector.insert_batch([{'cusip': '123456789', 'issue_date': '2026-11-01', 'record_date': '2026-11-02', 'amount': '10'}])
        sql = statements[-1][0].lower()
        self.assertIn('on duplicate key update', sql)
        self.assertNotIn('delete from', sql)
        self.assertIn('`cusip`', sql)
        self.assertIn('`issue_date`', sql)


if __name__ == '__main__':
    unittest.main()
