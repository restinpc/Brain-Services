import os
import runpy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def load_script(name, table, env=None):
    argv = [name, table, 'host', '3306', 'user', 'pass', 'brain']
    with patch.dict(os.environ, env or {}, clear=False), patch.object(sys, 'argv', argv):
        return runpy.run_path(str(ROOT / 'parser' / name), run_name='source_retry_test')


class SourceFetchRetryTests(unittest.TestCase):
    def test_oilprice_timeout_falls_through_to_available_endpoint(self):
        ns = load_script('OilPriceAPI.py', 'sasha_oilpriceapi_brent', {'OILPRICE_API_KEY': 'test-key'})
        class Response:
            status_code = 200
            def json(self):
                return {'data': [{'date': '2026-11-18', 'code': 'BRENT_CRUDE_USD', 'price': 70.0}]}
        with patch.object(ns['requests'], 'get', side_effect=[ns['requests'].ReadTimeout('slow'), Response()]) as get:
            rows = ns['fetch_data'](ns['DATASETS']['sasha_oilpriceapi_brent'])
        self.assertEqual(rows, [('2026-11-18', 'BRENT_CRUDE_USD', 70.0)])
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[0].kwargs['proxies']['https'], 'http://10.10.11.1:3128')

    def test_oilprice_all_endpoint_timeouts_are_a_fetch_failure(self):
        ns = load_script('OilPriceAPI.py', 'sasha_oilpriceapi_brent', {'OILPRICE_API_KEY': 'test-key'})
        with patch.object(ns['requests'], 'get', side_effect=ns['requests'].ReadTimeout('slow')):
            with self.assertRaisesRegex(RuntimeError, 'OILPRICE_API_ENDPOINTS_UNREACHABLE'):
                ns['fetch_data'](ns['DATASETS']['sasha_oilpriceapi_brent'])

    def test_aisstream_retries_transient_handshake_error(self):
        ns = load_script('WorldMonitor_ais_maritime.py', 'vlad_wm_ais_snapshot')
        failed = ([], {'error': 'handshake timeout'}, 'error')
        recovered = ([{'MMSI': '123456789'}], {'source': 'aisstream'}, 'ok')
        outcomes = iter([failed, recovered])
        def fake_run(coro):
            coro.close()
            return next(outcomes)
        with patch.object(ns['asyncio'], 'run', side_effect=fake_run) as run, \
             patch.object(ns['time'], 'sleep') as sleep:
            result = ns['_fetch_aisstream_vessels']()
        self.assertEqual(result, recovered)
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once()


    def test_coindesk_rss_retries_transient_timeout(self):
        ns = load_script('coindesk_crypto_news.py', 'vlad_coindesk_crypto_news')
        response = type('Response', (), {'content': b'<rss/>', 'raise_for_status': lambda self: None})()
        with patch.object(ns['requests'], 'get', side_effect=[ns['requests'].ReadTimeout('slow'), response]) as get, \
             patch('time.sleep') as sleep:
            payload = ns['fetch_rss_xml']()
        self.assertEqual(payload, b'<rss/>')
        self.assertEqual(get.call_count, 2)
        sleep.assert_called_once()


    def test_coindesk_rss_http_error_is_not_retried(self):
        ns = load_script('coindesk_crypto_news.py', 'vlad_coindesk_crypto_news')
        response = type('Response', (), {'content': b'', 'raise_for_status': lambda self: (_ for _ in ()).throw(ns['requests'].HTTPError('403'))})()
        with patch.object(ns['requests'], 'get', return_value=response) as get, \
             self.assertRaises(ns['requests'].HTTPError):
            ns['fetch_rss_xml']()
        get.assert_called_once()


if __name__ == '__main__':
    unittest.main()
