import importlib.util
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('v74_model80', ROOT / '80/model.py')
model80 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(model80)


class Model80CacheConcurrencyTest(unittest.TestCase):
    def setUp(self):
        model80._SINGLE_CACHE.clear()
        model80._BATCH_CACHE.clear()
        self.date = datetime(2026, 11, 26, 12)
        self.dataset = [{'date': self.date}]
        self.di = {'full_dataset': self.dataset}

    def concurrent_slots(self, batch):
        entered = threading.Event()
        release = threading.Event()
        duplicate = threading.Event()
        calls = []
        lock = threading.Lock()

        def compute(dataset, date, index):
            with lock:
                calls.append(date)
                if len(calls) > 1:
                    duplicate.set()
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test did not release compute')
            return {(0, 0): {'a': 1.0}, (1, 0): {'b': 2.0}}

        def invoke(slot):
            if batch:
                return model80.batch_model([], self.dataset, [self.date], type=slot, dataset_index=self.di)[self.date]
            return model80.model([], self.dataset, self.date, type=slot, dataset_index=self.di)

        with patch.object(model80, '_compute_all_slots_for_date', side_effect=compute), ThreadPoolExecutor(2) as pool:
            first = pool.submit(invoke, 0)
            self.assertTrue(entered.wait(2))
            second_started = threading.Event()
            def second_call():
                second_started.set()
                return invoke(1)
            second = pool.submit(second_call)
            self.assertTrue(second_started.wait(2))
            duplicate.wait(0.25)
            release.set()
            self.assertEqual(first.result(2), {'a': 1.0})
            self.assertEqual(second.result(2), {'b': 2.0})
        self.assertEqual(len(calls), 1, 'same snapshot/date cube computed twice')

    def test_single_different_slots_share_one_calculation(self):
        self.concurrent_slots(False)

    def test_batch_different_slots_share_one_calculation(self):
        self.concurrent_slots(True)

    def test_failed_calculation_does_not_poison_next_request(self):
        with patch.object(model80, '_compute_all_slots_for_date', side_effect=[RuntimeError('failure'), {(0, 0): {'a': 1.0}}]):
            with self.assertRaisesRegex(RuntimeError, 'failure'):
                model80.model([], self.dataset, self.date, dataset_index=self.di)
            self.assertEqual(model80.model([], self.dataset, self.date, dataset_index=self.di), {'a': 1.0})


if __name__ == '__main__':
    unittest.main()
