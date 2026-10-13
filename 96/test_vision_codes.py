"""Синтетические проверки индекса CV-паттернов без мастер-базы."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

import vision_codes as vc


def _series():
    """Пять часовых свечек; текущая — последняя, её T1 ещё нет."""
    start = datetime(2024, 1, 1, 12, 0, 0)
    dates = [start + timedelta(hours=i) for i in range(5)]
    dates_ns = np.array([int(item.timestamp()) for item in dates], dtype=np.int64)
    # 1x1 и 2x2. Строки 0 и 2 совпадают с текущей (строка 4) полностью.
    # Строка 1 совпадает только по 1x1.
    cells = np.array([
        [421, 13],
        [421, 99],
        [421, 13],
        [700, 80],
        [421, 13],
    ], dtype=np.uint16)
    t1 = np.array([10.0, 20.0, 30.0, 40.0, 0.0], dtype=np.float64)
    pairs = [(1, 1), (2, 2)]
    history = vc.build_from_arrays(dates_ns, cells, t1, pairs, max_depth=2)
    query = datetime.fromtimestamp(dates_ns[4] + 3600)
    return history, query


def test_nested_squares_sum_following_t1():
    history, query = _series()
    pairs = dict(history.codes_at(query, is_daily=False, max_depth=2))
    assert list(pairs) == ["0421", "04210013"]
    # 1x1 совпал у якорей 0,1,2 (3 отсечён: другая ячейка; 4 — текущий).
    # T1 следующих: 20 + 30 + 40 = 90.
    assert pairs["0421"] == 90.0
    # 2x2 совпал у якорей 0 и 2 → 20 + 40 = 60.
    assert pairs["04210013"] == 60.0


def test_current_window_is_excluded():
    history, _ = _series()
    # На закрытии свечки 2 в аналоги попадают только якоря 0 и 1, не 2/3/4.
    query_mid = datetime.fromtimestamp(int(history.dates_ns[2]) + 3600)
    pairs = dict(history.codes_at(query_mid, is_daily=False, max_depth=2))
    assert pairs["0421"] == 50.0
    assert pairs["04210013"] == 20.0


def test_missing_index_row_skips_current():
    dates_ns = np.arange(4, dtype=np.int64) * 3600 + 1_700_000_000
    cells = np.array([[1, 2], [1, 2], [1, 2], [9, 9]], dtype=np.uint16)
    t1 = np.array([1.0, 1.0, 1.0, 1.0])
    valid = np.array([True, True, True, False])
    history = vc.build_from_arrays(
        dates_ns, cells, t1, [(1, 1), (2, 2)], max_depth=2, valid=valid,
    )
    query = datetime.fromtimestamp(int(dates_ns[3]) + 3600)
    assert history.codes_at(query, is_daily=False) == []


def test_var_scales_by_cell_count():
    history, query = _series()
    pairs = history.codes_at(query, is_daily=False, max_depth=2)
    scaled = {code: value * (len(code) // 4) for code, value in pairs}
    assert scaled["0421"] == 90.0
    assert scaled["04210013"] == 120.0


def test_cell_name_parser_accepts_cyrillic_x():
    assert vc._parse_cell_name("1x1") == (1, 1)
    assert vc._parse_cell_name("2х1") == (2, 1)
    assert vc._parse_cell_name("12x12") == (12, 12)
    assert vc._parse_cell_name("rate_id") is None


def test_square_uses_all_cells_inside_the_window():
    dates_ns = np.arange(3, dtype=np.int64) * 3600 + 1_700_000_000
    cells = np.array([
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 9, 3, 4],
    ], dtype=np.uint16)
    t1 = np.array([5.0, 7.0, 0.0])
    history = vc.build_from_arrays(
        dates_ns, cells, t1, [(1, 1), (1, 2), (2, 1), (2, 2)], max_depth=2,
    )
    query = datetime.fromtimestamp(int(dates_ns[2]) + 3600)
    pairs = dict(history.codes_at(query, is_daily=False, max_depth=2))
    assert pairs["0001"] == 7.0
    assert pairs["0001000900030004"] == 0.0


if __name__ == "__main__":
    test_nested_squares_sum_following_t1()
    test_current_window_is_excluded()
    test_missing_index_row_skips_current()
    test_var_scales_by_cell_count()
    test_cell_name_parser_accepts_cyrillic_x()
    test_square_uses_all_cells_inside_the_window()
    print("ok")
