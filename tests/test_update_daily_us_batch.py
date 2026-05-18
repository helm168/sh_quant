"""scripts/update_daily.py — Polygon Grouped Daily US batch fast-path 测试.

测试覆盖 (#40 follow-up):
    - _recent_us_trading_days 边界 (周日 / lookback>1 / lookback=0)
    - _polygon_grouped_to_rows schema 正确性 (BRK.B → BRK-B.US, vol 单位)
    - _prefetch_polygon_us_batch mock requests → 填 cache + 行数
    - fast-path 命中 (US ticker + 缓存范围覆盖 → 不调 vendor)
    - fast-path 严格 fall-through (范围不覆盖, ticker 不在 batch)
    - --verify 强制 slow path
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

# scripts/ 不是 package, 用 importlib.spec 加载
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / 'scripts' / 'update_daily.py'


@pytest.fixture
def ud(monkeypatch):
    """加载 update_daily 模块, 每个测试都重置 module-level cache."""
    spec = importlib.util.spec_from_file_location('update_daily', SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['update_daily'] = mod
    spec.loader.exec_module(mod)
    # 重置全局 cache
    mod._POLY_BATCH_CACHE = {}
    mod._POLY_BATCH_MIN_DATE = None
    mod._POLY_BATCH_MAX_DATE = None
    mod._POLY_BATCH_HIT_COUNT = 0
    mod._POLY_BATCH_MISS_COUNT = 0
    mod._VERIFY_MODE = False
    yield mod
    # 清场
    mod._POLY_BATCH_CACHE = {}
    mod._POLY_BATCH_MIN_DATE = None
    mod._POLY_BATCH_MAX_DATE = None
    mod._POLY_BATCH_HIT_COUNT = 0
    mod._POLY_BATCH_MISS_COUNT = 0
    mod._VERIFY_MODE = False


def _fake_polygon_response(date_ms: int, tickers: list[tuple[str, float, int]]) -> dict:
    """构造 Polygon Grouped Daily 响应. tickers = [(T, close, vol), ...]."""
    results = []
    for tkr, close, vol in tickers:
        results.append(
            {
                'T': tkr,
                'v': vol,
                'o': close - 1.0,
                'c': close,
                'h': close + 0.5,
                'l': close - 1.5,
                't': date_ms,
                'vw': close,
                'n': 1234,
            }
        )
    return {
        'queryCount': len(results),
        'resultsCount': len(results),
        'results': results,
    }


# ---------- _recent_us_trading_days 边界 ----------


def test_recent_trading_days_weekend(ud):
    """today 是周日, lookback=1 → 返回 [Friday]."""
    sunday = pd.Timestamp('2025-05-18')  # 周日
    days = ud._recent_us_trading_days(sunday, 1)
    assert len(days) == 1
    assert days[0] == pd.Timestamp('2025-05-16')  # Friday


def test_recent_trading_days_lookback_7(ud):
    """lookback=7 个工作日 → 跨越约 9-10 个日历日."""
    # 2025-05-17 周六, 倒推 7 工作日 → 5/16 5/15 5/14 5/13 5/12 5/9 5/8
    sat = pd.Timestamp('2025-05-17')
    days = ud._recent_us_trading_days(sat, 7)
    assert len(days) == 7
    # 全是工作日
    assert all(d.weekday() < 5 for d in days)
    # 升序
    assert days == sorted(days)


def test_recent_trading_days_zero(ud):
    assert ud._recent_us_trading_days(pd.Timestamp('2025-05-18'), 0) == []


# ---------- _polygon_grouped_to_rows schema ----------


def test_polygon_rows_schema(ud):
    """ticker 转 ts_code (NVDA → NVDA.US), BRK.B 的 dot 转 dash."""
    ms = int(pd.Timestamp('2025-05-16').timestamp() * 1000)
    results = [
        {'T': 'NVDA', 'v': 1000, 'o': 100, 'c': 101, 'h': 102, 'l': 99, 't': ms, 'vw': 100.5},
        {'T': 'BRK.B', 'v': 500, 'o': 200, 'c': 201, 'h': 202, 'l': 199, 't': ms, 'vw': 200.0},
    ]
    rows = ud._polygon_grouped_to_rows(results)
    assert len(rows) == 2
    nvda, brk = rows
    assert nvda['ts_code'] == 'NVDA.US'
    assert nvda['vol'] == 1000  # 股, 直接用
    assert nvda['close'] == 101
    assert nvda['amount'] == 101 * 1000
    assert nvda['adj_factor'] == 1.0
    assert nvda['trade_date'] == pd.Timestamp('2025-05-16')
    # BRK.B → BRK-B.US (Polygon dot → sh_quant dash)
    assert brk['ts_code'] == 'BRK-B.US'


# ---------- _prefetch_polygon_us_batch (mock requests) ----------


def test_prefetch_fills_cache(ud, monkeypatch):
    """mock 1 个 API call 返回 3 ticker → cache 填好, 行数对."""
    monkeypatch.setenv('POLYGON_API_KEY', 'test-key')

    ms = int(pd.Timestamp('2025-05-16').timestamp() * 1000)
    payload = _fake_polygon_response(
        ms,
        [('AAPL', 200.0, 5000), ('NVDA', 800.0, 3000), ('TSLA', 250.0, 2000)],
    )

    call_count = {'n': 0}

    class FakeResp:
        status_code = 200

        def json(self):
            return payload

    def fake_get(url, timeout=30):
        call_count['n'] += 1
        return FakeResp()

    import requests

    monkeypatch.setattr(requests, 'get', fake_get)

    # Friday 2025-05-16, lookback=1 → 1 个 call
    today = pd.Timestamp('2025-05-16')
    n = ud._prefetch_polygon_us_batch(today, lookback_days=1)
    assert call_count['n'] == 1
    assert n == 3
    assert set(ud._POLY_BATCH_CACHE.keys()) == {'AAPL.US', 'NVDA.US', 'TSLA.US'}
    aapl = ud._POLY_BATCH_CACHE['AAPL.US']
    assert len(aapl) == 1
    assert aapl.iloc[0]['close'] == 200.0
    assert pd.Timestamp('2025-05-16') == ud._POLY_BATCH_MIN_DATE
    assert pd.Timestamp('2025-05-16') == ud._POLY_BATCH_MAX_DATE


def test_prefetch_lookback_5_calls_5_times(ud, monkeypatch):
    """lookback=7 个日历日 (5/12 周一 → 5/16 周五) → 5 个工作日 → 5 个 call."""
    monkeypatch.setenv('POLYGON_API_KEY', 'test-key')

    call_count = {'n': 0}

    class FakeResp:
        status_code = 200

        def json(self):
            ms = int(pd.Timestamp('2025-05-16').timestamp() * 1000)
            return _fake_polygon_response(ms, [('AAPL', 200.0, 5000)])

    def fake_get(url, timeout=30):
        call_count['n'] += 1
        return FakeResp()

    import requests

    monkeypatch.setattr(requests, 'get', fake_get)

    # Fri 5/16 lookback 5 个工作日 → 5/12 5/13 5/14 5/15 5/16 = 5 个 call
    n = ud._prefetch_polygon_us_batch(pd.Timestamp('2025-05-16'), lookback_days=5)
    assert call_count['n'] == 5
    assert n == 1  # 1 个 ticker (AAPL)


def test_prefetch_no_api_key_returns_zero(ud, monkeypatch):
    monkeypatch.delenv('POLYGON_API_KEY', raising=False)
    n = ud._prefetch_polygon_us_batch(pd.Timestamp('2025-05-16'), 1)
    assert n == 0


def test_prefetch_empty_results_skipped(ud, monkeypatch):
    """节假日: results=[] → 跳过那天, 不污染缓存."""
    monkeypatch.setenv('POLYGON_API_KEY', 'test-key')

    class FakeResp:
        status_code = 200

        def json(self):
            return {'queryCount': 0, 'resultsCount': 0, 'results': []}

    def fake_get(url, timeout=30):
        return FakeResp()

    import requests

    monkeypatch.setattr(requests, 'get', fake_get)
    n = ud._prefetch_polygon_us_batch(pd.Timestamp('2025-05-16'), 1)
    assert n == 0
    assert ud._POLY_BATCH_MIN_DATE is None


# ---------- fast-path hit / fall-through ----------


def _seed_cache(ud, ts_code: str, dates: list[pd.Timestamp], close: float = 100.0) -> None:
    """直接往 _POLY_BATCH_CACHE 塞一只 ticker 的 mini DataFrame."""
    rows = [
        {
            'trade_date': d,
            'ts_code': ts_code,
            'open': close - 1,
            'high': close + 1,
            'low': close - 2,
            'close': close + i * 0.1,
            'vol': 1000,
            'amount': close * 1000,
            'adj_factor': 1.0,
        }
        for i, d in enumerate(dates)
    ]
    ud._POLY_BATCH_CACHE[ts_code] = pd.DataFrame(rows)
    ud._POLY_BATCH_MIN_DATE = min(dates)
    ud._POLY_BATCH_MAX_DATE = max(dates)


def test_fetch_one_hits_batch_no_vendor_call(ud, monkeypatch):
    """US ticker + batch 覆盖 → fetch_one 用 polygon_batch, 不调 FMP/Polygon."""
    _seed_cache(ud, 'AAPL.US', [pd.Timestamp('2025-05-16')])

    called = {'fmp': 0, 'poly': 0, 'yf': 0}
    monkeypatch.setattr(
        ud,
        '_fetch_us_via_fmp',
        lambda *a, **kw: called.update(fmp=called['fmp'] + 1) or None,
    )
    monkeypatch.setattr(
        ud,
        '_fetch_us_via_polygon',
        lambda *a, **kw: called.update(poly=called['poly'] + 1) or None,
    )
    monkeypatch.setattr(
        ud,
        '_fetch_us_via_yfinance',
        lambda *a, **kw: called.update(yf=called['yf'] + 1) or None,
    )

    df, vendor = ud.fetch_one('AAPL.US', '2025-05-16', '2025-05-16')
    assert vendor == 'polygon_batch'
    assert df is not None and len(df) == 1
    # 关键: 没有调任何 per-ticker 接口
    assert called == {'fmp': 0, 'poly': 0, 'yf': 0}
    assert ud._POLY_BATCH_HIT_COUNT == 1


def test_fetch_one_falls_through_when_range_not_covered(ud, monkeypatch):
    """请求 [start, end] 超出 batch 范围 → fall-through 到 FMP."""
    _seed_cache(ud, 'AAPL.US', [pd.Timestamp('2025-05-16')])

    called = {'fmp': 0}

    def fake_fmp(ts_code, start, end):
        called['fmp'] += 1
        return pd.DataFrame(
            {
                'trade_date': [pd.Timestamp('2025-05-10')],
                'ts_code': [ts_code],
                'open': [100],
                'high': [101],
                'low': [99],
                'close': [100.5],
                'vol': [1000],
                'amount': [100500],
                'adj_factor': [1.0],
                'pre_close': [100],
                'change': [0.5],
                'pct_chg': [0.5],
            }
        )

    monkeypatch.setattr(ud, '_fetch_us_via_fmp', fake_fmp)

    # start=2025-05-10 < batch_min=2025-05-16 → 严格不命中
    df, vendor = ud.fetch_one('AAPL.US', '2025-05-10', '2025-05-16')
    assert vendor == 'fmp'
    assert called['fmp'] == 1
    assert ud._POLY_BATCH_HIT_COUNT == 0


def test_fetch_one_falls_through_when_ticker_not_in_batch(ud, monkeypatch):
    """ts_code 不在 cache (新上市/退市/dot-ticker 不匹配) → fall-through."""
    _seed_cache(ud, 'AAPL.US', [pd.Timestamp('2025-05-16')])

    called = {'fmp': 0}

    def fake_fmp(ts_code, start, end):
        called['fmp'] += 1
        return None  # FMP 也拉不到 → 接着调 polygon → yfinance

    monkeypatch.setattr(ud, '_fetch_us_via_fmp', fake_fmp)
    monkeypatch.setattr(ud, '_fetch_us_via_polygon', lambda *a, **kw: None)
    monkeypatch.setattr(ud, '_fetch_us_via_yfinance', lambda *a, **kw: None)

    df, vendor = ud.fetch_one('NEWIPO.US', '2025-05-16', '2025-05-16')
    # ts_code 不在 batch → batch path 标 miss + 返 None, fall-through FMP
    assert called['fmp'] == 1
    assert ud._POLY_BATCH_MISS_COUNT == 1


def test_verify_mode_skips_batch(ud, monkeypatch):
    """--verify 设 _VERIFY_MODE=True 时, 即使 batch 命中也走 slow path."""
    _seed_cache(ud, 'AAPL.US', [pd.Timestamp('2025-05-16')])
    ud._VERIFY_MODE = True

    called = {'fmp': 0}

    def fake_fmp(ts_code, start, end):
        called['fmp'] += 1
        return pd.DataFrame(
            {
                'trade_date': [pd.Timestamp('2025-05-16')],
                'ts_code': [ts_code],
                'open': [100],
                'high': [101],
                'low': [99],
                'close': [100.5],
                'vol': [1000],
                'amount': [100500],
                'adj_factor': [1.0],
                'pre_close': [100],
                'change': [0.5],
                'pct_chg': [0.5],
            }
        )

    monkeypatch.setattr(ud, '_fetch_us_via_fmp', fake_fmp)

    df, vendor = ud.fetch_one('AAPL.US', '2025-05-16', '2025-05-16')
    assert vendor == 'fmp'
    assert called['fmp'] == 1
    assert ud._POLY_BATCH_HIT_COUNT == 0
