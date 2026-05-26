"""scripts/pull_daily_basic.py — by-trade-date 路径测试.

覆盖 N ≥ ROUTE_BY_DATE_THRESHOLD 时, update_all 改走 by-trade-date 模式:
  - 调用 pro.trade_cal 拿区间内交易日 (1 call)
  - 每个交易日只调 pro.daily_basic(trade_date=d) 一次, 全市场分发
  - 调用次数 = N_trade_days, 跟 ticker 数无关 (修复限频爆 bug)

老 per-ticker 路径仍走 (force=True 或 N 小), 这里只新加 by-date 路径的覆盖.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / 'scripts' / 'pull_daily_basic.py'


@pytest.fixture
def pdb(monkeypatch, tmp_path):
    """加载 pull_daily_basic 模块, 重置全局 cache, CACHE_DIR_DB 指到 tmp."""
    spec = importlib.util.spec_from_file_location('pull_daily_basic', SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['pull_daily_basic'] = mod
    spec.loader.exec_module(mod)

    # 全局重置
    mod._BATCH_CACHE = {}
    mod._BATCH_DATE = None
    mod._BATCH_HIT_COUNT = 0
    mod._BATCH_MISS_COUNT = 0
    mod._TUSHARE_PRO = None

    # CACHE_DIR_DB → tmp, 避免污染真实数据
    monkeypatch.setattr(mod, 'CACHE_DIR_DB', tmp_path)
    yield mod


def _fake_daily_basic_for_date(trade_date: str, tickers: list[str]) -> pd.DataFrame:
    """构造某天全市场 daily_basic 返回 (Tushare 原生格式: trade_date 是 YYYYMMDD 字符串)."""
    return pd.DataFrame(
        {
            'ts_code': tickers,
            'trade_date': [trade_date] * len(tickers),
            'close': [10.0] * len(tickers),
            'pe': [15.0] * len(tickers),
            'pb': [1.5] * len(tickers),
            'ps': [2.0] * len(tickers),
            'total_mv': [1e9] * len(tickers),
        }
    )


def _fake_trade_cal(days: list[str]) -> pd.DataFrame:
    """模拟 pro.trade_cal 返回, days 全部 is_open=1."""
    return pd.DataFrame({'cal_date': days, 'is_open': [1] * len(days)})


def test_by_date_path_cold_start_calls_scale_with_days_not_tickers(pdb):
    """200 票冷启动 (无 parquet), 缺 3 天: 应只调 1 trade_cal + 3 daily_basic, 不是 200×N."""
    tickers = [f'{600000 + i}.SH' for i in range(200)]
    trade_days = ['20260520', '20260521', '20260522']

    pro = MagicMock()
    pro.trade_cal.return_value = _fake_trade_cal(trade_days)
    pro.daily_basic.side_effect = lambda trade_date: _fake_daily_basic_for_date(
        trade_date, tickers
    )
    pdb._TUSHARE_PRO = pro

    stats = pdb._update_all_by_trade_date(
        tickers,
        default_start='20260520',  # 3 天回看
        end_str='20260522',
        verbose=False,
    )

    # 关键断言: daily_basic 只被调 N_days 次, 不是 N_tickers × N_days
    assert pro.trade_cal.call_count == 1, 'trade_cal 应只调 1 次'
    assert pro.daily_basic.call_count == len(trade_days), (
        f'daily_basic 应调 {len(trade_days)} 次 (=交易日数), 实际 {pro.daily_basic.call_count}. '
        f'200 票 × 3 天 = 600 不应该出现.'
    )

    # 200 票全写盘成功, 每个 3 行
    assert stats['ok'] == 200
    assert stats['fresh'] == 0
    assert stats['error'] == 0
    assert stats['rows_added'] == 200 * 3


def test_by_date_path_incremental_skips_fresh(pdb, tmp_path):
    """有些票 parquet 已到今天 → 算 fresh, 不进 needs; 有些缺 2 天 → 拉 2 天."""
    tickers = [f'{600000 + i}.SH' for i in range(150)]
    end_day = '20260522'

    # 一半票已经更新到今天 (fresh), 另一半最后更新在 5-20 (缺 5-21 5-22)
    fresh_tickers = tickers[:50]
    stale_tickers = tickers[50:]
    for t in fresh_tickers:
        pd.DataFrame(
            {
                'trade_date': [pd.Timestamp('2026-05-22')],
                'ts_code': [t],
                'close': [10.0],
            }
        ).to_parquet(tmp_path / f'{t}.parquet', index=False)
    for t in stale_tickers:
        pd.DataFrame(
            {
                'trade_date': [pd.Timestamp('2026-05-20')],
                'ts_code': [t],
                'close': [10.0],
            }
        ).to_parquet(tmp_path / f'{t}.parquet', index=False)

    trade_days = ['20260521', '20260522']
    pro = MagicMock()
    pro.trade_cal.return_value = _fake_trade_cal(trade_days)
    pro.daily_basic.side_effect = lambda trade_date: _fake_daily_basic_for_date(
        trade_date, tickers
    )
    pdb._TUSHARE_PRO = pro

    stats = pdb._update_all_by_trade_date(
        tickers,
        default_start='20260101',
        end_str=end_day,
        verbose=False,
    )

    # fresh 50, stale 100 各写 2 天
    assert stats['fresh'] == 50
    assert stats['ok'] == 100
    assert stats['rows_added'] == 100 * 2
    # daily_basic 还是只调 2 次 (2 个交易日), 跟票数无关
    assert pro.daily_basic.call_count == 2


def test_dispatch_routes_large_to_by_date(pdb, monkeypatch):
    """update_all 顶层: N ≥ 100 且非 --force → 走 by-date, 不调 per-ticker."""
    tickers = [f'{600000 + i}.SH' for i in range(120)]

    called_per_ticker = {'flag': False}
    called_by_date = {'flag': False}

    def fake_by_date(cn, default_start, end_str, *, verbose):
        called_by_date['flag'] = True
        return {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0}

    def fake_per_ticker(cn, default_start, end_str, *, workers, force, verbose):
        called_per_ticker['flag'] = True
        return {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0}

    monkeypatch.setattr(pdb, '_update_all_by_trade_date', fake_by_date)
    monkeypatch.setattr(pdb, '_update_all_per_ticker', fake_per_ticker)

    pdb.update_all(tickers)

    assert called_by_date['flag'], 'N=120 应走 by-date 路径'
    assert not called_per_ticker['flag']


def test_dispatch_force_falls_back_to_per_ticker(pdb, monkeypatch):
    """force=True 永远走 per-ticker (要多年历史, 不是简单增量)."""
    tickers = [f'{600000 + i}.SH' for i in range(500)]  # 大批量但 force

    flags = {'by_date': False, 'per_ticker': False}

    monkeypatch.setattr(
        pdb,
        '_update_all_by_trade_date',
        lambda *a, **kw: flags.update(by_date=True)
        or {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0},
    )
    monkeypatch.setattr(
        pdb,
        '_update_all_per_ticker',
        lambda *a, **kw: flags.update(per_ticker=True)
        or {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0},
    )

    pdb.update_all(tickers, force=True)

    assert flags['per_ticker'], 'force=True 应强走 per-ticker'
    assert not flags['by_date']


def test_dispatch_small_batch_routes_to_per_ticker(pdb, monkeypatch):
    """N < ROUTE_BY_DATE_THRESHOLD (默认 100): 走 per-ticker, 即使非 force."""
    tickers = [f'{600000 + i}.SH' for i in range(50)]

    flags = {'by_date': False, 'per_ticker': False}
    monkeypatch.setattr(
        pdb,
        '_update_all_by_trade_date',
        lambda *a, **kw: flags.update(by_date=True)
        or {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0},
    )
    monkeypatch.setattr(
        pdb,
        '_update_all_per_ticker',
        lambda *a, **kw: flags.update(per_ticker=True)
        or {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0},
    )

    pdb.update_all(tickers)

    assert flags['per_ticker']
    assert not flags['by_date']


def test_by_date_merges_with_existing_parquet(pdb, tmp_path):
    """已存在的 parquet 应跟新拉的数据合并去重, 不覆盖."""
    tickers = [f'{600000 + i}.SH' for i in range(105)]
    target = tickers[0]

    # 已有 5-18 5-19 5-20 三天历史
    existing = pd.DataFrame(
        {
            'trade_date': pd.to_datetime(['2026-05-18', '2026-05-19', '2026-05-20']),
            'ts_code': [target] * 3,
            'close': [9.0, 9.5, 9.8],
        }
    )
    existing.to_parquet(tmp_path / f'{target}.parquet', index=False)

    # 新拉 5-21 5-22
    trade_days = ['20260521', '20260522']
    pro = MagicMock()
    pro.trade_cal.return_value = _fake_trade_cal(trade_days)
    pro.daily_basic.side_effect = lambda trade_date: _fake_daily_basic_for_date(
        trade_date, tickers
    )
    pdb._TUSHARE_PRO = pro

    pdb._update_all_by_trade_date(
        tickers, default_start='20260101', end_str='20260522', verbose=False
    )

    merged = pd.read_parquet(tmp_path / f'{target}.parquet')
    assert len(merged) == 5, '应该是 3 老 + 2 新 = 5 行'
    dates = sorted(pd.to_datetime(merged['trade_date']).dt.strftime('%Y-%m-%d').tolist())
    assert dates == [
        '2026-05-18',
        '2026-05-19',
        '2026-05-20',
        '2026-05-21',
        '2026-05-22',
    ]
