"""utils/data.py 的关键不变量测试。

测试覆盖：
    - 缓存命中不调用 tushare
    - qfq 末日 close 不变（基准是 end）
    - qfq 历史日按比例缩放
    - hfq 首日 close 不变（基准是 start）
    - adj=None 返回原始价
    - 非法 adj 抛 ValueError
    - 超出缓存范围抛 ValueError
    - 子区间过滤行数正确
    - tushare 返回空数据抛 ValueError
"""

from __future__ import annotations

import pandas as pd
import pytest

from utils import data as data_mod


# ---------- 缓存命中（不调 tushare）----------

def test_load_daily_cache_hit_does_not_call_tushare(
    tmp_stocks_dir, fake_daily_df, fake_adj_df, mocker,
):
    """缓存文件已存在时，应该直接读 parquet，不该调用 _get_tushare_pro。"""
    # 预先把 parquet 写到 tmp_stocks_dir
    df = fake_daily_df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    adj = fake_adj_df.copy()
    adj['trade_date'] = pd.to_datetime(adj['trade_date'], format='%Y%m%d')
    df = df.merge(adj[['trade_date', 'adj_factor']], on='trade_date', how='left')
    df.to_parquet(tmp_stocks_dir / '600519.SH.parquet', index=False)

    spy = mocker.patch('utils.data._get_tushare_pro')
    result = data_mod.load_daily('600519.SH', '20240102', '20240104', adj=None)

    spy.assert_not_called()
    assert len(result) == 3


# ---------- 复权数学（核心）----------

def test_load_daily_qfq_last_day_unchanged(tmp_stocks_dir, fake_pro):
    """qfq 基准是 end 当日的 adj_factor，所以 end 当日的 close 应该等于 raw。"""
    df = data_mod.load_daily('600519.SH', '20240102', '20240104', adj='qfq')
    assert df['close'].iloc[-1] == pytest.approx(1530.0)


def test_load_daily_qfq_historical_scaled_down(tmp_stocks_dir, fake_pro):
    """qfq 历史日：raw 1710，adj_factor=1.0，base=1.125 → 1710 * 1.0 / 1.125 ≈ 1520。"""
    df = data_mod.load_daily('600519.SH', '20240102', '20240104', adj='qfq')
    expected = 1710.0 * 1.0 / 1.125
    assert df['close'].iloc[0] == pytest.approx(expected)


def test_load_daily_hfq_first_day_unchanged(tmp_stocks_dir, fake_pro):
    """hfq 基准是 start 当日的 adj_factor，所以 start 当日的 close 应该等于 raw。"""
    df = data_mod.load_daily('600519.SH', '20240102', '20240104', adj='hfq')
    assert df['close'].iloc[0] == pytest.approx(1710.0)


def test_load_daily_none_returns_raw(tmp_stocks_dir, fake_pro):
    """adj=None 应该返回原始价，不做任何缩放。"""
    df = data_mod.load_daily('600519.SH', '20240102', '20240104', adj=None)
    assert df['close'].tolist() == [1710.0, 1720.0, 1530.0]


# ---------- 边界 / 错误 ----------

def test_load_daily_invalid_adj_raises(tmp_stocks_dir, fake_pro):
    """传入未知 adj 值应该抛 ValueError。"""
    with pytest.raises(ValueError, match='adj'):
        data_mod.load_daily('600519.SH', '20240102', '20240104', adj='bogus')


def test_load_daily_out_of_range_triggers_refetch(tmp_stocks_dir, fake_pro):
    """缓存范围不够时，应触发再次拉取（auto-补拉），不再 raise。"""
    # 第 1 次：填缓存（3 行 2024 数据）
    data_mod.load_daily('600519.SH', '20240102', '20240104', adj=None)
    fake_pro.daily.reset_mock()

    # 第 2 次：请求超过缓存的右边界 → 应触发 refetch
    data_mod.load_daily('600519.SH', '20240102', '20251231', adj=None)
    fake_pro.daily.assert_called()


def test_load_daily_no_data_in_range_raises(tmp_stocks_dir, fake_pro):
    """补拉之后请求区间内仍然没有数据 → 抛 ValueError。"""
    with pytest.raises(ValueError, match='没有交易数据'):
        data_mod.load_daily('600519.SH', '20300101', '20310101', adj=None)


def test_load_daily_date_range_filter(tmp_stocks_dir, fake_pro):
    """指定子区间应只返回该区间的行。"""
    df = data_mod.load_daily('600519.SH', '20240103', '20240103', adj=None)
    assert len(df) == 1
    assert df['close'].iloc[0] == 1720.0


def test_load_daily_empty_tushare_raises(tmp_stocks_dir, mocker):
    """tushare daily 返回空 DataFrame 时应抛 ValueError，不能默默存空 parquet。"""
    pro = mocker.MagicMock()
    pro.daily.return_value = pd.DataFrame()
    mocker.patch('utils.data._get_tushare_pro', return_value=pro)
    with pytest.raises(ValueError, match='返回空'):
        data_mod.load_daily('FAKE.SH', '20240102', '20240104', adj=None)
