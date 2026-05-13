"""共享 fixture——fake tushare 返回 + tmp_path 路径替换 + cache 清理。"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def fake_daily_df() -> pd.DataFrame:
    """模拟 pro.daily() 返回 3 天的茅台数据。第 3 天除权（close 大跌但实际是分红/送转）。"""
    return pd.DataFrame(
        {
            'ts_code': ['600519.SH'] * 3,
            'trade_date': ['20240102', '20240103', '20240104'],
            'open': [1700.0, 1710.0, 1530.0],
            'high': [1720.0, 1730.0, 1545.0],
            'low': [1690.0, 1700.0, 1515.0],
            'close': [1710.0, 1720.0, 1530.0],
            'pre_close': [1690.0, 1710.0, 1720.0],
            'change': [20.0, 10.0, -190.0],
            'pct_chg': [1.18, 0.58, -11.05],
            'vol': [10000.0, 12000.0, 9000.0],
            'amount': [17e6, 20e6, 14e6],
        }
    )


@pytest.fixture
def fake_adj_df() -> pd.DataFrame:
    """模拟 pro.adj_factor() 返回。第 3 天 adj_factor 从 1.0 跳到 1.125（除权事件）。"""
    return pd.DataFrame(
        {
            'ts_code': ['600519.SH'] * 3,
            'trade_date': ['20240102', '20240103', '20240104'],
            'adj_factor': [1.0, 1.0, 1.125],
        }
    )


@pytest.fixture
def fake_pro(fake_daily_df, fake_adj_df, mocker):
    """替换 _get_tushare_pro，返回一个 daily/adj_factor 都被 mock 的假 pro 对象。"""
    pro = mocker.MagicMock()
    pro.daily.return_value = fake_daily_df
    pro.adj_factor.return_value = fake_adj_df
    mocker.patch('utils.data._get_tushare_pro', return_value=pro)
    return pro


@pytest.fixture(autouse=True)
def reset_pro_cache():
    """每个测试开头/结尾清 _get_tushare_pro 的 @cache，避免测试间互相污染。"""
    from utils.data import _get_tushare_pro

    _get_tushare_pro.cache_clear()
    yield
    _get_tushare_pro.cache_clear()


@pytest.fixture
def tmp_stocks_dir(tmp_path, monkeypatch):
    """把 STOCKS_DIR 临时指到 tmp_path，避免污染真实 data_cache/stocks/。"""
    monkeypatch.setattr('utils.data.STOCKS_DIR', tmp_path)
    return tmp_path
