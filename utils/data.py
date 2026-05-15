"""数据层：行情拉取 + 本地 parquet 缓存。

第 1 月 - 周 1 待实现：
    - load_daily(ts_code, start, end, adj='qfq') -> pd.DataFrame
    - 命中缓存读本地，否则拉接口并落盘
    - trade_date 排序、列名标准化
"""

import os
from functools import cache
from typing import Literal

import pandas as pd
import tushare as ts
from dotenv import load_dotenv

from config import DATA_DIR, ROOT_DIR, STUDY_RANGE_END, STUDY_RANGE_START

from .themes import get_codes

STOCKS_DIR = DATA_DIR / 'stocks'


@cache
def _get_tushare_pro():
    load_dotenv(ROOT_DIR / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        raise RuntimeError('TUSHARE_TOKEN not found in .env file')
    ts.set_token(token)
    return ts.pro_api()


def _fetch_and_cache_from_tushare(ts_code: str):
    pro = _get_tushare_pro()

    df = pro.daily(ts_code=ts_code, start_date=STUDY_RANGE_START, end_date=STUDY_RANGE_END)
    if df is None or df.empty:
        raise ValueError(f'tushare 返回空数据: {ts_code} (请稍后再试)')

    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date').reset_index(drop=True)

    adj = pro.adj_factor(ts_code=ts_code, start_date=STUDY_RANGE_START, end_date=STUDY_RANGE_END)
    if adj is not None and not adj.empty:
        adj = adj[['trade_date', 'adj_factor']].drop_duplicates('trade_date')
        df = df.merge(adj, on='trade_date', how='left')
        df['adj_factor'] = df['adj_factor'].ffill().bfill().fillna(1.0)
    else:
        df['adj_factor'] = 1.0

    out = STOCKS_DIR / f'{ts_code}.parquet'
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)

    return df


def load_daily(ts_code: str, start: str, end: str, adj: str | None = 'qfq') -> pd.DataFrame:
    stock_file = STOCKS_DIR / f'{ts_code}.parquet'

    if stock_file.exists():
        df = pd.read_parquet(stock_file)
    else:
        df = _fetch_and_cache_from_tushare(ts_code)

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)

    start_at = pd.to_datetime(start)
    end_at = pd.to_datetime(end)

    if start_at < pd.to_datetime(STUDY_RANGE_START) or end_at > df['trade_date'].max():
        raise ValueError(
            f'{ts_code} 缓存范围 [{df["trade_date"].min().date()}, '
            f'{df["trade_date"].max().date()}]，请求 [{start}, {end}] 超出。'
        )

    df = df[(df['trade_date'] >= start_at) & (df['trade_date'] <= end_at)].copy()
    if df.empty:
        return df

    adj = adj.lower() if isinstance(adj, str) else adj
    if adj in (None, 'none'):
        return df

    if adj == 'qfq':
        base_factor = df['adj_factor'].iloc[-1]
    elif adj == 'hfq':
        base_factor = df['adj_factor'].iloc[0]
    else:
        raise ValueError(f"adj must be one of 'qfq', 'hfq', 'none', or None, got {adj}")

    ratio = df['adj_factor'] / base_factor
    for col in ['open', 'high', 'low', 'close', 'pre_close', 'change']:
        df[col] = df[col] * ratio

    return df


WeightingMethod = Literal['equal', 'nested_equal']
RebalanceFreq = Literal['M', 'Q', 'never']


def get_theme_index_by(
    theme_id: str,
    substack: str | None = None,
    weighting: WeightingMethod = 'equal',
    rebalance: RebalanceFreq = 'M',
    start: str = STUDY_RANGE_START,
    end: str = STUDY_RANGE_END,
) -> pd.DataFrame:
    ts_codes = get_codes(theme_id, subtrack=substack)

    df_price = pd.DataFrame()

    for ts_code in ts_codes:
        df = load_daily(ts_code, start, end, adj='qfq')
        if df.empty:
            continue
        df = df.set_index('trade_date').sort_index()
        df_price[ts_code] = df['close']

    returns = df_price.pct_change()
    index_ret = returns.mean(axis=1)
    theme_index = (1 + index_ret).cumprod()

    return pd.DataFrame(
        {
            'theme_index': theme_index,
            'index_ret': index_ret,
        }
    )

    raise NotImplementedError
