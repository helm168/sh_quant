"""探查 data_cache/stocks/*.US.parquet 里 close 到底是什么口径。

背景：审计发现 US 个股 adj_factor 全是 1.0，跟 A 股的「raw close + adj_factor」契约
不一致。可能是 (a) 已 split-adjusted、(b) 已 fully-adjusted、(c) 还是 raw。
本脚本对照 yfinance 三种口径快速定位真相。

依赖：yfinance（应在 venv 已装）。

用法：
  source .venv/bin/activate
  python /Users/helm/Documents/Code/sh_quant/.claude/worktrees/intelligent-shirley-c0e20e/scripts/probe_us_adj_factor.py

输出：每个 ticker × 几个有 corporate action 的关键日期，列 cache vs yfinance 三口径。
判定逻辑：
  - cache 接近 yf raw（auto_adjust=False, 'Close' 列）        → 真 raw OHLCV ✓ 契约一致，但 adj_factor=1 漏拉
  - cache 接近 yf split-adj（auto_adjust=False, splits only）→ 只拆股调整未除息 ✗
  - cache 接近 yf fully-adj（auto_adjust=True）              → 完全前复权 ✗ 契约违反
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    sys.exit('yfinance 未装。pip install yfinance')


DATA_ROOT = Path('/Users/helm/Documents/Code/sh_quant/data_cache/stocks')

# 选有显著 corporate action 的票：AAPL(2020-08-31 4:1 split), TSLA(2020-08-31 5:1 +
# 2022-08-25 3:1), NVDA(2021-07-20 4:1 + 2024-06-10 10:1)
PROBES = [
    ('AAPL.US', 'AAPL', ['2014-06-08', '2015-01-02', '2020-08-31', '2020-09-01', '2025-06-30']),
    ('TSLA.US', 'TSLA', ['2020-08-30', '2020-09-01', '2022-08-24', '2022-08-26', '2025-06-30']),
    ('NVDA.US', 'NVDA', ['2021-07-19', '2021-07-21', '2024-06-09', '2024-06-11', '2025-06-30']),
]


def fetch_yf(symbol: str, start: str, end: str) -> dict[str, pd.Series]:
    """拉 yfinance 三种口径的 Close。"""
    # 原始（含拆股不调整的"as-reported"）：actions=False, auto_adjust=False
    # 但 yfinance 默认 auto_adjust=False 返回的 Close 已 split-adjusted。
    # 真正 raw（split-unadjusted）：用 actions=True 拿到 splits 自己还原。
    df_split = yf.download(
        symbol, start=start, end=end,
        auto_adjust=False, actions=False, progress=False,
    )
    df_full = yf.download(
        symbol, start=start, end=end,
        auto_adjust=True, actions=False, progress=False,
    )
    # 同时拿 Adj Close 列（在 auto_adjust=False 时即为 full-adj，含 dividends）
    adj_close = df_split['Adj Close'] if 'Adj Close' in df_split.columns else None
    return {
        'yf_split_only': df_split['Close'].squeeze() if not df_split.empty else pd.Series(dtype=float),
        'yf_full_adj': df_full['Close'].squeeze() if not df_full.empty else pd.Series(dtype=float),
        'yf_adj_close': adj_close.squeeze() if adj_close is not None and not adj_close.empty else pd.Series(dtype=float),
    }


def main() -> int:
    print('US adj_factor 口径探查')
    print('=' * 100)

    for ts_code, yf_sym, dates in PROBES:
        fp = DATA_ROOT / f'{ts_code}.parquet'
        if not fp.exists():
            print(f'[skip] {ts_code} 缓存不存在')
            continue

        df = pd.read_parquet(fp).sort_values('trade_date')
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
        df = df.set_index('trade_date')

        start = min(dates)
        end = (pd.Timestamp(max(dates)) + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
        yfd = fetch_yf(yf_sym, start, end)

        print(f'\n### {ts_code}  (cache adj_factor unique = {df.adj_factor.nunique()})')
        print(f'  {"日期":<12} {"cache_close":>12} {"yf_split_only":>14} {"yf_full_adj":>13} {"yf_adj_close":>14}   判定')
        for d in dates:
            ts = pd.Timestamp(d)
            cache_v = df.close.loc[ts] if ts in df.index else None
            yf_split = yfd['yf_split_only'].loc[ts] if ts in yfd['yf_split_only'].index else None
            yf_full = yfd['yf_full_adj'].loc[ts] if ts in yfd['yf_full_adj'].index else None
            yf_adj = yfd['yf_adj_close'].loc[ts] if ts in yfd['yf_adj_close'].index else None

            verdict = ''
            if cache_v is not None:
                cands = [
                    ('split_only', yf_split),
                    ('full_adj', yf_full),
                ]
                hits = []
                for label, v in cands:
                    if v is not None and abs(cache_v - v) / cache_v < 0.005:
                        hits.append(label)
                verdict = ', '.join(hits) if hits else 'NO MATCH'

            def fmt(x):
                return f'{x:>12.4f}' if isinstance(x, (int, float)) and x == x else f'{"--":>12}'

            print(f'  {d:<12} {fmt(cache_v)} {fmt(yf_split):>14} {fmt(yf_full):>13} {fmt(yf_adj):>14}   {verdict}')

    print()
    print('=' * 100)
    print('结论解读：')
    print('  - cache 全部命中 split_only  → cache 是 split-adj close（漏除息调整）')
    print('  - cache 全部命中 full_adj    → cache 是完全前复权（违反 raw+adj_factor 契约）')
    print('  - 二者都命中（差异极小）     → 该票分红影响极小，无法靠这些日期分辨')
    print('  - 拆股日前后能区分二者：拆股日 cache 跟 split_only/full_adj 一致 vs raw 不一致')
    return 0


if __name__ == '__main__':
    sys.exit(main())
