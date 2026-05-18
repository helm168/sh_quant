"""
验证 sh_quant parquet 里 Futu 拉的 adj_factor 是不是对的.

方法
────
Yahoo Finance 同样提供 raw Close + Adj Close (历史复权), 比值是 forward 风格
的归一化复权因子 (历史 < 1, 最新 = 1). 数学上等价于 Tushare-style:
  Yahoo: adj_factor_yahoo = Adj Close / Close  (forward, latest=1)
  Tushare: adj_factor / latest_adj_factor       (forward 归一化, 等价于 Yahoo)

如果 Futu 拉的 adj_factor 语义对, 那 (futu_adj / futu_adj.tail(1)) 跟
Yahoo_adj 应该逐行匹配 (差异 < 0.1% 之内, 排除浮点误差和数据源 freshness 差).

如果差异大, 那说明 Futu backward_adj_factorB 不是我假设的语义, 需要换字段
(例如算 from forward_adj_factorB, 或者 reconstruct from per_cash_div +
per_share_div_ratio + close_prev).

用法
────
    source .venv/bin/activate
    pip install yfinance
    python scripts/validate_hk_adj_factor.py 00700.HK
    python scripts/validate_hk_adj_factor.py 00005.HK 00941.HK  # 多只
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    sys.exit('yfinance 没装. 跑: pip install yfinance')


DATA_DIR = Path.home() / '.market_data' / 'stocks'


def ts_code_to_yfinance(ts_code: str) -> str:
    """
    `00700.HK` (sh_quant 5 位) → `0700.HK` (yfinance 4 位).

    yfinance 港股 ticker 标准是 4 位补零 (腾讯 0700, 中芯 0981, 阿里 9988).
    sh_quant DATA_SCHEMA §2 用 Tushare 风格 5 位. 转换: lstrip 0 + zfill(4).
    """
    code, suffix = ts_code.split('.')
    return f'{code.lstrip("0").zfill(4)}.{suffix}'


def load_sh_quant(ts_code: str) -> pd.DataFrame:
    p = DATA_DIR / f'{ts_code}.parquet'
    if not p.exists():
        raise FileNotFoundError(f'{p} 不存在. 先跑 pull_hk_futu.py {ts_code}')
    df = pd.read_parquet(p)
    df = df[['trade_date', 'close', 'adj_factor']].copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)
    return df


def load_yahoo(ts_code: str, since: str, until: str) -> pd.DataFrame:
    yf_sym = ts_code_to_yfinance(ts_code)
    print(f'  [yahoo] fetching {yf_sym} {since} → {until}')
    df = yf.Ticker(yf_sym).history(
        start=since,
        end=until,
        auto_adjust=False,  # 关键: 保留 raw Close + Adj Close 两列
    )
    if len(df) == 0:
        raise RuntimeError(f'yfinance 返空数据: {yf_sym}')
    out = pd.DataFrame()
    out['trade_date'] = pd.to_datetime(df.index.date)
    out['close_yahoo'] = df['Close'].values
    out['adj_close_yahoo'] = df['Adj Close'].values
    out['adj_factor_yahoo'] = out['adj_close_yahoo'] / out['close_yahoo']  # forward, latest=1
    return out.sort_values('trade_date').reset_index(drop=True)


def compare(ts_code: str) -> None:
    print(f'\n{"=" * 70}')
    print(f'  {ts_code}')
    print('=' * 70)

    sq = load_sh_quant(ts_code)
    since = sq['trade_date'].min().strftime('%Y-%m-%d')
    # yfinance end 是 exclusive, 多拉一天保证覆盖
    until = (sq['trade_date'].max() + pd.Timedelta(days=2)).strftime('%Y-%m-%d')

    yh = load_yahoo(ts_code, since, until)

    # 归一化 sh_quant adj_factor 到 forward 风格 (latest=1, 历史 < 1)
    latest_factor = sq['adj_factor'].iloc[-1]
    sq['adj_factor_normalized'] = sq['adj_factor'] / latest_factor

    # Merge on trade_date
    merged = sq.merge(yh, on='trade_date', how='inner')
    if len(merged) == 0:
        print(f'  ⚠ 无重叠日期. sh_quant: {sq["trade_date"].min()}..{sq["trade_date"].max()}')
        print(f'    yahoo: {yh["trade_date"].min()}..{yh["trade_date"].max()}')
        return

    # 算差异
    merged['diff'] = merged['adj_factor_normalized'] - merged['adj_factor_yahoo']
    merged['diff_pct'] = 100 * merged['diff'] / merged['adj_factor_yahoo']

    # 关注: 除权日跳变点
    merged['adj_jump_sq'] = merged['adj_factor_normalized'].diff().abs()
    merged['adj_jump_yh'] = merged['adj_factor_yahoo'].diff().abs()

    # 打印 summary
    print(f'\n  匹配行数: {len(merged)}')
    print(f'  归一化 adj_factor 平均偏差: {merged["diff_pct"].abs().mean():.3f}%')
    print(f'  最大偏差: {merged["diff_pct"].abs().max():.3f}%')

    # 看跳变 (除权日)
    jumps_sq = merged[merged['adj_jump_sq'] > 0.001]
    jumps_yh = merged[merged['adj_jump_yh'] > 0.001]
    print(f'\n  sh_quant (Futu) 检测到的除权日: {len(jumps_sq)}')
    print(f'  yahoo 检测到的除权日:           {len(jumps_yh)}')

    if len(jumps_sq) > 0 or len(jumps_yh) > 0:
        print('\n  ── 除权日对比 ──')
        cmp = merged[(merged['adj_jump_sq'] > 0.001) | (merged['adj_jump_yh'] > 0.001)][
            ['trade_date', 'close', 'adj_factor_normalized', 'adj_factor_yahoo', 'diff_pct']
        ]
        print(cmp.to_string(index=False))

    # 打印最近 5 行做 sanity check
    print('\n  ── 最近 5 行 ──')
    tail = merged[
        [
            'trade_date',
            'close',
            'close_yahoo',
            'adj_factor_normalized',
            'adj_factor_yahoo',
            'diff_pct',
        ]
    ].tail(5)
    print(tail.to_string(index=False))

    # 结论
    print()
    if merged['diff_pct'].abs().max() < 1.0:
        print(f'  ✓ {ts_code}: Futu adj_factor 语义跟 Yahoo 一致 (最大偏差 < 1%)')
    elif merged['diff_pct'].abs().max() < 5.0:
        print(f'  △ {ts_code}: 有偏差但 < 5%, 可能是数据 freshness 差或浮点误差')
    else:
        print(f'  ✗ {ts_code}: 偏差 >= 5%, Futu 字段语义不对, 看跳变日哪个数据源对')


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        print('\nusage: python scripts/validate_hk_adj_factor.py <ts_code> [<ts_code> ...]')
        sys.exit(1)

    tickers = sys.argv[1:]
    for ts_code in tickers:
        try:
            compare(ts_code)
        except Exception as e:
            print(f'\n  ✗ {ts_code}: failed — {type(e).__name__}: {e}')


if __name__ == '__main__':
    main()
