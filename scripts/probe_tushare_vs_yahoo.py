"""探针：比对 Tushare 本地缓存 vs Yahoo Finance 的同一只票价格/收益率误差。

用途：
    诊断「价格对不上」到底是无害的复权口径差，还是会污染回测的逐日不一致。
    回测吃的是收益率序列不是价格绝对值，所以同时出两张对比：
      1) raw close 对比 —— 两边都是不复权官方收盘价，应几乎相等；
         这里有偏差 = 数据商对真实成交价就有分歧。
      2) 复权收益率对比 —— Tushare qfq close 的 pct_change vs
         Yahoo 'Adj Close' 的 pct_change；这是回测真正用的序列，
         除权除息/拆分处理不一致会在个别日期暴露成 return 跳变。

依赖：
    utils.data.load_daily（走 .env 的 TUSHARE_TOKEN，没 token 直接报错退出）
    yfinance（已装在仓库根 .venv，Python 3.14）

用法：
    /Users/helm/Documents/Code/sh_quant/.venv/bin/python \\
        scripts/probe_tushare_vs_yahoo.py                      # 默认 301205.SZ / 2025
    .../python scripts/probe_tushare_vs_yahoo.py --code 300750.SZ --year 2024

输出位置：
    控制台：匹配天数 + raw/return 误差统计 + return 偏差最大的 10 天
    文件：  outputs/probe_<code>_<year>_tushare_vs_yahoo.csv（逐日明细）
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import ROOT_DIR
from utils.data import load_daily


def _yahoo_daily(code: str, start: str, end: str) -> pd.DataFrame:
    # auto_adjust=False 才能同时拿到不复权 Close 和后复权 Adj Close
    raw = yf.Ticker(code).history(start=start, end=end, auto_adjust=False)
    if raw is None or raw.empty:
        raise SystemExit(f'Yahoo 返回空数据: {code} [{start}, {end}] —— 检查代码/网络')

    idx = raw.index
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    out = pd.DataFrame(
        {
            'yf_raw_close': raw['Close'].to_numpy(),
            'yf_adj_close': raw['Adj Close'].to_numpy(),
        },
        index=pd.DatetimeIndex(idx).normalize(),
    )
    out.index.name = 'date'
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description='Tushare vs Yahoo 价格/收益率误差探针')
    parser.add_argument('--code', default='301205.SZ', help='ts_code，如 301205.SZ')
    parser.add_argument('--year', type=int, default=2025, help='对比的自然年')
    args = parser.parse_args()

    code, year = args.code, args.year
    start, end = f'{year}-01-01', f'{year}-12-31'

    ts_raw = load_daily(code, start, end, adj=None).set_index('trade_date')
    ts_qfq = load_daily(code, start, end, adj='qfq').set_index('trade_date')
    # yfinance 的 end 是开区间，+1 天才包含 12-31
    yf_df = _yahoo_daily(code, start, f'{year + 1}-01-01')

    cmp = pd.DataFrame(
        {
            'ts_raw_close': ts_raw['close'],
            'yf_raw_close': yf_df['yf_raw_close'],
            'ts_qfq_ret': ts_qfq['close'].pct_change(),
            'yf_adj_ret': yf_df['yf_adj_close'].pct_change(),
        }
    ).dropna(subset=['ts_raw_close', 'yf_raw_close'])

    cmp['raw_diff_pct'] = (cmp['ts_raw_close'] - cmp['yf_raw_close']) / cmp['yf_raw_close'] * 100
    cmp['ret_diff_bps'] = (cmp['ts_qfq_ret'] - cmp['yf_adj_ret']) * 1e4

    out_path = ROOT_DIR / 'outputs' / f'probe_{code.replace(".", "")}_{year}_tushare_vs_yahoo.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmp.round(6).to_csv(out_path)

    raw_abs = cmp['raw_diff_pct'].abs()
    ret_abs = cmp['ret_diff_bps'].abs()

    print(f'\n=== {code}  {start} ~ {end} ===')
    print(f'匹配交易日: {len(cmp)}  '
          f'(Tushare {len(ts_raw)} / Yahoo {len(yf_df)})')

    print('\n--- raw close 误差 (Tushare 不复权 vs Yahoo Close, 单位 %) ---')
    print(f'  均值 {raw_abs.mean():.4f}  中位 {raw_abs.median():.4f}  '
          f'最大 {raw_abs.max():.4f}  >0.05% 的天数 {(raw_abs > 0.05).sum()}')

    print('\n--- 复权收益率误差 (Tushare qfq vs Yahoo AdjClose, 单位 bps) ---')
    print(f'  均值 {ret_abs.mean():.3f}  中位 {ret_abs.median():.3f}  '
          f'最大 {ret_abs.max():.3f}  >5bps 的天数 {(ret_abs > 5).sum()}')

    worst = cmp.reindex(ret_abs.sort_values(ascending=False).index).head(10)
    print('\n--- 收益率偏差最大的 10 天（重点看这里是不是除权日）---')
    cols = ['ts_raw_close', 'yf_raw_close', 'raw_diff_pct',
            'ts_qfq_ret', 'yf_adj_ret', 'ret_diff_bps']
    with pd.option_context('display.float_format', lambda v: f'{v:.4f}'):
        print(worst[cols].to_string())

    print(f'\nCSV 已写入: {out_path}')
    print('\n判读：raw 误差恒定小 & return 误差全程接近 0 → 纯口径差，回测无害。')
    print('      return 在个别日期冒大偏差（常是除权除息日）→ adj_factor/'
          '公司行为数据有 bug，必须先修再回测。')


if __name__ == '__main__':
    main()
