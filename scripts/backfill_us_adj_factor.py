"""一次性回填 data_cache/stocks/*.US.parquet 的 adj_factor 列。

背景：
  审计发现 US 缓存所有 adj_factor 全是 1.0（FMP 路径返回的 adjClose==close，
  实际只 split-adjusted，分红没复权）。导致 utils/data.py:load_daily(adj='qfq')
  对美股 = 一个 no-op（只是 split-adj close），跨除息日有假跳空 + 长期 total
  return 低估分红收益（13 年 AAPL 累积约 13%）。

方案：
  从 yfinance 拿 raw Close + Adj Close（auto_adjust=False），计算
    adj_factor[t] = Adj Close[t] / Close[t]
  写入 parquet 的 adj_factor 列覆盖。

  - yfinance 的 Close = split-adj close（跟我们 cache 一致 ✓）
  - yfinance 的 Adj Close = split + dividend full-adj close
  - 最新日 adj_factor 应 ≈ 1.0（除非数据有差异）
  - utils/data.py 的 qfq 公式 `close * adj_factor / adj_factor.iloc[-1]` 这样就对了

用法：
  python /Users/helm/Documents/Code/sh_quant/.claude/worktrees/intelligent-shirley-c0e20e/scripts/backfill_us_adj_factor.py        # dry-run, 抽 3 个验证
  python ... --apply --limit 20         # 跑 20 个测试
  python ... --apply                    # 全量

依赖：yfinance（已在 venv）。
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    sys.exit('yfinance 未装。pip install yfinance')


STOCKS = Path('/Users/helm/Documents/Code/sh_quant/data_cache/stocks')


def backfill_one(fp: Path) -> tuple[str, str, dict]:
    """处理单个 *.US.parquet。返回 (ts_code, status, info)。"""
    ts_code = fp.stem
    yf_sym = ts_code[:-3]  # AAPL.US -> AAPL

    df = pd.read_parquet(fp)
    if df.empty or 'trade_date' not in df.columns:
        return ts_code, 'skip_empty', {}

    df = df.sort_values('trade_date').reset_index(drop=True)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

    start = df['trade_date'].min().strftime('%Y-%m-%d')
    end = (df['trade_date'].max() + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        yfd = yf.download(
            yf_sym, start=start, end=end,
            auto_adjust=False, actions=False, progress=False, threads=False,
        )
    except Exception as e:  # noqa: BLE001
        return ts_code, 'yf_fail', {'err': str(e)[:80]}

    if yfd.empty or 'Adj Close' not in yfd.columns:
        return ts_code, 'no_adj_close', {}

    # yfinance 返回 MultiIndex 列 (Price, Ticker) — squeeze 掉
    adj = yfd['Adj Close'].squeeze()
    raw = yfd['Close'].squeeze()
    ratio = (adj / raw).dropna()
    ratio.index = pd.to_datetime(ratio.index).normalize()
    ratio.name = 'adj_factor'

    # 用 trade_date map 上 ratio；找不到的保留 1.0
    df = df.drop(columns=['adj_factor']) if 'adj_factor' in df.columns else df
    df = df.merge(
        ratio.reset_index().rename(columns={'Date': 'trade_date', 'index': 'trade_date'}),
        on='trade_date', how='left',
    )
    n_missing = df['adj_factor'].isna().sum()
    df['adj_factor'] = df['adj_factor'].fillna(1.0).astype(float)

    # 把 adj_factor 列放回原 schema 末尾位置（与其他文件一致即可，简单做：保持当前顺序）
    df.to_parquet(fp, index=False, compression='snappy')

    nonzero_changes = (df['adj_factor'].round(6) != 1.0).sum()
    return ts_code, 'ok', {
        'rows': len(df),
        'adj_changes': int(nonzero_changes),
        'missing': int(n_missing),
        'last_af': float(df['adj_factor'].iloc[-1]),
        'min_af': float(df['adj_factor'].min()),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='实际写盘')
    ap.add_argument('--limit', type=int, default=None, help='只处理前 N 个文件（验证用）')
    ap.add_argument('--workers', type=int, default=8, help='并行 worker 数')
    args = ap.parse_args()

    files = sorted(STOCKS.glob('*.US.parquet'))
    if args.limit:
        files = files[: args.limit]

    if not args.apply:
        # Dry run: 抽 3 个已知分红股
        probes = [STOCKS / f'{c}.parquet' for c in ['AAPL.US', 'KO.US', 'JNJ.US']]
        probes = [p for p in probes if p.exists()]
        print(f'Dry-run (3 sample) — 全量 {len(files)} 文件')
        for p in probes:
            ts_code, status, info = backfill_one(p)
            print(f'  {ts_code:<10} {status:<12} {info}')
        return 0

    print(f'Backfill US adj_factor: {len(files)} 文件，{args.workers} workers')
    t0 = time.time()

    n_ok = n_fail = n_changes_total = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(backfill_one, fp): fp for fp in files}
        for i, fut in enumerate(as_completed(futures), 1):
            ts_code, status, info = fut.result()
            if status == 'ok':
                n_ok += 1
                n_changes_total += info.get('adj_changes', 0)
            else:
                n_fail += 1
                print(f'  [{i}] FAIL {ts_code:<10} {status}  {info}')
            if i % 100 == 0:
                print(f'  …{i}/{len(files)}  ok={n_ok} fail={n_fail}  ({time.time()-t0:.0f}s)')

    print()
    print(f'完成: {n_ok} 成功 / {n_fail} 失败 / {len(files)} 总计   耗时 {time.time()-t0:.0f}s')
    print(f'其中 adj_factor != 1 的行总数: {n_changes_total}')
    return 0 if n_fail < len(files) * 0.05 else 1


if __name__ == '__main__':
    sys.exit(main())
