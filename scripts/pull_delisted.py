"""一次性补齐 A 股退市股票的历史日线 + 元数据，解 survivorship bias。

为什么必须做这个
─────────────────
SH_Quant 现状的 681 只成分股都是"今天还活着的"。2015-2026 这十年里，
A 股累计退市约 500+ 只股票（不进 themes.yaml，所以从未被 pull）。如果只
用现有数据回测：

  - 跑出来的策略夏普会偏高 10-30%（你不知道哪些股票"已经死了"）
  - 选股因子在"幸存者"上有效，但在所有股票上未必
  - 极端事件（暴雷退市）的影响被忽略

这是"幸存者偏差"，量化研究的经典陷阱。补这一份就能彻底解决。

依赖
────
tushare（必须；退市清单 + 日线都走它，efinance 不暴露退市清单接口）
PyYAML（用现有的，不要新增）
TUSHARE_TOKEN（.env 里）

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/pull_delisted.py                          # 默认拉所有 status='D' 的退市股
    python scripts/pull_delisted.py --start 20150101         # 起点（默认 2015-01-01）
    python scripts/pull_delisted.py --force                  # 已缓存的也重拉
    python scripts/pull_delisted.py --metadata-only          # 只刷新清单不拉日线

输出
────
    data_cache/universe/delisted.parquet
        列：ts_code, name, area, industry, list_date, delist_date, market, status='D'

    data_cache/stocks/<ts_code>.parquet
        和现有正常股票同 schema，is_delisted=True 隐含（去 universe/delisted.parquet 查）

注意
────
退市股的 ts_code 后缀依然是 .SH / .SZ（保留交易所归属），文件名约定与现存
一致。Tushare 的 daily 接口对已退市股仍可查到历史数据，不需要特殊处理。
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_STOCKS = PROJECT_ROOT / 'data_cache' / 'stocks'
CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'
META_FILE = CACHE_DIR_UNIVERSE / 'delisted.parquet'

PERMISSION_KEYWORDS = ('40203', '权限', '积分', 'permission')


class PermissionError_(RuntimeError):
    """tushare 权限/积分错误，触发后立即终止。"""


def load_token() -> str:
    try:
        from dotenv import load_dotenv  # noqa: WPS433
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh`。')
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        sys.exit('TUSHARE_TOKEN not found in .env。cp .env.example .env，再填你的真 token。')
    return token


def fetch_delisted_universe(pro) -> pd.DataFrame:
    """拉所有退市股的元数据清单。

    重要：Tushare stock_basic 必须显式 fields，否则 delist_date 不会返回。
    """
    try:
        df = pro.stock_basic(
            list_status='D',
            fields=('ts_code,symbol,name,area,industry,market,exchange,list_date,delist_date'),
        )
    except Exception as e:
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        raise
    if df is None or df.empty:
        sys.exit('stock_basic(list_status=D) 返回空，请确认 Tushare 权限。')

    keep_cols = [
        c
        for c in [
            'ts_code',
            'symbol',
            'name',
            'area',
            'industry',
            'market',
            'exchange',
            'list_date',
            'delist_date',
        ]
        if c in df.columns
    ]
    df = df[keep_cols].copy()

    # 日期列转 datetime
    for col in ('list_date', 'delist_date'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

    df['status'] = 'D'
    # 优先按退市日期排，没有就按上市日期
    sort_col = 'delist_date' if 'delist_date' in df.columns else 'list_date'
    return df.sort_values(sort_col).reset_index(drop=True)


def fetch_one(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """拉单只退市股的日线 + adj_factor，schema 对齐现存正常股票。"""
    try:
        daily = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        raise
    if daily is None or daily.empty:
        return pd.DataFrame()

    try:
        adj = pro.adj_factor(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        adj = None

    daily = daily.sort_values('trade_date').reset_index(drop=True)
    if adj is not None and not adj.empty:
        adj = adj[['trade_date', 'adj_factor']].drop_duplicates('trade_date')
        daily = daily.merge(adj, on='trade_date', how='left')
        daily['adj_factor'] = daily['adj_factor'].ffill().bfill().fillna(1.0)
    else:
        daily['adj_factor'] = 1.0

    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')
    return daily


def main() -> None:
    ap = argparse.ArgumentParser(description='补 A 股退市股历史数据')
    ap.add_argument('--start', default='20150101', help='起始日期 YYYYMMDD')
    ap.add_argument('--end', default='', help='结束日期 YYYYMMDD（默认到退市日）')
    ap.add_argument(
        '--sleep', type=float, default=0.3, help='每次调用之间 sleep 秒数（避开 tushare 速率限）'
    )
    ap.add_argument('--force', action='store_true', help='已缓存的也重拉')
    ap.add_argument(
        '--metadata-only',
        action='store_true',
        help='只刷新 universe/delisted.parquet，不拉个股日线',
    )
    ap.add_argument('--limit', type=int, default=0, help='只跑前 N 只（调试用，0 = 全部）')
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR_STOCKS.mkdir(parents=True, exist_ok=True)
    CACHE_DIR_UNIVERSE.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    # ---------- 1) 元数据清单 ----------
    print('拉退市股清单 (stock_basic, list_status=D)...')
    try:
        universe = fetch_delisted_universe(pro)
    except PermissionError_ as e:
        sys.exit(f'权限不足: {e}')

    universe.to_parquet(META_FILE, index=False)
    n = len(universe)
    earliest_delist = universe['delist_date'].min() if 'delist_date' in universe else None
    latest_delist = universe['delist_date'].max() if 'delist_date' in universe else None
    print(f'  {n} 只退市股 → {META_FILE.relative_to(PROJECT_ROOT)}')
    if earliest_delist is not None and latest_delist is not None:
        print(f'  退市日期范围: {earliest_delist.date()} → {latest_delist.date()}')

    if args.metadata_only:
        print('\n--metadata-only 模式，跳过日线拉取。')
        return

    # ---------- 2) 逐只拉日线 ----------
    # 过滤掉 2015 年之前已退市的股票（数据稀疏、影响小、Tushare 老数据质量也差）
    cutoff = pd.Timestamp(args.start)
    to_fetch = universe[universe['delist_date'] >= cutoff].copy()
    skipped = n - len(to_fetch)
    if skipped > 0:
        print(f'  跳过 {skipped} 只退市日期早于 {cutoff.date()} 的（数据稀疏）')

    if args.limit > 0:
        to_fetch = to_fetch.head(args.limit)
        print(f'  --limit {args.limit}，只跑前 {args.limit} 只')

    m = len(to_fetch)
    print(f'\n要拉日线: {m} 只  →  data_cache/stocks/')
    print('-' * 70)

    failed: list[tuple[str, str, str]] = []
    width = len(str(m))
    for i, row in enumerate(to_fetch.itertuples(index=False), 1):
        code = row.ts_code
        name = row.name
        out = CACHE_DIR_STOCKS / f'{code}.parquet'

        if out.exists() and not args.force:
            print(f'  [{i:>{width}}/{m}] skip  {code} {name}  (已缓存)')
            continue

        # 默认到该股的 delist_date，args.end 覆盖时用 args.end
        end_dt = row.delist_date if not args.end else pd.Timestamp(args.end)
        end_str = end_dt.strftime('%Y%m%d')

        try:
            df = fetch_one(pro, code, args.start, end_str)
            if df.empty:
                print(f'  [{i:>{width}}/{m}] empty {code} {name}  (可能 {args.start} 之后无交易)')
                failed.append((code, name, 'empty'))
            else:
                df.to_parquet(out, index=False)
                af_first, af_last = df['adj_factor'].iloc[0], df['adj_factor'].iloc[-1]
                af_mark = (
                    '' if af_first == af_last == 1.0 else f', adj={af_first:.3f}→{af_last:.3f}'
                )
                print(
                    f'  [{i:>{width}}/{m}] ok    {code} {name}  '
                    f'({len(df)} rows, 退市于 {end_dt.date()}{af_mark})'
                )
        except PermissionError_ as e:
            sys.exit(
                f'\n[{i}/{m}] 权限/积分不足: {code} {name}'
                f'\n  -> {e}'
                f'\n后续大概率同样失败。先解决权限问题再重跑。'
            )
        except Exception as e:  # noqa: BLE001
            print(f'  [{i:>{width}}/{m}] FAIL  {code} {name}  -> {e}')
            failed.append((code, name, str(e)))

        time.sleep(args.sleep)

    print('-' * 70)
    ok = m - len(failed)
    print(f'完成: {ok}/{m} 成功')

    if failed:
        print(f'\n失败/空 {len(failed)} 只:')
        for code, name, err in failed[:20]:
            print(f'  {code} {name}: {err}')
        if len(failed) > 20:
            print(f'  ... 还有 {len(failed) - 20} 只')

    print(f'\n元数据: {META_FILE.relative_to(PROJECT_ROOT)}')
    print(f'日线:   data_cache/stocks/ (新增 {ok} 只退市股，混在现有 681 只里)')
    print('\n要查"哪些是退市股"，读 universe/delisted.parquet 的 ts_code 列即可。')


if __name__ == '__main__':
    main()
