"""一次性拉所有可交易 ETF 的日线数据，落到 data_cache/etf/。

依赖：tushare / pandas / pyarrow / python-dotenv（都在 requirements.txt 里）
环境：项目根 .env 里需要 TUSHARE_TOKEN

用法（先 source .venv/bin/activate，在项目根目录执行）：

    python scripts/pull_etfs.py                            # 默认：当前可交易 ETF
    python scripts/pull_etfs.py --include-lof              # 同时拉 LOF（场内基金里的非 ETF）
    python scripts/pull_etfs.py --include-delisted         # 含退市/到期的基金（survivorship bias）
    python scripts/pull_etfs.py --start 20180101 --end 20251231
    python scripts/pull_etfs.py --force                    # 已缓存的也重拉

输出：
    data_cache/etf/_etfs.parquet         ETF 元数据（代码 / 名称 / 管理人 / 上市日 / 状态 / ...）
    data_cache/etf/510300.SH.parquet     沪深 300 ETF 日线
    data_cache/etf/510500.SH.parquet     中证 500 ETF
    data_cache/etf/159915.SZ.parquet     创业板 ETF
    ...
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = PROJECT_ROOT / 'data_cache' / 'etf'

PERMISSION_KEYWORDS = ('40203', '权限', '积分', 'permission')


class PermissionError_(RuntimeError):
    """tushare 权限/积分错误，触发后立即终止，不继续后续基金。"""


def load_token() -> str:
    try:
        from dotenv import load_dotenv  # noqa: WPS433
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh` 或 `pip install python-dotenv`。')
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        sys.exit('TUSHARE_TOKEN not found in .env。cp .env.example .env，再填你的真 token。')
    return token


def fetch_one(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """走 fund_daily。权限/积分错误抛 PermissionError_，由调用方决定是否中止。"""
    try:
        df = pro.fund_daily(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(str(e)) from e
        raise

    if df is None or df.empty:
        return pd.DataFrame()
    df = df.sort_values('trade_date').reset_index(drop=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    return df


def list_funds(pro, include_delisted: bool) -> pd.DataFrame:
    """拉场内基金清单。tushare 的 status 必须分次拉。"""
    statuses = ['L']
    if include_delisted:
        statuses += ['D', 'I']  # D 退市 / I 已发行未上市
    chunks = []
    for status in statuses:
        try:
            df = pro.fund_basic(market='E', status=status)
        except Exception as e:  # noqa: BLE001
            if any(s in str(e) for s in PERMISSION_KEYWORDS):
                raise PermissionError_(str(e)) from e
            raise
        if df is not None and not df.empty:
            df = df.copy()
            df['status'] = status
            chunks.append(df)
    if not chunks:
        sys.exit('fund_basic 返回空，无法继续。')
    return pd.concat(chunks, ignore_index=True).drop_duplicates('ts_code').reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='20150101', help='起始日期 YYYYMMDD')
    ap.add_argument('--end', default='20251231', help='结束日期 YYYYMMDD')
    ap.add_argument(
        '--sleep', type=float, default=0.3, help='每次调用之间等的秒数（避开 tushare 速率限）'
    )
    ap.add_argument('--force', action='store_true', help='忽略已有缓存，全部重拉')
    ap.add_argument(
        '--include-lof', action='store_true', help='同时拉 LOF（场内非 ETF 基金），默认仅 ETF'
    )
    ap.add_argument(
        '--include-delisted',
        action='store_true',
        help='包含已退市/到期的基金（默认只拉当前可交易）',
    )
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    # 1) 拿场内基金清单
    print('拉取基金清单（fund_basic, market=E）...')
    try:
        funds = list_funds(pro, include_delisted=args.include_delisted)
    except PermissionError_ as e:
        sys.exit(f'fund_basic 权限不足: {e}')

    # 2) 过滤：默认 name 含 "ETF"，剔除 "联接"（OTC 联接基金，理论上 market=E 不会有，保险起见过一道）
    if not args.include_lof:
        funds = funds[funds['name'].str.contains('ETF', na=False, regex=False)]
        funds = funds[~funds['name'].str.contains('联接', na=False, regex=False)]
        kind = 'ETF'
    else:
        kind = 'ETF + LOF'
    funds = funds.reset_index(drop=True)

    # 简单兼容 itertuples 的 name 字段冲突
    funds_to_iter = funds.rename(columns={'name': 'fund_name'})

    n = len(funds)
    if n == 0:
        sys.exit('过滤后 0 只基金。')
    print(f'[{kind}] {n} 只  →  {CACHE_DIR.relative_to(PROJECT_ROOT)}/')
    funds.to_parquet(CACHE_DIR / '_etfs.parquet', index=False)

    # 3) 逐只拉日线
    failed: list[tuple[str, str, str]] = []
    width = len(str(n))  # 进度宽度，自动适配 100 / 1000
    for i, row in enumerate(funds_to_iter.itertuples(index=False), 1):
        code = row.ts_code
        name = row.fund_name
        out = CACHE_DIR / f'{code}.parquet'
        if out.exists() and not args.force:
            print(f'  [{i:>{width}}/{n}] skip  {code} {name}  (已缓存)')
            continue
        try:
            df = fetch_one(pro, code, args.start, args.end)
            if df.empty:
                print(f'  [{i:>{width}}/{n}] empty {code} {name}')
                failed.append((code, name, 'empty'))
            else:
                df.to_parquet(out, index=False)
                print(f'  [{i:>{width}}/{n}] ok    {code} {name}  ({len(df)} rows)')
        except PermissionError_ as e:
            sys.exit(
                f'\n[{i}/{n}] 权限/积分不足: {code} {name}'
                f'\n  -> {e}'
                f'\n后续基金大概率同样失败。先解决权限问题再重跑。'
            )
        except Exception as e:  # noqa: BLE001
            print(f'  [{i:>{width}}/{n}] FAIL  {code} {name}  -> {e}')
            failed.append((code, name, str(e)))
        time.sleep(args.sleep)

    print()
    if failed:
        print(f'{len(failed)} 个失败/空：')
        for code, name, err in failed[:20]:
            print(f'  {code} {name}: {err}')
        if len(failed) > 20:
            print(f'  ... 还有 {len(failed) - 20} 个')
        sys.exit(1 if any(e != 'empty' for _, _, e in failed) else 0)
    print('all done.')


if __name__ == '__main__':
    main()
