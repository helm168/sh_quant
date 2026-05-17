"""拉 A 股每日基本面快照 (PE/PB/PS/总市值/换手率).

两种使用方式
────────────
1. **作为 update_daily.py 的子流程**(推荐, 日 cron 用)
       python scripts/update_daily.py
   update_daily.py 末尾会自动调 update_all() 一遍 daily_basic 增量更新.
   两个 endpoint 共享 ticker 列表和 .env 加载.

2. **独立 CLI** (首次回填多年历史, 或单独跑某天)
       python scripts/pull_daily_basic.py --years 5
       python scripts/pull_daily_basic.py --tickers 600519.SH

Tushare 接口区分
────────────────
pro.daily         → OHLCV (open/high/low/close/vol/amount/adj_factor) ← stocks/
pro.daily_basic   → 估值 (pe/pe_ttm/pb/ps/ps_ttm/dv/total_mv/circ_mv) ← daily_basic/

输出 parquet schema (见 docs/DATA_SCHEMA.md):
    trade_date, ts_code, close, pe, pe_ttm, pb, ps, ps_ttm,
    dv_ratio, dv_ttm, turnover_rate, turnover_rate_f, volume_ratio,
    total_share, float_share, free_share, total_mv, circ_mv

性能 (5500 A 股 ticker)
──────────────────────
- batch fast path:  ~5s   (一次 pro.daily_basic(trade_date=today) 拿全市场)
- per-ticker:       ~6min (5500 calls × 5 worker × 0.3s)
batch 启用条件: 非 --force, ticker 数 ≥ BATCH_MIN, 至少一只 ticker 缺今天数据.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_DB = PROJECT_ROOT / 'data_cache' / 'daily_basic'

# Tushare pro.daily_basic 原生列名 (我们直接用, 跟 Tushare 一致, 不 rename)
COLS = [
    'trade_date',
    'ts_code',
    'close',
    'pe',
    'pe_ttm',
    'pb',
    'ps',
    'ps_ttm',
    'dv_ratio',
    'dv_ttm',
    'turnover_rate',
    'turnover_rate_f',
    'volume_ratio',
    'total_share',
    'float_share',
    'free_share',
    'total_mv',
    'circ_mv',
]


# ─── ts_code 后缀过滤 ────────────────────────────────────────────────────
def is_cn_a(ts_code: str) -> bool:
    s = ts_code.upper()
    return s.endswith('.SH') or s.endswith('.SZ') or s.endswith('.BJ')


# ─── Tushare 客户端 (lazy + cached) ─────────────────────────────────────
_TUSHARE_PRO = None


def _get_tushare_pro():
    """Lazy init Tushare pro_api. 没 token 直接 sys.exit."""
    global _TUSHARE_PRO
    if _TUSHARE_PRO is not None:
        return _TUSHARE_PRO
    try:
        import tushare as ts
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('需要 tushare + python-dotenv: pip install tushare python-dotenv')
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        sys.exit('TUSHARE_TOKEN 未配置 (.env 或 shell env)')
    ts.set_token(token)
    _TUSHARE_PRO = ts.pro_api()
    return _TUSHARE_PRO


# ─── Batch fast path ─────────────────────────────────────────────────────
# 仿 update_daily.py 的 _prefetch_tushare_batch: 一次 pro.daily_basic(trade_date=
# today) 拿全市场 ~5500 行 (~5s), 替代 5000+ 次 per-ticker 调用 (~6min, 50x 慢).
#
# fetch_one 进来时若请求"单日 = batch_date", 直接走 cache, 不调网络.
_BATCH_CACHE: dict[str, pd.DataFrame] = {}
_BATCH_DATE: Optional[pd.Timestamp] = None
_BATCH_HIT_COUNT = 0
_BATCH_MISS_COUNT = 0


def _latest_trade_date_via_daily_basic(today: pd.Timestamp) -> Optional[str]:
    """对最近 5 个日历日各试 pro.daily_basic, 第一个返非空就是最近交易日."""
    pro = _get_tushare_pro()
    for delta in range(5):
        cand = today - timedelta(days=delta)
        td_str = cand.strftime('%Y%m%d')
        try:
            sample = pro.daily_basic(trade_date=td_str)
            if sample is not None and not sample.empty:
                return td_str
        except Exception:
            continue
    return None


def _prefetch_daily_basic_batch(today: pd.Timestamp) -> int:
    """拉最近交易日的全市场 daily_basic, 缓存到模块级 dict.

    返回缓存的 ts_code 数量; 失败 (无 token / API 挂) 返 0, fall through 到
    per-ticker.
    """
    global _BATCH_CACHE, _BATCH_DATE

    pro = _get_tushare_pro()
    td_str = _latest_trade_date_via_daily_basic(today)
    if td_str is None:
        print('  [db-batch] 找不到最近交易日, 跳过 batch', flush=True)
        return 0

    print(f'  [db-batch] 预拉 trade_date={td_str} 全市场 daily_basic', flush=True)
    t0 = time.time()
    try:
        df = pro.daily_basic(trade_date=td_str)
    except Exception as e:
        print(f'  [db-batch] daily_basic({td_str}) 失败: {e}', flush=True)
        return 0
    if df is None or df.empty:
        return 0

    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df[[c for c in COLS if c in df.columns]]

    by_code: dict[str, pd.DataFrame] = {}
    for code, sub in df.groupby('ts_code'):
        by_code[str(code)] = sub.reset_index(drop=True)

    _BATCH_CACHE = by_code
    _BATCH_DATE = pd.to_datetime(td_str, format='%Y%m%d')

    print(f'  [db-batch] {len(by_code)} 个 ts_code 缓存, {time.time() - t0:.1f}s', flush=True)
    return len(by_code)


# ─── 增量更新 per-ticker ────────────────────────────────────────────────
def _existing_max_date(ts_code: str) -> Optional[pd.Timestamp]:
    """读已有 parquet 拿 max(trade_date). 文件不存在 / 空 / 损坏 → None."""
    fp = CACHE_DIR_DB / f'{ts_code}.parquet'
    if not fp.exists():
        return None
    try:
        df = pd.read_parquet(fp, columns=['trade_date'])
        if df.empty:
            return None
        return pd.to_datetime(df['trade_date']).max()
    except Exception:
        return None


def fetch_one(ts_code: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """拉单只股票的 daily_basic.

    Fast path: 请求范围 [start, end] 收敛到 batch_date 单天且命中缓存 → 直接返
    cache 的行, 不调网络.
    Slow path: 调 pro.daily_basic(ts_code=..., start_date=..., end_date=...).
    """
    global _BATCH_HIT_COUNT, _BATCH_MISS_COUNT

    # Fast path 条件: batch 已 prefetch, 请求收敛到那一天
    if _BATCH_DATE is not None:
        try:
            start_dt = pd.to_datetime(start, format='%Y%m%d')
            end_dt = pd.to_datetime(end, format='%Y%m%d')
            if start_dt == end_dt == _BATCH_DATE:
                cached = _BATCH_CACHE.get(ts_code)
                if cached is not None and not cached.empty:
                    _BATCH_HIT_COUNT += 1
                    return cached
                # ts_code 不在 batch (停牌/退市/新股) — 落 miss 计数, fall through
                _BATCH_MISS_COUNT += 1
        except Exception:
            pass  # 任何 cache 路径异常 → 静默 fallback

    # Slow path
    pro = _get_tushare_pro()
    try:
        df = pro.daily_basic(ts_code=ts_code, start_date=start, end_date=end)
    except Exception as e:
        # 限速 / 5xx 时退一退再试一次
        if '频率' in str(e) or '超限' in str(e):
            time.sleep(1.0)
            df = pro.daily_basic(ts_code=ts_code, start_date=start, end_date=end)
        else:
            raise

    if df is None or df.empty:
        return None

    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df[[c for c in COLS if c in df.columns]]
    df = df.sort_values('trade_date').reset_index(drop=True)
    return df


def update_one(ts_code: str, default_start: str, end: str, force: bool) -> dict:
    """单只股票的增量更新逻辑.

    - 文件不存在 / --force: 从 default_start 全量拉
    - 文件存在: 从 max(trade_date) + 1 天拉到 end, append 写回
    返回 {ticker, status, rows_added, mode}
    """
    if not is_cn_a(ts_code):
        return {'ticker': ts_code, 'status': 'skip', 'reason': 'non-A-share'}

    CACHE_DIR_DB.mkdir(parents=True, exist_ok=True)
    fp = CACHE_DIR_DB / f'{ts_code}.parquet'

    if force or not fp.exists():
        new_df = fetch_one(ts_code, default_start, end)
        if new_df is None:
            return {'ticker': ts_code, 'status': 'empty'}
        new_df.to_parquet(fp, index=False)
        return {'ticker': ts_code, 'status': 'ok', 'rows_added': len(new_df), 'mode': 'full'}

    max_date = _existing_max_date(ts_code)
    if max_date is None:
        # 文件损坏, 兜底全量重拉
        new_df = fetch_one(ts_code, default_start, end)
        if new_df is None:
            return {'ticker': ts_code, 'status': 'empty'}
        new_df.to_parquet(fp, index=False)
        return {'ticker': ts_code, 'status': 'ok', 'rows_added': len(new_df), 'mode': 'full-repair'}

    start_dt = max_date + timedelta(days=1)
    if start_dt > pd.Timestamp(end):
        return {'ticker': ts_code, 'status': 'fresh'}

    new_df = fetch_one(ts_code, start_dt.strftime('%Y%m%d'), end)
    if new_df is None or new_df.empty:
        return {'ticker': ts_code, 'status': 'fresh'}

    old_df = pd.read_parquet(fp)
    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged = (
        merged.drop_duplicates('trade_date', keep='last')
        .sort_values('trade_date')
        .reset_index(drop=True)
    )
    merged.to_parquet(fp, index=False)
    return {'ticker': ts_code, 'status': 'ok', 'rows_added': len(new_df), 'mode': 'incremental'}


# ─── 主入口 (CLI + update_daily.py 复用) ────────────────────────────────
BATCH_MIN_TICKERS = 20


def update_all(
    tickers: list[str],
    *,
    years: int = 3,
    workers: int = 3,
    force: bool = False,
    verbose: bool = False,
    label: str = 'daily_basic',
) -> dict:
    """对一批 ticker 跑 daily_basic 增量更新. update_daily.py 末尾会调这个.

    返回 stats dict: {ok, fresh, empty, error, rows_added, elapsed_s}
    """
    today = datetime.now()
    end_str = today.strftime('%Y%m%d')
    default_start = (today - timedelta(days=years * 365)).strftime('%Y%m%d')

    cn_tickers = [t for t in tickers if is_cn_a(t)]
    if not cn_tickers:
        return {'ok': 0, 'fresh': 0, 'empty': 0, 'error': 0, 'rows_added': 0, 'elapsed_s': 0.0}

    print(f'\n=== {label} 增量更新 ({len(cn_tickers)} 只 A 股) ===', flush=True)
    print(f'  时间范围: {default_start} → {end_str} (首次全量 {years} 年, 增量从 max_date+1d)')
    print(f'  并发: {workers}, force: {force}')

    # 决定要不要 batch: --force 走 per-ticker (要多年历史), 小批量直接 per-ticker
    skip_batch = force or len(cn_tickers) < BATCH_MIN_TICKERS
    if not skip_batch:
        _prefetch_daily_basic_batch(pd.Timestamp(today).normalize())

    t0 = time.time()
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(update_one, t, default_start, end_str, force): t
            for t in cn_tickers
        }
        fresh_count = 0
        width = len(str(len(cn_tickers)))
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {'ticker': t, 'status': 'error', 'error': str(e)}
            results.append(r)

            # fresh 太多, 默认每 500 打一行心跳, 不刷屏
            if r['status'] == 'fresh':
                fresh_count += 1
                if not verbose and fresh_count % 500 == 0:
                    print(f'  [{i:>{width}}/{len(cn_tickers)}] · (已跳过 {fresh_count} 个 fresh)')
                if not verbose:
                    continue

            tag = {'ok': '✓', 'fresh': '·', 'skip': '=', 'empty': '○', 'error': '✗'}.get(
                r['status'], '?'
            )
            extra = ''
            if r['status'] == 'ok':
                extra = f"+{r.get('rows_added', 0):>4} 行 [{r.get('mode', '')}]"
            elif r['status'] == 'error':
                extra = f"!! {r.get('error', '')[:80]}"
            print(f'  [{i:>{width}}/{len(cn_tickers)}] {tag} {t:<14} {extra}', flush=True)

    elapsed = time.time() - t0
    ok = sum(1 for r in results if r['status'] == 'ok')
    fresh = sum(1 for r in results if r['status'] == 'fresh')
    empty = sum(1 for r in results if r['status'] == 'empty')
    err = sum(1 for r in results if r['status'] == 'error')
    total_rows = sum(r.get('rows_added', 0) for r in results)

    summary = f'  完成: {ok} 更新'
    if fresh:
        summary += f' / {fresh} 已最新(skip)'
    if empty:
        summary += f' / {empty} 空'
    if err:
        summary += f' / {err} 错误'
    summary += f', +{total_rows} 新行, 用时 {elapsed:.1f}s'
    print(summary)

    if _BATCH_HIT_COUNT or _BATCH_MISS_COUNT:
        total = _BATCH_HIT_COUNT + _BATCH_MISS_COUNT
        pct = 100 * _BATCH_HIT_COUNT // max(total, 1)
        print(
            f'  [db-batch] cache hit: {_BATCH_HIT_COUNT}/{total} ({pct}%), '
            f'miss (fall-through): {_BATCH_MISS_COUNT}'
        )

    if err > 0 and not verbose:
        print('\n  错误样例 (前 3):')
        for r in [x for x in results if x['status'] == 'error'][:3]:
            print(f"    - {r['ticker']}: {r.get('error', '')}")

    return {
        'ok': ok, 'fresh': fresh, 'empty': empty, 'error': err,
        'rows_added': total_rows, 'elapsed_s': elapsed,
    }


# ─── ticker 来源 (CLI 用) ────────────────────────────────────────────────
def load_tickers(args) -> list[str]:
    if args.tickers:
        return [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    if args.file:
        with open(args.file, 'r', encoding='utf-8') as f:
            return [line.strip().upper() for line in f if line.strip() and not line.startswith('#')]

    universe_dir = PROJECT_ROOT / 'data_cache' / 'universe'
    ts_set: set[str] = set()
    if universe_dir.exists():
        for uni_fp in sorted(universe_dir.glob('*.parquet')):
            try:
                df = pd.read_parquet(uni_fp, columns=['ts_code'])
                ts_set.update(df['ts_code'].dropna().astype(str).str.upper())
            except Exception:
                pass
    return sorted(t for t in ts_set if is_cn_a(t))


# ─── CLI ────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='拉 A 股 daily_basic (PE/PB/PS/总市值) 增量更新')
    ap.add_argument('--tickers', help='逗号分隔的 ticker, 跳过 universe 默认')
    ap.add_argument('--file', help='ticker 列表文件 (每行一个)')
    ap.add_argument('--years', type=int, default=3, help='首次全量拉的年数 (默认 3 年)')
    ap.add_argument('--workers', type=int, default=3, help='并发线程数 (默认 3)')
    ap.add_argument('--force', action='store_true', help='强制全量重拉, 不增量')
    ap.add_argument('--verbose', action='store_true', help='打印所有 ticker 状态(不默认跳过 fresh)')
    args = ap.parse_args()

    tickers = load_tickers(args)
    if not tickers:
        sys.exit('没拿到 ticker 列表. 检查 universe/*.parquet 或传 --tickers')

    stats = update_all(
        tickers,
        years=args.years,
        workers=args.workers,
        force=args.force,
        verbose=args.verbose,
        label='pull_daily_basic (CLI)',
    )
    return 0 if stats['error'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
