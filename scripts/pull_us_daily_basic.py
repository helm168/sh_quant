"""拉美股每日估值快照 (PE_TTM / PB / PS_TTM) → daily_basic/<ts_code>.parquet。

为什么需要这个
─────────────
A 股的 daily_basic 由 pull_daily_basic.py 经 Tushare pro.daily_basic 拉取，
但 pro.daily_basic 是 A 股专属接口，美股一直没有 daily_basic，导致下游
TradingAgents V-Score 对所有美股都算不出估值分。这个脚本用 FMP 补齐美股
那一份 daily_basic，schema 与 A 股 daily_basic 完全对齐，下游零改动。

数据源
──────
FMP /stable/ratios-ttm?symbol=<SYM>  (Starter 档可用，单只一次调用，单条 TTM 快照)
    priceToEarningsRatioTTM → pe_ttm
    priceToBookRatioTTM     → pb
    priceToSalesRatioTTM    → ps_ttm
FMP Starter 没有 /stable/ratios?period=quarter (402 Premium)，所以只能拿
"当前 TTM 快照"，不是日频历史。V-Score 只读最新一行，单条快照即满足；脚本按
日增量 append，每天 cron 跑一次就自然攒出一条/日的时间序列。

依赖
────
- requests, pandas, python-dotenv (见 requirements.txt)
- 项目根 .env 里的 FMP_API_KEY；worktree 里无 .env 时回落到主仓库根 .env。
  没 key 直接报错退出，不静默继续。

用法 (先 source .venv/bin/activate)
───────────────────────────────────
    # 默认：覆盖 financials/ 里全部美股 (与 financials parquet 同一批 ticker)
    python scripts/pull_us_daily_basic.py

    # 指定 ticker (调试用)
    python scripts/pull_us_daily_basic.py --tickers NVDA.US,AAPL.US

    # 强制重写 (丢弃旧快照, 只留今天这条)
    python scripts/pull_us_daily_basic.py --force

输出位置
────────
<data_root>/daily_basic/<ts_code>.parquet  (data_root = $SH_QUANT_DATA_DIR 或
~/.market_data，与 TradingAgents vscore.py 的 _data_root() 解析方式一致，
保证"写的地方"==“读的地方"，不受 worktree / 软链差异影响)

输出 parquet schema 与 A 股 daily_basic 完全一致 (列名/顺序对齐
pull_daily_basic.py COLS)，美股只填 trade_date/ts_code/pe_ttm/pb/ps_ttm，
其余列 NaN——V-Score 只硬依赖 pe_ttm/pb，不阻塞。
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 与 pull_daily_basic.py 完全一致的列与顺序 (A 股 Tushare pro.daily_basic 原生命名)
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

FMP_BASE = 'https://financialmodelingprep.com/stable'


# ─── 数据根目录 (与 tradingagents local_parquet_stock._data_root() 对齐) ──
def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR_DB = _data_root() / 'daily_basic'


# ─── .env 加载 (worktree 无 .env 时回落主仓库根) ─────────────────────────
def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('需要 python-dotenv: pip install python-dotenv')

    if (PROJECT_ROOT / '.env').exists():
        load_dotenv(PROJECT_ROOT / '.env')
        return
    # worktree (.claude/worktrees/*) 里没 .env，回落到主仓库根的 .env
    try:
        common = subprocess.check_output(
            ['git', 'rev-parse', '--path-format=absolute', '--git-common-dir'],
            cwd=PROJECT_ROOT,
            text=True,
        ).strip()
        main_env = Path(common).parent / '.env'
        if main_env.exists():
            load_dotenv(main_env)
    except Exception:
        pass  # 拿不到就算了，下面 FMP_API_KEY 缺失会显式 exit


# ─── ts_code 过滤 ───────────────────────────────────────────────────────
def is_us(ts_code: str) -> bool:
    return ts_code.upper().endswith('.US')


# ─── FMP 拉取 ───────────────────────────────────────────────────────────
def fetch_one(ts_code: str, key: str) -> pd.DataFrame | None:
    """拉单只美股的 TTM 估值快照，返回 1 行 (schema = COLS)。"""
    symbol = ts_code[:-3] if ts_code.upper().endswith('.US') else ts_code
    url = f'{FMP_BASE}/ratios-ttm'
    params = {'symbol': symbol, 'apikey': key}

    try:
        r = requests.get(url, params=params, timeout=30)
    except requests.exceptions.RequestException:
        time.sleep(2.0)
        r = requests.get(url, params=params, timeout=30)

    if r.status_code == 429:
        # 限速：退一退再试一次 (FMP Starter 300/min)
        time.sleep(2.0)
        r = requests.get(url, params=params, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f'FMP ratios-ttm {r.status_code}: {r.text[:120]}')

    data = r.json()
    if not isinstance(data, list) or not data:
        return None  # 退市 / 未知 symbol → 空

    row = data[0]
    pe = row.get('priceToEarningsRatioTTM')
    pb = row.get('priceToBookRatioTTM')
    ps = row.get('priceToSalesRatioTTM')
    if pe is None and pb is None:
        return None  # 该 symbol 没有任何估值字段，当空处理

    rec = {c: pd.NA for c in COLS}
    rec['trade_date'] = pd.Timestamp.now().normalize()
    rec['ts_code'] = ts_code
    rec['pe_ttm'] = float(pe) if pe is not None else pd.NA
    rec['pb'] = float(pb) if pb is not None else pd.NA
    rec['ps_ttm'] = float(ps) if ps is not None else pd.NA
    return pd.DataFrame([rec], columns=COLS)


# ─── 单只增量更新 ───────────────────────────────────────────────────────
def update_one(ts_code: str, key: str, force: bool) -> dict:
    if not is_us(ts_code):
        return {'ticker': ts_code, 'status': 'skip', 'reason': 'non-US'}

    CACHE_DIR_DB.mkdir(parents=True, exist_ok=True)
    fp = CACHE_DIR_DB / f'{ts_code}.parquet'

    new_df = fetch_one(ts_code, key)
    if new_df is None or new_df.empty:
        return {'ticker': ts_code, 'status': 'empty'}

    if force or not fp.exists():
        new_df.to_parquet(fp, index=False)
        return {'ticker': ts_code, 'status': 'ok', 'rows': len(new_df), 'mode': 'full'}

    try:
        old_df = pd.read_parquet(fp)
    except Exception:
        new_df.to_parquet(fp, index=False)
        return {'ticker': ts_code, 'status': 'ok', 'rows': len(new_df), 'mode': 'full-repair'}

    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged['trade_date'] = pd.to_datetime(merged['trade_date'], errors='coerce')
    merged = (
        merged.drop_duplicates('trade_date', keep='last')
        .sort_values('trade_date')
        .reset_index(drop=True)
    )
    merged = merged[COLS]
    merged.to_parquet(fp, index=False)
    return {'ticker': ts_code, 'status': 'ok', 'rows': len(merged), 'mode': 'incremental'}


# ─── ticker 来源 ────────────────────────────────────────────────────────
def collect_tickers(args) -> list[str]:
    if args.tickers:
        return [t.strip().upper() for t in args.tickers.split(',') if t.strip()]

    # 默认：与 financials parquet 同一批 ticker (financials 是 PEG 的前置, 天然对齐)
    fin_dir = _data_root() / 'financials'
    if not fin_dir.exists():
        return []
    return sorted(fp.stem for fp in fin_dir.glob('*.US.parquet') if is_us(fp.stem))


# ─── 主入口 ─────────────────────────────────────────────────────────────
def main() -> int:
    _load_env()
    key = os.getenv('FMP_API_KEY')
    if not key:
        sys.exit('FMP_API_KEY 未配置 (.env 或 shell env)')

    ap = argparse.ArgumentParser(description='拉美股 daily_basic 估值快照 (FMP ratios-ttm)')
    ap.add_argument(
        '--tickers', help='逗号分隔的 ts_code (如 NVDA.US,AAPL.US)，跳过 financials 默认'
    )
    ap.add_argument('--workers', type=int, default=4, help='并发线程数 (默认 4，避开 FMP 300/min)')
    ap.add_argument('--force', action='store_true', help='强制重写 (丢弃旧快照，只留今天)')
    args = ap.parse_args()

    tickers = collect_tickers(args)
    if not tickers:
        sys.exit('没拿到美股 ticker. 先跑 pull_financials.py --market us，或传 --tickers')

    print(f'拉 {len(tickers)} 只美股 daily_basic → {CACHE_DIR_DB}')
    print(f'并发 {args.workers}, force={args.force}')
    print('-' * 70)

    t0 = time.time()
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(update_one, t, key, args.force): t for t in tickers}
        width = len(str(len(tickers)))
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {'ticker': t, 'status': 'error', 'error': str(e)}
            results.append(r)

            tag = {'ok': '✓', 'skip': '=', 'empty': '○', 'error': '✗'}.get(r['status'], '?')
            extra = ''
            if r['status'] == 'ok':
                extra = f'{r.get("rows", 0):>4} 行 [{r.get("mode", "")}]'
            elif r['status'] == 'error':
                extra = f'!! {r.get("error", "")[:80]}'
            elif r['status'] == 'skip':
                extra = r.get('reason', '')
            print(f'  [{i:>{width}}/{len(tickers)}] {tag} {t:<14} {extra}', flush=True)

    elapsed = time.time() - t0
    ok = sum(1 for r in results if r['status'] == 'ok')
    empty = sum(1 for r in results if r['status'] == 'empty')
    err = sum(1 for r in results if r['status'] == 'error')

    print('-' * 70)
    print(f'完成: {ok} 更新 / {empty} 空 / {err} 错误, 用时 {elapsed:.1f}s')

    if err and err <= 20:
        print('\n错误:')
        for r in [x for x in results if x['status'] == 'error'][:20]:
            print(f'  {r["ticker"]}: {r.get("error", "?")}')

    return 0 if err == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
