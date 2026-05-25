"""Signal Engine 日终 orchestrator — sh_quant 数据层产物侧 (PRD §9 SIG-1..SIG-4).

用法
────
    source .venv/bin/activate
    python scripts/pull_signals.py                  # 三市场全跑
    python scripts/pull_signals.py --markets CN     # 只 CN
    python scripts/pull_signals.py --dry-run        # 算但不落盘 (调阈值用)

产物 (PRD §4):
    ~/.market_data/signals/<market>_<date>.json
    ~/.market_data/signals/<market>_latest.json

跟 pull_macro / pull_sector_turnover 同款 _data_root() 通路, **不**写 worktree
data_cache (worktree 跑会写到自己目录, UI 读 ~/.market_data 就漏数).

依赖 (本地 parquet, 不调外网, 无 token):
    ~/.market_data/macro/index_<benchmark>.parquet         (大盘信号)
    ~/.market_data/universe/<cn_a|us|cn_hk>.parquet        (扫 universe)
    ~/.market_data/stocks/<ts_code>.parquet                (个股 + 全市场聚合)
    ~/.market_data/sector_history/<market>_sector_turnover.parquet  (板块, 可选)

exit code:
    0 — 全市场跑通
    1 — 某市场抛异常 (其他市场仍会跑完)
    2 — 缺关键依赖 (benchmark 指数 / universe parquet 不存在)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))  # 允许任何 CWD 调用

from utils.signals_engine import (  # noqa: E402
    Signal,
    _data_root,
    load_previous,
    reconcile,
    write_output,
)
from utils.signals_rules import ALL_RULES  # noqa: E402


def load_cfg() -> dict:
    fp = PROJECT_ROOT / 'config' / 'signals.yaml'
    if not fp.exists():
        sys.exit(f'[ABORT] 缺 {fp}')
    return yaml.safe_load(fp.read_text())


def _load_universe(market: str, cfg: dict) -> pd.DataFrame:
    name = cfg['universe_files'][market]
    fp = _data_root() / 'universe' / f'{name}.parquet'
    if not fp.exists():
        sys.exit(f'[ABORT-DEPS] universe {fp} 不存在; 先跑 pull_{market.lower()}_universe.py')
    u = pd.read_parquet(fp, columns=['ts_code', 'name'])
    suffixes = tuple(cfg['suffixes'][market])
    u = u[u['ts_code'].str.endswith(suffixes)].reset_index(drop=True)
    return u


def _load_benchmark(market: str, cfg: dict) -> pd.DataFrame:
    fp = _data_root() / 'macro' / f'{cfg["benchmarks"][market]}.parquet'
    if not fp.exists():
        sys.exit(f'[ABORT-DEPS] 基准指数 {fp} 不存在; 先跑 pull_macro.py')
    df = pd.read_parquet(fp)
    if 'date' not in df.columns or 'close' not in df.columns:
        sys.exit(f'[ABORT-DEPS] {fp} 缺 date/close 列')
    df = df.sort_values('date').reset_index(drop=True)
    lookback = cfg['engine']['market_lookback_days']
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=lookback)
    df = df[df['date'] >= cutoff].reset_index(drop=True)
    return df


def _load_stocks(market: str, cfg: dict, universe: pd.DataFrame
                 ) -> tuple[list[tuple[str, str, pd.DataFrame]], dict]:
    """返回 (per-stock list, agg dict).

    per-stock list: [(ts_code, name, df[trade_date,close,high,low,amount,pct_chg])]
    agg: {
        'mkt_amount': Series indexed by date (全市场日成交额, 本币元),
        'mkt_breadth': DataFrame[date, n_up, n_total]
    }
    缺 parquet 的票静默跳过 (universe 经常落后 stocks pull 一两天).
    """
    stocks_dir = _data_root() / 'stocks'
    lookback = cfg['engine']['stock_lookback_days']
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=lookback)
    scale = cfg['amount_scale'][market]

    per_stock: list[tuple[str, str, pd.DataFrame]] = []
    amt_by_date: dict[pd.Timestamp, float] = {}
    breadth_up: dict[pd.Timestamp, int] = {}
    breadth_tot: dict[pd.Timestamp, int] = {}
    miss = 0

    for row in universe.itertuples(index=False):
        fp = stocks_dir / f'{row.ts_code}.parquet'
        if not fp.exists():
            miss += 1
            continue
        try:
            df = pd.read_parquet(
                fp, columns=['trade_date', 'close', 'high', 'low', 'amount', 'pct_chg'],
            )
        except Exception:  # noqa: BLE001 — 个别坏 parquet 不应阻断全市场
            miss += 1
            continue
        df = df[df['trade_date'] >= cutoff]
        if df.empty:
            continue
        df = df.copy()
        df['amount'] = df['amount'] * scale
        per_stock.append((row.ts_code, row.name, df))

        # 聚合 (按日累加; pct_chg 同日上涨标记)
        for trade_date, amt, pct in zip(df['trade_date'], df['amount'], df['pct_chg'],
                                        strict=True):
            if pd.notna(amt) and amt > 0:
                amt_by_date[trade_date] = amt_by_date.get(trade_date, 0.0) + amt
            if pd.notna(pct):
                breadth_tot[trade_date] = breadth_tot.get(trade_date, 0) + 1
                if pct > 0:
                    breadth_up[trade_date] = breadth_up.get(trade_date, 0) + 1

    mkt_amount = pd.Series(amt_by_date).sort_index()
    if breadth_tot:
        mkt_breadth = pd.DataFrame({
            'date': list(breadth_tot.keys()),
            'n_up': [breadth_up.get(d, 0) for d in breadth_tot],
            'n_total': [breadth_tot[d] for d in breadth_tot],
        }).sort_values('date').reset_index(drop=True)
    else:
        mkt_breadth = pd.DataFrame(columns=['date', 'n_up', 'n_total'])

    print(f'  [{market}] universe={len(universe)} loaded={len(per_stock)} '
          f'missing_parquet={miss} mkt_amount_days={len(mkt_amount)}')
    return per_stock, {'mkt_amount': mkt_amount, 'mkt_breadth': mkt_breadth}


def _load_sector_history(market: str) -> pd.DataFrame | None:
    fp = _data_root() / 'sector_history' / f'{market.lower()}_sector_turnover.parquet'
    if not fp.exists():
        print(f'  [{market}] sector_history 缺 ({fp.name}) — SEC_BURST 跳过')
        return None
    df = pd.read_parquet(fp)
    # date 列可能是 date 类型; 统一成 Timestamp 后比较更省心
    return df


def run_market(market: str, cfg: dict, dry_run: bool = False) -> int:
    print(f'\n=== {market} ===')
    t0 = time.time()
    universe = _load_universe(market, cfg)
    bench_df = _load_benchmark(market, cfg)
    per_stock, agg = _load_stocks(market, cfg, universe)
    sector_hist = _load_sector_history(market)

    if bench_df.empty or not per_stock:
        print(f'  [SKIP] {market}: 基准指数空 or 0 只可用 universe')
        return 0

    as_of = max(bench_df['date'].iloc[-1].date(), agg['mkt_amount'].index.max().date())
    as_of_str = as_of.isoformat()

    ctx = {
        'market': market,
        'as_of_date': as_of_str,
        'cfg': cfg,
        'benchmark_label': cfg['benchmarks'][market],
        'index_df': bench_df,
        'mkt_amount': agg['mkt_amount'],
        'mkt_breadth': agg['mkt_breadth'],
        'sector_history': sector_hist,
        'stocks': per_stock,
    }

    raw_signals: list[Signal] = []
    for rule in ALL_RULES:
        try:
            r = rule(ctx)
            raw_signals.extend(r)
        except Exception as ex:  # noqa: BLE001 — 一条 rule 炸不影响别人
            print(f'  [RULE FAIL] {rule.__name__}: {ex}')

    out_dir = _data_root() / cfg['engine']['out_subdir']
    previous = load_previous(market, out_dir)
    reconciled = reconcile(raw_signals, market, as_of_str, previous)

    new_count = sum(1 for s in reconciled if s.isNew)
    by_level = {'risk': 0, 'watch': 0, 'opportunity': 0}
    for s in reconciled:
        by_level[s.level] += 1
    print(f'  [{market}] signals={len(reconciled)} (new={new_count}) '
          f"risk={by_level['risk']} watch={by_level['watch']} "
          f"opportunity={by_level['opportunity']} as_of={as_of_str} "
          f'elapsed={time.time() - t0:.1f}s')

    if dry_run:
        from utils.signals_engine import sort_signals
        print(f'  [{market}] --dry-run, 不落盘')
        for s in sort_signals(reconciled)[:8]:
            print(f'    · [{s.level}/{s.severity}] {s.type} {s.subject.id} — {s.title}')
        return 0

    dated_fp, latest_fp = write_output(market, as_of_str, len(universe), reconciled, out_dir)
    print(f'  → wrote {dated_fp.name} + {latest_fp.name}')
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description='Signal Engine 日终批处理 (PRD SIG-1..SIG-4)')
    p.add_argument('--markets', default='CN,US,HK',
                   help='逗号分隔 (CN,US,HK), 默认全部')
    p.add_argument('--dry-run', action='store_true',
                   help='算但不落盘, 调阈值用')
    args = p.parse_args()

    cfg = load_cfg()
    markets = [m.strip().upper() for m in args.markets.split(',') if m.strip()]
    unknown = [m for m in markets if m not in cfg['benchmarks']]
    if unknown:
        sys.exit(f'未知 market: {unknown}, 可选 {list(cfg["benchmarks"])}')

    print(f'data_root = {_data_root()}')

    exit_code = 0
    for m in markets:
        try:
            run_market(m, cfg, dry_run=args.dry_run)
        except SystemExit as ex:
            # ABORT-DEPS 类: 缺关键文件, 让别市场继续跑
            print(f'  [{m}] ABORT: {ex.code}')
            exit_code = max(exit_code, 2)
        except Exception as ex:  # noqa: BLE001
            print(f'  [{m}] FAIL: {ex.__class__.__name__}: {ex}')
            import traceback
            traceback.print_exc()
            exit_code = max(exit_code, 1)

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
