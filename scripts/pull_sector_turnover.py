"""按日板块成交额 → `<data_root>/sector_history/<market>_sector_turnover.parquet`。

Billionaire HEAT-8 ③ 板块景气度信号（占比 E + 占比变化 F）需要的中间层
─────────────────────────────────────────────────────────────────
前端 cell 数据只能算「当前样本」的板块占比 E，无法回看历史 → 没法算占比变化
F = 占比(今) − 占比(N 日前)。500 只 × 30 天 × 3 市场拉到浏览器再 groupby 不
现实，所以在 sh_quant 这一层按日预聚合落盘，middleware /api/local/* 直接读。

口径
────
    每只 ts_code 的日线 amount  ×  市场单位换算 (CN: 千元→元；US: USD；HK: HKD)
        ↓ join universe.sector (single source — yaml 改不需要重拉个股)
        ↓ groupby (date, sector)
    → turnover = Σ(member amount)         (停牌/缺值跳过, 不补零)
      members  = 该日有效成员个数         (sanity check)

文件 schema (4 列, 3 市场各一文件):
    <data_root>/sector_history/cn_sector_turnover.parquet
    <data_root>/sector_history/us_sector_turnover.parquet
    <data_root>/sector_history/hk_sector_turnover.parquet

    date     DATE     交易日 YYYY-MM-DD
    sector   TEXT     与 universe.sector 字段**字符相等** (HK/US: 英文 GICS,
                      CN: 申万一级中文) — 否则前端 join 不上
    turnover DOUBLE   板块当日成交额 (元 / USD / HKD)
    members  INT32    当日参与求和的成员个数

数据落点 (与 pull_macro / pull_holders / pull_kpi 完全一致):
    `_data_root()` = $SH_QUANT_DATA_DIR 或 ~/.market_data。**不**写
    PROJECT_ROOT/data_cache —— worktree 跑时会写到 worktree 自己的目录,
    UI (读 ~/.market_data) 读不到。

历史深度
────────
默认 lookback=180 自然日 ≈ 120 交易日 (覆盖前端最长 N=60 + buffer; 注意
spec 「≥90 天」是交易日, 95 自然日只能拿 ~65 交易日, 不够)。每日 cron 跑
完后 parquet 整文件 rewrite (~30 KB US / ~80 KB CN, 全量重算便宜过 append
+ dedupe)。

用法
────
    source .venv/bin/activate
    python scripts/pull_sector_turnover.py                  # 三市场全跑
    python scripts/pull_sector_turnover.py --markets cn,us  # 指定市场
    python scripts/pull_sector_turnover.py --days 180       # 半年窗口
    python scripts/pull_sector_turnover.py --validate-only  # 只跑验收, 不落盘

依赖: pandas / pyarrow (已在 requirements.txt)
环境: 不需要 token, 全部读本地 parquet。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


# market → (universe parquet basename, ts_code 后缀过滤, amount→base-currency 倍率)
#  CN: Tushare amount 单位 千元 → 元 (×1000)
#  US: FMP/Polygon amount 单位 USD 直存 (×1)
#  HK: Futu amount 单位 HKD 直存 (×1) ※ 与需求文档「HK 可能 NULL」相反, 实测
#      stocks/<*.HK>.parquet 是真实成交额, 所以照常产出
MARKET_CONF: dict[str, tuple[str, tuple[str, ...], float]] = {
    'cn': ('cn_a', ('.SH', '.SZ', '.BJ'), 1000.0),
    'us': ('us', ('.US',), 1.0),
    'hk': ('cn_hk', ('.HK',), 1.0),
}


def _load_sector_map(market: str) -> dict[str, str]:
    """ts_code → sector, 跳过空 / `—` / `Unknown` 这种占位."""
    name, _suffixes, _scale = MARKET_CONF[market]
    fp = _data_root() / 'universe' / f'{name}.parquet'
    if not fp.exists():
        sys.exit(f'[ABORT] universe parquet 不存在: {fp}\n  先跑对应 pull_*_universe.py')
    u = pd.read_parquet(fp, columns=['ts_code', 'sector'])
    u = u.dropna(subset=['sector'])
    u = u[~u['sector'].isin(['—', '', 'Unknown'])]
    return dict(zip(u['ts_code'], u['sector']))


def build_market(market: str, lookback_days: int) -> pd.DataFrame:
    name, _suffixes, scale = MARKET_CONF[market]
    sector_map = _load_sector_map(market)
    if not sector_map:
        sys.exit(f'[ABORT] universe/{name}.parquet 全部 sector 为空, '
                 '先回填 universe 的 sector 列 (HEAT-5 已就绪)')

    cutoff = (pd.Timestamp.today().normalize() - pd.Timedelta(days=lookback_days))
    stocks_dir = _data_root() / 'stocks'

    frames: list[pd.DataFrame] = []
    miss = 0
    for ts_code, sector in sector_map.items():
        fp = stocks_dir / f'{ts_code}.parquet'
        if not fp.exists():
            miss += 1
            continue
        df = pd.read_parquet(fp, columns=['trade_date', 'amount'])
        df = df[(df['trade_date'] >= cutoff) & df['amount'].notna() & (df['amount'] > 0)]
        if df.empty:
            continue
        df = df.assign(sector=sector)
        frames.append(df)

    if not frames:
        sys.exit(f'[ABORT] {market}: 没拉到任何有效个股 amount (universe {len(sector_map)} 只, '
                 f'缺 parquet {miss} 只). 先跑 update_daily.py 或 pull_hk_futu.py')

    big = pd.concat(frames, ignore_index=True)
    big['amount'] = big['amount'] * scale

    grp = (
        big.groupby([big['trade_date'].dt.normalize().rename('date'), 'sector'], as_index=False)
        .agg(turnover=('amount', 'sum'), members=('amount', 'size'))
    )
    grp['date'] = grp['date'].dt.date  # 落盘成 DATE 而非 timestamp
    grp['members'] = grp['members'].astype('int32')
    grp = grp.sort_values(['date', 'sector']).reset_index(drop=True)

    print(f'[{market}] universe={len(sector_map)} miss_parquet={miss} '
          f'rows={len(grp)} dates={grp["date"].nunique()} '
          f'sectors={grp["sector"].nunique()} '
          f'span={grp["date"].min()}..{grp["date"].max()}')
    return grp


def _write(market: str, df: pd.DataFrame) -> Path:
    out_dir = _data_root() / 'sector_history'
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / f'{market}_sector_turnover.parquet'
    df.to_parquet(out_fp, compression='zstd', index=False)
    print(f'  → wrote {out_fp} ({out_fp.stat().st_size / 1024:.1f} KB)')
    return out_fp


def _validate(market: str, df: pd.DataFrame) -> list[str]:
    """post-write 验收, 返回错误清单 (空 = 通过)."""
    errs: list[str] = []
    expected_cols = ['date', 'sector', 'turnover', 'members']
    if list(df.columns) != expected_cols:
        errs.append(f'cols mismatch: got {list(df.columns)} want {expected_cols}')

    n_dates = df['date'].nunique()
    if n_dates < 90:
        errs.append(f'date count too low: {n_dates} 交易日 < 90 (spec hard floor); '
                    f'调大 --days 或检查 stocks/*.parquet 深度')

    # sector set 与 universe 等价 (允许 history 是 universe 的子集 —— 某些 sector
    # 在窗口内全员停牌, 但反方向不许)
    name, *_ = MARKET_CONF[market]
    u = pd.read_parquet(_data_root() / 'universe' / f'{name}.parquet', columns=['sector'])
    u_set = set(u['sector'].dropna()) - {'—', '', 'Unknown'}
    h_set = set(df['sector'].unique())
    in_h_not_u = h_set - u_set
    if in_h_not_u:
        errs.append(f'sector 在 history 但不在 universe (前端 join 漏): {sorted(in_h_not_u)}')

    # sanity: 最后一日 turnover 合计落在合理量级
    last = df[df['date'] == df['date'].max()]
    total = last['turnover'].sum()
    print(f'  validate[{market}]: last_day={last["date"].iloc[0]} '
          f'sectors_on_day={len(last)} total_turnover={total:.3e}')
    return errs


def main() -> int:
    p = argparse.ArgumentParser(description='按日板块成交额预聚合 (HEAT-8 ③)')
    p.add_argument('--markets', default='cn,us,hk',
                   help='逗号分隔 (cn,us,hk), 默认全部')
    p.add_argument('--days', type=int, default=180,
                   help='lookback 自然日, 默认 180 ≈ 120 交易日 (覆盖 N=60 + buffer)')
    p.add_argument('--validate-only', action='store_true',
                   help='只对已落盘文件跑验收, 不重建')
    args = p.parse_args()

    markets = [m.strip().lower() for m in args.markets.split(',') if m.strip()]
    unknown = [m for m in markets if m not in MARKET_CONF]
    if unknown:
        sys.exit(f'未知 market: {unknown}, 可选 {list(MARKET_CONF)}')

    print(f'data_root = {_data_root()}')

    exit_code = 0
    for m in markets:
        print(f'\n=== {m} ===')
        try:
            if args.validate_only:
                fp = _data_root() / 'sector_history' / f'{m}_sector_turnover.parquet'
                if not fp.exists():
                    print(f'  [SKIP] {fp} 不存在')
                    exit_code = 2
                    continue
                df = pd.read_parquet(fp)
            else:
                df = build_market(m, args.days)
                _write(m, df)
            errs = _validate(m, df)
            if errs:
                exit_code = 2
                for e in errs:
                    print(f'  [VALIDATE FAIL] {e}')
            else:
                print('  validate: OK')
        except SystemExit:
            raise
        except Exception as ex:  # noqa: BLE001
            exit_code = 1
            print(f'  [ERROR] {m}: {ex}')

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
