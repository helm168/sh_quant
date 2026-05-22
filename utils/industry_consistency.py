"""universe/*.parquet 的 sector/industry 两列一致性 + 填充率验收。

被 scripts/pull_universe.py / pull_us_universe.py / pull_hk_universe.py 复用。

HEAT-5 合同（详见 Billionaire 大盘云图需求）:
- sector / industry 两列名稳定（不写 industry_l2 / gics_industry 之类 alias）。
- industry 必须严格细于 sector：同一 sector 可以有多个 industry，但一个 industry
  不能横跨两个 sector，否则三级 treemap 父子关系崩。
- 两列填充率 ≥ 90%，缺失留 NaN，前端归到 "<sector> · 其它" 桶。
"""

from __future__ import annotations

import sys

import pandas as pd

PLACEHOLDER = '—'
MIN_FILL_PCT = 90.0


def _fill_pct(s: pd.Series) -> float:
    if len(s) == 0:
        return 0.0
    valid = s.notna() & (s.astype(str).str.strip() != '') & (s != PLACEHOLDER)
    return round(valid.sum() * 100.0 / len(s), 1)


def resolve_industry_sector(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """让每个 industry 只挂一个 sector：按 (industry, sector) 频次取多数票。

    df 必须有 sector / industry 两列。返回新 df（不改原 df）。
    """
    if 'sector' not in df.columns or 'industry' not in df.columns:
        sys.exit(f'[{market}] resolve: df 缺 sector/industry 列')

    df = df.copy()
    mask = df['industry'].notna() & df['sector'].notna() & (df['industry'] != PLACEHOLDER)
    sub = df[mask]
    if sub.empty:
        return df

    cross = sub.groupby('industry')['sector'].nunique()
    bad = cross[cross > 1]
    if bad.empty:
        return df

    print(f'[{market}] {len(bad)} 个 industry 横跨多个 sector，按多数票收敛:')
    for ind in bad.index:
        counts = sub[sub['industry'] == ind]['sector'].value_counts()
        winner = counts.idxmax()
        losers = [s for s in counts.index if s != winner]
        print(f'   {ind!r}: 保留 {winner!r}（{counts[winner]} 票），并入 {losers}')
        df.loc[df['industry'] == ind, 'sector'] = winner
    return df


def assert_consistency(df: pd.DataFrame, market: str, min_distinct_industries: int) -> None:
    """build 阶段验收：填充率 + 无 industry 横跨 sector + industry 粒度足够。

    任一条不满足直接 sys.exit（非零退出），让上游 cron / CI 看到红灯。
    """
    if 'sector' not in df.columns or 'industry' not in df.columns:
        sys.exit(f'[{market}] 验收失败: 缺 sector/industry 列')

    sector_pct = _fill_pct(df['sector'])
    industry_pct = _fill_pct(df['industry'])
    print(f'[{market}] 填充率: sector={sector_pct}%, industry={industry_pct}%')
    if sector_pct < MIN_FILL_PCT or industry_pct < MIN_FILL_PCT:
        sys.exit(
            f'[{market}] 填充率 < {MIN_FILL_PCT}%。HEAT-5 合同要求两列都 ≥ 90%，'
            f'否则前端 treemap 出一堆 "—" 污染分桶。',
        )

    # industry 粒度
    valid_ind = df['industry'][df['industry'].notna() & (df['industry'] != PLACEHOLDER)]
    n_distinct = valid_ind.nunique()
    print(f'[{market}] distinct industries: {n_distinct} (要求 ≥ {min_distinct_industries})')
    if n_distinct < min_distinct_industries:
        sys.exit(
            f'[{market}] industry 粒度太粗（{n_distinct} < {min_distinct_industries}）。'
            f'前端要的是 SW L2 / GICS Industry Group 级别，不是 sector 级。',
        )

    # 一致性 check：没有 industry 横跨多个 sector
    mask = df['industry'].notna() & df['sector'].notna() & (df['industry'] != PLACEHOLDER)
    sub = df[mask]
    cross = sub.groupby('industry')['sector'].nunique()
    bad = cross[cross > 1]
    if not bad.empty:
        print(f'[{market}] ✗ industry 横跨多个 sector:')
        for ind in bad.index[:10]:
            sectors = sub[sub['industry'] == ind]['sector'].unique().tolist()
            print(f'   {ind!r} -> {sectors}')
        sys.exit(
            f'[{market}] 三级 treemap 父子关系会崩。先跑 resolve_industry_sector() 收敛。',
        )

    print(f'[{market}] ✓ HEAT-5 合同通过')
