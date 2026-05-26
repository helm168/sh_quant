"""IN01 偏股基金月度新发行规模 → capital_flow/in01_new_fund_issuance.parquet。

PRD 口径(第 5.1 节)
────────────────────
    "当期新成立的普通股票型 + 偏股混合型基金募集份额/金额" (月度聚合)。

口径选择(v1)
─────────────
基金类型 ∈ {普通股票型, 偏股混合型, 指数型-股票} 三类：
  • 股票型(214)
  • 混合型-偏股(1587)
  • 指数型-股票(2053)
合计 3854 只基金。排除:
  • QDII / 海外股票指数(海外口径, 不算 A 股资金面)
  • 灵活配置 / 平衡型(股债皆可, 不算纯偏股)
  • 债券型 / FOF / 货币型 / 混合偏债

份额 vs 金额近似
─────────────────
akshare 给的是"募集份额"(亿份)。新基金面值 1 元/份, 所以 1 亿份 ≈ 1 亿元。
PRD 想要的是"金额", 这里用份额近似(场外公募实务中两者数量级几乎相等)。
若后续要严格金额, 改用 `tushare fund_basic.issue_amount`(probe 过基础会员可拉)。

数据源: akshare `fund_new_found_em` (东方财富, 全表无窗口参数)
────────────────────────────────────────────────────────────
**实际**只完整覆盖 2023-01 ~ 至今(~3.5 年)。早期年份(2013、2018)零星 1-2 行
形同无, 不是真历史。docstring 顶部别误以为有 13 年——之前 probe 的 6169 行
里 5167 行集中在 2023~2026。若要更长历史, 需换 csidc 月报 PDF / Wind / Choice。

末月数据警示
────────────
groupby month_end 会把"成立日期=本月某天"的所有基金归到月末。但 akshare 只
返回到「最新成立日」, 不到月底——例如今天 2026-05-26 拉, 5 月数据只统计了
5-01~5-20 间成立的基金。看末月数字低于趋势可能是"本月还没结束"而非"发行真低"。

跟 OUT03 同套路: 一次拉全, 不分页, 无 token 无配额。

落点 / 列契约
─────────────
单文件月度市场聚合 `_data_root()/capital_flow/in01_new_fund_issuance.parquet`：

    列名                    含义
    ────────────────────────────────────────────────
    date                    月末日(datetime, 把成立日期映射到月末)
    issue_amount_yi         当月偏股新发份额合计(亿份 ≈ 亿元)
    n_funds                 当月新成立基金数(只)
    issue_per_fund_yi       平均单只规模(亿份/只), 看个性化卖力度

注: date 用月末日(与 ENV06 / macro M1/M2 月频指标对齐)。

依赖: akshare(已在 venv) / pandas / pyarrow

用法
─────
    python scripts/pull_capital_flow_in01.py             # 全量(单次调用, 无窗口)
    python scripts/pull_capital_flow_in01.py --rebuild   # 删旧重拉

注: 接口一次返回全部历史, 无需增量逻辑, 每次跑都全量覆盖写入。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 偏股新发的「基金类型」白名单(PRD 口径)
EQUITY_TYPES = {
    '股票型',
    '混合型-偏股',
    '指数型-股票',
}


def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR = _data_root() / 'capital_flow'
OUT_FILE = CACHE_DIR / 'in01_new_fund_issuance.parquet'


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉偏股基金月度新发行规模 (IN01) → capital_flow/in01_new_fund_issuance.parquet'
    )
    ap.add_argument('--rebuild', action='store_true', help='删旧 parquet 重拉')
    args = ap.parse_args()

    try:
        import akshare as ak
    except ImportError:
        sys.exit('akshare 没装。pip install akshare')

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if args.rebuild and OUT_FILE.exists():
        OUT_FILE.unlink()
        print(f'--rebuild: 删除旧 {OUT_FILE.name}')

    print('拉新发基金 (akshare fund_new_found_em)')
    raw = ak.fund_new_found_em()
    if raw is None or raw.empty:
        sys.exit('akshare 返回空, 异常')
    print(f'  原始 {len(raw)} 行,覆盖 {raw["成立日期"].min()} ~ {raw["成立日期"].max()}')

    # 过滤偏股
    equity = raw[raw['基金类型'].isin(EQUITY_TYPES)].copy()
    print(f'  偏股过滤后 {len(equity)} 行 (白名单: {sorted(EQUITY_TYPES)})')

    # 字段清理
    equity['date'] = pd.to_datetime(equity['成立日期'], errors='coerce')
    equity['issue_amount'] = pd.to_numeric(equity['募集份额'], errors='coerce')
    equity = equity.dropna(subset=['date', 'issue_amount'])

    # 按月末聚合
    equity['month_end'] = equity['date'] + pd.offsets.MonthEnd(0)
    monthly = (
        equity.groupby('month_end', as_index=False)
        .agg(
            issue_amount_yi=('issue_amount', 'sum'),
            n_funds=('issue_amount', 'size'),
        )
        .rename(columns={'month_end': 'date'})
    )
    monthly['issue_per_fund_yi'] = (
        monthly['issue_amount_yi'] / monthly['n_funds']
    ).round(3)
    monthly = monthly.sort_values('date').reset_index(drop=True)

    monthly.to_parquet(OUT_FILE, index=False)

    print('-' * 72)
    print(
        f'写入 {OUT_FILE}  共 {len(monthly)} 行  '
        f'({monthly["date"].min().date()} ~ {monthly["date"].max().date()})'
    )
    # 历史峰值 + 近 6 月
    top = monthly.nlargest(5, 'issue_amount_yi')
    print('  历史 Top5 月度新发(经典阶段顶信号):')
    for _, r in top.iterrows():
        print(
            f'    {r["date"].date()}  {r["issue_amount_yi"]:>8.1f} 亿份  '
            f'{int(r["n_funds"]):>3} 只'
        )
    print('  近 6 月:')
    for _, r in monthly.tail(6).iterrows():
        print(
            f'    {r["date"].date()}  {r["issue_amount_yi"]:>8.1f} 亿份  '
            f'{int(r["n_funds"]):>3} 只  '
            f'均规模 {r["issue_per_fund_yi"]:>5.2f}'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
