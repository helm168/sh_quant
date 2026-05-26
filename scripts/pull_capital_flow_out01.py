"""OUT01 A 股 IPO 月度募资规模 → capital_flow/out01_ipo.parquet。

PRD 口径(第 5.2 节)
────────────────────
    "当期新股发行实际募集资金" (月度聚合)。

口径计算
─────────
单只 IPO 募资额 = 发行总数(万股) × 发行价格(元) / 10000 = 亿元
按上市日期月末聚合, 全市场月度合计 + 当月 IPO 只数。

数据源: akshare `stock_xgsglb_em(symbol='全部股票')` (东方财富新股申购列表)
────────────────────────────────────────────────────────────────────
单次调用拉全表(实测 3971 行, 覆盖 2010-01 ~ 2026-05 共 16 年完整历史)。
单位验证: 国货航 2024-12-30 / 永兴股份 2024-01-18 算出来 30 亿 / 24 亿与真实
披露募资额吻合, 单位换算正确。

无 token、无配额、单次调用即全量, 跟 OUT03 / IN01 / ENV06 同套路。

落点 / 列契约
─────────────
单文件月度市场聚合 `_data_root()/capital_flow/out01_ipo.parquet`：

    列名                  含义
    ────────────────────────────────────────────
    date                  月末日(datetime, 把上市日期映射到月末)
    ipo_raise_yi          当月 IPO 募资合计(亿元)
    n_ipos                当月 IPO 只数
    avg_raise_yi          当月平均单只募资(亿元/只), 看个体规模偏好

注: date 用月末日(与其他月频指标 ENV06 / IN01 对齐)。

注意点
──────
1. 接口数据含**未来计划上市**的新股(上市日期是已公告但今天还没到的日期),
   这些一并纳入。如果今天 5-26、5-27 有计划上市的, 5 月数据已含——这跟
   IN01 "末月部分数据" 情况类似, 看末月数字时心里有数。
2. 数据已含北交所、科创板、创业板、主板, 全口径无遗漏。

依赖: akshare(已在 venv) / pandas / pyarrow

用法
─────
    python scripts/pull_capital_flow_out01.py             # 全量(单次调用)
    python scripts/pull_capital_flow_out01.py --rebuild   # 删旧重拉
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


CACHE_DIR = _data_root() / 'capital_flow'
OUT_FILE = CACHE_DIR / 'out01_ipo.parquet'


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉 A 股 IPO 月度募资规模 (OUT01) → capital_flow/out01_ipo.parquet'
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

    print('拉 IPO 列表 (akshare stock_xgsglb_em)')
    raw = ak.stock_xgsglb_em(symbol='全部股票')
    if raw is None or raw.empty:
        sys.exit('akshare 返回空, 异常')
    print(f'  原始 {len(raw)} 行')

    # 字段清理
    raw['上市日期'] = pd.to_datetime(raw['上市日期'], errors='coerce')
    raw['发行总数'] = pd.to_numeric(raw['发行总数'], errors='coerce')
    raw['发行价格'] = pd.to_numeric(raw['发行价格'], errors='coerce')

    listed = raw.dropna(subset=['上市日期', '发行总数', '发行价格']).copy()
    print(f'  有上市日期 + 价格 {len(listed)} 行,'
          f' 覆盖 {listed["上市日期"].min().date()} ~ {listed["上市日期"].max().date()}')

    # 募资额: 万股 × 元 / 10000 → 亿元
    listed['raise_yi'] = listed['发行总数'] * listed['发行价格'] / 10000.0

    # 按月末聚合
    listed['month_end'] = listed['上市日期'] + pd.offsets.MonthEnd(0)
    monthly = (
        listed.groupby('month_end', as_index=False)
        .agg(
            ipo_raise_yi=('raise_yi', 'sum'),
            n_ipos=('raise_yi', 'size'),
        )
        .rename(columns={'month_end': 'date'})
    )
    monthly['avg_raise_yi'] = (monthly['ipo_raise_yi'] / monthly['n_ipos']).round(3)
    monthly = monthly.sort_values('date').reset_index(drop=True)

    monthly.to_parquet(OUT_FILE, index=False)

    print('-' * 72)
    print(
        f'写入 {OUT_FILE}  共 {len(monthly)} 行  '
        f'({monthly["date"].min().date()} ~ {monthly["date"].max().date()})'
    )
    # Top5 历史最大月度 IPO (注意会撞 2010-2011 IPO 高峰)
    top = monthly.nlargest(5, 'ipo_raise_yi')
    print('  历史 Top5 月度募资(IPO 高峰):')
    for _, r in top.iterrows():
        print(
            f'    {r["date"].date()}  {r["ipo_raise_yi"]:>7.1f} 亿  '
            f'{int(r["n_ipos"]):>3} 只  '
            f'均规模 {r["avg_raise_yi"]:>5.2f}'
        )
    print('  近 6 月:')
    for _, r in monthly.tail(6).iterrows():
        print(
            f'    {r["date"].date()}  {r["ipo_raise_yi"]:>7.1f} 亿  '
            f'{int(r["n_ipos"]):>3} 只  '
            f'均规模 {r["avg_raise_yi"]:>5.2f}'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
