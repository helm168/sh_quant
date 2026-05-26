"""ENV06 A 股新增投资者月度统计 → capital_flow/env06_new_investors.parquet。

PRD 口径(第 5.3 节)
────────────────────
    "当期新开立 A 股证券账户数(开户数)" —— 散户情绪温度的核心代理。

数据源 / 覆盖断档(重要)
────────────────────────
akshare `stock_account_statistics_em` (东方财富，源自中国结算月报)。
**实测覆盖只到 2023-08**，2023-09 至今数据 akshare 源未更新。可能原因：
  (a) 东财对应页面停更或迁移
  (b) 中国结算披露口径变化(实际 csidc.com 月报仍在更，只是 akshare 没抓)

短期：先落已有 2015-04 ~ 2023-08 共 101 个月度数据，足够回看历史散户情绪
与指数走势关系；长期需要：
  - 监控 akshare 升级
  - 或写补丁脚本爬 csidc.com 月报 PDF(中国结算官方源,但格式不稳)
  - 或换 Wind/Choice(本项目无采购)

落点 / 列契约
─────────────
单文件月度市场聚合 `_data_root()/capital_flow/env06_new_investors.parquet`：

    列名                       含义
    ────────────────────────────────────────────────
    date                       数据月末日(datetime, 把 'YYYY-MM' 转成月末)
    new_investors_wan          当月新增投资者数(万户)
    new_investors_mom          环比(小数, 0.05 = +5%)
    new_investors_yoy          同比(小数)
    total_investors_wan        期末投资者总数(万户)
    a_share_accounts_wan       期末 A 股账户数(万户)
    b_share_accounts_wan       期末 B 股账户数(万户)
    market_cap_yi              沪深总市值(亿元)
    avg_cap_per_household_wan  沪深户均市值(万元)
    sse_close                  上证指数月末收盘
    sse_pct_chg                上证指数月度涨跌幅(%)

注：date 用月末日(非月初)便于跟其他月频指标对齐(M1/M2 等都是月末口径)。

依赖: akshare(已在 venv) / pandas / pyarrow

用法
─────
    python scripts/pull_capital_flow_env06.py            # 全量(无窗口参数,接口一次返回所有)
    python scripts/pull_capital_flow_env06.py --rebuild  # 删旧重拉

注: 该接口一次返回全部历史(2015-04 起),无需增量逻辑,每次都全量覆盖写入。
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
OUT_FILE = CACHE_DIR / 'env06_new_investors.parquet'


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉 A 股新增投资者月度统计 (ENV06) → capital_flow/env06_new_investors.parquet'
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

    print('拉新增投资者月度统计 (akshare stock_account_statistics_em)')
    raw = ak.stock_account_statistics_em()
    if raw is None or raw.empty:
        sys.exit('akshare 返回空,异常')
    print(f'  原始 {len(raw)} 行,{raw["数据日期"].min()} ~ {raw["数据日期"].max()}')

    # 'YYYY-MM' → 月末日 datetime
    date = pd.to_datetime(raw['数据日期'].astype(str) + '-01') + pd.offsets.MonthEnd(0)

    out = pd.DataFrame(
        {
            'date': date,
            'new_investors_wan': pd.to_numeric(raw['新增投资者-数量'], errors='coerce'),
            'new_investors_mom': pd.to_numeric(raw['新增投资者-环比'], errors='coerce'),
            'new_investors_yoy': pd.to_numeric(raw['新增投资者-同比'], errors='coerce'),
            'total_investors_wan': pd.to_numeric(raw['期末投资者-总量'], errors='coerce'),
            'a_share_accounts_wan': pd.to_numeric(raw['期末投资者-A股账户'], errors='coerce'),
            'b_share_accounts_wan': pd.to_numeric(raw['期末投资者-B股账户'], errors='coerce'),
            'market_cap_yi': pd.to_numeric(raw['沪深总市值'], errors='coerce'),
            'avg_cap_per_household_wan': pd.to_numeric(raw['沪深户均市值'], errors='coerce'),
            'sse_close': pd.to_numeric(raw['上证指数-收盘'], errors='coerce'),
            'sse_pct_chg': pd.to_numeric(raw['上证指数-涨跌幅'], errors='coerce'),
        }
    ).sort_values('date').reset_index(drop=True)

    out.to_parquet(OUT_FILE, index=False)

    print('-' * 72)
    print(
        f'写入 {OUT_FILE}  共 {len(out)} 行  '
        f'({out["date"].min().date()} ~ {out["date"].max().date()})'
    )
    # 数据新鲜度提示(若末日落后今天 > 3 个月,提示)
    today = pd.Timestamp.now().normalize()
    last = out['date'].max()
    months_stale = (today.to_period('M') - last.to_period('M')).n
    if months_stale > 3:
        print(
            f'  ⚠ 末日 {last.date()} 已落后今天 {months_stale} 个月 '
            f'—— akshare 源未更新，详见 docstring「覆盖断档」段'
        )
    print(f'  近 6 月新增:')
    for _, r in out.tail(6).iterrows():
        mom = f'{r["new_investors_mom"]*100:+.1f}%' if pd.notna(r['new_investors_mom']) else '  N/A'
        yoy = f'{r["new_investors_yoy"]*100:+.1f}%' if pd.notna(r['new_investors_yoy']) else '  N/A'
        print(
            f'    {r["date"].date()}  新增 {r["new_investors_wan"]:>6.1f} 万户  '
            f'MoM {mom}  YoY {yoy}'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
