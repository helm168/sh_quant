"""OUT03 A 股限售解禁市值(按解禁日聚合) → capital_flow/out03_share_float.parquet。

PRD 口径(第 5.2 节)
────────────────────
    "当期解禁市值(潜在抛压，非实际流出，需单独标注口径)"

数据源：akshare → 东方财富 (Tushare share_float 弃用)
────────────────────────────────────────────────────
原本走 Tushare `pro.share_float` + 自己估市值。实测 Tushare 基础会员对该接口
有**日级配额墙**(不只是分钟级限频)，5 段 30→60→120→240→480s 退避都打不动，
昨天的 smoke + 今早的 parallel 失败重试 + 早些时候 10 个 chunk 加起来就把当日
余额吃光，第 11 个 chunk 起持续回 "查询数据失败，请确认参数"。

akshare 的 `stock_restricted_release_summary_em` 是东方财富同口径数据，且：
1) 一次调用拉任意窗口(实测 6 个月 117 行 / 1.5 年 349 行，无翻页)
2) 自带 "实际解禁数量" + "实际解禁市值"(已扣锁仓部分)，无需自估
3) 无 token 无配额
4) 历史 + 未来一锅端，沪深300指数 bonus 列(未来段 NaN)

落点 / 列契约
─────────────
单文件市场聚合 `_data_root()/capital_flow/out03_share_float.parquet`：

    列名                  含义
    ─────────────────────────────────────────────
    date                  解禁日(datetime)
    unlock_value_yi       当日实际解禁市值(亿元，akshare "实际解禁市值"/1e8)
    unlock_share_wan      当日实际解禁数量(万股，akshare "实际解禁数量"/1e4)
    unlock_share_total_wan 当日理论解禁数量(万股，含锁仓不实际解禁部分)
    n_stocks              当日解禁股票家数
    hs300_close           当日沪深 300 收盘(历史段)，未来段 NaN
    hs300_pct_chg         当日沪深 300 涨跌幅，未来段 NaN

注：akshare 的"实际解禁市值"按解禁当日 close 估，**历史段**是已实现市值
(PIT 友好)，**未来段**是按最近交易日 close 滚动估(每跑一次会刷新)。这跟旧
Tushare 路径"统一按 latest close 估"不同，更符合 PRD "潜在抛压" 语义。

落点 trap (与 pull_macro/pull_holders 一致)
────────────────────────────────────────
写到 `_data_root()/capital_flow/`，_data_root = $SH_QUANT_DATA_DIR 或
~/.market_data。**不**用 PROJECT_ROOT/data_cache(worktree 里跑会写错地方)。

依赖：akshare(已在 venv) / pandas / pyarrow

用法
─────
    python scripts/pull_capital_flow_out03.py             # 默认回填 [today-3y, today+1y]
    python scripts/pull_capital_flow_out03.py --start 20180101 --end 20271231
    python scripts/pull_capital_flow_out03.py --rebuild   # 删旧重拉(单次调用本就是全量)

注：share_float 是前瞻披露+事后修订都会变(股东减持公告会改解禁数量/日期)。
该接口单次调用即全量覆盖窗口，每次跑都自动获取最新修订，不需要增量逻辑。
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
OUT_FILE = CACHE_DIR / 'out03_share_float.parquet'


CHUNK_MONTHS = 6  # akshare 该接口单次返回硬上限 ~500 行(实测 4 年窗口被截到 500),
                  # 按 6 个月切足够安全(单 6 月窗口实测 117 行,远低于 500)


def _month_chunks(start: str, end: str, months: int) -> list[tuple[str, str]]:
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    out: list[tuple[str, str]] = []
    cur = s
    while cur <= e:
        nxt = min(cur + pd.DateOffset(months=months) - pd.Timedelta(days=1), e)
        out.append((cur.strftime('%Y%m%d'), nxt.strftime('%Y%m%d')))
        cur = nxt + pd.Timedelta(days=1)
    return out


def fetch_summary(start: str, end: str) -> pd.DataFrame:
    """按 CHUNK_MONTHS 月切片拉取(规避 ~500 行硬上限)，concat 去重返回。"""
    try:
        import akshare as ak
    except ImportError:
        sys.exit('akshare 没装。pip install akshare')

    chunks = _month_chunks(start, end, CHUNK_MONTHS)
    parts: list[pd.DataFrame] = []
    for i, (s, e) in enumerate(chunks, 1):
        df = ak.stock_restricted_release_summary_em(
            symbol='全部股票', start_date=s, end_date=e
        )
        n = 0 if df is None else len(df)
        print(f'  [{i:>2}/{len(chunks)}] {s} ~ {e}  {n:>4} 行')
        if df is not None and len(df) > 0:
            if len(df) >= 500:
                print(f'    ⚠ 该 chunk 行数 {len(df)} ≥ 500，可能被截，考虑改小 CHUNK_MONTHS')
            parts.append(df)
    if not parts:
        sys.exit(f'akshare 全窗口返回空 ({start} ~ {end})')
    out = pd.concat(parts, ignore_index=True)
    # chunk 边界可能重叠或东财对同一日返回不同快照,按日期去重保留最后一份
    out = out.drop_duplicates(subset='解禁时间', keep='last')
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉 A 股限售解禁市值聚合 (OUT03) → capital_flow/out03_share_float.parquet'
    )
    today = pd.Timestamp.now().normalize()
    ap.add_argument(
        '--start',
        default=(today - pd.Timedelta(days=365 * 3)).strftime('%Y%m%d'),
        help='起点(默认 today-3y)',
    )
    ap.add_argument(
        '--end',
        default=(today + pd.Timedelta(days=365)).strftime('%Y%m%d'),
        help='终点(默认 today+1y)',
    )
    ap.add_argument('--rebuild', action='store_true', help='删旧 parquet 重拉')
    args = ap.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if args.rebuild and OUT_FILE.exists():
        OUT_FILE.unlink()
        print(f'--rebuild: 删除旧 {OUT_FILE.name}')

    print(f'拉限售解禁聚合 {args.start} ~ {args.end} (akshare stock_restricted_release_summary_em)')
    raw = fetch_summary(args.start, args.end)
    print(f'  原始 {len(raw)} 行,覆盖 {raw["解禁时间"].min()} ~ {raw["解禁时间"].max()}')

    # 字段映射(顺手做单位换算 元→亿元 / 股→万股)
    out = pd.DataFrame(
        {
            'date': pd.to_datetime(raw['解禁时间']),
            'unlock_value_yi': pd.to_numeric(raw['实际解禁市值'], errors='coerce') / 1e8,
            'unlock_share_wan': pd.to_numeric(raw['实际解禁数量'], errors='coerce') / 1e4,
            'unlock_share_total_wan': pd.to_numeric(raw['解禁数量'], errors='coerce') / 1e4,
            'n_stocks': pd.to_numeric(raw['当日解禁股票家数'], errors='coerce').astype('Int64'),
            'hs300_close': pd.to_numeric(raw['沪深300指数'], errors='coerce'),
            'hs300_pct_chg': pd.to_numeric(raw['沪深300指数涨跌幅'], errors='coerce'),
        }
    ).sort_values('date').reset_index(drop=True)

    out.to_parquet(OUT_FILE, index=False)

    print('-' * 72)
    print(
        f'写入 {OUT_FILE}  共 {len(out)} 行  '
        f'({out["date"].min().date()} ~ {out["date"].max().date()})'
    )
    today_ts = pd.Timestamp.now().normalize()
    past = out[out['date'] <= today_ts]
    future = out[out['date'] > today_ts]
    if len(past) > 0:
        print(
            f'  历史段 {len(past)} 天 / 总解禁市值 {past["unlock_value_yi"].sum():>9.0f} 亿  '
            f'(单日均 {past["unlock_value_yi"].mean():>6.1f} 亿)'
        )
    if len(future) > 0:
        print(
            f'  未来段 {len(future)} 天 / 总潜在抛压 {future["unlock_value_yi"].sum():>9.0f} 亿  '
            f'(单日均 {future["unlock_value_yi"].mean():>6.1f} 亿)'
        )
        top5 = future.nlargest(5, 'unlock_value_yi')
        print('  未来 Top5 解禁日:')
        for _, r in top5.iterrows():
            print(
                f'    {r["date"].date()}  {r["unlock_value_yi"]:>7.0f} 亿  '
                f'{int(r["n_stocks"]):>3} 只股'
            )
    return 0


if __name__ == '__main__':
    sys.exit(main())
