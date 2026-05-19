"""拉港股财务数据 → data_cache/financials/<ts_code>.parquet（统一 schema）。

为什么是独立脚本（不并进 pull_financials.py）
─────────────────────────────────────────────
pull_financials.py（A 股 Tushare / 美股 FMP）是「每 ticker 一个线程」模型。
港股唯一可行源 Futu `get_stock_filter` 是**全市场批量**接口（按市值翻页返回
整个 HK 市场），逐只调 2700+ 次会被限速拖到几小时。批量 vs 逐只不兼容，
所以单独成脚本 —— 跟 pull_hk_futu.py 独立于 pull_theme_stocks.py 同理。
产物落同一个 data_cache/financials/，统一 schema 消费方不关心谁写的。

数据源 / 边界（2026-05-19 实测，详见 probe_futu_hk_financials.py）
──────────────────────────────────────────────────────────────
FMP 当前档对 HK 返 402、Tushare 无 hk 报表接口 → 只剩 Futu。Futu 给的是
**snapshot**（最新年报 FY + 最新中期 H1），不是可回测的财报历史时间序列：

  - get_stock_filter FinancialFilter：必须 is_no_filter=False + 超宽 range
    才回值（is_no_filter=True 会 KeyError，Futu 坑）。副作用：某字段为 NULL
    的股票会被该条 range 排除 → 拿到的是「核心财务齐全」的可筛选子集。
  - get_market_snapshot：补 net_asset（≈total_equity 绝对值），配 filter 的
    DEBT_ASSET_RATE 反推 total_assets / total_liabilities。snapshot 是
    point-in-time 最新值，无 FY/H1 归属（FY 与 H1 行复用同一 snapshot）。
  - **报告期无源**：Futu 任何接口都不返 HK 财报 end_date（get_rehab /
    get_financial_unusual / snapshot 都查过）。故 end_date=NaT，
    ann_date=as_of(运行日)，period 用请求粒度 FY/H1。**这不是历史回测数据。**
  - 真·永久 NaN：pretax_income / long_term_debt / short_term_debt /
    investing_cf / financing_cf / free_cash_flow / capex（Futu 无对应源）。
    operating_income / operating_cf / roa 仅 FY（H1 的 *_TTM 字段 Futu 空）。

前置
────
  1. cn_hk.parquet 已生成（python scripts/pull_hk_universe.py）
  2. FutuOpenD 已启动并登录，HK Lv1 已开通（lsof -i :11111）

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/pull_hk_financials.py                 # 全 HK universe
    python scripts/pull_hk_financials.py --min-mv 50     # 只跑市值 >= 50 亿港元
    python scripts/pull_hk_financials.py --tickers 00700.HK,09988.HK   # 调试
    python scripts/pull_hk_financials.py --force         # 覆盖已有缓存

输出
────
    data_cache/financials/<ts_code>.parquet，每只 1-2 行（period=FY / H1），
    列 = pull_financials.py 的 COMMON_COLS（缺失列 NaN）。
"""

from __future__ import annotations

import argparse
import contextlib
import sys
import time
from pathlib import Path

import pandas as pd

try:
    from futu import (
        RET_OK,
        FinancialFilter,
        FinancialQuarter,
        Market,
        OpenQuoteContext,
        SimpleFilter,
        SortDir,
        StockField,
    )
except ImportError:
    sys.exit('futu-api 没装. 跑: pip install futu-api')

PROJECT_ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_FILE = PROJECT_ROOT / 'data_cache' / 'universe' / 'cn_hk.parquet'
CACHE_DIR_FIN = PROJECT_ROOT / 'data_cache' / 'financials'

HOST = '127.0.0.1'
PORT = 11111
PAGE_SIZE = 200
MAX_PAGES = 60
# get_stock_filter 限速 10 次/30s → 页间 3.5s（沿用 pull_hk_universe.py）
SLEEP_SEC = 3.5
SNAPSHOT_BATCH = 350  # get_market_snapshot 单次 <= 400

# 必须与 pull_financials.py 的 COMMON_COLS 完全一致（统一 schema 契约，
# 故意复制而非跨 script import —— scripts/ 之间不互相 import）
COMMON_COLS = [
    'ann_date',
    'end_date',
    'period',
    'fiscal_year',
    'currency',
    'revenue',
    'gross_profit',
    'operating_income',
    'pretax_income',
    'net_income',
    'eps_basic',
    'eps_diluted',
    'total_assets',
    'total_liabilities',
    'total_equity',
    'cash_and_equivalents',
    'long_term_debt',
    'short_term_debt',
    'operating_cf',
    'investing_cf',
    'financing_cf',
    'free_cash_flow',
    'capex',
    'roe',
    'roa',
    'gross_margin',
    'net_margin',
    'debt_to_equity',
    'current_ratio',
]

# ─── Futu StockField → 统一 schema 字段 ──────────────────────────────────
# 绝对值字段：一只正常公司这些会同时有，分一批（is_no_filter=False 的 range
# 是 AND，缺任一字段的股票整批掉出 → 拿到的就是核心财务齐全的可筛子集）
ABS_FIELDS = {
    'revenue': StockField.SUM_OF_BUSINESS,
    'net_income': StockField.NET_PROFIT,
    'cash_and_equivalents': StockField.CASH_AND_CASH_EQUIVALENTS,
    'eps_basic': StockField.BASIC_EPS,
    'eps_diluted': StockField.DILUTED_EPS,
}
# 比率字段：Futu 给的是百分数（56.21 = 56.21%），与 Tushare/FMP 的 %
# 约定一致 —— 唯独 current_ratio 在统一 schema 里是倍数（1.44），需 /100
RATIO_FIELDS = {
    'gross_margin': StockField.GROSS_PROFIT_RATE,
    'net_margin': StockField.NET_PROFIT_RATE,
    'roe': StockField.RETURN_ON_EQUITY_RATE,
    # 注：与 pull_financials A 股侧一致 —— debt_to_equity 实际装的是资产负债率
    'debt_to_equity': StockField.DEBT_ASSET_RATE,
    'current_ratio': StockField.CURRENT_RATIO,
}
# 仅 ANNUAL 有值（对应 *_TTM，Futu 在 INTERIM 全空）
FY_ONLY_FIELDS = {
    'operating_income': StockField.OPERATING_PROFIT_TTM,
    'roa': StockField.ROA_TTM,
    'operating_cf': StockField.OPERATING_CASH_FLOW_TTM,
}


def make_mc_floor(min_mv_hkd: float) -> SimpleFilter:
    f = SimpleFilter()
    f.stock_field = StockField.MARKET_VAL
    f.filter_min = min_mv_hkd
    f.is_no_filter = False
    f.sort = SortDir.DESCEND
    return f


def make_fin_filters(fields: dict, quarter) -> list:
    out = []
    for sf in fields.values():
        ff = FinancialFilter()
        ff.stock_field = sf
        ff.is_no_filter = False  # True 会 KeyError（Futu 坑）
        ff.filter_min = -1e18  # 超宽 = 等效不筛（但 NULL 值股仍被排除）
        ff.filter_max = 1e18
        ff.quarter = quarter
        out.append(ff)
    return out


def pull_filter_batch(
    ctx: OpenQuoteContext, mc: SimpleFilter, fields: dict, quarter
) -> dict[str, dict]:
    """翻页拉一批字段，回 {symbol(00700): {col: value}}。"""
    keys = list(fields.keys())
    ffs = make_fin_filters(fields, quarter)
    acc: dict[str, dict] = {}
    begin = 0
    for _ in range(MAX_PAGES):
        ret, data = ctx.get_stock_filter(
            market=Market.HK,
            filter_list=[mc, *ffs],
            begin=begin,
            num=PAGE_SIZE,
        )
        if ret != RET_OK:
            raise RuntimeError(f'get_stock_filter failed begin={begin}: {data}')
        last_page, _all_count, ret_list = data
        if not ret_list:
            break
        for item in ret_list:
            symbol = item.stock_code.split('.')[1]  # HK.00700 → 00700
            row = acc.setdefault(symbol, {})
            for col, ff in zip(keys, ffs, strict=True):
                with contextlib.suppress(KeyError, TypeError):
                    row[col] = item[ff]
        if last_page:
            break
        begin += PAGE_SIZE
        time.sleep(SLEEP_SEC)
    return acc


def pull_snapshot_equity(ctx: OpenQuoteContext, codes: list[str]) -> dict[str, float]:
    """get_market_snapshot 取 net_asset(≈total_equity 绝对值)。回 {symbol: equity}。"""
    out: dict[str, float] = {}
    for i in range(0, len(codes), SNAPSHOT_BATCH):
        batch = codes[i : i + SNAPSHOT_BATCH]
        ret, snap = ctx.get_market_snapshot(batch)
        if ret != RET_OK or snap is None or 'net_asset' not in snap.columns:
            continue
        for _, r in snap.iterrows():
            sym = str(r['code']).split('.')[1]
            try:
                v = float(r['net_asset'])
                out[sym] = v if v != 0 else float('nan')
            except (TypeError, ValueError):
                pass
        time.sleep(1.0)
    return out


def build_rows(
    symbol: str,
    fy: dict,
    h1: dict,
    equity: float | None,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """一只股票 → 1-2 行（FY / H1）的 COMMON_COLS DataFrame。"""
    rows = []
    for period, src in (('FY', fy), ('H1', h1)):
        if not src:
            continue
        r = {c: None for c in COMMON_COLS}
        r['period'] = period
        r['ann_date'] = as_of  # 报告期无源，as_of = 运行日
        r['end_date'] = pd.NaT
        r['fiscal_year'] = pd.NA
        r['currency'] = None  # Futu 不返报告币种（腾讯报 RMB、港交所报 HKD）

        for col in (*ABS_FIELDS, *RATIO_FIELDS, *FY_ONLY_FIELDS):
            if col in src and src[col] is not None:
                r[col] = src[col]

        # current_ratio: Futu 给 % (144.3) → 统一 schema 是倍数 (1.443)
        if r['current_ratio'] is not None:
            r['current_ratio'] = r['current_ratio'] / 100.0

        # 金融/交易所类无 COGS，Futu GROSS_PROFIT_RATE 返 0 是「不适用」哨兵，
        # 不是真 0% 毛利 → 留 None，别让下游把 N/A 当 0 排名
        if r['gross_margin'] == 0:
            r['gross_margin'] = None

        # gross_profit 派生 = revenue * gross_margin%/100
        if r['revenue'] is not None and r['gross_margin'] is not None:
            r['gross_profit'] = r['revenue'] * r['gross_margin'] / 100.0

        # 资产负债表绝对额：snapshot net_asset = total_equity（point-in-time，
        # FY/H1 共用），配 debt_to_equity(实为资产负债率%) 反推 assets/liab
        if equity is not None and equity == equity:  # not NaN
            r['total_equity'] = equity
            dar = r['debt_to_equity']
            if dar is not None and 0 <= dar < 100:
                assets = equity / (1.0 - dar / 100.0)
                r['total_assets'] = assets
                r['total_liabilities'] = assets - equity

        rows.append(r)

    df = pd.DataFrame(rows, columns=COMMON_COLS)
    return df


def load_target_symbols(tickers: str | None, min_mv: float) -> set[str] | None:
    """返回要保留的 symbol 集合（00700 形式）；None = 不限（全市场）。

    --tickers 显式指定时只跑这些。否则用 cn_hk.parquet 排除 RMB 双柜台后
    的全集做白名单（市值地板由 get_stock_filter 的 min_mv 在拉取时控制）。
    """
    if tickers:
        return {t.strip().split('.')[0].zfill(5) for t in tickers.split(',') if t.strip()}
    if not UNIVERSE_FILE.exists():
        sys.exit(f'{UNIVERSE_FILE} 不存在，先跑 python scripts/pull_hk_universe.py')
    uni = pd.read_parquet(UNIVERSE_FILE)
    uni = uni[uni['board'] != 'RMB_COUNTER']  # 双柜台与主柜台值重复，排除
    if min_mv > 0 and 'market_cap' in uni.columns:
        uni = uni[uni['market_cap'].fillna(0) >= min_mv]
    return set(uni['symbol'].astype(str).tolist())


def main() -> int:
    ap = argparse.ArgumentParser(description='拉港股财务（Futu snapshot，统一 schema）')
    ap.add_argument('--tickers', help='逗号分隔 ts_code（如 00700.HK,09988.HK），调试用')
    ap.add_argument(
        '--min-mv',
        type=float,
        default=0.0,
        help='最低市值（亿港元）；0 = 全集。同时作为 get_stock_filter 拉取地板',
    )
    ap.add_argument('--force', action='store_true', help='覆盖已有缓存')
    args = ap.parse_args()

    targets = load_target_symbols(args.tickers, args.min_mv)
    print(f'目标 {len(targets) if targets else "全市场"} 只 → {CACHE_DIR_FIN.relative_to(PROJECT_ROOT)}/')

    print(f'[Futu] connecting {HOST}:{PORT} ...')
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(f'OpenQuoteContext 失败: {e}\n→ OpenD 启动了吗? lsof -i :{PORT}')

    as_of = pd.Timestamp.today().normalize()
    try:
        mc = make_mc_floor(args.min_mv * 1e8)

        print('拉 ANNUAL (→ FY): 绝对值批 ...')
        fy = pull_filter_batch(ctx, mc, ABS_FIELDS, FinancialQuarter.ANNUAL)
        time.sleep(SLEEP_SEC)
        print('拉 ANNUAL: 比率批 ...')
        _merge(fy, pull_filter_batch(ctx, mc, RATIO_FIELDS, FinancialQuarter.ANNUAL))
        time.sleep(SLEEP_SEC)
        print('拉 ANNUAL: FY-only 批 (operating/roa/cf) ...')
        _merge(fy, pull_filter_batch(ctx, mc, FY_ONLY_FIELDS, FinancialQuarter.ANNUAL))
        time.sleep(SLEEP_SEC)

        print('拉 INTERIM (→ H1): 绝对值批 ...')
        h1 = pull_filter_batch(ctx, mc, ABS_FIELDS, FinancialQuarter.INTERIM)
        time.sleep(SLEEP_SEC)
        print('拉 INTERIM: 比率批 ...')
        _merge(h1, pull_filter_batch(ctx, mc, RATIO_FIELDS, FinancialQuarter.INTERIM))

        all_syms = set(fy) | set(h1)
        if targets is not None:
            all_syms &= targets
        if not all_syms:
            sys.exit('无任何股票返回财务数据，检查 OpenD 登录 / HK Lv1 / 市值地板')

        print(f'拉 snapshot net_asset ({len(all_syms)} 只) ...')
        equity = pull_snapshot_equity(ctx, [f'HK.{s}' for s in sorted(all_syms)])
    finally:
        with contextlib.suppress(Exception):
            ctx.close()

    CACHE_DIR_FIN.mkdir(parents=True, exist_ok=True)
    ok = skip = empty = 0
    for sym in sorted(all_syms):
        ts_code = f'{sym}.HK'
        fp = CACHE_DIR_FIN / f'{ts_code}.parquet'
        if fp.exists() and not args.force:
            skip += 1
            continue
        df = build_rows(sym, fy.get(sym, {}), h1.get(sym, {}), equity.get(sym), as_of)
        if df.empty:
            empty += 1
            continue
        df.to_parquet(fp, index=False, compression='snappy')
        ok += 1

    print('-' * 60)
    print(f'完成: {ok} 写入 / {skip} 跳过(已存在) / {empty} 空, as_of={as_of.date()}')
    if ok:
        sample = sorted(all_syms)[0]
        print(f'\n抽样 {sample}.HK:')
        print(
            pd.read_parquet(CACHE_DIR_FIN / f'{sample}.HK.parquet')[
                ['period', 'revenue', 'net_income', 'roe', 'gross_margin', 'total_equity']
            ].to_string(index=False)
        )
    return 0 if (ok or skip) else 2


def _merge(base: dict[str, dict], extra: dict[str, dict]) -> None:
    for sym, vals in extra.items():
        base.setdefault(sym, {}).update(vals)


if __name__ == '__main__':
    sys.exit(main())
