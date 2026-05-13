"""拉所有股票的季度财务数据（利润表 + 资产负债表 + 现金流 + 财务指标）。

输出到 data_cache/financials/<ts_code>.parquet，每只股票一份文件，行 = 报告期。

为什么需要这个
─────────────
sh_quant 现有的 stocks/*.parquet 只有 OHLC + adj_factor，没有任何财务数据。
LocalDataProvider（Billionaire 用）需要 ROE、净利润、毛利率等指标来跑价值筛选。
这个脚本是 LocalDataProvider 可工作的前置条件。

数据源
──────
A 股 (.SH/.SZ/.BJ):
    Tushare pro.income / balancesheet / cashflow / fina_indicator
    （5000 VIP 档可用，要消耗积分，每只股票 4 次调用）
美股 (.US):
    FMP /stable/income-statement / balance-sheet-statement / cash-flow-statement / key-metrics
    （付费档 US 全套，300 calls/min，每只股票 4 次调用）
港股 (.HK):
    暂未实现

统一 schema（A 股 + 美股都对齐到这套字段，便于跨市场研究）
────────────────────────────────────────────────────
索引层:
    ann_date          公告日期
    end_date          报告期末 (Q1/Q2/Q3/Q4 末日)
    period            'Q1' / 'Q2' / 'Q3' / 'Q4'（'Q4' = 年报）
    fiscal_year       会计年度
    currency          'CNY' / 'USD'

利润表:
    revenue           营业收入
    gross_profit      毛利
    operating_income  营业利润
    pretax_income     税前利润
    net_income        净利润
    eps_basic         基本每股收益
    eps_diluted       稀释每股收益

资产负债表:
    total_assets         总资产
    total_liabilities    总负债
    total_equity         所有者权益
    cash_and_equivalents 现金及等价物
    long_term_debt       长期债务
    short_term_debt      短期债务

现金流:
    operating_cf        经营现金流
    investing_cf        投资现金流
    financing_cf        筹资现金流
    free_cash_flow      自由现金流
    capex               资本支出

财务指标:
    roe                 净资产收益率
    roa                 总资产收益率
    gross_margin        毛利率
    net_margin          净利率
    debt_to_equity      资产负债率
    current_ratio       流动比率

用法（先 source .venv/bin/activate）
──────────────────────────────────
    # 默认：扫 universe/*.parquet 取并集
    python scripts/pull_financials.py

    # 只跑 A 股 / 只跑美股
    python scripts/pull_financials.py --market cn
    python scripts/pull_financials.py --market us

    # 指定 ticker（调试用）
    python scripts/pull_financials.py --tickers 600519.SH,NVDA.US

    # 强制重拉（覆盖已有缓存）
    python scripts/pull_financials.py --force

    # 控制并发（FMP 300/min, Tushare 视积分档）
    python scripts/pull_financials.py --workers 5
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_FIN = PROJECT_ROOT / 'data_cache' / 'financials'

# 统一 schema（最终 parquet 列）
COMMON_COLS = [
    'ann_date', 'end_date', 'period', 'fiscal_year', 'currency',
    # 利润表
    'revenue', 'gross_profit', 'operating_income', 'pretax_income', 'net_income',
    'eps_basic', 'eps_diluted',
    # 资产负债表
    'total_assets', 'total_liabilities', 'total_equity',
    'cash_and_equivalents', 'long_term_debt', 'short_term_debt',
    # 现金流
    'operating_cf', 'investing_cf', 'financing_cf',
    'free_cash_flow', 'capex',
    # 财务指标
    'roe', 'roa', 'gross_margin', 'net_margin',
    'debt_to_equity', 'current_ratio',
]


# ─── ts_code 后缀 → market ────────────────────────────────────────────
def parse_market(ts_code: str) -> str:
    s = ts_code.upper()
    if s.endswith('.SH') or s.endswith('.SZ') or s.endswith('.BJ'):
        return 'cn_a'
    if s.endswith('.US'):
        return 'us'
    if s.endswith('.HK'):
        return 'cn_hk'
    return 'unknown'


# ─── A 股 via Tushare ──────────────────────────────────────────────────
_TUSHARE_PRO = None


def _get_tushare_pro():
    global _TUSHARE_PRO
    if _TUSHARE_PRO is not None:
        return _TUSHARE_PRO
    try:
        import tushare as ts
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('tushare 或 python-dotenv 没装。')
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        sys.exit('TUSHARE_TOKEN 未配置')
    ts.set_token(token)
    _TUSHARE_PRO = ts.pro_api()
    return _TUSHARE_PRO


# A 股 → 通用 schema 字段映射（Tushare 字段在右）
TUSHARE_INCOME_MAP = {
    'revenue': 'revenue',
    'gross_profit': 'oper_cost',  # 注: Tushare 给的是营业成本，毛利 = revenue - oper_cost
    'operating_income': 'operate_profit',
    'pretax_income': 'total_profit',
    'net_income': 'n_income',
    'eps_basic': 'basic_eps',
    'eps_diluted': 'diluted_eps',
}
TUSHARE_BALANCE_MAP = {
    'total_assets': 'total_assets',
    'total_liabilities': 'total_liab',
    'total_equity': 'total_hldr_eqy_inc_min_int',
    'cash_and_equivalents': 'money_cap',
    'long_term_debt': 'lt_borr',
    'short_term_debt': 'st_borr',
}
TUSHARE_CASHFLOW_MAP = {
    'operating_cf': 'n_cashflow_act',
    'investing_cf': 'n_cashflow_inv_act',
    'financing_cf': 'n_cash_flows_fnc_act',
    'capex': 'c_pay_acq_const_fiolta',  # 购建固定资产现金支出
}
TUSHARE_INDICATOR_MAP = {
    'roe': 'roe',
    'roa': 'roa',
    'gross_margin': 'grossprofit_margin',
    'net_margin': 'netprofit_margin',
    'debt_to_equity': 'debt_to_assets',
    'current_ratio': 'current_ratio',
}


def fetch_a_share_financials(ts_code: str) -> pd.DataFrame | None:
    """A 股一只股票的完整财务历史。"""
    pro = _get_tushare_pro()

    def safe(fn_name: str, **kw):
        # 注意：不能写 `getattr(...) or pd.DataFrame()` ——
        # Tushare 返回 DataFrame 时 `df or df2` 会触发"真值歧义"错误
        try:
            df = getattr(pro, fn_name)(ts_code=ts_code, **kw)
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            print(f'    {fn_name} 失败: {e}')
            return pd.DataFrame()

    income = safe('income')
    balance = safe('balancesheet')
    cashflow = safe('cashflow')
    indicator = safe('fina_indicator')

    if income.empty:
        return None

    # 用 end_date 作为对齐主键
    income = income.drop_duplicates('end_date', keep='first')
    balance = balance.drop_duplicates('end_date', keep='first')
    cashflow = cashflow.drop_duplicates('end_date', keep='first')
    indicator = indicator.drop_duplicates('end_date', keep='first')

    # merge
    df = income[['ann_date', 'end_date'] + list(TUSHARE_INCOME_MAP.values())].copy()
    df = df.merge(
        balance[['end_date'] + list(TUSHARE_BALANCE_MAP.values())],
        on='end_date', how='left',
    ) if not balance.empty else df
    df = df.merge(
        cashflow[['end_date'] + list(TUSHARE_CASHFLOW_MAP.values())],
        on='end_date', how='left',
    ) if not cashflow.empty else df
    df = df.merge(
        indicator[['end_date'] + list(TUSHARE_INDICATOR_MAP.values())],
        on='end_date', how='left',
    ) if not indicator.empty else df

    # 重命名到统一 schema
    rename_dict = {}
    rename_dict.update({v: k for k, v in TUSHARE_INCOME_MAP.items()})
    rename_dict.update({v: k for k, v in TUSHARE_BALANCE_MAP.items()})
    rename_dict.update({v: k for k, v in TUSHARE_CASHFLOW_MAP.items()})
    rename_dict.update({v: k for k, v in TUSHARE_INDICATOR_MAP.items()})
    df = df.rename(columns=rename_dict)

    # 派生字段
    # gross_profit = revenue - oper_cost (这里 oper_cost 我刚才命名成了 gross_profit，需要修)
    # 实际 Tushare 是 oper_cost = 营业成本。我们要的 gross_profit = revenue - oper_cost
    if 'gross_profit' in df.columns and 'revenue' in df.columns:
        # 此时 gross_profit 列还是 oper_cost 的值，转一下
        df['gross_profit'] = df['revenue'] - df['gross_profit']

    # 日期 + period
    df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d', errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
    df['fiscal_year'] = df['end_date'].dt.year
    df['period'] = df['end_date'].dt.month.map({3: 'Q1', 6: 'Q2', 9: 'Q3', 12: 'Q4'})
    df['currency'] = 'CNY'

    # free_cash_flow 派生
    if 'operating_cf' in df.columns and 'capex' in df.columns:
        df['free_cash_flow'] = df['operating_cf'] - df['capex'].fillna(0).abs()

    # 保留 COMMON_COLS（缺失列补 NaN）
    for c in COMMON_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[COMMON_COLS].copy()
    return df.sort_values('end_date').reset_index(drop=True)


# ─── 美股 via FMP ──────────────────────────────────────────────────────
FMP_BASE = 'https://financialmodelingprep.com/stable'

FMP_INCOME_MAP = {
    'revenue': 'revenue',
    'gross_profit': 'grossProfit',
    'operating_income': 'operatingIncome',
    'pretax_income': 'incomeBeforeTax',
    'net_income': 'netIncome',
    'eps_basic': 'eps',
    'eps_diluted': 'epsdiluted',
}
FMP_BALANCE_MAP = {
    'total_assets': 'totalAssets',
    'total_liabilities': 'totalLiabilities',
    'total_equity': 'totalStockholdersEquity',
    'cash_and_equivalents': 'cashAndCashEquivalents',
    'long_term_debt': 'longTermDebt',
    'short_term_debt': 'shortTermDebt',
}
FMP_CASHFLOW_MAP = {
    'operating_cf': 'operatingCashFlow',
    'investing_cf': 'netCashUsedForInvestingActivites',
    'financing_cf': 'netCashUsedProvidedByFinancingActivities',
    'free_cash_flow': 'freeCashFlow',
    'capex': 'capitalExpenditure',
}
# FMP 的 /stable/key-metrics?period=quarter 需要 Premium 档（Starter 是 402）
# 所以我们不调 key-metrics，直接从 income + balance + cashflow 算这些指标：
#   roe = net_income / total_equity
#   roa = net_income / total_assets
#   gross_margin = gross_profit / revenue
#   net_margin = net_income / revenue
#   debt_to_equity = total_liabilities / total_equity
# current_ratio 需要 current_assets/current_liabilities，FMP balance sheet 有这俩字段
FMP_BALANCE_EXTRA_MAP = {
    'total_current_assets': 'totalCurrentAssets',
    'total_current_liabilities': 'totalCurrentLiabilities',
}


def _fmp_get(endpoint: str, symbol: str, key: str) -> list[dict]:
    """调 FMP /stable/<endpoint>?symbol=...&period=quarter&limit=80。"""
    url = f'{FMP_BASE}/{endpoint}'
    params = {'symbol': symbol, 'period': 'quarter', 'limit': 80, 'apikey': key}
    try:
        r = requests.get(url, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f'    FMP {endpoint} 网络错误: {e}')
        return []
    if r.status_code != 200:
        print(f'    FMP {endpoint} {r.status_code}: {r.text[:150]}')
        return []
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_us_financials(ts_code: str, key: str) -> pd.DataFrame | None:
    """美股一只股票的完整季度财务历史。

    FMP Starter 档没有 /stable/key-metrics?period=quarter（要 Premium），
    所以 ROE/margins/debt_to_equity 由 income + balance 即时算出来。
    """
    symbol = ts_code[:-3] if ts_code.endswith('.US') else ts_code

    income = _fmp_get('income-statement', symbol, key)
    if not income:
        return None
    balance = _fmp_get('balance-sheet-statement', symbol, key)
    cashflow = _fmp_get('cash-flow-statement', symbol, key)

    def to_df(rows: list[dict], field_map: dict) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        cols = ['date'] + [v for v in field_map.values() if v in df.columns]
        df = df[cols].copy()
        df = df.rename(columns={'date': 'end_date'})
        df = df.rename(columns={v: k for k, v in field_map.items() if v in df.columns})
        return df

    df_inc = to_df(income, FMP_INCOME_MAP)
    # balance 含两个 map：核心字段 + 额外（current_assets/liabilities for current_ratio）
    df_bal = to_df(balance, {**FMP_BALANCE_MAP, **FMP_BALANCE_EXTRA_MAP})
    df_cf = to_df(cashflow, FMP_CASHFLOW_MAP)

    if df_inc.empty:
        return None

    # 三张表都先按 end_date dedupe (keep latest filing). FMP 偶尔会对同一报告期
    # 先后 file 两份 (restate)，留两份会让 left merge 复制行 → 后面对 df 赋值
    # 长度对不上 → "Length of values (29) does not match length of index (30)".
    # JCAP / RCAT 出过这个 bug.
    for d in (df_inc, df_bal, df_cf):
        if not d.empty:
            d.drop_duplicates('end_date', keep='first', inplace=True)

    df = df_inc.reset_index(drop=True)
    for other in (df_bal, df_cf):
        if not other.empty:
            df = df.merge(other, on='end_date', how='left')

    # 公告日 FMP 在 fillingDate。用 date→ann_date 映射, 不依赖 df 长度
    # (即使 merge 出意外行复制也不崩, dedupe 已经覆盖了, 这是双保险)
    ann_date_by_end = {
        r.get('date'): r.get('fillingDate') or r.get('acceptedDate') or r.get('date')
        for r in income
    }
    df['ann_date'] = pd.to_datetime(
        df['end_date'].map(ann_date_by_end), errors='coerce',
    )
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    # FMP income 返回里自带 period ('Q1'/'Q2'/'Q3'/'Q4'/'FY')，按 date 对齐
    # 注意：NVDA 等非日历年财报（财年 Jan 末）月份不在 {3,6,9,12}，不能用月份推
    income_period_by_date = {r.get('date'): r.get('period') for r in income}
    income_year_by_date = {r.get('date'): r.get('calendarYear') or r.get('fiscalYear')
                            for r in income}
    df['period'] = df['end_date'].dt.strftime('%Y-%m-%d').map(income_period_by_date)
    df['fiscal_year'] = df['end_date'].dt.strftime('%Y-%m-%d').map(
        income_year_by_date,
    ).fillna(df['end_date'].dt.year).astype('Int64')
    df['currency'] = 'USD'

    # 从原始字段算出财务指标（FMP Starter 没有 key-metrics quarter）
    def _safe_div(a, b):
        return pd.Series(a) / pd.Series(b).replace(0, pd.NA)

    if 'net_income' in df.columns and 'total_equity' in df.columns:
        # ROE 是百分比（Tushare 风格），所以 *100
        df['roe'] = _safe_div(df['net_income'], df['total_equity']) * 100
    if 'net_income' in df.columns and 'total_assets' in df.columns:
        df['roa'] = _safe_div(df['net_income'], df['total_assets']) * 100
    if 'gross_profit' in df.columns and 'revenue' in df.columns:
        df['gross_margin'] = _safe_div(df['gross_profit'], df['revenue']) * 100
    if 'net_income' in df.columns and 'revenue' in df.columns:
        df['net_margin'] = _safe_div(df['net_income'], df['revenue']) * 100
    if 'total_liabilities' in df.columns and 'total_equity' in df.columns:
        df['debt_to_equity'] = _safe_div(df['total_liabilities'],
                                          df['total_equity'])
    if ('total_current_assets' in df.columns and
            'total_current_liabilities' in df.columns):
        df['current_ratio'] = _safe_div(df['total_current_assets'],
                                         df['total_current_liabilities'])

    # 补齐缺失列
    for c in COMMON_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[COMMON_COLS].copy()
    return df.sort_values('end_date').reset_index(drop=True)


# ─── 路由 ────────────────────────────────────────────────────────────
def fetch_one(ts_code: str, fmp_key: str | None) -> tuple[pd.DataFrame | None, str]:
    market = parse_market(ts_code)
    if market == 'cn_a':
        df = fetch_a_share_financials(ts_code)
        return df, 'tushare'
    if market == 'us':
        if not fmp_key:
            return None, 'no_fmp_key'
        df = fetch_us_financials(ts_code, fmp_key)
        return df, 'fmp'
    if market == 'cn_hk':
        return None, 'hk_not_implemented'
    return None, f'unknown_market({ts_code})'


# ─── 单只更新 ─────────────────────────────────────────────────────────
def update_one(ts_code: str, fmp_key: str | None, force: bool) -> dict:
    fp = CACHE_DIR_FIN / f'{ts_code}.parquet'
    if fp.exists() and not force:
        return {'ticker': ts_code, 'status': 'skip', 'reason': 'cached'}

    df, vendor = fetch_one(ts_code, fmp_key)
    if df is None or len(df) == 0:
        return {'ticker': ts_code, 'status': 'empty', 'vendor': vendor}

    CACHE_DIR_FIN.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fp, index=False, compression='snappy')

    return {
        'ticker': ts_code, 'status': 'ok', 'vendor': vendor,
        'rows': len(df),
        'earliest': df['end_date'].min().strftime('%Y-%m-%d') if 'end_date' in df else '-',
        'latest': df['end_date'].max().strftime('%Y-%m-%d') if 'end_date' in df else '-',
    }


# ─── 主入口 ───────────────────────────────────────────────────────────
def collect_tickers(args) -> list[str]:
    if args.tickers:
        return [t.strip() for t in args.tickers.split(',') if t.strip()]

    universe_dir = PROJECT_ROOT / 'data_cache' / 'universe'
    ts_set: set[str] = set()
    if universe_dir.exists():
        for uni_fp in sorted(universe_dir.glob('*.parquet')):
            try:
                df = pd.read_parquet(uni_fp)
                if 'ts_code' in df.columns:
                    ts_set.update(df['ts_code'].tolist())
            except Exception as e:
                print(f'  ! 读 {uni_fp.name} 失败: {e}')

    if args.market:
        markets = set(args.market.split(','))
        def keep(t: str) -> bool:
            m = parse_market(t)
            return ((m == 'cn_a' and 'cn' in markets) or
                    (m == 'us' and 'us' in markets) or
                    (m == 'cn_hk' and 'hk' in markets))
        ts_set = {t for t in ts_set if keep(t)}

    return sorted(ts_set)


def main() -> int:
    # 入口加载 .env
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / '.env')
    except ImportError:
        pass

    ap = argparse.ArgumentParser(
        description='拉所有股票的季度财务数据（统一 schema）',
    )
    ap.add_argument('--tickers', help='逗号分隔的 ts_code 列表')
    ap.add_argument('--market', help='只跑特定市场 (cn/us/hk)，逗号分隔')
    ap.add_argument('--workers', type=int, default=3,
                    help='并发线程数（默认 3，避开 Tushare 速率限）')
    ap.add_argument('--force', action='store_true', help='覆盖已有缓存')
    args = ap.parse_args()

    fmp_key = os.getenv('FMP_API_KEY') or None
    tickers = collect_tickers(args)

    if not tickers:
        sys.exit('没有 ticker 可拉，先跑 pull_universe.py / pull_us_universe.py')

    print(f'拉 {len(tickers)} 只股票财务数据 → {CACHE_DIR_FIN.relative_to(PROJECT_ROOT)}/')
    print(f'并发 {args.workers}, force={args.force}, market={args.market or "all"}')
    if fmp_key:
        print(f'FMP key: 已配置')
    else:
        print(f'⚠️  FMP_API_KEY 未配置，美股将跳过')
    print('-' * 70)

    t0 = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(update_one, t, fmp_key, args.force): t for t in tickers
        }
        width = len(str(len(tickers)))
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {'ticker': t, 'status': 'error', 'error': str(e)}
            results.append(r)
            status_tag = {'ok': '✓', 'skip': '=', 'empty': '○',
                          'error': '✗'}.get(r['status'], '?')
            if r['status'] == 'ok':
                extra = (f"  {r['rows']} 季报, {r['earliest']}→{r['latest']}, "
                         f"vendor={r['vendor']}")
            elif r['status'] == 'skip':
                extra = f"  已缓存"
            elif r['status'] == 'empty':
                extra = f"  无数据 ({r.get('vendor', '?')})"
            else:
                extra = f"  ERR: {r.get('error', '?')}"
            print(f'  [{i:>{width}}/{len(tickers)}] {status_tag} {t:<14}{extra}')

    elapsed = time.time() - t0
    ok = sum(1 for r in results if r['status'] == 'ok')
    skipped = sum(1 for r in results if r['status'] == 'skip')
    failed = [r for r in results if r['status'] in ('empty', 'error')]

    print('-' * 70)
    print(f'完成: {ok} 拉取 / {skipped} 跳过 / {len(failed)} 失败, 用时 {elapsed:.1f}s')

    if failed and len(failed) <= 20:
        print('\n失败:')
        for r in failed[:20]:
            print(f"  {r['ticker']}: {r['status']} - "
                  f"{r.get('error', r.get('vendor', '?'))}")

    return 0 if not failed else (1 if ok > 0 else 2)


if __name__ == '__main__':
    sys.exit(main())
