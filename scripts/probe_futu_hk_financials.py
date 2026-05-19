"""一次性探针：摸清 Futu get_stock_filter 能给港股哪些财务字段、值长什么样。

为什么需要这个
─────────────
pull_financials.py 港股分支待实现（FMP 当前档对 HK 返 402，Tushare 无 hk 报表
接口，Futu get_stock_filter 的 FinancialFilter 是唯一零新依赖路线）。但实现前
必须实测三件事，否则字段映射只能猜：

  1. ~25 个 StockField 哪些对港股真返回非空值（Futu 多给比率，营收绝对值只有
     SUM_OF_BUSINESS；资产负债表绝对额疑似缺失，只有 DEBT_ASSET_RATE 等比率）。
  2. get_stock_filter 返回里有没有报告期锚点（end_date / ann_date）。若没有，
     pull_financials 的 end_date/period/fiscal_year 只能外部推断。
  3. ANNUAL vs INTERIM 两个 quarter 各自返回的值是否不同、是否都有数据
     （港股半年报制，对应 sh_quant 统一 schema 的 period=FY / H1）。

依赖
────
  - futu-api（已在 requirements）
  - FutuOpenD 已启动并登录，HK Lv1 已开通（lsof -i :11111 应有 LISTEN）

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/probe_futu_hk_financials.py

输出
────
    纯 stdout 报告（不落盘）。三只大盘样本 00700/09988/00388 ×
    {ANNUAL, INTERIM} 的全字段值表 + 缺口结论。
"""

from __future__ import annotations

import contextlib
import sys
import time

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

HOST = '127.0.0.1'
PORT = 11111

# 三只 mega-cap，按市值降序必落首页（无需翻页）
TARGETS = {'HK.00700': '腾讯控股', 'HK.09988': '阿里巴巴-W', 'HK.00388': '香港交易所'}

# 候选字段：尽量覆盖 pull_financials COMMON_COLS。注释 = 想映射到的统一 schema 列
PROBE_FIELDS = [
    ('SUM_OF_BUSINESS', 'revenue 营业额(绝对)'),
    ('NET_PROFIT', 'net_income 净利润(绝对)'),
    ('SHAREHOLDER_NET_PROFIT_TTM', 'net_income 归母净利 TTM'),
    ('OPERATING_PROFIT_TTM', 'operating_income 营业利润 TTM'),
    ('EBIT_TTM', 'EBIT TTM'),
    ('EBITDA', 'EBITDA(绝对)'),
    ('BASIC_EPS', 'eps_basic 基本每股收益'),
    ('DILUTED_EPS', 'eps_diluted 稀释每股收益'),
    ('GROSS_PROFIT_RATE', 'gross_margin 毛利率'),
    ('NET_PROFIT_RATE', 'net_margin 净利率'),
    ('OPERATING_MARGIN_TTM', 'operating_margin TTM'),
    ('EBIT_MARGIN', 'ebit_margin'),
    ('RETURN_ON_EQUITY_RATE', 'roe 净资产收益率'),
    ('ROA_TTM', 'roa 总资产收益率 TTM'),
    ('DEBT_ASSET_RATE', 'debt_to_equity≈资产负债率'),
    ('CURRENT_RATIO', 'current_ratio 流动比率'),
    ('QUICK_RATIO', 'quick_ratio 速动比率'),
    ('EQUITY_MULTIPLIER', 'equity_multiplier 权益乘数'),
    ('CASH_AND_CASH_EQUIVALENTS', 'cash_and_equivalents 现金(绝对)'),
    ('OPERATING_CASH_FLOW_TTM', 'operating_cf 经营现金流 TTM'),
    ('NET_PROFIT_CASH_COVER_TTM', '净利现金含量 TTM'),
    ('TOTAL_ASSET_TURNOVER', '总资产周转率'),
    ('NET_PROFIX_GROWTH', '净利增速(sic Futu 拼写)'),
    ('TOTAL_ASSETS_GROWTH_RATE', '总资产增速'),
    ('EPS_GROWTH_RATE', 'EPS 增速'),
]

# Futu filter_list 单次别塞太多 FinancialFilter，分批
BATCH = 8
# get_stock_filter 限速 10 次/30s → 页间 3.5s（沿用 pull_hk_universe.py）
SLEEP_SEC = 3.5


def make_mc_floor() -> SimpleFilter:
    """市值 >= 2000 亿港元降序：保证 3 只 mega-cap 落首页 num=200。"""
    f = SimpleFilter()
    f.stock_field = StockField.MARKET_VAL
    f.filter_min = 2000 * 1e8
    f.is_no_filter = False
    f.sort = SortDir.DESCEND
    return f


def make_fin_filter(field_name: str, quarter) -> FinancialFilter:
    """要拿值必须 is_no_filter=False + 超宽 range（is_no_filter=True 时 item[ff]
    会 KeyError，Futu 实测坑）。range 取 ±1e18，几乎所有股票都通过 = 等效不筛。
    副作用：该字段为 NULL 的股票会被这条 range 排除（探针 3 只 mega-cap 无影响，
    但实现侧需注意：批量混筛时缺值股会整批掉出）。"""
    ff = FinancialFilter()
    ff.stock_field = getattr(StockField, field_name)
    ff.is_no_filter = False
    ff.filter_min = -1e18
    ff.filter_max = 1e18
    ff.quarter = quarter
    return ff


def probe_quarter(ctx: OpenQuoteContext, quarter, quarter_name: str) -> dict:
    """对一个 quarter，分批拉所有候选字段，回 {stock_code: {field: value}}。"""
    out: dict[str, dict] = {code: {} for code in TARGETS}
    mc = make_mc_floor()

    for i in range(0, len(PROBE_FIELDS), BATCH):
        batch = PROBE_FIELDS[i : i + BATCH]
        fin_filters = [make_fin_filter(fn, quarter) for fn, _ in batch]
        ret, data = ctx.get_stock_filter(
            market=Market.HK,
            filter_list=[mc, *fin_filters],
            begin=0,
            num=200,
        )
        if ret != RET_OK:
            print(f'  ✗ [{quarter_name}] batch {i // BATCH} 失败: {data}')
            time.sleep(SLEEP_SEC)
            continue
        _last, _all_count, ret_list = data
        for item in ret_list:
            if item.stock_code not in TARGETS:
                continue
            for (fn, _desc), ff in zip(batch, fin_filters, strict=True):
                try:
                    out[item.stock_code][fn] = item[ff]
                except (KeyError, TypeError):
                    out[item.stock_code][fn] = None
        time.sleep(SLEEP_SEC)
    return out


def print_report(quarter_name: str, vals: dict) -> None:
    print(f'\n{"=" * 78}\n  quarter = {quarter_name}\n{"=" * 78}')
    hdr = f'{"StockField":<28}{"→ 统一 schema":<26}'
    for code in TARGETS:
        hdr += f'{TARGETS[code]:<14}'
    print(hdr)
    print('-' * 78)
    for fn, desc in PROBE_FIELDS:
        line = f'{fn:<28}{desc:<26}'
        for code in TARGETS:
            v = vals.get(code, {}).get(fn)
            line += f'{("∅" if v is None else f"{v:.4g}" if isinstance(v, float) else str(v)):<14}'
        print(line)


def main() -> None:
    print(f'[Futu] connecting {HOST}:{PORT} ...')
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(f'OpenQuoteContext 失败: {e}\n→ OpenD 启动了吗? lsof -i :{PORT}')

    try:
        for q, qname in (
            (FinancialQuarter.ANNUAL, 'ANNUAL (→ period=FY)'),
            (FinancialQuarter.INTERIM, 'INTERIM (→ period=H1)'),
        ):
            vals = probe_quarter(ctx, q, qname)
            print_report(qname, vals)

        # 报告期锚点探测：get_stock_filter 不返报告日；看 snapshot 有无线索
        print(f'\n{"=" * 78}\n  报告期锚点探测 (get_market_snapshot)\n{"=" * 78}')
        ret, snap = ctx.get_market_snapshot(list(TARGETS))
        if ret == RET_OK:
            date_like = [c for c in snap.columns if 'date' in c.lower() or 'time' in c.lower()]
            print(f'  snapshot 含日期类列: {date_like}')
            print(snap[['code', *date_like]].to_string(index=False))
        else:
            print(f'  snapshot 失败: {snap}')
    finally:
        with contextlib.suppress(Exception):
            ctx.close()

    print(
        '\n判读要点:\n'
        '  - ∅ = Futu 对该字段/该季度无值 → COMMON_COLS 对应列只能留 NaN\n'
        '  - 资产负债表绝对额(total_assets/liab/equity)若全 ∅ → 确认 Futu 缺口\n'
        '  - ANNUAL vs INTERIM 值不同 → 确认两期独立可取 (period=FY / H1)\n'
        '  - snapshot 无报告期列 → end_date/fiscal_year 须外部推断 (运行时点反推)'
    )


if __name__ == '__main__':
    main()
