"""侦察 A 股资金面 PRD 里 sh_quant 仍有空白的指标数据源可行性。

用途
    PRD（A-Share Capital Flow Data Module v0.1）列了 ~30 个指标。sh_quant 已落：
    ENV01-05（pull_macro.py）、IN06 季度北向（pull_holders.py）、ENV08/09 部分
    （sector_turnover/daily_basic）。本脚本只探"未覆盖"的几项，决定哪些进 P0
    puller、哪些降级 manual、哪些需要换数据源。

    侦察结论分四档（与 probe_kpi_sources.py 同语义）：
        OK           接口可调、有数据、字段够写 puller 直接进 P0
        NEEDS_WORK   接口可达但要解析/拼装/校准口径，能做但脆
        NO_ACCESS    基础会员无权限/配额不足 —— 需换数据源（loud-fail，exit 2）
        DEAD         端点不可达或源头停披露，建议从 PRD 删除/降级 manual

依赖
    utils.data._get_tushare_pro（读 .env 的 TUSHARE_TOKEN），akshare（已在 venv）。

用法（先 source .venv/bin/activate，在项目根执行）
    python scripts/probe_capital_flow_sources.py            # 跑全部
    python scripts/probe_capital_flow_sources.py --list     # 只列计划
    python scripts/probe_capital_flow_sources.py --only in04 # 只跑某项

输出
    无文件产物；可行性表打到 stdout。NO_ACCESS（权限/配额）单列段落 + exit 2，
    符合 feedback memory「surface permission failures loudly」。
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

PERMISSION_HINTS = (
    '没有权限', '权限', '积分', '不足', 'permission', 'forbidden', '40203', '40004',
)


def _is_permission_error(err: Exception) -> bool:
    s = str(err).lower()
    return any(h.lower() in s for h in PERMISSION_HINTS)


def _recent_trade_date() -> str:
    """近 1 个交易日（粗略），格式 YYYYMMDD。周末退到周五。"""
    d = dt.date.today()
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d.strftime('%Y%m%d')


def _tushare():
    from utils.data import _get_tushare_pro

    return _get_tushare_pro()


def probe_in04_etf_share() -> tuple[str, str]:
    """IN04 ETF 净申购：fund_share（日频份额）× fund_nav（单位净值）。

    取一只代表性宽基股票 ETF（510300 沪深 300）最近 ~10 行试调。
    """
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    end = _recent_trade_date()
    start = (dt.datetime.strptime(end, '%Y%m%d') - dt.timedelta(days=20)).strftime('%Y%m%d')
    try:
        share = pro.fund_share(ts_code='510300.SH', start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'fund_share 无权限: {e!r} —— 试 akshare fund_etf_fund_info_em'
        return 'DEAD', f'fund_share 调用失败: {e!r}'
    if share is None or len(share) == 0:
        return 'NEEDS_WORK', 'fund_share 通但 510300.SH 近 20 天空，需换样本或扩窗口'
    try:
        nav = pro.fund_nav(ts_code='510300.SH', start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'fund_share OK 但 fund_nav 无权限: {e!r}'
        return 'NEEDS_WORK', f'fund_share OK 但 fund_nav 失败: {e!r}'
    n_share, n_nav = len(share), 0 if nav is None else len(nav)
    cols_share = '/'.join(list(share.columns)[:6])
    return 'OK', (
        f'fund_share {n_share} 行 / fund_nav {n_nav} 行；share 列含 {cols_share}…'
        ' —— 全市场股票 ETF 列表过滤后逐只拉，跑批可行'
    )


def probe_in01_new_fund_issuance() -> tuple[str, str]:
    """IN01 偏股基金新发规模：fund_basic 按 found_date 过滤 + invest_type 取股票/混合。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    try:
        df = pro.fund_basic(market='E', status='L')  # E=场内, L=存续
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'fund_basic 无权限: {e!r} —— 试 akshare fund_em_open_fund_info'
        return 'DEAD', f'fund_basic 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'fund_basic 通但场内存续基金为空，异常'
    cols = set(df.columns)
    needed = {'found_date', 'invest_type', 'issue_amount'}
    miss = needed - cols
    if miss:
        return 'NEEDS_WORK', f'fund_basic 通但缺字段 {miss}；现有 {sorted(cols)[:10]}…'
    return 'OK', (
        f'fund_basic {len(df)} 行，含 found_date/invest_type/issue_amount —— '
        '按 invest_type 取股票型+偏股混合，按 found_date 月聚合 issue_amount'
    )


def probe_in13_holder_trade() -> tuple[str, str]:
    """IN13/OUT04 重要股东增减持：stk_holdertrade。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    end = _recent_trade_date()
    start = (dt.datetime.strptime(end, '%Y%m%d') - dt.timedelta(days=30)).strftime('%Y%m%d')
    try:
        df = pro.stk_holdertrade(start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'stk_holdertrade 无权限: {e!r}'
        return 'DEAD', f'stk_holdertrade 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'stk_holdertrade 通但近 30 天空，可能要扩窗口'
    cols = '/'.join(list(df.columns)[:8])
    return 'OK', f'stk_holdertrade {len(df)} 行；列含 {cols}… —— 按 in_de 区分增减持聚合'


def probe_out01_ipo() -> tuple[str, str]:
    """OUT01 IPO 募资：new_share。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    try:
        df = pro.new_share()
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'new_share 无权限: {e!r}'
        return 'DEAD', f'new_share 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'new_share 通但返回空'
    cols = set(df.columns)
    if 'amount' not in cols and 'funds' not in cols:
        return 'NEEDS_WORK', f'new_share 通但未见 amount/funds 字段；现有 {sorted(cols)[:10]}'
    return 'OK', f'new_share {len(df)} 行；按 ipo_date 月聚合 amount/funds'


def probe_out02_refinance() -> tuple[str, str]:
    """OUT02 再融资：增发 pro.cb_call / pro.sub_*；基础会员通常没有完整再融资接口。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    # 试一个常被 base 会员锁掉的接口
    try:
        df = pro.cb_issue()  # 可转债发行
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', (
                f'cb_issue 无权限: {e!r} —— Tushare 再融资类接口（增发/配股/可转债）'
                '基础会员普遍受限；建议 akshare stock_em_yzxdr / 同花顺爬'
            )
        return 'DEAD', f'cb_issue 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'cb_issue 通但返回空；增发/配股还需另外两个接口'
    return 'NEEDS_WORK', (
        f'cb_issue {len(df)} 行 OK，但完整再融资 = 增发(pro.sf_a/sf_b?)+配股+可转债，'
        '需要 probe 另外两个接口并拼合'
    )


def probe_out03_share_float() -> tuple[str, str]:
    """OUT03 限售解禁：share_float（前瞻排期）。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    today = dt.date.today().strftime('%Y%m%d')
    end = (dt.date.today() + dt.timedelta(days=60)).strftime('%Y%m%d')
    try:
        df = pro.share_float(start_date=today, end_date=end)
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'share_float 无权限: {e!r}'
        return 'DEAD', f'share_float 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'share_float 通但未来 60 天无解禁排期，异常'
    cols = '/'.join(list(df.columns)[:8])
    return 'OK', f'share_float {len(df)} 行；列含 {cols}… —— 按 float_date 周/月聚合市值'


def probe_out05_repurchase() -> tuple[str, str]:
    """OUT05 回购：repurchase。"""
    try:
        pro = _tushare()
    except RuntimeError:
        return 'NO_ACCESS', '缺 TUSHARE_TOKEN'
    end = _recent_trade_date()
    start = (dt.datetime.strptime(end, '%Y%m%d') - dt.timedelta(days=60)).strftime('%Y%m%d')
    try:
        df = pro.repurchase(start_date=start, end_date=end)
    except Exception as e:  # noqa: BLE001
        if _is_permission_error(e):
            return 'NO_ACCESS', f'repurchase 无权限: {e!r}'
        return 'DEAD', f'repurchase 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'repurchase 通但近 60 天空'
    cols = '/'.join(list(df.columns)[:8])
    return 'OK', f'repurchase {len(df)} 行；列含 {cols}… —— 按 ann_date 聚合 amount'


def probe_env06_new_investors() -> tuple[str, str]:
    """ENV06 新增投资者数：akshare stock_account_statistics_em（东财，月频）。"""
    try:
        import akshare as ak
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'akshare 不可用: {e!r}'
    try:
        df = ak.stock_account_statistics_em()
    except Exception as e:  # noqa: BLE001
        return 'NEEDS_WORK', f'ak.stock_account_statistics_em 失败: {e!r}'
    if df is None or len(df) == 0:
        return 'NEEDS_WORK', 'akshare 返回空'
    cols = '/'.join(list(df.columns)[:6])
    return 'OK', f'akshare 开户数 {len(df)} 行；列含 {cols}…'


PROBES = {
    'in04_etf_share': probe_in04_etf_share,
    'in01_new_fund_issuance': probe_in01_new_fund_issuance,
    'in13_holder_trade': probe_in13_holder_trade,
    'out01_ipo': probe_out01_ipo,
    'out02_refinance': probe_out02_refinance,
    'out03_share_float': probe_out03_share_float,
    'out05_repurchase': probe_out05_repurchase,
    'env06_new_investors': probe_env06_new_investors,
}


def main() -> int:
    parser = argparse.ArgumentParser(description='侦察 A 股资金面 PRD 未覆盖指标的数据源可行性')
    parser.add_argument('--list', action='store_true', help='只列计划，不联网')
    parser.add_argument('--only', metavar='ID', help='只跑某个探针')
    args = parser.parse_args()

    targets = list(PROBES.keys())
    if args.only:
        if args.only not in PROBES:
            print(f'{args.only!r} 不在探针列表: {targets}')
            return 2
        targets = [args.only]

    print(f'探针目标: {len(targets)} 项 —— {targets}\n')

    if args.list:
        for k in targets:
            print(f'  {k}')
        return 0

    results: list[tuple[str, str, str]] = []
    for k in targets:
        verdict, detail = PROBES[k]()
        results.append((k, verdict, detail))
        print(f'  [{verdict:11s}] {k}')
        print(f'               {detail}')

    no_access = [r for r in results if r[1] == 'NO_ACCESS']
    dead = [r for r in results if r[1] == 'DEAD']

    if no_access:
        print('\n' + '!' * 60)
        print('⚠  以下指标 Tushare 基础会员无权限 / 配额不足，需换数据源：')
        for k, _, d in no_access:
            print(f'   - {k}: {d}')
        print('!' * 60)

    if dead:
        print('\n以下端点不可达 / 接口失败，建议从 P0 移除或降级 manual：')
        for k, _, d in dead:
            print(f'   - {k}: {d}')

    print(
        '\n图例: OK=可直接写 puller | NEEDS_WORK=可做但要拼装/校准 | '
        'NO_ACCESS=换源（loud-fail）| DEAD=移除/降级'
    )

    if no_access:
        return 2
    if dead or any(r[1] == 'NEEDS_WORK' for r in results):
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
