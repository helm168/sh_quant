"""探测 core_kpi.yaml 里 auto-fetchable（source.kind != manual）KPI 的数据源可行性。

用途
    在写正式 pull_kpi.py 爬虫之前先侦察：每个标了 api/url 的 KPI，它的源到底
    能不能免费拿到、要不要解析 HTML/JS、是不是其实没有可靠免费端点（该降级
    manual）。**只读不写**——不落 parquet，输出一张可行性表到 stdout。

    侦察结论分四档：
        OK          直接能拿到结构化值（含样本值/as-of），pull_kpi 可放心写
        NEEDS_WORK  端点可达但要解析 HTML/JS 或校准口径，能做但脆
        DEAD        无可靠免费端点 → 建议把 core_kpi.yaml 的 source.kind 降级 manual
        NEEDS_TOKEN 缺 .env 凭证（如 TUSHARE_TOKEN），配置后重跑

依赖
    requests（tushare 间接依赖，已可用）；smic 探针复用 utils.data 的 Tushare 网关。

用法（先 source .venv/bin/activate，在项目根执行）
    python scripts/probe_kpi_sources.py            # 跑全部 auto KPI 探针
    python scripts/probe_kpi_sources.py --list     # 只列计划，不联网
    python scripts/probe_kpi_sources.py --only bdi # 只跑某个 kpi_id

输出
    无文件产物；可行性表打到 stdout。据此决定哪些进 pull_kpi、哪些降级 manual。

注：本脚本是 survey 性质，缺 token 不整体退出（那会废掉其余 5 个探针），而是把
    该 KPI 标 NEEDS_TOKEN 继续——这是显式上报，不是 AGENTS.md 禁止的"静默继续"。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import requests  # noqa: E402

from utils.core_kpi import auto_fetchable, get_kpi  # noqa: E402

TIMEOUT = 12
HEADERS = {'User-Agent': 'Mozilla/5.0 (sh_quant kpi-source probe)'}


def _get(url: str) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=TIMEOUT)


def probe_tsmc() -> tuple[str, str]:
    """台积电月营收：台交所 OpenAPI 全上市公司当月营收 JSON，筛公司代号 2330。"""
    url = 'https://openapi.twse.com.tw/v1/opendata/t187ap05_P'
    try:
        r = _get(url)
        r.raise_for_status()
        rows = r.json()
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'TWSE OpenAPI 不可达/非 JSON: {e!r}'
    if not isinstance(rows, list) or not rows:
        return 'NEEDS_WORK', 'TWSE 返回非预期结构（非数组），需重新核对端点'
    hit = next((d for d in rows if isinstance(d, dict)
                and any(str(v).strip() == '2330' for v in d.values())), None)
    if hit is None:
        return 'NEEDS_WORK', f'端点可达（{len(rows)} 家），但未匹配到 2330，需核对字段名'
    keys = '/'.join(list(hit.keys())[:6])
    return 'OK', f'找到 2330，字段含: {keys}… —— pull_kpi 取「当月营收 + 去年同月增减%」'


def probe_bdi() -> tuple[str, str]:
    """BDI：波罗的海交易所官方付费，无可靠免费 API（已知事实，不联网）。"""
    return 'DEAD', ('Baltic Exchange 官方数据付费；无稳定免费 API。'
                    '建议 core_kpi.yaml 把 bdi 降级 source.kind=manual（公开新闻周记），'
                    '或单独评估付费航运数据源')


def probe_ccfi() -> tuple[str, str]:
    """CCFI：上海航运交易所英文指数页，周更，值在 HTML 表里。"""
    url = 'https://en.sse.net.cn/indices/ccfinew.jsp'
    try:
        r = _get(url)
        ok = r.status_code == 200 and ('CCFI' in r.text or 'Container' in r.text)
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'上海航交所页面不可达: {e!r}'
    if ok:
        return 'NEEDS_WORK', '页面可达且含 CCFI，但指数值在 HTML 表格内需解析；周更'
    return 'DEAD', '页面可达性/关键字校验未过，端点或已变更，需重新定位'


def probe_semi_na_bb() -> tuple[str, str]:
    """SEMI 北美设备 B/B：官网 billing report 新闻稿，月度，仅 headline。"""
    url = 'https://www.semi.org/en/news-resources/market-data'
    try:
        r = _get(url)
        ok = r.status_code == 200 and ('billing' in r.text.lower()
                                       or 'book-to-bill' in r.text.lower())
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'SEMI 站点不可达: {e!r}'
    if ok:
        return 'NEEDS_WORK', ('SEMI 站点可达且含 billing 字样，但月度 B/B 在新闻稿正文，'
                              '需定位每月稿件 URL + 抽取 headline 数字')
    return 'NEEDS_WORK', 'SEMI 站点可达但未命中关键字，需人工找准 billing report 落地页'


def probe_moutai() -> tuple[str, str]:
    """飞天批价：聚合站结构不稳且常反爬，先给侦察建议（不联网猜 URL）。"""
    return 'NEEDS_WORK', ('今日酒价等聚合站结构不稳定、常反爬，且整箱/散瓶/地区口径差异大。'
                          '建议：先人工记 1-2 周作口径校准基准，再选定单一站点自动化')


def probe_smic() -> tuple[str, str]:
    """中芯稼动率/ASP：Tushare 能取财报，但稼动率/wafer 出货非 Tushare 字段。"""
    try:
        from utils.data import _get_tushare_pro
        pro = _get_tushare_pro()
    except RuntimeError:
        return 'NEEDS_TOKEN', '缺 TUSHARE_TOKEN —— 在 .env 配置后重跑本探针'
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'Tushare 初始化失败: {e!r}'
    try:
        df = pro.fina_indicator(ts_code='688981.SH')
        n = 0 if df is None else len(df)
    except Exception as e:  # noqa: BLE001
        return 'DEAD', f'Tushare fina_indicator 调用失败: {e!r}'
    if n == 0:
        return 'NEEDS_WORK', 'Tushare 通但 688981.SH fina_indicator 空，需换接口/期次'
    return 'NEEDS_WORK', (f'Tushare 可取中芯财报（{n} 期指标）✓，但稼动率/12寸 wafer 出货'
                          '不是 Tushare 字段 → ASP 只能用营收近似、稼动率仍需手填。'
                          '建议 core_kpi.yaml 拆：ASP 走 api，稼动率 manual')


PROBES = {
    'tsmc_monthly_rev_yoy': probe_tsmc,
    'bdi': probe_bdi,
    'ccfi': probe_ccfi,
    'semi_na_bb': probe_semi_na_bb,
    'moutai_wholesale_price': probe_moutai,
    'smic_utilization_asp': probe_smic,
}


def main() -> int:
    parser = argparse.ArgumentParser(description='侦察 auto-fetchable KPI 数据源可行性')
    parser.add_argument('--list', action='store_true', help='只列计划，不联网')
    parser.add_argument('--only', metavar='KPI_ID', help='只跑某个 kpi_id')
    args = parser.parse_args()

    targets = auto_fetchable()
    if args.only:
        if args.only not in targets:
            print(f'{args.only!r} 不在 auto-fetchable 列表: {targets}')
            return 2
        targets = [args.only]

    print(f'auto-fetchable KPI: {len(targets)} 个 —— {targets}\n')

    if args.list:
        for kid in targets:
            src = get_kpi(kid)['source']
            impl = '有探针' if kid in PROBES else '【缺探针】'
            print(f'  {kid:24s} kind={src["kind"]:6s} {impl}  ref={src["ref"]}')
        return 0

    worst = 0
    rank = {'OK': 0, 'NEEDS_WORK': 1, 'NEEDS_TOKEN': 1, 'DEAD': 2}
    for kid in targets:
        if kid not in PROBES:
            verdict, detail = 'DEAD', ('core_kpi.yaml 标了 auto 但本脚本没有对应探针 —— '
                                       '要么补 probe，要么把 source.kind 降级 manual')
        else:
            verdict, detail = PROBES[kid]()
        worst = max(worst, rank.get(verdict, 2))
        print(f'  [{verdict:11s}] {kid}')
        print(f'               {detail}')

    print('\n图例: OK=可直接写 pull_kpi | NEEDS_WORK=可做但要解析/校准 | '
          'DEAD=建议降级 manual | NEEDS_TOKEN=配 .env 后重跑')
    return 1 if worst == 2 else 0


if __name__ == '__main__':
    raise SystemExit(main())
