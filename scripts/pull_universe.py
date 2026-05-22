"""生成 A 股标的池清单（单文件），写到 data_cache/universe/cn_a.parquet。

设计原则
────────
"数据完整 + 消费侧过滤"。这个脚本只生成 **一份** universe 文件——市值 ≥ 100 亿
的全集（含 ST/次新），物理覆盖到 stocks/。各消费方根据 name / list_date /
total_mv 列在自己代码里做过滤，避免多份 universe 文件的维护负担。

为什么不在 universe 层做多份过滤:
  - 物理数据相同，只是过滤规则不同 → 多份 parquet 就是冗余
  - 过滤规则未来会改（市值阈值、ST 包含与否），改规则不应该要重跑 pull
  - 单源真相：所有人看到的 ticker 来源一致，分歧只在过滤规则本身

为什么 100 亿是合理底线（用户实操不交易 100 亿以下）:
  - 数据底座边界 = 用户实际研究/交易范围
  - 100 亿以下股票每天增量更新没有 ROI

消费方代码示例
─────────────
    import pandas as pd
    universe = pd.read_parquet('data_cache/universe/cn_a.parquet')

    # Billionaire 用：全集扫信号
    billionaire_pool = universe

    # sh_quant 因子研究用：清洁池
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=60)
    research_pool = universe[
        (~universe['name'].str.contains('ST')) &
        (universe['list_date'] <= cutoff)
    ]

    # 蓝筹策略用
    blue_chip_pool = universe[universe['total_mv'] >= 500]

依赖
────
tushare（必须；stock_basic + daily_basic）
TUSHARE_TOKEN（.env 里）

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/pull_universe.py                       # 默认 100 亿+ 全集
    python scripts/pull_universe.py --min-mv 50           # 阈值改成 50 亿
    python scripts/pull_universe.py --trade-date 20260509 # 指定快照日

输出
────
    data_cache/universe/cn_a.parquet
        列: ts_code, symbol, name, area, sector (SW L1), industry (SW L2),
            market, exchange, list_date, total_mv (亿元), circ_mv (亿元),
            pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share,
            float_share, turnover_rate, snapshot_date

        注：sector/industry 是申万 2021 体系（HEAT-5 大盘云图三级分层用），
        通过 Tushare index_member 拉每个 L2 行业的成员 join。前置依赖：
        scripts/pull_sw_industries.py 已落盘 sw_l1/sw_l2/_industries.parquet。

接下来怎么用清单拉历史
─────────────────────
    python scripts/pull_universe.py        # 刷新池子清单
    python scripts/update_daily.py         # 自动读 universe + 已缓存，拉数据

    后续每天 cron 同样跑 `python scripts/update_daily.py` 即可
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))  # noqa: E402

from utils.industry_consistency import assert_consistency, resolve_industry_sector  # noqa: E402

CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'
SW_MEMBER_CACHE = PROJECT_ROOT / 'data_cache' / 'sw_member_map.parquet'
SW_L1_META = PROJECT_ROOT / 'data_cache' / 'sw_l1' / '_industries.parquet'
SW_L2_META = PROJECT_ROOT / 'data_cache' / 'sw_l2' / '_industries.parquet'

OUT_FILE = CACHE_DIR_UNIVERSE / 'cn_a.parquet'

PERMISSION_KEYWORDS = ('40203', '权限', '积分', 'permission')


class PermissionError_(RuntimeError):
    """tushare 权限/积分错误。"""


def load_token() -> str:
    try:
        from dotenv import load_dotenv  # noqa: WPS433
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh`。')
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        sys.exit('TUSHARE_TOKEN not found in .env。')
    return token


def safe_call(pro, name: str, **kwargs) -> pd.DataFrame:
    """统一调用 Tushare API，权限错误统一抛 PermissionError_。"""
    try:
        return getattr(pro, name)(**kwargs)
    except Exception as e:
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionError_(f'{name}: {e}') from e
        raise


def latest_trade_date_with_data(pro, max_lookback: int = 10) -> str:
    """从今天往前回溯，找最近一个 daily_basic 已发布的交易日。

    Tushare daily_basic 通常在收盘后 1-2 小时（17:00-19:00 CST）才发布。
    如果用户在白天跑，今天的数据还没到，自动回退到前一个交易日。
    """
    today = pd.Timestamp.today().strftime('%Y%m%d')
    cal = safe_call(
        pro,
        'trade_cal',
        exchange='SSE',
        start_date=(pd.Timestamp.today() - pd.Timedelta(days=max_lookback * 2)).strftime('%Y%m%d'),
        end_date=today,
        is_open='1',
    )
    if cal is None or cal.empty:
        sys.exit('trade_cal 返回空，无法判定交易日。')

    trade_days = sorted(cal['cal_date'].tolist(), reverse=True)
    for i, date in enumerate(trade_days[:max_lookback]):
        # 探测：这一天 daily_basic 有数据吗？只拿 1 列省流量
        snap = safe_call(pro, 'daily_basic', trade_date=date, fields='ts_code,total_mv')
        if snap is not None and not snap.empty and len(snap) > 100:
            if i > 0:
                print(f'  注: 今日 ({today}) daily_basic 暂未发布，回退使用 {date} 作为快照日')
            return date

    sys.exit(
        f'回溯 {max_lookback} 个交易日都没找到 daily_basic 数据，可能 Tushare 异常或权限不足。'
    )


# 别名保留向后兼容
most_recent_trade_date = latest_trade_date_with_data


def _fetch_sw_member_map(pro, *, refresh: bool = False) -> pd.DataFrame:
    """构建 ts_code → (sw_l1_name, sw_l2_name) 映射。

    走 Tushare index_member(index_code=L2_code) 逐个 L2 拉成员，取当前在册
    （out_date 为空）的 con_code。L2 ~134 个，串行调用，给本地 parquet 缓存
    避免重复消耗 Tushare 积分。

    返回列: ts_code, sector (SW L1 name), industry (SW L2 name)
    """
    if SW_MEMBER_CACHE.exists() and not refresh:
        cached = pd.read_parquet(SW_MEMBER_CACHE)
        print(f'  SW 成员映射: 命中缓存 ({len(cached)} 条, --refresh-sw 强制重拉)')
        return cached

    print('  SW 成员映射: 缓存缺失或 --refresh-sw，逐个 L2 拉 index_member...')

    # 1) L1 meta：用 index_classify 拿 ts_code + industry_code 双列（L1 _industries
    # 里没存 industry_code，只能现拉一次）
    l1_classify = safe_call(pro, 'index_classify', level='L1', src='SW2021')
    if l1_classify is None or l1_classify.empty:
        sys.exit('index_classify(L1) 返回空，无法拿 SW L1 行业表。')
    l1_map = dict(zip(l1_classify['industry_code'], l1_classify['industry_name']))

    # 2) L2 meta（本地）
    l2_raw = pd.read_parquet(SW_L2_META)
    l2_codes = l2_raw['ts_code'].tolist()
    l2_to_name = dict(zip(l2_raw['ts_code'], l2_raw['industry_name']))
    l2_to_parent = dict(zip(l2_raw['ts_code'], l2_raw['parent_code']))

    # 3) 逐个 L2 拉成员
    rows: list[dict] = []
    for i, l2_code in enumerate(l2_codes, 1):
        try:
            df = safe_call(pro, 'index_member', index_code=l2_code)
        except PermissionError_ as e:
            sys.exit(f'index_member({l2_code}) 权限不足: {e}')
        if df is None or df.empty:
            continue
        # 只要当前在册的（out_date 空）
        if 'out_date' in df.columns:
            df = df[df['out_date'].isna() | (df['out_date'] == '')]
        l2_name = l2_to_name[l2_code]
        l1_name = l1_map.get(l2_to_parent[l2_code], None)
        for ts_code in df['con_code'].unique():
            rows.append({'ts_code': ts_code, 'sector': l1_name, 'industry': l2_name})
        if i % 20 == 0:
            print(f'    {i}/{len(l2_codes)} L2 完成')
        time.sleep(0.05)  # Tushare 没具体节流，给个小间隔保险

    if not rows:
        sys.exit('SW 成员映射全空，可能是 Tushare index_member 权限不足。')

    member = pd.DataFrame(rows)
    # 同一只股票理论上只属于一个 L2；若 Tushare 返回有重叠（历史合并/分拆），
    # 保留首条
    member = member.drop_duplicates(subset='ts_code', keep='first').reset_index(drop=True)
    SW_MEMBER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    member.to_parquet(SW_MEMBER_CACHE, index=False)
    print(f'  SW 成员映射: {len(member)} 条已写 {SW_MEMBER_CACHE.relative_to(PROJECT_ROOT)}')
    return member


def fetch_universe(pro, trade_date: str, *, refresh_sw: bool = False) -> pd.DataFrame:
    """拿 A 股基础信息 + 当日市值快照 + SW L1/L2 行业，merge 后返回。

    industry 列含义已切换：从 Tushare stock_basic 的自由文本 industry
    （~106 个非标准分类）改为 SW L2 行业名（HEAT-5 要求三级 treemap）。
    sector 列新增：SW L1 行业名。
    """
    # 1) 上市状态 L 的全 A 股（包含主板/创业/科创/北交所）
    # 注意：stock_basic 自带的 industry 是 Tushare 自由文本，跟 SW 体系不一致，
    # 不取。下面用 index_member 自己 join SW L1/L2。
    basic = safe_call(
        pro,
        'stock_basic',
        exchange='',
        list_status='L',
        fields='ts_code,symbol,name,area,market,exchange,list_date',
    )
    if basic is None or basic.empty:
        sys.exit('stock_basic 返回空。')
    basic['list_date'] = pd.to_datetime(basic['list_date'], format='%Y%m%d', errors='coerce')

    # 2) 当日所有股票的市值快照（一次调用拿全市场，比逐股调便宜得多）
    daily_basic = safe_call(
        pro,
        'daily_basic',
        trade_date=trade_date,
        fields=(
            'ts_code,total_mv,circ_mv,pe,pe_ttm,pb,ps,ps_ttm,'
            'dv_ratio,dv_ttm,total_share,float_share,turnover_rate'
        ),
    )
    if daily_basic is None or daily_basic.empty:
        sys.exit(f'daily_basic({trade_date}) 返回空。换一个 --trade-date 试试？')

    # Tushare 的 total_mv 单位是万元，转成亿元（实操更直观）
    daily_basic['total_mv'] = daily_basic['total_mv'] / 1e4  # 万元 → 亿元
    daily_basic['circ_mv'] = daily_basic['circ_mv'] / 1e4

    df = basic.merge(daily_basic, on='ts_code', how='inner')

    # 3) SW L1/L2 行业 overlay（HEAT-5：sector=SW L1, industry=SW L2）
    sw_map = _fetch_sw_member_map(pro, refresh=refresh_sw)
    df = df.merge(sw_map, on='ts_code', how='left')

    df['snapshot_date'] = pd.to_datetime(trade_date, format='%Y%m%d')
    return df.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description='生成 A 股标的池清单（单文件，含 ST/次新，消费侧自行过滤）',
    )
    ap.add_argument(
        '--min-mv',
        type=float,
        default=100.0,
        help='最低总市值（亿元，默认 100。实操底线，不建议低于此）',
    )
    ap.add_argument(
        '--trade-date', default='', help='市值快照日 YYYYMMDD（默认最近有数据的交易日）'
    )
    ap.add_argument('--out', default=str(OUT_FILE), help='输出 parquet 路径')
    ap.add_argument(
        '--refresh-sw',
        action='store_true',
        help='强制重拉 SW L2 成员映射（默认走 data_cache/sw_member_map.parquet 缓存）',
    )
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR_UNIVERSE.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    # 1) 决定取数日
    trade_date = args.trade_date or latest_trade_date_with_data(pro)
    print(f'快照日: {trade_date}')
    print(f'底线: 市值 ≥ {args.min_mv} 亿（含 ST/次新，消费侧自行过滤）')

    # 2) 拉数据
    print('\n拉 stock_basic + daily_basic...')
    try:
        df = fetch_universe(pro, trade_date, refresh_sw=args.refresh_sw)
    except PermissionError_ as e:
        sys.exit(f'权限/积分不足: {e}')

    # 3) 过滤——只按市值，不剔除 ST/次新（留给消费侧）
    n0 = len(df)
    print(f'\n  上市中: {n0} 只')
    filtered = df[df['total_mv'] >= args.min_mv].reset_index(drop=True)
    print(f'  市值 ≥ {args.min_mv} 亿: {len(filtered)} 只 (剔除 {n0 - len(filtered)} 只)')

    # 4) HEAT-5 验收：sector / industry 一致性 + 填充率
    filtered = resolve_industry_sector(filtered, market='CN')
    # SW L1 28 个，SW L2 ~100 个；index_member 实际能 join 上的 distinct L2
    # 数量取决于 universe 覆盖面，30 是 HEAT-5 合同里的下限
    assert_consistency(filtered, market='CN', min_distinct_industries=30)

    # 5) 写出
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_parquet(out_path, index=False)

    print(f'\n✓ {len(filtered)} 只标的写入 {out_path.relative_to(PROJECT_ROOT)}')

    # 5) 池子结构
    print('\n池子组成:')
    if 'exchange' in filtered.columns:
        print(' 按交易所:')
        print(filtered['exchange'].value_counts().to_string())
    if 'market' in filtered.columns:
        print('\n 按板块:')
        print(filtered['market'].value_counts().to_string())
    print(
        f'\n 总市值范围: {filtered["total_mv"].min():.0f} 亿 → {filtered["total_mv"].max():.0f} 亿'
    )
    print(f' 中位市值: {filtered["total_mv"].median():.0f} 亿')

    # 6) ST / 次新 的统计（让用户知道有多少，消费侧可以过滤）
    st_count = filtered['name'].str.contains('ST', na=False, regex=False).sum()
    if 'list_date' in filtered.columns:
        cutoff_60 = pd.Timestamp.today() - pd.Timedelta(days=60)
        new_count = (filtered['list_date'] > cutoff_60).sum()
    else:
        new_count = 0
    print(f'\n 其中含 ST/*ST: {st_count} 只')
    print(f' 其中上市<60 天: {new_count} 只')
    print(' （消费侧按需过滤，参考 docstring 顶部的示例）')

    print('\n下一步拉物理日线:')
    print('  python scripts/update_daily.py --workers 10')
    print('  （默认自动用 universe + 缓存的并集，不用传别的参数）')


if __name__ == '__main__':
    main()
