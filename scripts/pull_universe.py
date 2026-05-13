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
        列: ts_code, symbol, name, area, industry, market, exchange,
            list_date, total_mv (亿元), circ_mv (亿元), pe, pe_ttm, pb,
            ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share,
            turnover_rate, snapshot_date

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
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'

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
        pro, 'trade_cal',
        exchange='SSE',
        start_date=(pd.Timestamp.today() - pd.Timedelta(days=max_lookback * 2))
                   .strftime('%Y%m%d'),
        end_date=today,
        is_open='1',
    )
    if cal is None or cal.empty:
        sys.exit('trade_cal 返回空，无法判定交易日。')

    trade_days = sorted(cal['cal_date'].tolist(), reverse=True)
    for i, date in enumerate(trade_days[:max_lookback]):
        # 探测：这一天 daily_basic 有数据吗？只拿 1 列省流量
        snap = safe_call(pro, 'daily_basic', trade_date=date,
                         fields='ts_code,total_mv')
        if snap is not None and not snap.empty and len(snap) > 100:
            if i > 0:
                print(f'  注: 今日 ({today}) daily_basic 暂未发布，'
                      f'回退使用 {date} 作为快照日')
            return date

    sys.exit(f'回溯 {max_lookback} 个交易日都没找到 daily_basic 数据，'
             '可能 Tushare 异常或权限不足。')


# 别名保留向后兼容
most_recent_trade_date = latest_trade_date_with_data


def fetch_universe(pro, trade_date: str) -> pd.DataFrame:
    """拿 A 股基础信息 + 当日市值快照，merge 后返回。"""
    # 1) 上市状态 L 的全 A 股（包含主板/创业/科创/北交所）
    basic = safe_call(
        pro, 'stock_basic',
        exchange='', list_status='L',
        fields='ts_code,symbol,name,area,industry,market,exchange,list_date',
    )
    if basic is None or basic.empty:
        sys.exit('stock_basic 返回空。')
    basic['list_date'] = pd.to_datetime(basic['list_date'], format='%Y%m%d',
                                        errors='coerce')

    # 2) 当日所有股票的市值快照（一次调用拿全市场，比逐股调便宜得多）
    daily_basic = safe_call(
        pro, 'daily_basic',
        trade_date=trade_date,
        fields=('ts_code,total_mv,circ_mv,pe,pe_ttm,pb,ps,ps_ttm,'
                'dv_ratio,dv_ttm,total_share,float_share,turnover_rate'),
    )
    if daily_basic is None or daily_basic.empty:
        sys.exit(f'daily_basic({trade_date}) 返回空。换一个 --trade-date 试试？')

    # Tushare 的 total_mv 单位是万元，转成亿元（实操更直观）
    daily_basic['total_mv'] = daily_basic['total_mv'] / 1e4   # 万元 → 亿元
    daily_basic['circ_mv'] = daily_basic['circ_mv'] / 1e4

    df = basic.merge(daily_basic, on='ts_code', how='inner')
    df['snapshot_date'] = pd.to_datetime(trade_date, format='%Y%m%d')
    return df.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description='生成 A 股标的池清单（单文件，含 ST/次新，消费侧自行过滤）',
    )
    ap.add_argument('--min-mv', type=float, default=100.0,
                    help='最低总市值（亿元，默认 100。实操底线，不建议低于此）')
    ap.add_argument('--trade-date', default='',
                    help='市值快照日 YYYYMMDD（默认最近有数据的交易日）')
    ap.add_argument('--out', default=str(OUT_FILE),
                    help='输出 parquet 路径')
    args = ap.parse_args()

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR_UNIVERSE.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    # 1) 决定取数日
    if args.trade_date:
        trade_date = args.trade_date
    else:
        trade_date = latest_trade_date_with_data(pro)
    print(f'快照日: {trade_date}')
    print(f'底线: 市值 ≥ {args.min_mv} 亿（含 ST/次新，消费侧自行过滤）')

    # 2) 拉数据
    print('\n拉 stock_basic + daily_basic...')
    try:
        df = fetch_universe(pro, trade_date)
    except PermissionError_ as e:
        sys.exit(f'权限/积分不足: {e}')

    # 3) 过滤——只按市值，不剔除 ST/次新（留给消费侧）
    n0 = len(df)
    print(f'\n  上市中: {n0} 只')
    filtered = df[df['total_mv'] >= args.min_mv].reset_index(drop=True)
    print(f'  市值 ≥ {args.min_mv} 亿: {len(filtered)} 只 '
          f'(剔除 {n0 - len(filtered)} 只)')

    # 4) 写出
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
    print(f'\n 总市值范围: {filtered["total_mv"].min():.0f} 亿 → '
          f'{filtered["total_mv"].max():.0f} 亿')
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

    print(f'\n下一步拉物理日线:')
    print(f'  python scripts/update_daily.py --workers 10')
    print(f'  （默认自动用 universe + 缓存的并集，不用传别的参数）')


if __name__ == '__main__':
    main()
