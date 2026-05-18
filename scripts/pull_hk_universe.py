"""生成港股标的池清单，写到 data_cache/universe/cn_hk.parquet。

跟 pull_universe.py（A 股）/ pull_us_universe.py（美股）配套。三个 universe
文件 update_daily.py 默认会自动 union，所以不用单独传参就能一起拉 OHLC。

为什么 Futu get_stock_filter 而非 Tushare hk_basic
──────────────────────────────────────────────────
US 的 FMP company-screener 一次调用同时给 name + marketCap —— HK 的结构等价物
就是 Futu get_stock_filter：返回 stock_code + stock_name + MARKET_VAL（可降序）。
Tushare hk_basic 只有 name 没市值，HK 市值要走 hk_daily（10 次/天，废）。所以
Futu screener 才是跟 US 同构的选择。get_stock_filter 不吃 request_history_kline
1000/天 配额，但**有独立限速：Maximum 10 times / 30s**。全集 ~2600 需翻 ~14 页，
脚本页间 sleep 3.5s 限速，整体 build 一次约 45s（一次性脚本，可接受）。

为什么市值底线 + board 列而非多份文件
──────────────────────────────────────
跟 pull_universe.py 一致的哲学："数据完整 + 消费侧过滤"。本脚本只产 **一份**
universe，物理覆盖到 stocks/；各消费方（pull_hk_futu --backfill / 前端 screener）
根据 name / market_cap / board 列在自己代码里做过滤，避免多份 universe 维护负担。
board 列把 RMB 双柜台（8 字头，e.g. 80700 = 00700 腾讯的人民币柜台）/ GEM 标出，
消费方按需排除 —— 解决 get_stock_filter 头部混入 80700 杂线的问题。

前置
────
  1. FutuOpenD 已启动并登录（GUI 见"连接服务器成功 + 登录账号成功"）
  2. HK Lv1 已开通；不同时开富途 App（会顶号）
  3. pip install futu-api

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/pull_hk_universe.py                  # 默认 10 亿港元+ 全 HK
    python scripts/pull_hk_universe.py --min-mv 50      # 阈值改成 50 亿港元
    python scripts/pull_hk_universe.py --min-mv 0       # 全集（含微盘，~2600）

输出
────
    data_cache/universe/cn_hk.parquet
        列: ts_code (00700.HK), symbol (00700), name (腾讯控股),
            market (cn_hk), exchange (HKEX), currency (HKD),
            market_cap (亿港元), board (MAIN/GEM/RMB_COUNTER), snapshot_date

下一步
──────
    python scripts/update_daily.py --market hk
    # update_daily 自动 union cn_hk.parquet 的 ts_code，HK 票走 Futu snapshot 日更
    # 历史回填仍走 scripts/pull_hk_futu.py --backfill
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
        Market,
        OpenQuoteContext,
        SimpleFilter,
        SortDir,
        StockField,
    )
except ImportError:
    sys.exit('futu-api 没装. 跑: pip install futu-api')

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'
OUT_FILE = CACHE_DIR_UNIVERSE / 'cn_hk.parquet'

HOST = '127.0.0.1'
PORT = 11111

# get_stock_filter 单次返回上限
PAGE_SIZE = 200
# 全 HK universe ~2600, 给足翻页余量防死循环
MAX_PAGES = 60
# get_stock_filter 限速: 富途 "Maximum 10 times per 30 seconds" = 1 次/3s.
# 翻页拿全集必须页间限速, 否则 begin>=2000 处必报 high-frequency.
STOCK_FILTER_SLEEP_SEC = 3.5


def classify_board(symbol: str) -> str:
    """5 位港股代码 → 板块/柜台分类（启发式，消费方可再细分）。

    - 80000–89999 : RMB_COUNTER（人民币双柜台，跟主柜台同公司，回测应排除）
    - 08000–08999 : GEM（创业板）
    - 其余         : MAIN（主板）
    """
    try:
        n = int(symbol)
    except ValueError:
        return 'MAIN'
    if 80000 <= n <= 89999:
        return 'RMB_COUNTER'
    if 8000 <= n <= 8999:
        return 'GEM'
    return 'MAIN'


def fetch_hk_universe(ctx: OpenQuoteContext, min_mv_hkd: float) -> pd.DataFrame:
    """调 Futu get_stock_filter 翻页拿全 HK（市值 >= floor，降序）。

    每个 StockFilterData item：.stock_code ('HK.00700')、.stock_name ('腾讯控股')、
    item[mc_filter] = 该股 MARKET_VAL（港元）。get_stock_filter 独立配额池。
    """
    mc_filter = SimpleFilter()
    mc_filter.stock_field = StockField.MARKET_VAL
    mc_filter.filter_min = min_mv_hkd
    mc_filter.is_no_filter = False
    mc_filter.sort = SortDir.DESCEND

    rows: list[dict] = []
    begin = 0
    for _ in range(MAX_PAGES):
        ret, data = ctx.get_stock_filter(
            market=Market.HK,
            filter_list=[mc_filter],
            begin=begin,
            num=PAGE_SIZE,
        )
        if ret != RET_OK:
            raise RuntimeError(f'get_stock_filter failed at begin={begin}: {data}')
        last_page, _all_count, ret_list = data
        if not ret_list:
            break
        for item in ret_list:
            symbol = item.stock_code.split('.')[1]
            try:
                mc_hkd = float(item[mc_filter])
            except (KeyError, TypeError, ValueError):
                mc_hkd = float('nan')
            rows.append(
                {
                    'symbol': symbol,
                    'name': item.stock_name,
                    'market_cap': round(mc_hkd / 1e8, 2),  # 港元 → 亿港元
                }
            )
        if last_page:
            break
        begin += PAGE_SIZE
        time.sleep(STOCK_FILTER_SLEEP_SEC)  # 限速: 10 次/30s

    if not rows:
        sys.exit('get_stock_filter 返回空。检查 OpenD 是否登录 + HK Lv1 是否开通。')

    df = pd.DataFrame(rows).drop_duplicates(subset='symbol').reset_index(drop=True)
    n_nan = int(df['market_cap'].isna().sum())
    if n_nan:
        print(f'  ⚠ {n_nan} 只 market_cap 取值失败 (留 NaN, 消费侧过滤)')
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description='生成港股标的池（Futu get_stock_filter）')
    ap.add_argument(
        '--min-mv',
        type=float,
        default=10.0,
        help='最低市值（亿港元，默认 10 = 10 亿港元；0 = 全集含微盘）',
    )
    ap.add_argument('--out', default=str(OUT_FILE), help='输出 parquet 路径')
    args = ap.parse_args()

    print(f'[Futu] connecting to {HOST}:{PORT} ...')
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(f'OpenQuoteContext 创建失败: {e}\n→ OpenD 启动了吗? lsof -i :{PORT}')

    try:
        print(f'拉 Futu get_stock_filter (HK, market_cap >= {args.min_mv} 亿港元, 降序)...\n')
        df = fetch_hk_universe(ctx, min_mv_hkd=args.min_mv * 1e8)
    finally:
        with contextlib.suppress(Exception):
            ctx.close()

    # sh_quant universe schema（对齐 cn_a.parquet / us.parquet）
    df['ts_code'] = df['symbol'] + '.HK'
    df['market'] = 'cn_hk'
    df['exchange'] = 'HKEX'
    df['currency'] = 'HKD'
    df['board'] = df['symbol'].map(classify_board)
    df['snapshot_date'] = pd.Timestamp.today().normalize()

    keep_cols = [
        'ts_code',
        'symbol',
        'name',
        'market',
        'exchange',
        'currency',
        'market_cap',
        'board',
        'snapshot_date',
    ]
    out = df[keep_cols].copy()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)

    print(f'\n✓ {len(out)} 只标的写入 {out_path.relative_to(PROJECT_ROOT)}')

    print('\n池子组成:')
    print(' 按 board:')
    print(out['board'].value_counts().to_string())
    mc = out['market_cap'].dropna()
    if len(mc):
        print(f'\n 市值范围: {mc.min():.1f} 亿 → {mc.max() / 1e4:.2f} 万亿港元')
        print(f' 中位市值: {mc.median():.1f} 亿港元')
    print('\n 头部 5 只:')
    print(
        out.sort_values('market_cap', ascending=False)
        .head(5)[['ts_code', 'name', 'market_cap', 'board']]
        .to_string(index=False)
    )

    print('\n下一步:')
    print('  python scripts/update_daily.py --market hk   # 日更 (Futu snapshot)')
    print('  python scripts/pull_hk_futu.py --backfill    # 历史回填')


if __name__ == '__main__':
    main()
