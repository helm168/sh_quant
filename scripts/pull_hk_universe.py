"""生成港股标的池清单，写到 data_cache/universe/cn_hk.parquet。

跟 pull_universe.py（A 股）/ pull_us_universe.py（美股）配套。三个 universe
文件 update_daily.py 默认会自动 union，所以不用单独传参就能一起拉 OHLC。

两源 merge（跟 pull_universe.py 同构）
──────────────────────────────────────
pull_universe.py（A 股）= Tushare stock_basic（中文名）+ daily_basic（市值）merge。
HK 完全同构，只是市值那源换成 Futu（HK 市值 Tushare 走 hk_daily 限 10 次/天，废）：

  - Tushare hk_basic   → name（中文，如 腾讯控股）+ list_date。canonical 身份源，
                          跟 DATA_SCHEMA / 前端 screener 期望一致。非 _vip，基础
                          付费档可用，不吃 kline 配额。
  - Futu get_stock_filter → market_cap（MARKET_VAL）+ board。Futu 的 stock_name
                          是英文短名（TENCENT），仅作 Tushare 缺失时回退。
                          有独立限速 Maximum 10 times / 30s，全集 ~2700 翻 ~14 页，
                          脚本页间 sleep 3.5s，build 一次约 45s（可接受）。

按 ts_code（00700.HK，两源格式一致）left-merge。无 token / hk_basic 失败 →
**显眼警告**并降级用 Futu 英文名（不静默），universe 对 backfill 仍可用。

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
        列: ts_code (00700.HK), symbol (00700), name (腾讯控股, Tushare 中文),
            market (cn_hk), exchange (HKEX), currency (HKD), list_date,
            market_cap (亿港元, Futu), board (MAIN/GEM/RMB_COUNTER),
            sector (Yahoo GICS), industry (Yahoo GICS), snapshot_date

        sector/industry 来自 Yahoo Finance summaryProfile（HEAT-5 大盘云图
        三级分层用），增量缓存在 data_cache/hk_industry_map.parquet，
        --refresh-industry 强制重拉。

下一步
──────
    python scripts/update_daily.py --market hk
    # update_daily 自动 union cn_hk.parquet 的 ts_code，HK 票走 Futu snapshot 日更
    # 历史回填仍走 scripts/pull_hk_futu.py --backfill
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))  # noqa: E402

from utils.industry_consistency import assert_consistency, resolve_industry_sector  # noqa: E402

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

CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'
OUT_FILE = CACHE_DIR_UNIVERSE / 'cn_hk.parquet'

# Yahoo HK 行业 cache（增量更新，单次跑 2700 只 ~30-40 分钟，命中后秒级）
HK_INDUSTRY_CACHE = PROJECT_ROOT / 'data_cache' / 'hk_industry_map.parquet'

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


def fetch_tushare_hk_names() -> pd.DataFrame:
    """Tushare hk_basic → 中文名 + list_date（canonical 身份源）。

    跟 pull_universe.py 用 Tushare stock_basic 取 A 股中文名同构。best-effort：
    无 token / 接口失败 → 返回空 df（列齐），调用方降级用 Futu 英文名 + 打**显眼**
    警告（不静默）。hk_basic 非 _vip，基础付费档可用，不吃 history_kline 配额。
    """
    empty = pd.DataFrame(columns=['ts_code', 'name', 'list_date'])
    try:
        from dotenv import load_dotenv
    except ImportError:
        print('  ⚠ python-dotenv 没装, 跳过中文名 overlay (名字将是 Futu 英文)')
        return empty
    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        print('  ⚠ TUSHARE_TOKEN 未配置, 名字将是 Futu 英文短名 (非中文)')
        return empty
    try:
        import tushare as ts

        ts.set_token(token)
        pro = ts.pro_api()
        df = pro.hk_basic(fields='ts_code,name,list_date')
    except Exception as e:
        print(f'  ⚠ Tushare hk_basic 失败, 名字降级英文: {type(e).__name__}: {e}')
        return empty
    if df is None or df.empty:
        print('  ⚠ Tushare hk_basic 返回空, 名字降级英文')
        return empty
    return df[['ts_code', 'name', 'list_date']]


def _ts_code_to_yahoo(ts_code: str) -> str | None:
    """sh_quant '00700.HK' → Yahoo '0700.HK'。

    HK 主柜台 ts_code 是 5 位零填充（'00700.HK'），Yahoo 用 4 位（'0700.HK'）。
    RMB 双柜台（80700 / 8 字头）Yahoo 没有独立条目，跳过，返回 None。
    """
    symbol = ts_code.split('.')[0]
    try:
        n = int(symbol)
    except ValueError:
        return None
    # RMB 双柜台没法直接查 Yahoo；走主柜台路径太脏，留 NaN（前端归 "—"）
    if 80000 <= n <= 89999:
        return None
    # 标准化 4 位（HK Yahoo 约定）
    return f'{n:04d}.HK'


def _fetch_one_yahoo(ts_code: str, *, max_retries: int = 3) -> dict:
    """单只港股 Yahoo summaryProfile.{sector, industry}。

    Yahoo 经常 429 限流（YFRateLimitError），重试 max_retries 次，每次指数退避。
    限流外的失败立即返回空。RMB 双柜台（8xxxx）跳过，由调用方走主柜台继承。
    """
    yahoo_code = _ts_code_to_yahoo(ts_code)
    if yahoo_code is None:
        return {'ts_code': ts_code, 'sector': None, 'industry': None}
    import yfinance as yf

    last_err = ''
    for attempt in range(max_retries):
        try:
            info = yf.Ticker(yahoo_code).info or {}
            return {
                'ts_code': ts_code,
                'sector': info.get('sector') or None,
                'industry': info.get('industry') or None,
            }
        except Exception as e:  # noqa: BLE001
            last_err = f'{type(e).__name__}: {e}'[:80]
            if 'RateLimit' in type(e).__name__ or '429' in str(e) or 'Too Many' in str(e):
                # 指数退避 + 抖动
                time.sleep(2 ** attempt + (attempt * 0.3))
                continue
            break
    return {'ts_code': ts_code, 'sector': None, 'industry': None, '_err': last_err}


def _inherit_rmb_counter(df: pd.DataFrame) -> pd.DataFrame:
    """RMB 双柜台（80700.HK）继承主柜台（00700.HK）的 sector/industry。

    Yahoo 没收 8xxxx 双柜台条目，但它们跟主柜台是同一家公司，必须用同样的分类，
    否则前端 treemap 会出空桶。
    """
    df = df.copy()

    def _main_code(ts: str) -> str | None:
        sym = ts.split('.')[0]
        try:
            n = int(sym)
        except ValueError:
            return None
        if 80000 <= n <= 89999:
            # 8XXXX → 0XXXX（5 位零填充）
            return f'{n - 80000:05d}.HK'
        return None

    df['_main'] = df['ts_code'].map(_main_code)
    main_map = df.set_index('ts_code')[['sector', 'industry']]
    mask = df['_main'].notna() & df['sector'].isna()
    n_filled = 0
    for idx in df[mask].index:
        main_ts = df.at[idx, '_main']
        if main_ts in main_map.index:
            df.at[idx, 'sector'] = main_map.at[main_ts, 'sector']
            df.at[idx, 'industry'] = main_map.at[main_ts, 'industry']
            if pd.notna(df.at[idx, 'sector']):
                n_filled += 1
    if n_filled:
        print(f'  RMB 双柜台继承主柜台: {n_filled} 只补齐 sector/industry')
    return df.drop(columns=['_main'])


def fetch_hk_industries(
    ts_codes: list[str],
    *,
    refresh: bool,
    workers: int = 2,
) -> pd.DataFrame:
    """ts_code 列表 → DataFrame(ts_code, sector, industry)，磁盘 cache 增量更新。

    首次跑 ~2700 票 × ~0.5-1s/票 ≈ 25-45 分钟（yfinance 内部限流，并发收益有限）。
    后续 cron 跑：cache 已有的直接跳过，只补新上市，秒级完成。
    """
    try:
        import yfinance  # noqa: F401
    except ImportError:
        sys.exit('yfinance 没装. 跑: uv pip install yfinance')

    if HK_INDUSTRY_CACHE.exists() and not refresh:
        cached = pd.read_parquet(HK_INDUSTRY_CACHE)
    else:
        cached = pd.DataFrame(columns=['ts_code', 'sector', 'industry'])

    # 已缓存但 sector 为空 → 上次拉失败（多半 429），这次重试。
    # RMB 双柜台（_ts_code_to_yahoo 返回 None）Yahoo 永远查不到，留 cache 不重试，
    # 后面统一靠 _inherit_rmb_counter 从主柜台继承。
    cached_hit = cached['sector'].notna()
    cached_rmb = cached['ts_code'].map(_ts_code_to_yahoo).isna()
    keep_cached_set = set(cached.loc[cached_hit | cached_rmb, 'ts_code'].tolist())
    todo = [c for c in ts_codes if c not in keep_cached_set]
    cached = cached[cached['ts_code'].isin(keep_cached_set)]
    if not todo:
        print(f'  Yahoo 行业映射: 全命中缓存 ({len(cached)} 条)')
        return cached

    print(
        f'  Yahoo 行业映射: 缓存 {len(keep_cached_set)} 条命中, '
        f'本轮拉 {len(todo)} 只新增/--refresh 票（耗时约 {len(todo) * 0.5 / 60:.0f}-'
        f'{len(todo) / 60:.0f} 分钟）',
    )

    new_rows: list[dict] = []
    n_err = 0
    n_done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one_yahoo, c): c for c in todo}
        for fut in as_completed(futures):
            row = fut.result()
            if row.get('_err'):
                n_err += 1
            row.pop('_err', None)
            new_rows.append(row)
            n_done += 1
            if n_done % 100 == 0:
                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (len(todo) - n_done) / rate if rate > 0 else 0
                print(
                    f'    {n_done}/{len(todo)} ({rate:.1f}/s, ETA {eta / 60:.1f} min, '
                    f'err {n_err})',
                )

    new = pd.DataFrame(new_rows)
    new = new[['ts_code', 'sector', 'industry']]  # 丢 _err，不进 cache
    combined = pd.concat([cached, new], ignore_index=True).drop_duplicates(
        subset='ts_code', keep='last'
    )
    # RMB 双柜台从主柜台继承
    combined = _inherit_rmb_counter(combined)
    HK_INDUSTRY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(HK_INDUSTRY_CACHE, index=False)
    n_hit = combined['sector'].notna().sum()
    print(
        f'  Yahoo 行业映射: {n_hit}/{len(combined)} 命中 sector/industry, '
        f'{n_err} 只本轮接口失败 (重试 cache 留空), 已写 '
        f'{HK_INDUSTRY_CACHE.relative_to(PROJECT_ROOT)}',
    )
    return combined


def main() -> None:
    ap = argparse.ArgumentParser(description='生成港股标的池（Futu get_stock_filter）')
    ap.add_argument(
        '--min-mv',
        type=float,
        default=10.0,
        help='最低市值（亿港元，默认 10 = 10 亿港元；0 = 全集含微盘）',
    )
    ap.add_argument('--out', default=str(OUT_FILE), help='输出 parquet 路径')
    ap.add_argument(
        '--refresh-industry',
        action='store_true',
        help='强制重拉 Yahoo sector/industry 映射（默认走 hk_industry_map.parquet 增量缓存）',
    )
    ap.add_argument(
        '--skip-industry',
        action='store_true',
        help='跳过 Yahoo 行业 overlay（调试用；正常 build 不要传，HEAT-5 合同要求两列填充率 ≥ 90%）',
    )
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

    # 中文名 overlay: Tushare hk_basic 优先, 缺失回退 Futu 英文短名 (不静默)
    df = df.rename(columns={'name': 'name_futu'})
    tu = fetch_tushare_hk_names()
    df = df.merge(tu, on='ts_code', how='left')
    n_cn = int(df['name'].notna().sum())
    df['name'] = df['name'].where(df['name'].notna(), df['name_futu'])
    df = df.drop(columns=['name_futu'])
    n_fallback = len(df) - n_cn
    print(
        f'  中文名 overlay: {n_cn}/{len(df)} 命中 Tushare hk_basic'
        f'{f", {n_fallback} 只回退 Futu 英文 (RMB 柜台/新股/Tushare 缺失)" if n_fallback else ""}'
    )

    # Yahoo summaryProfile sector/industry overlay（HEAT-5 大盘云图三级分层）
    if not args.skip_industry:
        ind = fetch_hk_industries(df['ts_code'].tolist(), refresh=args.refresh_industry)
        df = df.merge(ind, on='ts_code', how='left')
    else:
        print('  ⚠ --skip-industry 跳过 Yahoo 行业，HEAT-5 合同将失败')
        df['sector'] = None
        df['industry'] = None

    keep_cols = [
        'ts_code',
        'symbol',
        'name',
        'market',
        'exchange',
        'currency',
        'list_date',
        'market_cap',
        'board',
        'sector',
        'industry',
        'snapshot_date',
    ]
    out = df[keep_cols].copy()

    # HEAT-5 验收：sector / industry 一致性 + 填充率
    # 注：RMB 双柜台 + 微盘新股 sector/industry 会留 NaN，--min-mv 默认 10 亿
    # 已经把大部分长尾过滤掉，填充率应能过 90%。HK 恒生分类未直接用 Yahoo 给的
    # GICS 体系（Yahoo HK 也走 GICS），所以下限 15 个 industry 是 HEAT-5 合同写死的
    if not args.skip_industry:
        out = resolve_industry_sector(out, market='HK')
        assert_consistency(out, market='HK', min_distinct_industries=15)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)

    try:
        shown = out_path.relative_to(PROJECT_ROOT)
    except ValueError:
        shown = out_path
    print(f'\n✓ {len(out)} 只标的写入 {shown}')

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
