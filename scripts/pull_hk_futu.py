"""
港股日线 puller (via 富途 OpenD).

为什么是 Futu 而不是 Yahoo / Tushare hk_daily:
  - Yahoo unofficial 数据漏更 + 429 限流不稳, schema 也飘 (turnover 字段时有时无)
  - Tushare 5000 档 hk_daily 限 10次/天 + 2次/分钟硬上限, 等于不能用
  - Futu OpenD 是富途券商, HK Lv1 免费, 数据笔记准确, 端口 11111

配额限制 (Futu 文档):
  - request_history_kline (历史 K 线): **1000 次/天**
  - get_market_snapshot / get_stock_basicinfo: 无限制
  - 一次 request_history_kline 拉一只票 ≤ 1000 根 K, 算 1 次配额

策略
────
  Cold start (第一次跑): 拉 6M 历史 ≤ 200 根, 1 配额/股.
    HK 全集 ~2700 支, 1000 配额一天拉不完 → 限 --max-tickers 800 (留 200 buffer
    给重试 + universe-info), 默认按主板 + ADV 排序拉前 800 高液态股.
  Daily update: 拉 since=last_trade_date+1 → today, 通常 1-3 根 K, 1 配额/股.
    增量逻辑: 读现有 parquet 最后一行 trade_date, 没有就 cold start 全 6M.

数据 schema (沿用 sh_quant docs/DATA_SCHEMA.md §1):
  ~/.market_data/stocks/{ts_code}.HK.parquet  (5 位补零, 例 00700.HK.parquet)
  columns: trade_date, ts_code, open, high, low, close, pre_close,
           change, pct_chg, vol, amount, adj_factor

前置
────
  1. 已下载并启动 FutuOpenD (https://www.futunn.com/download/openAPI)
     - GUI 看到 '连接服务器成功' + '登录账号成功'
     - HK Lv1 已开通 (App 行情 → 升级)
  2. 同时不开富途 App / 富途牛牛 App (会顶号)
  3. pip install futu-api

用法
────
    source .venv/bin/activate
    pip install futu-api

    # Dry-run: 拉 universe + 印前 10 个 ticker 的 cold 计划, 不打 API
    python scripts/pull_hk_futu.py --dry-run

    # 第一次 cold start: 拉前 800 支高液态股 6M 历史
    python scripts/pull_hk_futu.py --max-tickers 800

    # Daily 增量更新 (读现有 parquet 推 since)
    python scripts/pull_hk_futu.py --incremental --max-tickers 800

    # 只跑指定 ticker (debug 用)
    python scripts/pull_hk_futu.py --tickers 00700.HK,00981.HK
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from futu import (
        RET_OK,
        AuType,
        KLType,
        Market,
        OpenQuoteContext,
        SecurityType,
        SimpleFilter,
        SortDir,
        StockField,
    )
except ImportError:
    sys.exit('futu-api 没装. 跑: pip install futu-api')


# ── Constants ─────────────────────────────────────────────────────────────────

HOST = '127.0.0.1'
PORT = 11111

# Futu 文档: history_kline 配额 1000/天. 保 200 给 retry / 偶发失败.
DAILY_KLINE_QUOTA = 1000
SAFE_QUOTA_BUDGET = 800

# 数据落盘位置: 跟 DATA_SCHEMA.md §1 一致, ~/.market_data/stocks/
DEFAULT_DATA_DIR = Path.home() / '.market_data' / 'stocks'
UNIVERSE_DIR = Path.home() / '.market_data' / 'universe'

# Cold start 历史长度
COLD_LOOKBACK_DAYS = 200


# ── Universe ──────────────────────────────────────────────────────────────────


def fetch_hk_universe(ctx: OpenQuoteContext) -> pd.DataFrame:
    """从 Futu 拉港股全 universe (主板 + GEM). 不算 history_kline 配额."""
    ret, data = ctx.get_stock_basicinfo(
        market=Market.HK,
        stock_type=SecurityType.STOCK,
    )
    if ret != RET_OK:
        raise RuntimeError(f'get_stock_basicinfo failed: {data}')
    # data columns: code, name, lot_size, stock_type, stock_child_type,
    #   stock_owner, listing_date, stock_id, delisting
    # code 形如 'HK.00700'
    df = pd.DataFrame(data)
    df = df[df['delisting'] == False].copy()  # noqa: E712
    return df


def universe_to_ts_codes(uni_df: pd.DataFrame) -> list[str]:
    """`HK.00700` (Futu) → `00700.HK` (sh_quant)."""
    return [c.split('.')[1] + '.HK' for c in uni_df['code']]


def ts_code_to_futu(ts_code: str) -> str:
    """`00700.HK` (sh_quant) → `HK.00700` (Futu)."""
    code, _ = ts_code.split('.')
    return f'HK.{code}'


# ── Schema mapping ────────────────────────────────────────────────────────────


def futu_kline_to_sh_quant(df: pd.DataFrame, ts_code: str) -> pd.DataFrame:
    """
    Futu request_history_kline 输出 → sh_quant DATA_SCHEMA columns.

    Futu columns: code, time_key, open, close, high, low, pe_ratio,
        turnover_rate, volume, turnover, change_rate, last_close
    sh_quant   : trade_date, ts_code, open, high, low, close, pre_close,
        change, pct_chg, vol, amount, adj_factor

    Mapping:
      time_key (YYYY-MM-DD 00:00:00) → trade_date (YYYY-MM-DD)
      volume → vol         (Futu volume 单位 = 股, 跟 sh_quant 一致, 无需 ×100)
      turnover → amount    (Futu turnover 单位 = 港元, 跟 sh_quant amount 一致)
      last_close → pre_close
      change_rate → pct_chg  (%, 同符号)
      close - last_close → change
      adj_factor → 留 NaN 等 attach_adj_factor() 填 (从 get_rehab 拿)
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()
    out = pd.DataFrame()
    out['trade_date'] = pd.to_datetime(df['time_key']).dt.strftime('%Y-%m-%d')
    out['ts_code'] = ts_code
    out['open'] = df['open'].astype(float)
    out['high'] = df['high'].astype(float)
    out['low'] = df['low'].astype(float)
    out['close'] = df['close'].astype(float)
    out['pre_close'] = df['last_close'].astype(float)
    out['change'] = (df['close'] - df['last_close']).astype(float)
    out['pct_chg'] = df['change_rate'].astype(float)
    out['vol'] = df['volume'].astype('int64')
    out['amount'] = df['turnover'].astype(float)
    # adj_factor 不在这里加 — attach_adj_factor() 用 merge_asof 添加. 提前加占位
    # 会跟 rehab 表的同名列在 merge 时冲突 (suffix _x/_y), 找不到 adj_factor.
    return out


def get_rehab_factors(ctx: OpenQuoteContext, ts_code: str) -> pd.DataFrame:
    """
    拉这只股的除权除息事件 + 前复权因子 (A, B).

    Futu 官方文档 (openapi.futunn.com/futu-api-doc/quote/get-rehab.html):
        前复权价格 = 不复权价格 × forward_adj_factorA + forward_adj_factorB
        后复权价格 = 不复权价格 × backward_adj_factorA + backward_adj_factorB

    这是**仿射变换** (ax + b), 不是简单 ratio. A 是斜率 (含 split 因子),
    B 是截距 (含累计派息).

    实测 00700.HK + Yahoo 对照 (5/14):
        Yahoo adj_factor = 0.988483
        Futu forward_A = 1.0, forward_B = -5.3
        ratio = 1.0 + (-5.3) / 460.2 = 0.98848  ✓ 完美吻合

    踩坑 (我之前错): 直接拿 backward_adj_factorB (= 5.3) 当 Tushare 风格归一化
    因子用. B 是截距不是 ratio, 5.3 实际是"累计派息加回历史价" 的港币额, 不是
    什么 normalized 因子.

    get_rehab 是独立 quota (10/s 限速), 不算 history_kline 1000/天.

    Returns:
        DataFrame columns=['ex_div_date', 'forward_a', 'forward_b'],
        按 ex_div_date asc. 没有 rehab 事件返空 df.
    """
    ret, data = ctx.get_rehab(ts_code_to_futu(ts_code))
    if ret != RET_OK:
        return pd.DataFrame(columns=['ex_div_date', 'forward_a', 'forward_b'])
    if data is None or len(data) == 0:
        return pd.DataFrame(columns=['ex_div_date', 'forward_a', 'forward_b'])
    df = data[['ex_div_date', 'forward_adj_factorA', 'forward_adj_factorB']].copy()
    df = df.rename(
        columns={
            'forward_adj_factorA': 'forward_a',
            'forward_adj_factorB': 'forward_b',
        }
    )
    df['ex_div_date'] = pd.to_datetime(df['ex_div_date']).dt.strftime('%Y-%m-%d')
    df['forward_a'] = df['forward_a'].astype(float)
    df['forward_b'] = df['forward_b'].astype(float)
    df = df.sort_values('ex_div_date').reset_index(drop=True)
    return df


def attach_adj_factor(
    kline_df: pd.DataFrame, rehab_df: pd.DataFrame
) -> pd.DataFrame:
    """
    用 Futu 官方文档的仿射变换公式算 forward adj_factor.

    Futu 文档: 前复权价格 = 不复权价格 × forward_adj_factorA + forward_adj_factorB
    转 Tushare 风格 ratio (Billionaire middleware 公式 close × adj_factor /
    latest_adj_factor 接受 ratio, 不接受 affine):

        adj_factor = forward_A + forward_B / raw_close

    Latest 行: forward_A=1.0, forward_B=0.0 (没有 ex_div 应用到最新) → adj=1.0
    历史行: forward_B < 0 (累计派息要减) → adj < 1.0

    Merge: rehab 表按 ex_div_date asc, 用 merge_asof backward 把每个 trade_date
    映射到 ≤ 它的最新 ex_div_date 对应的 (A, B). trade_date 早于第一个 ex_div
    → A=1.0, B=0.0 兜底 (实际上极少触发, kline 窗口通常起始在第一次除权后很久).

    rehab_df 为空 → 全填 1.0.
    """
    if len(kline_df) == 0:
        return kline_df
    out = kline_df.copy().sort_values('trade_date').reset_index(drop=True)
    if len(rehab_df) == 0:
        out['adj_factor'] = 1.0
        return out

    # 关键: 每行 ex_div 的 (A, B) 适用 trade_date < ex_div_date (严格小于).
    # 实测 00700.HK 2026-05-15 ex_div 这行 forward_B=-5.3, 适用 5/14 及之前;
    # 5/15 当天及之后这次 ex_div 已发生过, 不再应用本行 (要找下一次 ex_div,
    # 没有则不复权).
    # → merge_asof direction='forward' + allow_exact_matches=False (严格 >).
    out['_td'] = pd.to_datetime(out['trade_date'])
    right = rehab_df.copy()
    right['_td'] = pd.to_datetime(right['ex_div_date'])
    right = right[['_td', 'forward_a', 'forward_b']].sort_values('_td').reset_index(drop=True)

    merged = pd.merge_asof(
        out.sort_values('_td'),
        right,
        on='_td',
        direction='forward',
        allow_exact_matches=False,
    )
    # trade_date 晚于最后一次 ex_div_date → NaN, 用 (1.0, 0.0) 兜底 (= 不复权,
    # 即"当前 baseline" 行为)
    merged['forward_a'] = merged['forward_a'].fillna(1.0).astype(float)
    merged['forward_b'] = merged['forward_b'].fillna(0.0).astype(float)

    # 文档公式: 前复权价 = close × forward_A + forward_B
    # 转 Tushare 风格 ratio (Billionaire middleware 用 close × adj / latest):
    #   adj_factor = forward_A + forward_B / close
    merged['adj_factor'] = (
        merged['forward_a'] + merged['forward_b'] / merged['close']
    )

    merged = merged.drop(columns=['_td', 'forward_a', 'forward_b'])
    merged = merged.sort_values('trade_date').reset_index(drop=True)
    return merged


# ── Pull logic ────────────────────────────────────────────────────────────────


def existing_last_date(ts_code: str, data_dir: Path) -> str | None:
    """读现有 parquet 拿最后一行 trade_date, 没有返 None."""
    p = data_dir / f'{ts_code}.parquet'
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p, columns=['trade_date'])
        if len(df) == 0:
            return None
        return df['trade_date'].max()
    except Exception:
        return None


def pull_one(
    ctx: OpenQuoteContext,
    ts_code: str,
    since: str,
    until: str,
) -> tuple[bool, pd.DataFrame | str]:
    """
    拉单只票 since→until 的日 K + 复权因子.

    两步 API:
      1. request_history_kline (AuType.NONE, 算 history_kline 1000/天 quota)
      2. get_rehab (独立 quota, 限速 10/s) — 拿后复权因子表, attach 到 kline
    """
    futu_sym = ts_code_to_futu(ts_code)
    ret, data, _page_req = ctx.request_history_kline(
        futu_sym,
        start=since,
        end=until,
        ktype=KLType.K_DAY,
        autype=AuType.NONE,
        max_count=1000,
    )
    if ret != RET_OK:
        return False, f'ret={ret} err={data}'
    kline_df = futu_kline_to_sh_quant(data, ts_code)
    if len(kline_df) == 0:
        return True, kline_df

    # 拉除权事件 + forward 复权因子 (A, B), 算 forward adj_factor.
    # 不算 history_kline quota, 失败时降级用 1.0.
    try:
        rehab_df = get_rehab_factors(ctx, ts_code)
        kline_df = attach_adj_factor(kline_df, rehab_df)
    except Exception as e:
        # 打印让用户能看到, 不是静默 1.0
        print(
            f'  ⚠ attach_adj_factor({ts_code}) failed: {type(e).__name__}: {e}, '
            'fallback adj_factor=1.0',
            file=sys.stderr,
        )
        kline_df = kline_df.copy()
        kline_df['adj_factor'] = 1.0

    return True, kline_df


def write_parquet(ts_code: str, df_new: pd.DataFrame, data_dir: Path) -> int:
    """
    Merge df_new 进现有 parquet (drop_duplicates by trade_date, keep='last'),
    返写入的总行数. 幂等: 同一天重复跑结果一致.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    p = data_dir / f'{ts_code}.parquet'
    if p.exists():
        old = pd.read_parquet(p)
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new
    merged = merged.drop_duplicates(subset=['trade_date'], keep='last')
    merged = merged.sort_values('trade_date').reset_index(drop=True)
    merged.to_parquet(p, index=False)
    return len(merged)


def fetch_universe_by_filter(
    ctx: OpenQuoteContext,
    max_n: int,
    min_market_cap_hkd: float = 1e9,
) -> list[str]:
    """
    用 Futu get_stock_filter 拿"市值 >= X + 按市值降序" 的 ticker 列表.

    比 get_stock_basicinfo + 启发式排序准: 直接拿到流动性 / 规模意义上的 HK 头部
    股票. get_stock_filter 单独配额池 (远超 history_kline 1000/天 限制), 安全.

    分页: Futu 单次返 ≤ 200, 翻页 begin=0,200,400... 直到拿满 max_n.

    Args:
        max_n: 最多拿多少只 ticker
        min_market_cap_hkd: 市值下限 (港元). 默认 10 亿过滤垃圾股.
    """
    # Filter: 市值 >= min_market_cap_hkd
    mc_filter = SimpleFilter()
    mc_filter.stock_field = StockField.MARKET_VAL
    mc_filter.filter_min = min_market_cap_hkd
    mc_filter.is_no_filter = False
    mc_filter.sort = SortDir.DESCEND

    tickers: list[str] = []
    page_size = 200
    begin = 0

    while len(tickers) < max_n:
        ret, data = ctx.get_stock_filter(
            market=Market.HK,
            filter_list=[mc_filter],
            begin=begin,
            num=page_size,
        )
        if ret != RET_OK:
            raise RuntimeError(f'get_stock_filter failed at begin={begin}: {data}')
        # data = (last_page, all_count, ret_list); ret_list = List[StockFilterData]
        last_page, _all_count, ret_list = data
        if not ret_list:
            break
        for item in ret_list:
            # item.stock_code 形如 'HK.00700'
            code = item.stock_code.split('.')[1] + '.HK'
            tickers.append(code)
            if len(tickers) >= max_n:
                break
        if last_page:
            break
        begin += page_size

    return tickers


def select_tickers_heuristic(uni_df: pd.DataFrame, max_n: int) -> list[str]:
    """
    Fallback: get_stock_filter API 失败时用启发式排序.

      - 排除 GEM (HK 创业板, 8 开头 5 位代码) 优先主板
      - 按 stock_id 排序 (Futu 内部 ID, 大致跟上市时间 + 流动性相关)
      - 取前 max_n
    """
    df = uni_df.copy()
    df['code_num'] = df['code'].str.split('.').str[1]
    main_board = df[~df['code_num'].str.startswith('8')].copy()
    if len(main_board) >= max_n:
        df = main_board
    df = df.sort_values('stock_id').head(max_n)
    return universe_to_ts_codes(df)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description='港股日线 puller via Futu OpenD')
    parser.add_argument(
        '--max-tickers',
        type=int,
        default=SAFE_QUOTA_BUDGET,
        help=f'最多拉多少只 (默认 {SAFE_QUOTA_BUDGET}, 1000 配额留 200 buffer)',
    )
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='增量模式: 读现有 parquet 推 since (默认 cold 6M)',
    )
    parser.add_argument(
        '--tickers',
        type=str,
        default=None,
        help='只跑指定 ticker 列表 (逗号分隔, 例 00700.HK,00981.HK). 覆盖 --max-tickers',
    )
    parser.add_argument('--dry-run', action='store_true', help='打印 plan 不真拉')
    parser.add_argument(
        '--min-market-cap',
        type=float,
        default=1e9,
        help='市值下限 (港元, 默认 1e9 = 10 亿过滤垃圾股)',
    )
    parser.add_argument(
        '--use-heuristic',
        action='store_true',
        help='跳过 get_stock_filter, 用 stock_id 启发式 (备用, 不推荐)',
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=str(DEFAULT_DATA_DIR),
        help=f'parquet 落盘目录 (默认 {DEFAULT_DATA_DIR})',
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    today = datetime.now().strftime('%Y-%m-%d')

    # ── 1. 连 OpenD ──────────────────────────────────────────────────────────
    print(f'[Futu] connecting to {HOST}:{PORT} ...')
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(
            f'OpenQuoteContext 创建失败: {e}\n'
            '→ OpenD 启动了吗? lsof -i :11111 看一下.'
        )

    try:
        # ── 2. 选 tickers ──────────────────────────────────────────────────
        if args.tickers:
            tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
            print(f'[plan] explicit tickers: {len(tickers)} 只')
        elif args.use_heuristic:
            print('[plan] using heuristic (stock_id sort, no market-cap filter)')
            uni = fetch_hk_universe(ctx)
            print(f'[plan] HK active universe size: {len(uni)}')
            tickers = select_tickers_heuristic(uni, args.max_tickers)
            print(
                f'[plan] selected top {len(tickers)} 主板 (by stock_id), '
                f'first 5: {tickers[:5]}'
            )
        else:
            print(
                f'[plan] fetching HK universe via get_stock_filter '
                f'(market_cap >= {args.min_market_cap:.0e} HKD, descending) ...'
            )
            try:
                tickers = fetch_universe_by_filter(
                    ctx, args.max_tickers, args.min_market_cap
                )
                print(
                    f'[plan] selected top {len(tickers)} by market cap, '
                    f'first 5: {tickers[:5]}'
                )
            except Exception as e:
                print(f'⚠️  get_stock_filter failed: {e}')
                print('[plan] fallback to heuristic')
                uni = fetch_hk_universe(ctx)
                tickers = select_tickers_heuristic(uni, args.max_tickers)
                print(
                    f'[plan] selected top {len(tickers)} 主板 (heuristic), '
                    f'first 5: {tickers[:5]}'
                )

        if len(tickers) > SAFE_QUOTA_BUDGET:
            print(
                f'⚠️  warning: 选了 {len(tickers)} 只 > 安全配额 {SAFE_QUOTA_BUDGET}. '
                '可能超过 1000 history_kline/天 限制.'
            )

        # ── 3. dry-run? ────────────────────────────────────────────────────
        if args.dry_run:
            print('\n[dry-run] would pull:')
            for t in tickers[:10]:
                last = existing_last_date(t, data_dir)
                if args.incremental and last:
                    next_day = (
                        datetime.strptime(last, '%Y-%m-%d') + timedelta(days=1)
                    ).strftime('%Y-%m-%d')
                    print(f'  {t}: incremental since={next_day} → {today}')
                else:
                    since = (
                        datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)
                    ).strftime('%Y-%m-%d')
                    print(f'  {t}: cold since={since} → {today}')
            if len(tickers) > 10:
                print(f'  ... 还有 {len(tickers) - 10} 只')
            print('\n[dry-run] not actually pulling. 跑去掉 --dry-run 真打.')
            return

        # ── 4. 批量拉 ──────────────────────────────────────────────────────
        success = 0
        failed: list[tuple[str, str]] = []
        skipped: list[str] = []
        t_start = time.time()

        for i, ts_code in enumerate(tickers, 1):
            # 决定 since
            if args.incremental:
                last = existing_last_date(ts_code, data_dir)
                if last is None:
                    since = (
                        datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)
                    ).strftime('%Y-%m-%d')
                else:
                    since = (
                        datetime.strptime(last, '%Y-%m-%d') + timedelta(days=1)
                    ).strftime('%Y-%m-%d')
                if since > today:
                    skipped.append(ts_code)
                    if i % 50 == 0 or i == len(tickers):
                        print(f'  [{i}/{len(tickers)}] ✓{success} ✗{len(failed)} ⏭{len(skipped)}')
                    continue
            else:
                since = (
                    datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)
                ).strftime('%Y-%m-%d')

            ok, payload = pull_one(ctx, ts_code, since, today)
            if not ok:
                failed.append((ts_code, str(payload)))
            else:
                df_new = payload
                if isinstance(df_new, pd.DataFrame) and len(df_new) > 0:
                    rows = write_parquet(ts_code, df_new, data_dir)
                    success += 1
                else:
                    # API ok 但空返回 (例如周末跑 incremental)
                    skipped.append(ts_code)

            # 每 50 只汇报一次
            if i % 50 == 0 or i == len(tickers):
                elapsed = time.time() - t_start
                rate = i / elapsed if elapsed > 0 else 0
                print(
                    f'  [{i}/{len(tickers)}] ✓{success} ✗{len(failed)} ⏭{len(skipped)} '
                    f'({rate:.1f}/s, {elapsed:.0f}s)'
                )

            # 轻量限速 (Futu 文档建议 ≤10次/秒, 这里保 5/s = 200ms/call)
            time.sleep(0.2)

        # ── 5. 总结 ────────────────────────────────────────────────────────
        elapsed = time.time() - t_start
        print('\n' + '=' * 60)
        print(f'港股日线 pull 完成. 用时 {elapsed:.0f}s')
        print(f'  ✓ 成功: {success}')
        print(f'  ✗ 失败: {len(failed)}')
        print(f'  ⏭ 跳过: {len(skipped)} (周末/已是最新/空返回)')
        if failed:
            print('\n失败 sample (前 5):')
            for t, err in failed[:5]:
                print(f'  {t}: {err}')

        # 配额警告
        api_calls = success + len(failed)
        print(f'\nhistory_kline 配额估算: ~{api_calls}/1000')
        if api_calls > 900:
            print('⚠️  接近 1000 上限. 明天再跑前等配额重置 (UTC+8 0:00).')
    finally:
        try:
            ctx.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
