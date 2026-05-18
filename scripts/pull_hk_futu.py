"""
港股日线 puller (via 富途 OpenD).

为什么是 Futu 而不是 Yahoo / Tushare hk_daily:
  - Yahoo unofficial 数据漏更 + 429 限流不稳, schema 也飘 (turnover 字段时有时无)
  - Tushare 5000 档 hk_daily 限 10次/天 + 2次/分钟硬上限, 等于不能用
  - Futu OpenD 是富途券商, HK Lv1 免费, 数据笔记准确, 端口 11111

配额限制 (Futu 实测, 重要 — 不是按天重置):
  - request_history_kline (历史 K 线): **滚动 30 天配额**, 不是 "1000/天".
    每 cold-pull 一只新票消耗 1 个 slot, 该 slot 在 30 天后才释放. 账户级
    总额随富途资产/等级浮动 (本机实测 ~1000 只/30天: 旧 775 + 新 200 ≈ 975
    后即报 "Insufficient historical candlestick quota ... released after 30 days").
    => 全集 ~2700 在这一档**根本拉不完**; 跑到上限后当天/次日重跑仍会失败,
       只能等 30 天前用掉的 slot 按天滴漏释放. 详见下方"策略".
  - get_market_snapshot / get_stock_basicinfo / get_stock_filter: 不吃此配额
  - 一次 request_history_kline 拉一只票 ≤ 1000 根 K, 算 1 个 slot

策略
────
  Cold start (--backfill, 推荐): 滚动回填全集. ticker 来自 canonical universe
    data_cache/universe/cn_hk.parquet (先跑 pull_hk_universe.py 生成), 按市值
    降序, **排除 RMB_COUNTER** (跟主柜台同公司同价, 回填=重复拉白烧配额).
    **跳过已落盘的票** (parquet 已存在且非空, 0 配额), 对缺失的票 cold-pull 6M
    ≤ 200 根 (1 slot/股), --max-tickers 是单轮上限 (默认 800).
    现实预期 (因为是滚动 30 天配额, 不是按天重置):
      - 一轮能拉多少 = min(--max-tickers, 账户剩余 30 天 slot). 撞到账户上限
        后, 当天/次日重跑那些失败票仍会失败 — 不会"几天滚完全集".
      - 失败票没落盘, 会随 30 天前 slot **按天滴漏释放**逐步补上. 建议
        cron **每周**跑一次 --backfill 让它自愈, 而非每天 (每天无用功).
      - ≥10亿那档 ~1264 只基本一档资产能覆盖; 全集 ~2700 这一档拉不完,
        要么升富途账户等级, 要么接受只覆盖高市值流动池 (研究足够).
    已落盘的会被跳过 => 下一轮自动从断点继续 (前提是配额已释放).
    universe 步骤纯本地读 parquet, 零网络.
  Cold start (legacy, 无 --backfill): inline get_stock_filter 取前 --max-tickers
    只无脑 cold-pull, 不跳过已有 — 每次重拉同一批, 只适合 --tickers debug.
  Daily update: 拉 since=last_trade_date+1 → today, 通常 1-3 根 K, 1 slot/股.
    增量逻辑: 读现有 parquet 最后一行 trade_date, 没有就 cold start 全 6M.
    注: 全集 daily 增量已迁到 scripts/update_daily.py (Futu snapshot 批量,
    不吃 history_kline 配额, 不受上面 30 天配额限制). --incremental 仅留 debug.

数据 schema (沿用 sh_quant docs/DATA_SCHEMA.md §1):
  data_cache/stocks/{ts_code}.HK.parquet  (5 位补零, 例 00700.HK.parquet;
  跟 update_daily.py 读写同目录, 故 backfill 的历史日更能直接接上)
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

    # 前置 (只需一次, 之后偶尔刷新): 生成 canonical universe
    python scripts/pull_hk_universe.py --min-mv 0

    # Dry-run: 读 cn_hk.parquet 印本轮 cold 计划, 不打 API
    python scripts/pull_hk_futu.py --backfill --dry-run

    # cold start: 一轮拉 ≤800 (或撞账户 30 天配额上限即停). 滚动 30 天配额,
    # 不是按天重置 => 别每天跑 (次日多半还在配额上限). cron 每周一轮自愈:
    #   0 7 * * 1  cd ~/Documents/Code/sh_quant && .venv/bin/python scripts/pull_hk_futu.py --backfill
    python scripts/pull_hk_futu.py --backfill                # 已落盘跳过, 拉缺失

    # Daily 增量更新 (读现有 parquet 推 since) — debug 用, 全集走 update_daily.py
    python scripts/pull_hk_futu.py --incremental --max-tickers 800

    # 只跑指定 ticker (debug 用)
    python scripts/pull_hk_futu.py --tickers 00700.HK,00981.HK
"""

from __future__ import annotations

import argparse
import contextlib
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

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

# --backfill 单轮 ticker 上限 (--max-tickers 默认值). 注意: history_kline 是
# **滚动 30 天**账户配额 (见模块 docstring), 不是按天重置 — 这个数只是单轮
# 礼貌上限, 真正的天花板是账户剩余 slot, 撞到就提前停 (loop 里据实报告).
SAFE_QUOTA_BUDGET = 800

# get_stock_filter 限速: 富途文档 "Maximum 10 times per 30 seconds" = 1 次/3s.
# 翻页拿全 universe (~14 页) 必须页间限速, 否则 begin>=2000 处必报 high-frequency.
STOCK_FILTER_SLEEP_SEC = 3.5

# 数据落盘位置: 跟 update_daily.py / DATA_SCHEMA.md 一致, 项目内 data_cache/.
# (旧版指向 ~/.market_data/, 跟日更读的目录不一致导致 backfill 历史日更看不到;
#  统一到 data_cache/ 是单一真相源.)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / 'data_cache' / 'stocks'
UNIVERSE_DIR = PROJECT_ROOT / 'data_cache' / 'universe'
# --backfill 的 canonical universe 源 (pull_hk_universe.py 产物)
DEFAULT_UNIVERSE_FILE = UNIVERSE_DIR / 'cn_hk.parquet'

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

    get_rehab 是独立 quota (10/s 限速), 不算 history_kline 30 天配额.

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


def attach_adj_factor(kline_df: pd.DataFrame, rehab_df: pd.DataFrame) -> pd.DataFrame:
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
    merged['adj_factor'] = merged['forward_a'] + merged['forward_b'] / merged['close']

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
      1. request_history_kline (AuType.NONE, 消耗 1 个 30 天滚动配额 slot)
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
    股票. get_stock_filter 不吃 history_kline 30 天配额, 但**有独立限速**:
    Maximum 10 times / 30s. 翻页拿全集时页间 sleep STOCK_FILTER_SLEEP_SEC.

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
        time.sleep(STOCK_FILTER_SLEEP_SEC)  # 限速: 10 次/30s

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


def load_universe_tickers(
    universe_file: Path,
    min_market_cap_hkd: float,
    exclude_boards: tuple[str, ...] = ('RMB_COUNTER',),
) -> list[str]:
    """从 pull_hk_universe.py 产物 cn_hk.parquet 读 --backfill 的 ticker 列表.

    单一真相源: code / 市值 / board 都来自 universe 文件, 不再 inline
    get_stock_filter (也就不吃它的 10 次/30s 限速, backfill universe 步骤零网络).

      - 排除 board ∈ exclude_boards (默认 RMB_COUNTER: 跟主柜台同公司同价,
        回填 = 重复拉, 白烧 history_kline 配额). GEM 是独立公司, 保留.
      - market_cap >= 阈值 (universe 单位=亿港元; 入参港元, /1e8 对齐)
      - 按 market_cap 降序 (高市值优先, 配合滚动断点)

    universe 文件缺失 → 直接报错退出 (不静默回退 get_stock_filter — 那会破坏
    单一真相源, 且违背 "拿不到就报错不静默" 原则). 提示先跑 pull_hk_universe.py.
    """
    if not universe_file.exists():
        sys.exit(
            f'universe 文件不存在: {universe_file}\n'
            '→ 先生成: python scripts/pull_hk_universe.py --min-mv 0'
        )
    df = pd.read_parquet(universe_file)
    n0 = len(df)
    df = df[~df['board'].isin(exclude_boards)]
    n_excl = n0 - len(df)
    threshold_yi = min_market_cap_hkd / 1e8
    df = df[df['market_cap'] >= threshold_yi]
    df = df.sort_values('market_cap', ascending=False)
    tickers = df['ts_code'].tolist()
    print(
        f'[plan] universe {universe_file.name}: {n0} 行, '
        f'排除 {n_excl} ({"/".join(exclude_boards)}), '
        f'market_cap>={threshold_yi:.0f}亿 后 {len(tickers)} 只, '
        f'first 5: {tickers[:5]}'
    )
    return tickers


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
        help='增量模式: 读现有 parquet 推 since (默认 cold 6M). debug 用',
    )
    parser.add_argument(
        '--backfill',
        action='store_true',
        help='滚动回填模式: 从 cn_hk.parquet (pull_hk_universe.py 产物) 读 ticker, '
        '市值降序遍历, 排除 RMB_COUNTER, 跳过已落盘的票, 对缺失的 cold-pull 6M, '
        '单轮 --max-tickers 或撞账户 30 天配额上限即停. 已落盘跳过=断点续传, '
        '但 history_kline 是滚动 30 天配额非按天重置, 建议 cron 每周跑一次自愈.',
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
        help=f'parquet 落盘目录 (默认 {DEFAULT_DATA_DIR}, 跟 update_daily 一致)',
    )
    parser.add_argument(
        '--universe-file',
        type=str,
        default=str(DEFAULT_UNIVERSE_FILE),
        help=f'--backfill 的 universe 源 (默认 {DEFAULT_UNIVERSE_FILE})',
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    today = datetime.now().strftime('%Y-%m-%d')

    # ── 1. 连 OpenD ──────────────────────────────────────────────────────────
    print(f'[Futu] connecting to {HOST}:{PORT} ...')
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(f'OpenQuoteContext 创建失败: {e}\n→ OpenD 启动了吗? lsof -i :11111 看一下.')

    try:
        # ── 2. 选 tickers ──────────────────────────────────────────────────
        if args.tickers:
            tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
            print(f'[plan] explicit tickers: {len(tickers)} 只')
        elif args.backfill:
            # 单一真相源: 从 pull_hk_universe.py 产物读, 不再 inline
            # get_stock_filter. 市值降序 = 滚动回填优先级序列, 配额上限交给
            # 拉取循环 (跳过已落盘 + api_used 预算).
            tickers = load_universe_tickers(Path(args.universe_file), args.min_market_cap)
        elif args.use_heuristic:
            print('[plan] using heuristic (stock_id sort, no market-cap filter)')
            uni = fetch_hk_universe(ctx)
            print(f'[plan] HK active universe size: {len(uni)}')
            tickers = select_tickers_heuristic(uni, args.max_tickers)
            print(f'[plan] selected top {len(tickers)} 主板 (by stock_id), first 5: {tickers[:5]}')
        else:
            # legacy (无 --backfill): 旧 get_stock_filter 路径, 取前 max_tickers.
            print(
                f'[plan] fetching HK universe via get_stock_filter '
                f'(market_cap >= {args.min_market_cap:.0e} HKD, descending, '
                f'up to {args.max_tickers}) ...'
            )
            try:
                tickers = fetch_universe_by_filter(ctx, args.max_tickers, args.min_market_cap)
                print(f'[plan] universe size {len(tickers)} by market cap, first 5: {tickers[:5]}')
            except Exception as e:
                print(f'⚠️  get_stock_filter failed: {e}')
                print('[plan] fallback to heuristic')
                uni = fetch_hk_universe(ctx)
                tickers = select_tickers_heuristic(uni, args.max_tickers)
                print(
                    f'[plan] universe size {len(tickers)} 主板 (heuristic), first 5: {tickers[:5]}'
                )

        # backfill 模式 tickers = 全 universe, 必然 > 配额, 但循环按 api_used
        # 预算自停, 不是 over-quota — 只在 legacy 模式才告警.
        if not args.backfill and len(tickers) > SAFE_QUOTA_BUDGET:
            print(
                f'⚠️  warning: 选了 {len(tickers)} 只 > 安全配额 {SAFE_QUOTA_BUDGET}. '
                '可能超过 1000 history_kline/天 限制.'
            )

        # ── 3. dry-run? ────────────────────────────────────────────────────
        if args.dry_run:
            cold_since = (datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
            if args.backfill:
                on_disk = [t for t in tickers if existing_last_date(t, data_dir) is not None]
                missing = [t for t in tickers if existing_last_date(t, data_dir) is None]
                this_run = missing[: args.max_tickers]
                print(
                    f'\n[dry-run][backfill] universe={len(tickers)} '
                    f'已落盘={len(on_disk)} 缺失={len(missing)} '
                    f'本轮预算={args.max_tickers}'
                )
                print(
                    f'[dry-run][backfill] 本轮会 cold-pull {len(this_run)} 只 '
                    f'(since={cold_since} → {today}):'
                )
                for t in this_run[:10]:
                    print(f'  {t}')
                if len(this_run) > 10:
                    print(f'  ... 还有 {len(this_run) - 10} 只')
                left = len(missing) - len(this_run)
                if left > 0:
                    print(
                        f'[dry-run][backfill] 本轮后还剩 ~{left} 只未覆盖. '
                        '注: history_kline 是滚动 30 天配额, 撞账户上限会更早停; '
                        '没拉完的随 30 天前 slot 释放逐周补 (cron 每周一轮).'
                    )
                else:
                    print(
                        '[dry-run][backfill] 缺口 ≤ 单轮上限 — 配额够的话一轮覆盖; '
                        '若账户 30 天 slot 不足, 会拉到上限即停, 余下逐周自愈.'
                    )
            else:
                print('\n[dry-run] would pull:')
                for t in tickers[:10]:
                    last = existing_last_date(t, data_dir)
                    if args.incremental and last:
                        next_day = (
                            datetime.strptime(last, '%Y-%m-%d') + timedelta(days=1)
                        ).strftime('%Y-%m-%d')
                        print(f'  {t}: incremental since={next_day} → {today}')
                    else:
                        print(f'  {t}: cold since={cold_since} → {today}')
                if len(tickers) > 10:
                    print(f'  ... 还有 {len(tickers) - 10} 只')
            print('\n[dry-run] not actually pulling. 跑去掉 --dry-run 真打.')
            return

        # ── 4. 批量拉 ──────────────────────────────────────────────────────
        success = 0
        failed: list[tuple[str, str]] = []
        skipped: list[str] = []
        on_disk = 0  # backfill: 已落盘被跳过 (0 配额)
        api_used = 0  # 实际打 request_history_kline 的次数 (≈ 配额)
        quota_stop = False  # backfill: 因预算用完提前停
        scanned = 0  # 实际遍历到第几只 (backfill resume 用)
        t_start = time.time()

        for i, ts_code in enumerate(tickers, 1):
            scanned = i

            # backfill: 已落盘的票直接跳过, 不打接口 (这是滚动回填的断点机制 —
            # 下一轮跑时这些已是 on_disk, 游标自然下移到市值序下一只未覆盖的).
            if args.backfill and existing_last_date(ts_code, data_dir) is not None:
                on_disk += 1
                if i % 200 == 0:
                    print(
                        f'  [{i}/{len(tickers)}] 已落盘跳过 {on_disk}, '
                        f'本轮已拉 {api_used}/{args.max_tickers}'
                    )
                continue

            # 决定 since: backfill 永远 cold (回填缺失的票, 忽略 --incremental)
            if args.incremental and not args.backfill:
                last = existing_last_date(ts_code, data_dir)
                if last is None:
                    since = (datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)).strftime(
                        '%Y-%m-%d'
                    )
                else:
                    since = (datetime.strptime(last, '%Y-%m-%d') + timedelta(days=1)).strftime(
                        '%Y-%m-%d'
                    )
                if since > today:
                    skipped.append(ts_code)
                    if i % 50 == 0 or i == len(tickers):
                        print(f'  [{i}/{len(tickers)}] ✓{success} ✗{len(failed)} ⏭{len(skipped)}')
                    continue
            else:
                since = (datetime.now() - timedelta(days=COLD_LOOKBACK_DAYS)).strftime('%Y-%m-%d')

            ok, payload = pull_one(ctx, ts_code, since, today)
            api_used += 1
            if not ok:
                failed.append((ts_code, str(payload)))
            else:
                df_new = payload
                if isinstance(df_new, pd.DataFrame) and len(df_new) > 0:
                    write_parquet(ts_code, df_new, data_dir)
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
                    f'api={api_used} ({rate:.1f}/s, {elapsed:.0f}s)'
                )

            # backfill: 单轮 --max-tickers 上限用完即停 (注: 真正天花板是账户
            # 30 天滚动配额, 撞到会更早地以 quota 错误失败, 见下方总结).
            if args.backfill and api_used >= args.max_tickers:
                quota_stop = True
                print(
                    f'\n[backfill] 本轮配额预算 {args.max_tickers} 用完, '
                    f'扫描到第 {scanned}/{len(tickers)} 只, 停.'
                )
                break

            # 轻量限速 (Futu 文档建议 ≤10次/秒, 这里保 5/s = 200ms/call)
            time.sleep(0.2)

        # ── 5. 总结 ────────────────────────────────────────────────────────
        elapsed = time.time() - t_start
        print('\n' + '=' * 60)
        print(f'港股日线 pull 完成. 用时 {elapsed:.0f}s')
        print(f'  ✓ 成功: {success}')
        print(f'  ✗ 失败: {len(failed)}')
        print(f'  ⏭ 跳过: {len(skipped)} (周末/已是最新/空返回)')
        # 区分"配额耗尽失败"与普通失败 (网络/停牌). 富途配额错文案含下列关键字.
        quota_failed = sum(
            1
            for _, e in failed
            if 'historical candlestick quota' in e or 'released after 30 days' in e
        )
        if args.backfill:
            print(f'  ⏩ 已落盘跳过: {on_disk} (0 配额)')
            tail = len(tickers) - scanned
            remaining = tail + len(failed)
            if quota_failed > 0:
                print(
                    f'\n[backfill] 撞到账户 history_kline **滚动 30 天配额**上限: '
                    f'{quota_failed}/{len(failed)} 只失败是配额耗尽 (非网络错). '
                    f'还剩 ~{remaining} 只未覆盖.\n'
                    '  → 非按天重置: 当天/次日重跑这些仍会失败. 没拉的会随 30 天前\n'
                    '    用掉的 slot 按天滴漏释放逐步补上 — cron **每周**跑一次\n'
                    '    --backfill 自愈即可, 别每天跑. 已落盘历史不受影响, 日更走\n'
                    '    update_daily.py snapshot (独立配额) 照常.'
                )
            elif quota_stop and remaining > 0:
                print(
                    f'\n[backfill] 单轮 --max-tickers={args.max_tickers} 用完, '
                    f'还剩 ~{remaining} 只 (未扫描 {tail} + 失败 {len(failed)}). '
                    '未撞账户 30 天配额, 下一轮 --backfill 继续 (已落盘跳过).'
                )
            elif not quota_stop and len(failed) == 0:
                print('\n[backfill] 全集已覆盖. cold start 完成, 后续走 update_daily.py 日更.')
            else:
                print(
                    f'\n[backfill] universe 扫完, {len(failed)} 只失败无落盘 '
                    '(非配额, 多为网络/停牌). 下一轮 --backfill 自动重试.'
                )
        if failed:
            print('\n失败 sample (前 5):')
            for t, err in failed[:5]:
                print(f'  {t}: {err}')

        # 配额提示 (滚动 30 天, 不是按天重置 — 没有"明天就满血"这回事)
        print(f'\n本轮消耗 history_kline slot: {api_used} (30 天滚动账户配额)')
        if quota_failed > 0:
            print(
                '⚠️  已撞账户配额上限. 30 天滚动释放, 无次日重置; '
                'cron 每周一轮自愈, 别每天空跑.'
            )
    finally:
        with contextlib.suppress(Exception):
            ctx.close()


if __name__ == '__main__':
    main()
