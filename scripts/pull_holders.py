"""拉 A 股按季度的北向(陆股通)持股占比 → <data_root>/holders/<ts_code>.parquet。

为什么是"季度"而不是"日频"
──────────────────────────
2024-08 起交易所停发 per-stock 每日北向持股披露(East Money / HKEX 同步停),
30 日北向变动这类日频派生量行业性拿不到了。但**季末快照仍在**(probe 确认
hk_hold trade_date=20251231 有数据)。所以这里只落季度截面 + 季度环比,
不再尝试任何日频/30D 口径——那是结构性死掉的,不是 bug。

机构持股(institution_*)本次**不落**:基础档 Tushare 没有干净的机构持股占比
接口,top10_floatholders 只覆盖前十大流通股东、且控股股东/产业资本混进
holder_type 分类,系统性偏低且噪声大。owner 决策:北向季度上,机构砍掉
(消费侧 Billionaire 同步删机构筛选/列)。

消费侧契约(producer 必须逐字对齐)
──────────────────────────────────
sibling app Billionaire 的价值筛选器读 holders/<ts_code>.parquet,列固定:

    列名                      含义
    ─────────────────────────────────────────────────
    end_date                  报告期(季度末日历日, datetime)
    northbound_hold_pct       该季末北向持股占总股本比例 (%)
    northbound_hold_pct_qoq   环比上一季度的百分点变动 (pp)
    consecutive_increase_q    至该季为止的"连续 QoQ>0"季数(含本季);
                              本季 QoQ≤0 / NaN → 0. 趋势性累积信号,单季
                              抬一下不算.

每个 parquet 一只股票、一季一行,按 end_date 升序。只含曾出现在陆股通的
A 股(.SH/.SZ);hk_hold 返回的 HK 行是南向港股通,丢弃。

数据源(Tushare 基础会员,无 VIP)
──────────────────────────────────
    pro.trade_cal  季度末日历日 → 最近一个 ≤ 它的 SSE 交易日
    pro.hk_hold(trade_date=<那天>)  全市场一次返回 (ratio=持股占比%)
        2024-08 后日频停披,但季末这一天的快照仍可取

落点(关键,避免 worktree/主仓踩坑)
──────────────────────────────────
写到 `_data_root()/holders/`,_data_root = $SH_QUANT_DATA_DIR 或
~/.market_data,与 pull_macro.py / pull_us_daily_basic.py 完全一致。
**不**用 PROJECT_ROOT/data_cache——worktree 里跑会写进 worktree 自己的
data_cache,UI(读 ~/.market_data)读不到。

依赖:tushare / pandas / pyarrow / python-dotenv(requirements.txt)
环境:项目根 .env 的 TUSHARE_TOKEN;worktree 无 .env 时回落主仓库根。

用法(先 source .venv/bin/activate)
────────────────────────────────────
    python scripts/pull_holders.py                  # 默认 2019Q1 至今全重建
    python scripts/pull_holders.py --start 20160101 # 更长历史
    python scripts/pull_holders.py --tickers 600519.SH,000858.SZ  # 调试单只
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PERMISSION_KEYWORDS = ('40203', '权限', '积分', 'permission', 'points')
RATELIMIT_KEYWORDS = ('频率超限', '超限', 'rate limit', '每分钟')


# ─── 数据根(与 Billionaire getDataRoot / pull_macro 对齐)────────────────
def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR = _data_root() / 'holders'


# ─── .env 加载(worktree 无 .env 回落主仓库根)────────────────────────────
def load_token() -> str:
    try:
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh`。')

    if (PROJECT_ROOT / '.env').exists():
        load_dotenv(PROJECT_ROOT / '.env')
    else:
        try:
            common = subprocess.check_output(
                ['git', 'rev-parse', '--path-format=absolute', '--git-common-dir'],
                cwd=PROJECT_ROOT,
                text=True,
            ).strip()
            main_env = Path(common).parent / '.env'
            if main_env.exists():
                load_dotenv(main_env)
        except Exception:
            pass

    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        sys.exit('TUSHARE_TOKEN not found in .env。cp .env.example .env 再填真 token。')
    return token


def _call(fn, *, retries: int = 1):
    """单次调用;权限错直接 exit;频率超限退避 60s 重试。"""
    try:
        return fn()
    except Exception as ex:  # noqa: BLE001
        msg = str(ex)
        if any(k in msg for k in PERMISSION_KEYWORDS):
            sys.exit(f'hk_hold 权限/积分不足,无法继续: {msg[:120]}')
        if any(k in msg for k in RATELIMIT_KEYWORDS) and retries > 0:
            time.sleep(60)
            return _call(fn, retries=retries - 1)
        raise


# ─── 季度末 → 最近一个 ≤ 它的交易日 ──────────────────────────────────────
def quarter_snapshot_dates(pro, start: str, end: str) -> list[tuple[pd.Timestamp, str]]:
    """返回 [(季末日历日, 该季最后一个 SSE 交易日 YYYYMMDD), ...]。"""
    cal = _call(
        lambda: pro.trade_cal(
            exchange='SSE', start_date=start, end_date=end, is_open='1'
        )
    )
    open_days = pd.to_datetime(sorted(cal['cal_date'].astype(str)), format='%Y%m%d')
    if len(open_days) == 0:
        return []

    qends = pd.date_range(
        pd.Timestamp(start), pd.Timestamp(end), freq='QE'
    ).normalize()
    out: list[tuple[pd.Timestamp, str]] = []
    for qe in qends:
        prior = open_days[open_days <= qe]
        if len(prior) == 0:
            continue
        out.append((qe, prior[-1].strftime('%Y%m%d')))
    return out


# ─── 单个季末快照:全市场一次拉,留北向(.SH/.SZ),丢南向(HK)──────────────
def fetch_quarter(pro, trade_date: str) -> pd.DataFrame:
    """返回 [ts_code, northbound_hold_pct]。空 → 空 df。"""
    # hk_hold 全市场单次有服务端行数上限(实测 ~4200, 总量可达 ~4900),
    # 必须 ≤ 服务端整页粒度才能让 "短页=最后一页" 成立. 2000 是 Tushare
    # 通用安全页, 实测整页恒回 2000 → 翻页正确, 不会漏标的.
    limit = 2000
    parts: list[pd.DataFrame] = []
    # 整季首页为空通常是限频窗口踩到(_call 已对显式"频率超限"重试一次,
    # 但 Tushare 偶尔在限频时直接回空 df 不抛错). 外层再补一次 30s 退避
    # 重试,防止整季静默丢——之前 limit=5000 bug 已让人吃过一次截断的亏.
    for attempt in range(2):
        parts = []
        offset = 0
        while True:
            chunk = _call(
                lambda o=offset: pro.hk_hold(
                    trade_date=trade_date, offset=o, limit=limit
                )
            )
            if chunk is None or chunk.empty:
                break
            parts.append(chunk)
            if len(chunk) < limit:
                break
            offset += limit
            time.sleep(0.4)
        if parts:
            break
        if attempt == 0:
            time.sleep(30)

    if not parts:
        return pd.DataFrame(columns=['ts_code', 'northbound_hold_pct'])

    df = pd.concat(parts, ignore_index=True)
    # exchange ∈ {SH, SZ} = 陆股通(北向);HK = 港股通(南向),丢弃
    # 北向可投资 ETF (如 ETF331.SZ 沪深300ETF) 会被 hk_hold 一起返回. 当前 UI
    # 用 cn_a 个股 universe, ETF 行不会上榜但会占 parquet/meta 行数, 一律砍.
    df = df[df['exchange'].isin(('SH', 'SZ')) & ~df['ts_code'].str.startswith('ETF')].copy()
    df['northbound_hold_pct'] = pd.to_numeric(df['ratio'], errors='coerce')
    df = df.dropna(subset=['northbound_hold_pct'])
    return df[['ts_code', 'northbound_hold_pct']].drop_duplicates('ts_code')


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉 A 股季度北向持股占比 → holders/<ts_code>.parquet'
    )
    ap.add_argument('--start', default='20190101', help='起始 YYYYMMDD(季末序列起点)')
    ap.add_argument('--end', default='', help='结束 YYYYMMDD(默认今天)')
    ap.add_argument('--tickers', help='逗号分隔 ts_code,只写这几只(调试用)')
    ap.add_argument('--sleep', type=float, default=0.5, help='季度间隔秒')
    args = ap.parse_args()

    end = args.end or pd.Timestamp.now().strftime('%Y%m%d')

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    ts.set_token(load_token())
    pro = ts.pro_api()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    snaps = quarter_snapshot_dates(pro, args.start, end)
    if not snaps:
        sys.exit(f'区间 {args.start}~{end} 没有季度交易日,检查 --start/--end。')

    print(f'拉 {len(snaps)} 个季末北向快照 → {CACHE_DIR}')
    print(f'区间 {args.start} ~ {end}')
    print('-' * 70)

    long_parts: list[pd.DataFrame] = []
    n = len(snaps)
    for i, (qe, td) in enumerate(snaps, 1):
        df = fetch_quarter(pro, td)
        if df.empty:
            print(f'  [{i:>2}/{n}] {qe.date()} (交易日 {td})  空,跳过')
            time.sleep(args.sleep)
            continue
        df['end_date'] = qe
        long_parts.append(df)
        print(f'  [{i:>2}/{n}] {qe.date()} (交易日 {td})  {len(df):>4} 只北向标的')
        time.sleep(args.sleep)

    if not long_parts:
        sys.exit('所有季度都空,没数据可写。')

    long = pd.concat(long_parts, ignore_index=True)
    if args.tickers:
        want = {t.strip().upper() for t in args.tickers.split(',') if t.strip()}
        long = long[long['ts_code'].isin(want)]
        if long.empty:
            sys.exit(f'--tickers 指定的 {sorted(want)} 在北向数据里一条都没有。')

    print('-' * 70)
    written = 0
    meta: list[dict] = []
    for ts_code, g in long.groupby('ts_code'):
        g = g.sort_values('end_date').reset_index(drop=True)
        g['northbound_hold_pct_qoq'] = g['northbound_hold_pct'].diff()
        # 连续 QoQ>0 季数(含本季): 每遇 ≤0 / NaN 重置为 0, 否则在前值上 +1.
        # 标准 idiom: 用 "本季不是正增" 作为分组边界, 组内 cumsum 即流式 streak.
        pos = (g['northbound_hold_pct_qoq'] > 0).astype(int)
        reset_group = (pos == 0).cumsum()
        g['consecutive_increase_q'] = pos.groupby(reset_group).cumsum().astype('int32')
        out = g[
            [
                'end_date',
                'northbound_hold_pct',
                'northbound_hold_pct_qoq',
                'consecutive_increase_q',
            ]
        ]
        out.to_parquet(CACHE_DIR / f'{ts_code}.parquet', index=False)
        written += 1
        meta.append(
            {
                'ts_code': ts_code,
                'n_quarters': len(out),
                'date_min': out['end_date'].min(),
                'date_max': out['end_date'].max(),
                'latest_pct': float(out['northbound_hold_pct'].iloc[-1]),
                'latest_streak': int(out['consecutive_increase_q'].iloc[-1]),
            }
        )

    pd.DataFrame(meta).sort_values('ts_code').to_parquet(
        CACHE_DIR / '_holders.parquet', index=False
    )
    print(f'完成: {written} 只标的写入 {CACHE_DIR}  (+ _holders.parquet 索引)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
