"""IN04 股票型 ETF 全市场日频净申购 → capital_flow/in04_etf_net_subscription.parquet。

PRD 口径(第 6 节第 2 条)
────────────────────────
    单 ETF 当日净申购金额 ≈ (fd_share_T − fd_share_{T-1}) × close_T
    全市场 IN04 = Σ 所有股票型 ETF 当日净申购

「股票型」= fund_basic.fund_type == '股票型' 的白名单(排除债券/商品/REITs/
货币/QDII)。close 用 fund_daily(二级市场收盘价)作 NAV 近似——场内 ETF 二级
价对 IOPV 偏离通常 <0.5%，业内通用近似(Wind/Choice 同口径)。

数据源(Tushare 基础会员)
────────────────────────
    pro.fund_basic(market='E')                    全场内基金清单(用 fund_type 过滤股票型)
    pro.fund_share(trade_date=YYYYMMDD)           当日全市场份额(~1600 行，含全部 ETF)
    pro.fund_daily(trade_date=YYYYMMDD)           当日全市场行情(~2000 行)

fd_share 单位 = 万份(实测 510300.SH 份额 × close 与真实在管规模 ~1800 亿对得上)。

落点
─────
写到 `_data_root()/capital_flow/in04_etf_net_subscription.parquet`，单文件市场聚合：

    列名                     含义
    ─────────────────────────────────────
    date                     交易日(datetime)
    net_subscription_yi      当日净申购金额(亿元)
    n_active_etfs            当日有 share/close 配对的股票型 ETF 数(诊断用)
    aum_yi                   当日股票型 ETF 全市场在管规模(亿元，= Σ share × close)

注：单 ETF 明细暂不落盘——若后续 Billionaire 需要"宽基 vs 行业"分项(DV03)
再补 capital_flow/in04_etf_daily/<ts_code>.parquet。

增量
─────
若 parquet 已存在，只从 last_date + 1 开始增量；--start 强制覆写起点。
首次回填默认 2023-01-01 至今(约 3 年)，可 --start 20200101 拉更长。

用法(先 source .venv/bin/activate，项目根执行)
────────────────────────────────────────────
    python scripts/pull_capital_flow_in04.py                # 增量(或首次 backfill 至 2023-01-01)
    python scripts/pull_capital_flow_in04.py --start 20200101
    python scripts/pull_capital_flow_in04.py --rebuild      # 删旧重拉
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


def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR = _data_root() / 'capital_flow'
OUT_FILE = CACHE_DIR / 'in04_etf_net_subscription.parquet'


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
    """单次调用；权限错直接 exit；频率超限退避 60s 重试。"""
    try:
        return fn()
    except Exception as ex:  # noqa: BLE001
        msg = str(ex)
        if any(k in msg for k in PERMISSION_KEYWORDS):
            sys.exit(f'⚠ 接口权限/积分不足，需换源: {msg[:140]}')
        if any(k in msg for k in RATELIMIT_KEYWORDS) and retries > 0:
            time.sleep(60)
            return _call(fn, retries=retries - 1)
        raise


def stock_etf_universe(pro) -> set[str]:
    """股票型 ETF 白名单：fund_basic.market='E' & fund_type='股票型'。

    注意 fund_basic 还要并上已退市的(market='E' status='D')—— 历史日期的份额
    要算进当日聚合，但退市 ETF 后续 share/close 为空自然不会污染。
    """
    parts = []
    for status in ('L', 'D'):  # L 存续 / D 退市
        df = _call(lambda s=status: pro.fund_basic(market='E', status=s))
        if df is not None and len(df) > 0:
            parts.append(df)
    if not parts:
        sys.exit('fund_basic 一条没拉到，异常。')
    fb = pd.concat(parts, ignore_index=True)
    stock = fb[fb['fund_type'] == '股票型'].copy()
    return set(stock['ts_code'].astype(str))


def trading_days(pro, start: str, end: str) -> list[str]:
    cal = _call(
        lambda: pro.trade_cal(exchange='SSE', start_date=start, end_date=end, is_open='1')
    )
    return sorted(cal['cal_date'].astype(str).tolist())


def fetch_day_share(pro, trade_date: str, universe: set[str]) -> pd.DataFrame:
    """当日全市场 fund_share，过滤股票型 ETF 白名单，返回 [ts_code, fd_share]。"""
    df = _call(lambda: pro.fund_share(trade_date=trade_date))
    if df is None or df.empty:
        return pd.DataFrame(columns=['ts_code', 'fd_share'])
    df = df[df['ts_code'].isin(universe)].copy()
    df['fd_share'] = pd.to_numeric(df['fd_share'], errors='coerce')
    return df[['ts_code', 'fd_share']].dropna().drop_duplicates('ts_code')


def fetch_day_close(pro, trade_date: str, universe: set[str]) -> pd.DataFrame:
    """当日 fund_daily close，过滤白名单，返回 [ts_code, close]。"""
    df = _call(lambda: pro.fund_daily(trade_date=trade_date))
    if df is None or df.empty:
        return pd.DataFrame(columns=['ts_code', 'close'])
    df = df[df['ts_code'].isin(universe)].copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['ts_code', 'close']].dropna().drop_duplicates('ts_code')


def main() -> int:
    ap = argparse.ArgumentParser(
        description='拉股票型 ETF 全市场日频净申购 (IN04) → capital_flow/in04_etf_net_subscription.parquet'
    )
    ap.add_argument('--start', default='20230101', help='首次回填起点(YYYYMMDD); 增量时被忽略')
    ap.add_argument('--end', default='', help='结束 YYYYMMDD(默认今天)')
    ap.add_argument('--rebuild', action='store_true', help='删旧 parquet 重拉')
    ap.add_argument('--sleep', type=float, default=0.25, help='每个交易日间隔秒(限频保护)')
    args = ap.parse_args()

    end = args.end or pd.Timestamp.now().strftime('%Y%m%d')

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    ts.set_token(load_token())
    pro = ts.pro_api()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 增量 vs 全量
    existing: pd.DataFrame | None = None
    if OUT_FILE.exists() and not args.rebuild:
        existing = pd.read_parquet(OUT_FILE)
        last = pd.to_datetime(existing['date']).max()
        start = (last + pd.Timedelta(days=1)).strftime('%Y%m%d')
        print(f'增量模式: existing 最后日 = {last.date()}，从 {start} 开始拉')
    else:
        start = args.start
        if args.rebuild and OUT_FILE.exists():
            OUT_FILE.unlink()
            print(f'--rebuild: 删除旧 {OUT_FILE.name}')
        print(f'全量回填: {start} ~ {end}')

    if start > end:
        print('已是最新，无新增交易日。')
        return 0

    print(f'拉股票型 ETF 白名单 (fund_basic market=E fund_type=股票型)...')
    universe = stock_etf_universe(pro)
    print(f'  白名单 {len(universe)} 只(含已退市)')

    days = trading_days(pro, start, end)
    if not days:
        print(f'区间 {start} ~ {end} 没有交易日，结束。')
        return 0

    # 为了算 Δshare，需要 start 前一交易日的 share 作为基期
    # 取上一交易日：用 trade_cal 往前 14 天找
    prev_cal_start = (pd.Timestamp(start) - pd.Timedelta(days=14)).strftime('%Y%m%d')
    prev_days = trading_days(pro, prev_cal_start, start)
    prev_day = next((d for d in reversed(prev_days) if d < days[0]), None)
    if prev_day is None and existing is None:
        # 首次回填且找不到 start 前一交易日：第一天 Δshare 设为 NaN，跳过当日聚合
        print('警告: 找不到 start 前一交易日，首日将无 Δshare 基期，第一行 NaN')

    print(f'拉 {len(days)} 个交易日 × (fund_share + fund_daily) → 市场净申购聚合')
    print('-' * 72)

    # 上一日份额表(ts_code → fd_share); 增量时从 existing 推不出来(只存了市场聚合)，
    # 仍需从 API 拉 prev_day 一次填充基期
    prev_share: dict[str, float] = {}
    if prev_day is not None:
        s0 = fetch_day_share(pro, prev_day, universe)
        prev_share = dict(zip(s0['ts_code'], s0['fd_share']))
        print(f'  基期 {prev_day}: {len(prev_share)} 只 ETF 有份额')

    rows: list[dict] = []
    for i, d in enumerate(days, 1):
        share_df = fetch_day_share(pro, d, universe)
        close_df = fetch_day_close(pro, d, universe)
        merged = share_df.merge(close_df, on='ts_code', how='inner')
        if merged.empty:
            print(f'  [{i:>4}/{len(days)}] {d}  share/close 配对为空，跳过')
            time.sleep(args.sleep)
            continue

        merged['prev_share'] = merged['ts_code'].map(prev_share)
        merged['delta_share'] = merged['fd_share'] - merged['prev_share']
        # 净申购 = Δ万份 × 元/份 / 10000 → 亿元
        merged['net_sub_yi'] = merged['delta_share'] * merged['close'] / 10000.0
        # 当日 AUM = 万份 × 元/份 / 10000 → 亿元
        merged['aum_yi'] = merged['fd_share'] * merged['close'] / 10000.0

        # 第一日(prev_share 全 NaN)：net_sub_yi 全 NaN，记为空
        valid = merged.dropna(subset=['net_sub_yi'])
        n_valid = len(valid)
        net = float(valid['net_sub_yi'].sum()) if n_valid > 0 else float('nan')
        aum = float(merged['aum_yi'].sum())

        rows.append(
            {
                'date': pd.Timestamp(d),
                'net_subscription_yi': net,
                'n_active_etfs': n_valid,
                'aum_yi': aum,
            }
        )

        # 更新 prev_share 用于下一日
        prev_share = dict(zip(merged['ts_code'], merged['fd_share']))

        if i % 20 == 0 or i == len(days):
            net_str = f'{net:+8.2f}亿' if n_valid > 0 else '   (基期空)'
            print(
                f'  [{i:>4}/{len(days)}] {d}  ETF×{n_valid:>4}  '
                f'net={net_str}  aum={aum:>9.1f}亿'
            )
        time.sleep(args.sleep)

    if not rows:
        print('没有新数据。')
        return 0

    new_df = pd.DataFrame(rows)
    if existing is not None:
        out = pd.concat([existing, new_df], ignore_index=True)
        out = out.drop_duplicates('date', keep='last').sort_values('date').reset_index(drop=True)
    else:
        out = new_df

    # 末行残缺检查：盘中或刚收盘几分钟跑时，fund_share 当日数据可能只录入部分基金,
    # 导致 n_active_etfs 显著低于近 5 日均值。历史早期 ETF 数本来就少不能用固定阈值,
    # 只对末行(且 len>=6)做相对检查,低于近 5 日均值 60% 视为盘中残缺,剔除并提示重跑。
    if len(out) >= 6:
        recent_mean = out['n_active_etfs'].iloc[-6:-1].mean()
        last_n = int(out['n_active_etfs'].iloc[-1])
        last_date = out['date'].iloc[-1].date()
        if last_n < 0.6 * recent_mean:
            print(
                f'⚠ 末日 {last_date} n_active_etfs={last_n} 远低于近 5 日均 {recent_mean:.0f},'
                f' 疑为盘中残缺数据(fund_share 当日未录全),剔除。收盘后重跑会自动补回。'
            )
            out = out.iloc[:-1].copy().reset_index(drop=True)

    out.to_parquet(OUT_FILE, index=False)
    print('-' * 72)
    print(f'写入 {OUT_FILE}  共 {len(out)} 行  '
          f'({out["date"].min().date()} ~ {out["date"].max().date()})')
    valid_out = out.dropna(subset=['net_subscription_yi'])
    if len(valid_out) > 0:
        print(
            f'  近 5 日净申购(亿元): '
            f'{valid_out.tail(5)[["date", "net_subscription_yi"]].to_dict("records")}'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
