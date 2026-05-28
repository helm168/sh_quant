"""
检测 parquet 里有没有"盘中半值"行 —— 即某行的 fetched_at 落在它自己
trade_date 的对应市场交易时段内 (收盘前), 说明那行是盘内 snapshot 而非终值.

支持 US / CN-A / HK 三市场, 各按自己的日历 + 收盘时间 (本地时区) 判定.

背景 (2026-05-26 踩坑)
──────────────────────
update_daily.py 的 fresh-skip 只看"parquet 里有没有 last_td 那一行", 不看那
行是不是终值. 一旦在盘中跑了一次 (尤其美股: cron 在 CN 时区, 跟美股盘时段
错位最易中招), 当天就被半日 OHLCV 卡死, 之后 cron 都跳过, 当日终值永远不补.
一次性污染了 2074/2090 只 US 票的 5/26 行.

预防已在 update_daily._last_trading_day_approx + 批量锚点做掉. 本脚本是
belt-and-suspenders 检测网 + 可回溯审计, 用 update_one 写的 provenance 列
`fetched_at` (UTC) 判定. CN/HK 的 cron 跟市场同时区且有 after-close 阈值,
本来就不易中招, 但泛化到三市场做统一对账网.

判定规则 (逐 ticker 看最新一行)
──────────────────────────────
  - fetched_at 缺失 / NaT          → WARN  (legacy 行, 该脚本上线前写的, 无从判定)
  - fetched_at(本地tz) < 该日收盘   → FLAG  (盘中半值, 需 --force 重拉修)
  - fetched_at >= 收盘              → OK    (终值)

收盘时间优先用 pandas_market_calendars (能识别提前收盘日, 如美股感恩节次日
13:00 ET / 港股圣诞前夕半日市); 没装 mcal 时退到各市场固定收盘时间.

市场 → (日历, 时区, 默认收盘)
  US     NYSE   America/New_York  16:00
  CN-A   SSE    Asia/Shanghai     15:00
  HK     HKEX   Asia/Hong_Kong    16:00

用法
────
    source .venv/bin/activate
    python scripts/validate_freshness.py                  # 扫全部市场
    python scripts/validate_freshness.py --market us      # 只 US (us/cn/hk)
    python scripts/validate_freshness.py MU.US 600519.SH  # 指定几只 (跨市场可混)
    python scripts/validate_freshness.py --quiet          # 只打 FLAG/WARN + 汇总

退出码: 有任何 FLAG → 1 (可当 cron gate); 否则 0.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path.home() / '.market_data' / 'stocks'

# 市场 → 收盘判定配置. cal = pandas_market_calendars 日历名.
_MARKET_CFG = {
    'us': {'cal': 'NYSE', 'tz': 'America/New_York', 'close': '16:00'},
    'cn_a': {'cal': 'SSE', 'tz': 'Asia/Shanghai', 'close': '15:00'},
    'cn_hk': {'cal': 'HKEX', 'tz': 'Asia/Hong_Kong', 'close': '16:00'},
}

# --market 别名 → 内部 market key
_MARKET_ALIAS = {'us': 'us', 'cn': 'cn_a', 'cn_a': 'cn_a', 'hk': 'cn_hk', 'cn_hk': 'cn_hk'}

# 内部 market key → parquet 文件名后缀 (扫目录用)
_MARKET_SUFFIXES = {
    'us': ('.US',),
    'cn_a': ('.SH', '.SZ', '.BJ'),
    'cn_hk': ('.HK',),
}


def market_of(ts_code: str) -> str | None:
    """ts_code 后缀 → 内部 market key. 认不出返回 None."""
    u = ts_code.upper()
    if u.endswith('.US'):
        return 'us'
    if u.endswith('.HK'):
        return 'cn_hk'
    if u.endswith(('.SH', '.SZ', '.BJ')):
        return 'cn_a'
    return None


def _market_close(trade_date: pd.Timestamp, market: str) -> pd.Timestamp:
    """该 trade_date 在对应市场的收盘墙钟 (tz-aware 本地时区).

    优先 pandas_market_calendars 拿精确 market_close (识别提前收盘日);
    没装 / 查不到 → 各市场固定收盘时间兜底.
    """
    cfg = _MARKET_CFG[market]
    d = trade_date.strftime('%Y-%m-%d')
    try:
        import pandas_market_calendars as mcal

        sched = mcal.get_calendar(cfg['cal']).schedule(start_date=d, end_date=d)
        if not sched.empty:
            return pd.Timestamp(sched.iloc[0]['market_close']).tz_convert(cfg['tz'])
    except Exception:
        pass
    return pd.Timestamp(f"{d} {cfg['close']}:00").tz_localize(cfg['tz'])


def _latest_row(fp: Path) -> dict | None:
    """读最新一行的 trade_date + fetched_at (+ close 给打印用). 失败返 None."""
    try:
        cols = ['trade_date', 'close']
        import pyarrow.parquet as pq

        schema_names = pq.ParquetFile(fp).schema_arrow.names
        if 'fetched_at' in schema_names:
            cols.append('fetched_at')
        df = pd.read_parquet(fp, columns=cols)
    except Exception:
        return None
    if df.empty:
        return None
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    row = df.sort_values('trade_date').iloc[-1]
    fetched = row['fetched_at'] if 'fetched_at' in df.columns else pd.NaT
    return {
        'trade_date': row['trade_date'],
        'close': row.get('close'),
        'fetched_at': pd.to_datetime(fetched, utc=True) if pd.notna(fetched) else pd.NaT,
    }


def classify(info: dict, market: str) -> str:
    """返回 'OK' / 'FLAG' / 'WARN'."""
    if info['fetched_at'] is pd.NaT or pd.isna(info['fetched_at']):
        return 'WARN'
    cfg = _MARKET_CFG[market]
    fetched_local = info['fetched_at'].tz_convert(cfg['tz'])
    close_local = _market_close(info['trade_date'], market)
    return 'FLAG' if fetched_local < close_local else 'OK'


def _collect_files(tickers: list[str], market_filter: str | None) -> list[Path]:
    """决定要扫哪些文件. 显式 tickers 优先; 否则按 market 过滤 glob."""
    if tickers:
        return [DATA_DIR / f'{t.upper()}.parquet' for t in tickers]
    if market_filter:
        key = _MARKET_ALIAS.get(market_filter.lower())
        if key is None:
            sys.exit(f'未知 --market {market_filter} (可选: us / cn / hk)')
        markets = [key]
    else:
        markets = list(_MARKET_SUFFIXES)
    files: list[Path] = []
    for m in markets:
        for suf in _MARKET_SUFFIXES[m]:
            files.extend(DATA_DIR.glob(f'*{suf}.parquet'))
    return sorted(set(files))


def main() -> int:
    ap = argparse.ArgumentParser(description='检测 parquet 盘中半值行 (fetched_at < 该市场收盘)')
    ap.add_argument('tickers', nargs='*', help='指定 ts_code (如 MU.US / 600519.SH); 不传则按 --market 扫')
    ap.add_argument('--market', help='只扫某市场: us / cn / hk (不传 = 全部)')
    ap.add_argument('--quiet', action='store_true', help='只打 FLAG/WARN 行 + 汇总, 不打 OK')
    args = ap.parse_args()

    files = _collect_files(args.tickers, args.market)
    if not files:
        print(f'没找到任何 parquet (查找目录: {DATA_DIR})')
        return 0

    n_ok = n_flag = n_warn = n_skip = 0
    flagged: list[str] = []
    for fp in files:
        ts_code = fp.name[:-len('.parquet')]
        market = market_of(ts_code)
        if market is None:
            print(f'  SKIP  {ts_code:<14} 认不出市场后缀')
            n_skip += 1
            continue
        if not fp.exists():
            print(f'  SKIP  {ts_code:<14} 文件不存在')
            n_skip += 1
            continue
        info = _latest_row(fp)
        if info is None:
            print(f'  SKIP  {ts_code:<14} 读不出 / 空')
            n_skip += 1
            continue
        verdict = classify(info, market)
        if verdict == 'OK':
            n_ok += 1
            if not args.quiet:
                print(
                    f'  OK    {ts_code:<14} {info["trade_date"].date()} '
                    f'close={info["close"]} fetched={info["fetched_at"]}'
                )
        elif verdict == 'FLAG':
            n_flag += 1
            flagged.append(ts_code)
            cfg = _MARKET_CFG[market]
            fetched_local = info['fetched_at'].tz_convert(cfg['tz'])
            print(
                f'  FLAG  {ts_code:<14} {info["trade_date"].date()} 盘中半值! '
                f'close={info["close"]} fetched={fetched_local} '
                f'< 收盘 {_market_close(info["trade_date"], market)}'
            )
        else:  # WARN
            n_warn += 1
            if not args.quiet:
                print(
                    f'  WARN  {ts_code:<14} {info["trade_date"].date()} '
                    f'无 fetched_at (legacy 行, 无从判定)'
                )

    print('-' * 70)
    print(f'共 {len(files)} 只: OK {n_ok} / FLAG {n_flag} / WARN {n_warn} / SKIP {n_skip}')
    if flagged:
        sample = ','.join(flagged[:20]) + (' ...' if len(flagged) > 20 else '')
        print(f'\nFLAG (盘中半值, 需修): {len(flagged)} 只')
        print(f'  {sample}')
        print('\n修复: python scripts/update_daily.py --tickers <列表> --force')
    return 1 if n_flag else 0


if __name__ == '__main__':
    sys.exit(main())
