"""拉宏观/流动性序列 → <data_root>/macro/<series>.parquet。

为什么是这 7 个序列（不是随便挑）
──────────────────────────────────
消费侧（sibling app Billionaire）的宏观面板
`src/features/macro/macroPanel.config.ts` + `localDataMiddleware.ts`
已按**固定的 series 文件名 + 固定列名**写死。生产侧（本脚本）必须逐字
对齐这个契约，否则 UI 每张卡都 404（"全部序列读不到"）。契约：

    series 文件               必须有的列（除 date 外都是数值）
    ─────────────────────────────────────────────────────────
    index_000300.SH           close
    north_money               north_cum
    margin                    margin_bal
    money_supply              m1_yoy, m2_yoy
    lpr                       lpr_1y, lpr_5y
    fred_DGS10                value
    china_us_spread           spread

中间件 SQL 是 `SELECT * FROM read_parquet(?) WHERE date>=? AND date<=?
ORDER BY date ASC`，并把除 date 外每列 `Number(v)` —— 所以每个 parquet
必须有一列 `date`（datetime）且其余列纯数值，不能掺字符串列（ts_code 等）。

数据源（都在 Tushare 基础会员，无 VIP）
─────────────────────────────────────
    index_000300.SH  index_daily(000300.SH).close
    north_money      moneyflow_hsgt.north_money 升序累加（百万元）
                     ※ 2024-08 后交易所停发实时北向，序列自然走平，非 bug
    margin           margin.rzrqye 按日跨 SSE/SZSE/BSE 求和 ÷1e8（亿元）
    money_supply     cn_m.m1_yoy / m2_yoy（月度）
    lpr              shibor_lpr.1y→lpr_1y / 5y→lpr_5y（月度）
    fred_DGS10       us_tycr.y10 → value（美 10Y，源走 Tushare 非 FRED，
                     文件名沿用契约不改）
    china_us_spread  yc_cb 中债 10Y − us_tycr 美 10Y，按日 inner join
                     ※ 契约注解写 "OECD 月度"，这里用 Tushare 日频同口径
                       近似（更新更勤），经济含义一致，notebook 引用时注明

数据落点（关键，避免 worktree/主仓踩坑）
──────────────────────────────────────
写到 `_data_root()/macro/`，_data_root = $SH_QUANT_DATA_DIR 或
~/.market_data，与 Billionaire `getDataRoot()` 及 pull_us_daily_basic.py
完全一致。**不**用 PROJECT_ROOT/data_cache —— 那样在 worktree 里跑会写到
worktree 自己的 data_cache，UI（读 ~/.market_data）读不到。

依赖：tushare / pandas / pyarrow / python-dotenv（requirements.txt）
环境：项目根 .env 的 TUSHARE_TOKEN；worktree 无 .env 时回落主仓库根。

用法（先 source .venv/bin/activate）
────────────────────────────────────
    python scripts/pull_macro.py                    # 全部 7 个
    python scripts/pull_macro.py --only lpr,margin  # 指定
    python scripts/pull_macro.py --start 20100101   # 日频序列起始
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


class PermissionSkip(RuntimeError):
    """该序列权限/积分不足，只跳过这一个，不中断其他。"""


# ─── 数据根（与 Billionaire getDataRoot / pull_us_daily_basic 对齐）──────
def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR = _data_root() / 'macro'


# ─── .env 加载（worktree 无 .env 回落主仓库根）────────────────────────────
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


# ─── 工具：按时间窗分段拉（绕开 Tushare 单次 2000 行上限）──────────────────
def _windows(start: str, end: str, days: int):
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    while s <= e:
        w_end = min(s + pd.Timedelta(days=days - 1), e)
        yield s.strftime('%Y%m%d'), w_end.strftime('%Y%m%d')
        s = w_end + pd.Timedelta(days=1)


def _call(fn, s: str, e: str):
    """单次调用；权限错抛 PermissionSkip；频率超限退避 60s 重试一次。"""
    try:
        return fn(s, e)
    except Exception as ex:  # noqa: BLE001
        msg = str(ex)
        if any(k in msg for k in PERMISSION_KEYWORDS):
            raise PermissionSkip(msg) from ex
        if any(k in msg for k in RATELIMIT_KEYWORDS):
            time.sleep(60)
            return fn(s, e)
        raise


def _paged(fn, start: str, end: str, days: int, pause: float = 0.0) -> pd.DataFrame:
    """fn(start_date, end_date) → df；按窗口分段拉，每次间隔 pause 秒后拼接。"""
    parts = []
    for s, e in _windows(start, end, days):
        chunk = _call(fn, s, e)
        if chunk is not None and not chunk.empty:
            parts.append(chunk)
        if pause:
            time.sleep(pause)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _ymd_to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.astype(str).str.strip(), format='%Y%m%d')


def _finalize(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    """统一：保留 date + 指定数值列，去重排序，数值列转 float。"""
    out = df[['date', *value_cols]].copy()
    out = out.dropna(subset=['date']).drop_duplicates('date', keep='last')
    out = out.sort_values('date').reset_index(drop=True)
    for c in value_cols:
        out[c] = pd.to_numeric(out[c], errors='coerce')
    return out


# ─── 7 个序列各自的 builder ──────────────────────────────────────────────
def build_index_000300(pro, start, end) -> pd.DataFrame:
    df = _paged(
        lambda s, e: pro.index_daily(ts_code='000300.SH', start_date=s, end_date=e),
        start, end, 365 * 6,
    )
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['trade_date'])
    return _finalize(df, ['close'])


def build_north_money(pro, start, end) -> pd.DataFrame:
    df = _paged(
        lambda s, e: pro.moneyflow_hsgt(start_date=s, end_date=e),
        start, end, 365 * 3,
    )
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['trade_date'])
    df = df.sort_values('date')
    df['north_money'] = pd.to_numeric(df['north_money'], errors='coerce')
    # 停发期 north_money 为 NaN：填 0 再累加，曲线走平而非断裂
    df['north_cum'] = df['north_money'].fillna(0).cumsum()
    return _finalize(df, ['north_cum'])


def build_margin(pro, start, end) -> pd.DataFrame:
    df = _paged(lambda s, e: pro.margin(start_date=s, end_date=e), start, end, 365)
    if df.empty:
        return df
    df['rzrqye'] = pd.to_numeric(df['rzrqye'], errors='coerce')
    agg = df.groupby('trade_date', as_index=False)['rzrqye'].sum()
    agg['date'] = _ymd_to_date(agg['trade_date'])
    agg['margin_bal'] = agg['rzrqye'] / 1e8  # 元 → 亿元
    return _finalize(agg, ['margin_bal'])


def build_money_supply(pro, start, end) -> pd.DataFrame:
    df = pro.cn_m()
    if df is None or df.empty:
        return pd.DataFrame()
    per = pd.PeriodIndex(pd.to_datetime(df['month'].astype(str), format='%Y%m'), freq='M')
    df['date'] = per.to_timestamp(how='end').normalize()
    return _finalize(df, ['m1_yoy', 'm2_yoy'])


def build_lpr(pro, start, end) -> pd.DataFrame:
    df = _paged(lambda s, e: pro.shibor_lpr(start_date=s, end_date=e), start, end, 365 * 3)
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['date'])
    df = df.rename(columns={'1y': 'lpr_1y', '5y': 'lpr_5y'})
    return _finalize(df, ['lpr_1y', 'lpr_5y'])


def build_fred_dgs10(pro, start, end) -> pd.DataFrame:
    df = _paged(lambda s, e: pro.us_tycr(start_date=s, end_date=e), start, end, 365 * 3)
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['date'])
    df = df.rename(columns={'y10': 'value'})
    return _finalize(df, ['value'])


def build_china_us_spread(pro, start, end) -> pd.DataFrame:
    """中美 10Y 利差（月度，对齐契约 "OECD 月度同口径" 注解）。

    yc_cb(1001.CB) 一个交易日返回约 500 个期限点（0.08/0.1/0.17… 细插值），
    90 天窗口直接撞 2000 行上限、只剩 ~4 天 → 序列稀疏。日频不可行，故取月度：
    每个月末一个 ~12 天尾窗各拉一次（含若干交易日，含 10Y 点），取窗口内最后
    一个 10Y。yc_cb 限 20 次/分钟 → 每月间隔 3.2s。增量：已在旧 parquet 里的
    月份跳过，所以日常 cron 每月只新增 1 次调用，不会每次重跑全史。
    """
    out_fp = CACHE_DIR / 'china_us_spread.parquet'
    have_months: set[str] = set()
    old = pd.DataFrame()
    if out_fp.exists():
        try:
            cand = pd.read_parquet(out_fp)
            cand['date'] = pd.to_datetime(cand['date'])
            # 只信"干净的月度"旧数据；旧版日频遗留(date 非月末)整盘弃用重建
            is_month_end = (cand['date'] == cand['date'] + pd.offsets.MonthEnd(0)).all()
            if is_month_end and not cand.empty:
                old = cand
                have_months = set(cand['date'].dt.strftime('%Y%m'))
        except Exception:
            old = pd.DataFrame()

    us = _paged(lambda s, e: pro.us_tycr(start_date=s, end_date=e), start, end, 365 * 3)
    if us.empty:
        return old if not old.empty else pd.DataFrame()
    us['date'] = _ymd_to_date(us['date'])
    us = us[['date', 'y10']].rename(columns={'y10': 'us10y'})
    us['us10y'] = pd.to_numeric(us['us10y'], errors='coerce')

    cn_start = max(pd.Timestamp(start), pd.Timestamp('20160101'))
    month_ends = pd.date_range(cn_start, pd.Timestamp(end), freq='ME')
    recs: list[dict] = []
    for me in month_ends:
        ym = me.strftime('%Y%m')
        if ym in have_months:
            continue
        s = (me - pd.Timedelta(days=12)).strftime('%Y%m%d')
        e = me.strftime('%Y%m%d')
        df = _call(
            lambda a, b: pro.yc_cb(
                ts_code='1001.CB', curve_type='0', start_date=a, end_date=b
            ),
            s, e,
        )
        time.sleep(3.2)
        if df is None or df.empty:
            continue
        df = df[pd.to_numeric(df['curve_term'], errors='coerce') == 10.0].copy()
        if df.empty:
            continue
        df['d'] = _ymd_to_date(df['trade_date'])
        last = df.sort_values('d').iloc[-1]
        recs.append({'date': me.normalize(), 'cn10y': float(last['yield'])})

    cn_new = pd.DataFrame(recs)
    if cn_new.empty:
        return old if not old.empty else pd.DataFrame()

    us_m = (
        us.set_index('date')['us10y']
        .resample('ME').last()
        .reset_index()
    )
    us_m['date'] = us_m['date'].dt.normalize()
    m = cn_new.merge(us_m, on='date', how='inner')
    m['spread'] = m['cn10y'] - m['us10y']
    fresh = _finalize(m, ['spread'])
    if not old.empty:
        old = old[['date', 'spread']].copy()
        old['date'] = pd.to_datetime(old['date'])
        fresh = (
            pd.concat([old, fresh], ignore_index=True)
            .drop_duplicates('date', keep='last')
            .sort_values('date')
            .reset_index(drop=True)
        )
    return fresh


BUILDERS = {
    'index_000300.SH': build_index_000300,
    'north_money': build_north_money,
    'margin': build_margin,
    'money_supply': build_money_supply,
    'lpr': build_lpr,
    'fred_DGS10': build_fred_dgs10,
    'china_us_spread': build_china_us_spread,
}


def main() -> int:
    ap = argparse.ArgumentParser(description='拉 Billionaire 宏观面板 7 序列 → macro/')
    ap.add_argument('--only', help=f'逗号分隔，子集。可选: {",".join(BUILDERS)}')
    ap.add_argument('--start', default='20100101', help='日频序列起始 YYYYMMDD')
    ap.add_argument('--end', default='', help='结束 YYYYMMDD（默认今天）')
    ap.add_argument('--sleep', type=float, default=0.4, help='序列间隔秒')
    args = ap.parse_args()

    end = args.end or pd.Timestamp.now().strftime('%Y%m%d')
    names = list(BUILDERS)
    if args.only:
        want = {s.strip() for s in args.only.split(',') if s.strip()}
        bad = want - set(BUILDERS)
        if bad:
            sys.exit(f'未知序列: {sorted(bad)}。可选: {names}')
        names = [n for n in names if n in want]

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    print(f'拉 {len(names)} 个宏观序列 → {CACHE_DIR}')
    print(f'日频区间 {args.start} ~ {end}')
    print('-' * 70)

    ok: list[dict] = []
    skipped: list[tuple[str, str]] = []
    n = len(names)
    for i, name in enumerate(names, 1):
        try:
            df = BUILDERS[name](pro, args.start, end)
        except PermissionSkip as e:
            print(f'  [{i}/{n}] skip  {name:<17} 权限/积分 -> {str(e)[:70]}')
            skipped.append((name, str(e)))
            time.sleep(args.sleep)
            continue
        except Exception as e:  # noqa: BLE001
            print(f'  [{i}/{n}] FAIL  {name:<17} -> {e}')
            skipped.append((name, str(e)))
            time.sleep(args.sleep)
            continue

        if df is None or df.empty:
            print(f'  [{i}/{n}] empty {name:<17} 接口返回空')
            skipped.append((name, 'empty'))
            time.sleep(args.sleep)
            continue

        df.to_parquet(CACHE_DIR / f'{name}.parquet', index=False)
        cols = [c for c in df.columns if c != 'date']
        d0, d1 = df['date'].min().date(), df['date'].max().date()
        print(f'  [{i}/{n}] ok    {name:<17} {len(df):>5} 行  {d0} ~ {d1}  {cols}')
        ok.append({'series': name, 'rows': len(df),
                   'date_min': df['date'].min(), 'date_max': df['date'].max()})
        time.sleep(args.sleep)

    if ok:
        pd.DataFrame(ok).to_parquet(CACHE_DIR / '_series.parquet', index=False)

    print('-' * 70)
    print(f'完成: {len(ok)} 成功 / {len(skipped)} 跳过 → {CACHE_DIR}')
    if skipped:
        print('\n跳过明细：')
        for name, err in skipped:
            print(f'  {name}: {err[:100]}')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
