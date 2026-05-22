"""拉宏观/流动性序列 → <data_root>/macro/<series>.parquet。

为什么是这一组序列（不是随便挑）
──────────────────────────────────
消费侧（sibling app Billionaire）的宏观面板
`src/features/macro/macroPanel.config.ts` + `localDataMiddleware.ts`
已按**固定的 series 文件名 + 固定列名**写死。生产侧（本脚本）必须逐字
对齐这个契约，否则 UI 每张卡都 404（"全部序列读不到"）。契约：

    series 文件               必须有的列（除 date 外都是数值）
    ─────────────────────────────────────────────────────────
    index_000300.SH           close
    index_399006.SZ           close            ※ 创业板指
    index_000688.SH           close            ※ 科创50
    index_HSI                 close            ※ 港股大盘
    index_HSTECH              close            ※ 恒生科技
    index_NDX                 close            ※ 纳斯达克 100
    index_SPX                 close            ※ 标普 500
    fx_usdcnh                 value            ※ 离岸人民币
    north_money               north_cum        ※ 截断于 2024-08-16（停披露）
    north_hold_q              hold_mv          ※ 季度，亿元
    hk_short_position         total_position_hkd_yi  ※ 半月度，亿 HKD (SFC)
    south_money               south_cum        ※ 港股通，南向未停披露
    margin                    margin_bal
    money_supply              m1_yoy, m2_yoy
    lpr                       lpr_1y, lpr_5y
    cn_ppi                    ppi_yoy          ※ 月度
    cn_cpi                    cpi_yoy          ※ 月度
    cn_dgs10                  value            ※ 中债 10Y 日度
    equity_bond_yield         spread           ※ HS300 股息率 − CN10Y, 日度
    fred_DGS10                value
    china_us_spread           spread

中间件 SQL 是 `SELECT * FROM read_parquet(?) WHERE date>=? AND date<=?
ORDER BY date ASC`，并把除 date 外每列 `Number(v)` —— 所以每个 parquet
必须有一列 `date`（datetime）且其余列纯数值，不能掺字符串列（ts_code 等）。

数据源（都在 Tushare 基础会员，无 VIP）
─────────────────────────────────────
    index_000300.SH  index_daily(000300.SH).close
    index_HSI/SPX    Tushare index_global(HSI/SPX).close   港股大盘 + 标普 500
    index_HSTECH     akshare.stock_hk_index_daily_em('HSTECH').latest
                     ※ Tushare 不收恒科（2020-07 才发布），走 akshare 东财
    index_NDX        akshare.index_us_stock_sina('.NDX').close
                     ※ Tushare index_global 只收 IXIC（纳指综合）无 NDX，
                       走 akshare 新浪美股
    fx_usdcnh        fx_daily(USDCNH.FXCM).bid_close → value 离岸人民币
    north_money      moneyflow_hsgt.north_money 升序累加（百万元）
                     ※ 2024-08-18 交易所停披露日度北向净买入。Tushare 之后
                       返回的不是真实净流入（实测每天恒定 ~+9万 百万元 垃圾值），
                       cumsum 会冲到 ~90 万亿元。所以 ≤ 2024-08-16 截断，
                       parquet 到此为止（不是补 0 补持平——那等于撒谎说还在该水位）。
    north_hold_q     纯本地聚合：holders/<ts>.parquet 的 northbound_hold_pct
                     × daily_basic/<ts>.parquet 的 total_mv，按 end_date 汇总。
                     不调 Tushare（pull_holders 已落数）。季度颗粒。
    hk_short_position 派生：读 macro/hk_short_position.parquet（由独立 puller
                     scripts/pull_sfc_short_positions.py 拉 SFC SPR archive）。
                     半月度颗粒，2012-09 至今。本 builder 不调外部接口。
    south_money      akshare.stock_hsgt_hist_em('南向资金') 升序累加（百万元）。
                     ※ Tushare moneyflow_hsgt.south_money 2023-11-24 后失效，
                       且 Tushare 无独立"港股通(深)"接口，故走 akshare 东财源。
                       含港股通(沪+深) 合计。详见 build_south_money 注释。
    margin           margin.rzrqye 按日跨 SSE/SZSE/BSE 求和 ÷1e8（亿元）
    money_supply     cn_m.m1_yoy / m2_yoy（月度）
    lpr              shibor_lpr.1y→lpr_1y / 5y→lpr_5y（月度）
    cn_ppi           cn_ppi.nt_yoy → ppi_yoy 工业品出厂价格同比（月度，全国）
    cn_cpi           cn_cpi.nt_yoy → cpi_yoy 居民消费价格同比（月度，全国）
    cn_dgs10         akshare.bond_china_yield '10年' → value 中债 10Y 日度
                     ※ Tushare yc_cb 限 20 次/分钟、每次 ~500 期限点撞 2000
                       上限，做日度极慢。走 akshare 中债（一次取一年内日度全量）
    equity_bond_yield  A 股股息率(上证A股代理) − cn_dgs10.value，按日 inner join。
                     ※ akshare 没有「沪深300 股息率」日度长史接口，故用
                       ak.stock_a_gxl_lg('上证A股') 当代理（HS300 约 60% 权重
                       在上证，相关性 > 0.95，作为「股债性价比」择时足够）。
                     store raw spread, ±σ 通道由 UI 渲染时 rolling 计算。
                     依赖 cn_dgs10.parquet 先落盘。
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


# ─── 各 builder ──────────────────────────────────────────────────────────
def _build_index_daily_cn(ts_code: str):
    """Tushare index_daily builder factory — A 股指数（000300.SH / 399006.SZ / 000688.SH）。"""
    def builder(pro, start, end) -> pd.DataFrame:
        df = _paged(
            lambda s, e: pro.index_daily(ts_code=ts_code, start_date=s, end_date=e),
            start, end, 365 * 6,
        )
        if df.empty:
            return df
        df['date'] = _ymd_to_date(df['trade_date'])
        return _finalize(df, ['close'])
    return builder


build_index_000300 = _build_index_daily_cn('000300.SH')
build_index_399006 = _build_index_daily_cn('399006.SZ')
build_index_000688 = _build_index_daily_cn('000688.SH')


# 交易所 2024-08-18 起停止披露日度北向净买入。原以为停披露后 Tushare
# moneyflow_hsgt.north_money 会变 NaN（fillna(0)→cumsum 走平），实测它
# 返回每日恒定 ~+9万 百万元 的非真实值，cumsum 会一路冲到 ~90 万亿元。
# 既然没有真实数据了，正确做法是**直接截断**——padding 成 0 等价于
# 「我们知道之后是 0」，而真相是「我们什么都不知道」。
NORTH_HALT = pd.Timestamp('2024-08-16')  # 最后一个有效披露日


def _build_index_global(ts_code: str):
    """Tushare index_global builder。实测可用：HSI / SPX。
    HSTECH（2020-07 发布）、NDX（纳斯达克 100）Tushare 基础会员都不收，
    走 akshare 替代源——见 build_index_HSTECH / build_index_NDX。"""
    def builder(pro, start, end) -> pd.DataFrame:
        df = _paged(
            lambda s, e: pro.index_global(ts_code=ts_code, start_date=s, end_date=e),
            start, end, 365 * 3,
        )
        if df.empty:
            return df
        df['date'] = _ymd_to_date(df['trade_date'])
        return _finalize(df, ['close'])
    return builder


def build_index_HSTECH(pro, start, end) -> pd.DataFrame:
    """恒生科技指数 — Tushare index_global 不收（2020-07 发布），走 akshare 东财。

    ak.stock_hk_index_daily_em(symbol='HSTECH') 返回日度 OHLC，列名中文。
    pro / start / end 保留签名一致但不使用（akshare 一次返回全历史）。
    """
    import akshare as ak
    df = ak.stock_hk_index_daily_em(symbol='HSTECH')
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={'latest': 'close'})
    df['date'] = pd.to_datetime(df['date'])
    return _finalize(df, ['close'])


def build_index_NDX(pro, start, end) -> pd.DataFrame:
    """纳斯达克 100 — Tushare index_global 只收 IXIC（纳指综合）无 NDX，走 akshare 新浪。

    ak.index_us_stock_sina(symbol='.NDX') 返回日度，'date' / 'close' 列。
    pro / start / end 保留签名一致但不使用（新浪一次返回全历史）。
    """
    import akshare as ak
    df = ak.index_us_stock_sina(symbol='.NDX')
    if df is None or df.empty:
        return pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    return _finalize(df, ['close'])


def build_fx_usdcnh(pro, start, end) -> pd.DataFrame:
    df = _paged(
        lambda s, e: pro.fx_daily(ts_code='USDCNH.FXCM', start_date=s, end_date=e),
        start, end, 365 * 3,
    )
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['trade_date'])
    # fx_daily 返回 bid/ask 各 4 价，取 bid_close 作为日收盘（业内默认）
    df = df.rename(columns={'bid_close': 'value'})
    return _finalize(df, ['value'])


def build_north_money(pro, start, end) -> pd.DataFrame:
    df = _paged(
        lambda s, e: pro.moneyflow_hsgt(start_date=s, end_date=e),
        start, end, 365 * 3,
    )
    if df.empty:
        return df
    df['date'] = _ymd_to_date(df['trade_date'])
    df = df.sort_values('date')
    df = df[df['date'] <= NORTH_HALT]
    if df.empty:
        return df
    df['north_money'] = pd.to_numeric(df['north_money'], errors='coerce')
    df['north_cum'] = df['north_money'].fillna(0).cumsum()
    return _finalize(df, ['north_cum'])


def build_south_money(pro, start, end) -> pd.DataFrame:
    """南向(港股通沪+深)累计净流入。

    数据源：akshare.stock_hsgt_hist_em(symbol='南向资金')
    （东方财富官方接口，含沪+深，全历史 2014-11-17 起）。

    踩坑历史：
    1) 原用 Tushare moneyflow_hsgt.south_money，2023-11-24 起字段静默变质
       （不再是日净流入，cumsum 后虚高 8 倍）。
    2) 切到 Tushare ggt_daily，单位/含义对了，但只有港股通(沪)无(深)，
       累计低估约 45%（深市占南向 ~45%）。
    3) Tushare 无独立的港股通(深)接口（已验证 ggtb_daily 等不存在）。
    故彻底放弃 Tushare 源，走 akshare 东财。

    pro/start/end 参数保留接口一致但不使用（akshare 一次返回全历史）。
    """
    import akshare as ak
    df = ak.stock_hsgt_hist_em(symbol='南向资金')
    if df.empty:
        return df
    df = df.rename(columns={'日期': 'date', '当日成交净买额': 'south_net_yi'})
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    # 东财 当日成交净买额 单位亿元 → ×100 转百万元，与 north_money 口径一致
    df['south_net'] = pd.to_numeric(df['south_net_yi'], errors='coerce') * 100
    df['south_cum'] = df['south_net'].fillna(0).cumsum()
    return _finalize(df, ['south_cum'])


def build_north_hold_q(pro, start, end) -> pd.DataFrame:
    """北向(陆股通)季度持股市值聚合。

    不调 Tushare —— 纯本地聚合两份已有 parquet：
      • holders/<ts>.parquet     pull_holders.py 落, northbound_hold_pct (%)
      • daily_basic/<ts>.parquet Tushare daily_basic, total_mv (万元)

    每只股票按 holders 里的 end_date 在 daily_basic 里 merge_asof 取最近一个
    trade_date ≤ end_date 的 total_mv，再按季末聚合：
        hold_mv_万元 = Σᵢ total_mvᵢ × pctᵢ / 100
    最后 /1e4 → 亿元，符合契约 hold_mv 列。

    pro / start / end 参数为了和 BUILDERS 签名一致，这里不用。
    """
    holders_dir = _data_root() / 'holders'
    db_dir = _data_root() / 'daily_basic'
    if not holders_dir.exists() or not db_dir.exists():
        print(f'  跳过 north_hold_q: 缺 holders/ 或 daily_basic/ '
              f'(holders={holders_dir.exists()}, daily_basic={db_dir.exists()})')
        return pd.DataFrame()

    pieces = []
    n_total = n_ok = 0
    for hf in sorted(holders_dir.glob('*.parquet')):
        n_total += 1
        ts = hf.stem
        db_f = db_dir / f'{ts}.parquet'
        if not db_f.exists():
            continue
        try:
            hd = pd.read_parquet(hf, columns=['end_date', 'northbound_hold_pct'])
            db = pd.read_parquet(db_f, columns=['trade_date', 'total_mv'])
        except Exception:
            continue
        if hd.empty or db.empty:
            continue
        hd = hd.dropna(subset=['northbound_hold_pct'])
        hd = hd[hd['northbound_hold_pct'] > 0]
        if hd.empty:
            continue
        hd = hd.rename(columns={'end_date': 'date'})
        hd['date'] = pd.to_datetime(hd['date'])
        db = db.rename(columns={'trade_date': 'date'})
        db['date'] = pd.to_datetime(db['date'])
        hd = hd.sort_values('date')
        db = db.sort_values('date').dropna(subset=['total_mv'])
        if db.empty:
            continue
        merged = pd.merge_asof(hd, db, on='date', direction='backward')
        merged = merged.dropna(subset=['total_mv'])
        if merged.empty:
            continue
        merged['hold_mv_wan'] = merged['total_mv'] * merged['northbound_hold_pct'] / 100.0
        pieces.append(merged[['date', 'hold_mv_wan']])
        n_ok += 1

    if not pieces:
        print(f'  north_hold_q: 0 / {n_total} 文件出数 — 无聚合结果')
        return pd.DataFrame()
    print(f'  north_hold_q: 聚合 {n_ok} / {n_total} 只 A 股的季度持股市值')
    big = pd.concat(pieces, ignore_index=True)
    agg = big.groupby('date', as_index=False)['hold_mv_wan'].sum()
    agg['hold_mv'] = agg['hold_mv_wan'] / 1e4   # 万元 → 亿元
    return _finalize(agg, ['hold_mv'])


def build_hk_short_position(pro, start, end) -> pd.DataFrame:
    """港股空头持仓总量（SFC 半月度）— 派生序列 passthrough。

    数据由独立 puller scripts/pull_sfc_short_positions.py 落到
    macro/hk_short_position.parquet（puller 直接以契约单位「亿 HKD」写出列
    total_position_hkd_yi）。本 builder 仅 read+finalize 转发。pro/start/end 未用。
    """
    fp = CACHE_DIR / 'hk_short_position.parquet'
    if not fp.exists():
        print(f'  跳过 hk_short_position: 缺 {fp}'
              f'，请先跑 `python scripts/pull_sfc_short_positions.py`')
        return pd.DataFrame()
    df = pd.read_parquet(fp)
    df['date'] = pd.to_datetime(df['date'])
    return _finalize(df, ['total_position_hkd_yi'])


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


def build_cn_ppi(pro, start, end) -> pd.DataFrame:
    """全国 PPI 当月同比 (月度)。Tushare cn_ppi 一次返回全历史，无需分页。"""
    df = pro.cn_ppi()
    if df is None or df.empty:
        return pd.DataFrame()
    per = pd.PeriodIndex(pd.to_datetime(df['month'].astype(str), format='%Y%m'), freq='M')
    df['date'] = per.to_timestamp(how='end').normalize()
    df = df.rename(columns={'nt_yoy': 'ppi_yoy'})
    return _finalize(df, ['ppi_yoy'])


def build_cn_cpi(pro, start, end) -> pd.DataFrame:
    """全国 CPI 当月同比 (月度)。"""
    df = pro.cn_cpi()
    if df is None or df.empty:
        return pd.DataFrame()
    per = pd.PeriodIndex(pd.to_datetime(df['month'].astype(str), format='%Y%m'), freq='M')
    df['date'] = per.to_timestamp(how='end').normalize()
    df = df.rename(columns={'nt_yoy': 'cpi_yoy'})
    return _finalize(df, ['cpi_yoy'])


def build_cn_dgs10(pro, start, end) -> pd.DataFrame:
    """中债 10Y 国债到期收益率 (日度) — 走 akshare 中债。

    Tushare yc_cb 思路：每日 500+ 期限点 × 2000 行上限 → 90 天窗口只剩 ~4 天，
    日度不可行（已在 china_us_spread 里折中成月度）。akshare bond_china_yield
    返回中债国债收益率曲线，取 '10年' 列即可。

    踩坑：bond_china_yield 对大时间窗会静默返空（实测 ≥ ~1 年就有概率失败，
    源站后端按页返回），所以按年度分窗逐段拉再 concat。
    """
    import akshare as ak
    s_ts = pd.Timestamp(start)
    e_ts = pd.Timestamp(end)
    parts = []
    cur = s_ts
    while cur <= e_ts:
        nxt = min(cur + pd.Timedelta(days=364), e_ts)
        chunk = ak.bond_china_yield(
            start_date=cur.strftime('%Y%m%d'),
            end_date=nxt.strftime('%Y%m%d'),
        )
        if chunk is not None and not chunk.empty:
            parts.append(chunk)
        cur = nxt + pd.Timedelta(days=1)
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    # akshare 当前列：'曲线名称','日期','3月','6月','1年','3年','5年','7年','10年','30年'
    # 取国债曲线（曲线名称 == '中债国债收益率曲线'）；不同 akshare 版本可能没这列
    if '曲线名称' in df.columns:
        df = df[df['曲线名称'].astype(str).str.contains('国债', na=False)].copy()
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={'日期': 'date', '10年': 'value'})
    df['date'] = pd.to_datetime(df['date'])
    return _finalize(df, ['value'])


def build_equity_bond_yield(pro, start, end) -> pd.DataFrame:
    """A 股股息率（上证A股代理）− CN10Y 国债收益率（股债性价比，日度）。

    依赖：cn_dgs10.parquet 必须先在 macro/ 落盘（BUILDERS 顺序已保证）。

    数据源踩坑历史：
    1) Tushare index_dailybasic 对指数只返 pe/pb/mv，没有 dv_ratio。
    2) akshare 没有「沪深300 股息率」日度长史接口：
       - stock_zh_index_value_csindex('000300') 只返 20 行近期数据
       - index_value_hist_funddb 不存在（≤ akshare 1.18.63）
    3) 最终选 ak.stock_a_gxl_lg('上证A股')：5000+ 行 2005-now '日期'/'股息率'。
       它**不是** HS300 股息率，是上证 A 股大盘股息率——但 HS300 约 60% 权重
       在上证，且都是大盘价值股集中区，作为「股债性价比」的代理够用，
       与 HS300 实际股息率相关性 > 0.95。卡片文案需明确这是代理。

    存的是 raw spread。±1σ/±2σ 通道是「rolling 10y」的动态量，UI 渲染时算。
    """
    cn_fp = CACHE_DIR / 'cn_dgs10.parquet'
    if not cn_fp.exists():
        print('  跳过 equity_bond_yield: 缺 cn_dgs10.parquet，请先把 cn_dgs10 跑过')
        return pd.DataFrame()
    bond = pd.read_parquet(cn_fp).rename(columns={'value': 'cn10y'})
    bond['date'] = pd.to_datetime(bond['date'])

    import akshare as ak
    dv = ak.stock_a_gxl_lg(symbol='上证A股')
    if dv is None or dv.empty:
        return pd.DataFrame()
    dv = dv.rename(columns={'日期': 'date', '股息率': 'dv_ratio'})
    dv['date'] = pd.to_datetime(dv['date'])
    dv['dv_ratio'] = pd.to_numeric(dv['dv_ratio'], errors='coerce')

    m = dv[['date', 'dv_ratio']].merge(bond[['date', 'cn10y']], on='date', how='inner')
    m['spread'] = m['dv_ratio'] - m['cn10y']
    return _finalize(m, ['spread'])


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
    'index_399006.SZ': build_index_399006,
    'index_000688.SH': build_index_000688,
    'index_HSI': _build_index_global('HSI'),
    'index_HSTECH': build_index_HSTECH,
    'index_NDX': build_index_NDX,
    'index_SPX': _build_index_global('SPX'),
    'fx_usdcnh': build_fx_usdcnh,
    'north_money': build_north_money,
    'north_hold_q': build_north_hold_q,
    'hk_short_position': build_hk_short_position,
    'south_money': build_south_money,
    'margin': build_margin,
    'money_supply': build_money_supply,
    'lpr': build_lpr,
    'cn_ppi': build_cn_ppi,
    'cn_cpi': build_cn_cpi,
    'cn_dgs10': build_cn_dgs10,
    # equity_bond_yield 依赖 cn_dgs10.parquet，必须排在它后面
    'equity_bond_yield': build_equity_bond_yield,
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
    perm_denied: list[tuple[str, str]] = []  # 权限/积分 → 需换数据源
    failed: list[tuple[str, str]] = []        # 其他错（接口异常/网络/解析）
    empties: list[str] = []                   # 接口成功但无数据
    guarded: list[tuple[str, str]] = []       # 行数退化保护
    n = len(names)
    for i, name in enumerate(names, 1):
        try:
            df = BUILDERS[name](pro, args.start, end)
        except PermissionSkip as e:
            print(f'  [{i}/{n}] PERM  {name:<17} 权限/积分不足 -> {str(e)[:80]}')
            perm_denied.append((name, str(e)))
            time.sleep(args.sleep)
            continue
        except Exception as e:  # noqa: BLE001
            print(f'  [{i}/{n}] FAIL  {name:<17} -> {e}')
            failed.append((name, str(e)))
            time.sleep(args.sleep)
            continue

        if df is None or df.empty:
            print(f'  [{i}/{n}] empty {name:<17} 接口返回空')
            empties.append(name)
            time.sleep(args.sleep)
            continue

        # Sanity check: 防止上游接口抽风（返回部分数据/改 schema）覆盖好数据。
        # 全量 pull 模式下，新数据行数不应少于旧数据 5% 以上。
        out_fp = CACHE_DIR / f'{name}.parquet'
        if out_fp.exists():
            old_rows = len(pd.read_parquet(out_fp, columns=['date']))
            if len(df) < old_rows * 0.95:
                print(f'  [{i}/{n}] GUARD {name:<17} 行数退化 {old_rows} → {len(df)}，保留旧数据')
                guarded.append((name, f'rows degraded {old_rows}->{len(df)}'))
                time.sleep(args.sleep)
                continue

        df.to_parquet(out_fp, index=False)
        cols = [c for c in df.columns if c != 'date']
        d0, d1 = df['date'].min().date(), df['date'].max().date()
        print(f'  [{i}/{n}] ok    {name:<17} {len(df):>5} 行  {d0} ~ {d1}  {cols}')
        ok.append({'series': name, 'rows': len(df),
                   'date_min': df['date'].min(), 'date_max': df['date'].max()})
        time.sleep(args.sleep)

    if ok:
        idx_path = CACHE_DIR / '_series.parquet'
        new_idx = pd.DataFrame(ok)
        if idx_path.exists():
            old_idx = pd.read_parquet(idx_path)
            merged = pd.concat([old_idx, new_idx], ignore_index=True)
            merged = merged.drop_duplicates(subset='series', keep='last')
            merged = merged.sort_values('series').reset_index(drop=True)
        else:
            merged = new_idx
        merged.to_parquet(idx_path, index=False)

    print('-' * 70)
    total_skip = len(perm_denied) + len(failed) + len(empties) + len(guarded)
    print(f'完成: {len(ok)} 成功 / {total_skip} 跳过 → {CACHE_DIR}')

    # 权限不足是「Tushare 这条路走不通，需要换数据源」的明确信号，单独高亮——
    # 不要混在 generic 失败里，否则会被以为是临时错而被忽略。
    if perm_denied:
        print('\n' + '!' * 70)
        print(f'⚠  {len(perm_denied)} 个序列 Tushare 权限/积分不足，需考虑备选数据源：')
        for name, err in perm_denied:
            print(f'   • {name}')
            print(f'     原因: {err[:120]}')
        print('   建议：评估 Yahoo Finance / FRED / akshare 等替代源，或决定放弃该序列。')
        print('!' * 70)

    if failed:
        print('\n失败（非权限，可能网络/解析/接口异常，可重试）：')
        for name, err in failed:
            print(f'  {name}: {err[:120]}')

    if empties:
        print('\n空数据（接口返回 0 行，检查参数 / 时间窗）：')
        for name in empties:
            print(f'  {name}')

    if guarded:
        print('\n行数退化保护（旧数据保留，未覆盖）：')
        for name, msg in guarded:
            print(f'  {name}: {msg}')

    # exit code 区分场景：
    #   0 — 全部成功
    #   1 — 完全失败
    #   2 — 部分成功，但有权限问题（CI / cron 应当告警，让人来看）
    if not ok:
        return 1
    if perm_denied:
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
