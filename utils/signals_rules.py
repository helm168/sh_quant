"""Signal rules — v1 实现的 10 条信号.

Rule 风格: 每条 rule = 纯函数, 接收 ctx (预加载好的数据) 返回 list[Signal].
不读盘, 不带边沿状态. 边沿触发由 engine.reconcile 统一处理.

ctx 结构 (pull_signals.py 组装):
{
  'market': 'CN',
  'as_of_date': '2026-05-21' (str, parquet 里最新交易日),
  'cfg': dict,                          # config/signals.yaml 全量
  'index_df': DataFrame[date, close],   # 基准指数 (近 market_lookback_days 天)
  'mkt_amount': Series indexed by date, # 全市场日成交额 (本币元, 已 scale)
  'mkt_breadth': DataFrame[date, n_up, n_total],  # 大盘 breadth 每日
  'sector_history': DataFrame[date,sector,turnover,members] (可能 None),
  'stocks': list of (ts_code, name, df), # df 列: trade_date,close,high,low,amount,pct_chg
}

不在 v1 实现的信号 (留 v1.1): STK_DIVERGENCE / STK_VALUATION_EXT /
STK_EVENT_RESONANCE / SEC_FLOW / SEC_RS / MKT_DIVERGENCE / watchlist 联动.
"""

from __future__ import annotations

import pandas as pd

from utils.signals_engine import Signal, Subject

# ───────────────────────── helpers ─────────────────────────


def _ts_to_code(ts_code: str) -> str:
    """'603986.SH' → '603986', 'NVDA.US' → 'NVDA'."""
    return ts_code.rsplit('.', 1)[0]


def _streak_same_sign(series: pd.Series) -> tuple[int, int]:
    """末尾连续同号长度; 返回 (up_streak, down_streak), 至多一个非零."""
    if series.empty:
        return 0, 0
    sign = (series > 0).astype(int) - (series < 0).astype(int)
    # 从末尾倒数, 直到符号变
    last = sign.iloc[-1]
    if last == 0:
        return 0, 0
    n = 0
    for v in sign.iloc[::-1]:
        if v == last:
            n += 1
        else:
            break
    return (n, 0) if last > 0 else (0, n)


def _clip(v: float, lo: float = 0, hi: float = 100) -> int:
    return int(max(lo, min(hi, round(v))))


# ───────────────────────── 大盘级 ─────────────────────────


def rule_mkt_streak(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['mkt_streak']
    idx = ctx['index_df']
    if len(idx) < 2:
        return []
    diff = idx['close'].diff().dropna()
    up, down = _streak_same_sign(diff)
    n = max(up, down)
    if n < cfg['yellow_days']:
        return []
    direction = 'up' if up else 'down'
    level = 'risk' if n >= cfg['red_days'] else 'watch'
    severity = _clip(50 + (n - cfg['yellow_days']) * 10)
    title = f'指数{"连涨" if up else "连跌"} {n} 个交易日'
    detail = (
        f'基准指数 {ctx["benchmark_label"]} 已连续 {n} 个交易日'
        f'{"上涨" if up else "下跌"}, 注意{"过热" if up else "超跌"}风险.'
    )
    return [Signal(
        type='MKT_STREAK',
        scope='market',
        level=level,
        severity=severity,
        subject=Subject(kind='market', id=ctx['market'], name=ctx['benchmark_label']),
        title=title,
        detail=detail,
        metrics={'streakDays': n, 'direction': direction,
                 'latestClose': float(idx['close'].iloc[-1])},
    )]


def rule_mkt_turnover_hot(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['mkt_turnover_hot']
    mkt = ctx['mkt_amount']
    if len(mkt) < 60:
        return []
    abs_thr = cfg['abs_threshold'].get(ctx['market'], float('inf'))
    ma60 = mkt.rolling(60).mean()
    rel = mkt / ma60
    tail_n = cfg['consecutive_days']
    last_amt = mkt.iloc[-tail_n:]
    last_rel = rel.iloc[-tail_n:]
    if last_amt.notna().sum() < tail_n:
        return []
    abs_hit = (last_amt > abs_thr).all()
    rel_hit = (last_rel > cfg['rel_ratio']).all()
    if not (abs_hit or rel_hit):
        return []
    severity = _clip(60 + (last_rel.iloc[-1] - cfg['rel_ratio']) * 30)
    level = 'risk' if abs_hit else 'watch'
    return [Signal(
        type='MKT_TURNOVER_HOT',
        scope='market',
        level=level,
        severity=severity,
        subject=Subject(kind='market', id=ctx['market']),
        title='全市场成交额持续高位',
        detail=(f'全市场近 {tail_n} 个交易日成交额'
                f'{"突破绝对阈值" if abs_hit else "持续高于 60 日均额 × " + str(cfg["rel_ratio"])}, '
                '情绪偏热, 注意拥挤回撤风险.'),
        metrics={
            'consecutiveDays': tail_n,
            'latestAmount': float(last_amt.iloc[-1]),
            'latestRatio': float(last_rel.iloc[-1]),
            'absoluteThreshold': float(abs_thr) if abs_thr != float('inf') else None,
        },
    )]


def rule_mkt_turnover_cold(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['mkt_turnover_cold']
    mkt = ctx['mkt_amount']
    if len(mkt) < 60:
        return []
    ma60 = mkt.rolling(60).mean()
    rel = mkt / ma60
    tail_n = cfg['consecutive_days']
    last_rel = rel.iloc[-tail_n:]
    if last_rel.notna().sum() < tail_n or not (last_rel < cfg['rel_ratio']).all():
        return []
    severity = _clip(50 + (cfg['rel_ratio'] - last_rel.iloc[-1]) * 60)
    return [Signal(
        type='MKT_TURNOVER_COLD',
        scope='market',
        level='watch',
        severity=severity,
        subject=Subject(kind='market', id=ctx['market']),
        title='全市场成交额骤缩',
        detail=(f'全市场近 {tail_n} 个交易日成交额持续低于 60 日均额 × {cfg["rel_ratio"]}, '
                '情绪冷清, 留意观望情绪.'),
        metrics={
            'consecutiveDays': tail_n,
            'latestRatio': float(last_rel.iloc[-1]),
        },
    )]


def rule_mkt_deviation(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['mkt_deviation']
    idx = ctx['index_df']
    w = cfg['ma_window']
    if len(idx) < w:
        return []
    ma = idx['close'].rolling(w).mean()
    last_close = idx['close'].iloc[-1]
    last_ma = ma.iloc[-1]
    if pd.isna(last_ma):
        return []
    dev = (last_close - last_ma) / last_ma
    if abs(dev) <= cfg['pct_threshold']:
        return []
    up = dev > 0
    severity = _clip(50 + (abs(dev) - cfg['pct_threshold']) * 400)
    return [Signal(
        type='MKT_DEVIATION',
        scope='market',
        level='watch',
        severity=severity,
        subject=Subject(kind='market', id=ctx['market'], name=ctx['benchmark_label']),
        title=f'指数距 MA{w} {"上方" if up else "下方"}乖离 {dev * 100:.1f}%',
        detail=(f'基准指数收盘距 {w} 日均线偏离 {dev * 100:.1f}%, '
                f'{"过热" if up else "超跌"}, 历史上属于均值回归区.'),
        metrics={'deviationPct': float(dev), 'maWindow': w,
                 'latestClose': float(last_close), 'latestMa': float(last_ma)},
    )]


def rule_mkt_breadth_ext(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['mkt_breadth_ext']
    br = ctx['mkt_breadth']
    if br is None or br.empty:
        return []
    last = br.iloc[-1]
    n_total = float(last['n_total'])
    if n_total <= 0:
        return []
    pct_up = float(last['n_up']) / n_total
    hot = pct_up > cfg['hot_pct']
    cold = pct_up < cfg['cold_pct']
    if not (hot or cold):
        return []
    severity = _clip(60 + abs(pct_up - 0.5) * 80)
    return [Signal(
        type='MKT_BREADTH_EXT',
        scope='market',
        level='watch',
        severity=severity,
        subject=Subject(kind='market', id=ctx['market']),
        title=f'当日{"普涨" if hot else "普跌"}极端 ({pct_up * 100:.0f}% 上涨)',
        detail=(f'今日上涨家数占比 {pct_up * 100:.1f}%, '
                f'{"亢奋" if hot else "恐慌"}情绪较极端, 关注短期反向.'),
        metrics={'pctUp': float(pct_up), 'nUp': int(last['n_up']),
                 'nTotal': int(n_total)},
    )]


# ───────────────────────── 板块级 ─────────────────────────


def rule_sec_burst(ctx: dict) -> list[Signal]:
    cfg = ctx['cfg']['sec_burst']
    sh = ctx['sector_history']
    if sh is None or sh.empty:
        return []
    w = cfg['ma_window']
    out: list[Signal] = []
    # 按 sector groupby; sector_history.date 是 date 类型
    last_date = sh['date'].max()
    for sector, g in sh.groupby('sector'):
        g = g.sort_values('date')
        if len(g) < w + 1:
            continue
        last_row = g.iloc[-1]
        if last_row['date'] != last_date:
            continue  # 该 sector 今天无数据 (停牌全板?)
        ma = g['turnover'].iloc[-w - 1:-1].mean()
        if ma <= 0 or pd.isna(ma):
            continue
        ratio = last_row['turnover'] / ma
        if ratio <= cfg['ratio']:
            continue
        severity = _clip(50 + (ratio - cfg['ratio']) * 25)
        out.append(Signal(
            type='SEC_BURST',
            scope='sector',
            level='watch',
            severity=severity,
            subject=Subject(kind='sector', id=sector, name=sector),
            title=f'{sector} 板块放量 {ratio:.1f}x',
            detail=(f'{sector} 板块当日成交额 = 20 日均额 × {ratio:.2f}, '
                    '资金涌入, 关注板块景气延续性.'),
            metrics={'ratio': float(ratio),
                     'latestTurnover': float(last_row['turnover']),
                     'maTurnover': float(ma),
                     'members': int(last_row['members'])},
        ))
    return out


# ───────────────────────── 个股级 ─────────────────────────


def _iter_stock_signals(ctx: dict) -> list[Signal]:
    """所有 STK_* rule 共用一遍 universe 扫描, 省 5000× IO."""
    cfg = ctx['cfg']
    out: list[Signal] = []
    vol_persist_cfg = cfg['stk_vol_persist']
    breakout_cfg = cfg['stk_breakout']
    breakdown_cfg = cfg['stk_breakdown']
    strength_cfg = cfg['stk_strength']

    for ts_code, name, df in ctx['stocks']:
        if df is None or len(df) < 20:
            continue
        df = df.sort_values('trade_date')
        close = df['close'].to_numpy()
        amount = df['amount'].to_numpy()
        pct = df['pct_chg'].to_numpy() if 'pct_chg' in df.columns else None

        subj = Subject(kind='stock', id=ts_code, ticker=_ts_to_code(ts_code), name=name)

        # ── STK_VOL_PERSIST ────────────────────────────────
        w = vol_persist_cfg['ma_window']
        nd = vol_persist_cfg['consecutive_days']
        if len(amount) >= w + nd:
            ma = pd.Series(amount).rolling(w).mean().to_numpy()
            tail_amt = amount[-nd:]
            tail_ma = ma[-nd:]
            if (tail_ma > 0).all():
                ratios = tail_amt / tail_ma
                if (ratios > vol_persist_cfg['ratio']).all():
                    severity = _clip(60 + (ratios[-1] - vol_persist_cfg['ratio']) * 15)
                    out.append(Signal(
                        type='STK_VOL_PERSIST', scope='stock', level='watch',
                        severity=severity, subject=subj,
                        title=f'成交量连续 {nd} 日超预期',
                        detail=(f'连续 {nd} 个交易日成交额 > {w} 日均额 × '
                                f'{vol_persist_cfg["ratio"]}, 资金持续涌入.'),
                        metrics={'consecutiveDays': nd,
                                 'latestRatio': float(ratios[-1]),
                                 'avgRatio': float(ratios.mean())},
                    ))

        # ── STK_BREAKOUT ─────────────────────────────────
        hw = breakout_cfg['high_window']
        vw = breakout_cfg['vol_ma_window']
        if len(close) >= hw and len(amount) >= vw:
            # 收盘创 hw 日新高 = 今日 close >= max(过去 hw 个 close)
            window = close[-hw:]
            if close[-1] >= window.max():  # >= 含相等也算 (新高 ≥ 前高)
                vol_ma = amount[-vw - 1:-1].mean() if len(amount) > vw else float('nan')
                if vol_ma > 0:
                    vr = amount[-1] / vol_ma
                    if vr > breakout_cfg['vol_ratio']:
                        severity = _clip(60 + (vr - breakout_cfg['vol_ratio']) * 10)
                        out.append(Signal(
                            type='STK_BREAKOUT', scope='stock', level='opportunity',
                            severity=severity, subject=subj,
                            title=f'放量突破 {hw} 日新高',
                            detail=(f'收盘创 {hw} 日新高 且当日成交额 = {vw} 日均额 × '
                                    f'{vr:.2f}, 趋势确认.'),
                            metrics={'highWindow': hw,
                                     'latestClose': float(close[-1]),
                                     'volRatio': float(vr)},
                        ))

        # ── STK_BREAKDOWN ────────────────────────────────
        mw = breakdown_cfg['ma_window']
        bvw = breakdown_cfg['vol_ma_window']
        if len(close) > mw and len(amount) > bvw:
            ma250 = pd.Series(close).rolling(mw).mean().to_numpy()
            prev_close = close[-2]
            prev_ma = ma250[-2]
            last_close = close[-1]
            last_ma = ma250[-1]
            if (
                not (pd.isna(prev_ma) or pd.isna(last_ma))
                and prev_close >= prev_ma and last_close < last_ma
            ):
                vol_ma = amount[-bvw - 1:-1].mean()
                if vol_ma > 0 and amount[-1] / vol_ma > breakdown_cfg['vol_ratio']:
                    vr = amount[-1] / vol_ma
                    severity = _clip(70 + (vr - breakdown_cfg['vol_ratio']) * 10)
                    out.append(Signal(
                        type='STK_BREAKDOWN', scope='stock', level='risk',
                        severity=severity, subject=subj,
                        title=f'放量跌破 MA{mw} (年线)',
                        detail=(f'收盘跌破 {mw} 日均线 且当日放量 ({vr:.2f}x), '
                                '长期趋势走弱.'),
                        metrics={'maWindow': mw,
                                 'latestClose': float(last_close),
                                 'latestMa': float(last_ma),
                                 'volRatio': float(vr)},
                    ))

        # ── STK_STRENGTH ─────────────────────────────────
        if pct is not None:
            sd = strength_cfg['streak_days']
            tail = pct[-sd:]
            if len(tail) == sd and (tail > 0).all():
                # 连涨 sd 日
                cum = (1 + tail / 100).prod() - 1
                severity = _clip(60 + cum * 200)
                out.append(Signal(
                    type='STK_STRENGTH', scope='stock', level='opportunity',
                    severity=severity, subject=subj,
                    title=f'连涨 {sd} 个交易日',
                    detail=(f'连续 {sd} 个交易日上涨, 累计 {cum * 100:.1f}%, 持续强势.'),
                    metrics={'streakDays': sd, 'cumReturn': float(cum)},
                ))

    return out


# ───────────────────────── 注册表 ─────────────────────────


ALL_RULES = [
    rule_mkt_streak,
    rule_mkt_turnover_hot,
    rule_mkt_turnover_cold,
    rule_mkt_deviation,
    rule_mkt_breadth_ext,
    rule_sec_burst,
    _iter_stock_signals,
]
