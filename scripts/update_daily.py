"""
A 股 + 美股 日线增量更新器。

设计原则:
  - 幂等：重复跑同一天结果一致（drop_duplicates(trade_date, keep='last')）
  - 鲁棒：每次拉最近 7 天而非 1 天，覆盖跨周末/节假日/源头回填
  - 多源 fallback：efinance/yfinance/polygon/Tushare 自动切
  - 多线程加速：默认 5 并发（efinance/yfinance 不限速，可放心并发）

数据源选择策略
─────────────────
  A 股 (.SH/.SZ/.BJ):  Tushare 主 → efinance 备
                       理由：Tushare 数据清洗更彻底、adj_factor 即时完整、schema 一致
  美股 (.US):           FMP 主 → Polygon 备 → yfinance 兜底
                       理由：FMP 付费档给 30+ 年历史（深度优先），Polygon Starter
                       只有 5 年滚动；Polygon 留作备源做"数据校验对账"
  港股 (.HK):           暂未实现，等富途 vendor 完成（周末）

文件 schema (沿用 sh_quant 现有约定，详见 docs/DATA_SCHEMA.md):
  路径:  data_cache/stocks/<ts_code>.parquet
  列:   trade_date, ts_code, open, high, low, close, pre_close,
         change, pct_chg, vol, amount, adj_factor (个股)
  美股: 同列名，但 adj_factor 由 yfinance Adj Close / Close 反推

用法
────
    source .venv/bin/activate
    pip install efinance yfinance         # 如果还没装

    # 1) 默认行为（最常用）：扫描 universe/*.parquet + stocks/ 取并集，全更新
    python scripts/update_daily.py

    # 2) 指定单个或几个 ticker（调试 / 临时增加）
    python scripts/update_daily.py --tickers 600519.SH,000001.SZ,NVDA.US

    # 3) 从文件读 ticker 列表
    python scripts/update_daily.py --file tickers.txt

    # 4) 控制并发 / 回退窗口
    python scripts/update_daily.py --workers 10 --lookback 14

    # 5) 强制全量重拉（破缓存，分红事件后用）
    python scripts/update_daily.py --tickers 600519.SH --force

默认 ticker 来源
────────────────
默认不传任何 ticker 参数时，自动扫两处取并集：
  - data_cache/universe/*.parquet 里所有 ts_code 列
  - data_cache/stocks/*.parquet 已缓存的所有 ticker

后者是为了兼容"用 --tickers 临时拉过但没进 universe"的股票（如美股 NVDA.US
没有正式的 us universe 文件时，靠 stocks/ 自动拾起来）

Cron 推荐（A 股收盘后 1 小时，美股收盘后 1 小时）:
    # 北京时间 17:00 跑 A 股
    0 17 * * 1-5 cd ~/Documents/Code/sh_quant && .venv/bin/python scripts/update_daily.py --market cn
    # 美股次日 6:00（北京时间）跑
    0 6 * * 2-6 cd ~/Documents/Code/sh_quant && .venv/bin/python scripts/update_daily.py --market us
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = PROJECT_ROOT / 'data_cache' / 'stocks'

# ---------- ts_code 后缀解析 ----------
SUFFIX_TO_MARKET = {
    '.SH': 'cn_a',
    '.SZ': 'cn_a',
    '.BJ': 'cn_a',
    '.HK': 'cn_hk',
    '.US': 'us',
    '.SI': 'cn_index',  # 申万指数，updater 不处理
}


def parse_market(ts_code: str) -> str:
    """从 ts_code 后缀判断市场。"""
    for suf, market in SUFFIX_TO_MARKET.items():
        if ts_code.endswith(suf):
            return market
    # 没后缀的，默认按美股处理（NVDA / TSLA 这种裸字母代码）
    if ts_code.isalpha() or '-' in ts_code:
        return 'us'
    return 'unknown'


# ---------- 各市场的 fetch 函数 ----------

_WARN_SEEN: set[str] = set()

# ---------- Tushare 批量预拉缓存 ----------
# 设计目标: daily cron 的本质 = 给每只股票 append 今天的 bar. 直接调
# `pro.daily(trade_date=今天)` 一次拿全市场 ~5500 行, ~2-4s, 替代 5000+ 次
# per-ticker 调用.
#
# 范围: batch 只覆盖最近 1 个交易日 (latest_td). 这是 fast-path 服务的窗口.
#   - Fresh ticker (last_cached = 昨天): 请求 [昨天-1, 今天]. fast-path 返回今天
#     的 1 行. update_one 的 merge 把它接到老数据后面. ✓
#   - 部分覆盖也接受: fast-path 返回缓存里能匹配的子集, 缺的天数由 old 老数据
#     提供 (drop_duplicates 兜底). 这等价于"信任 batch 的最新数据, 不主动校
#     验更早的源头回填". 想要源头校验请用 --verify.
#
# 收益: 2549 只 A 股 × 2 endpoint = 5098 次 (Tushare 500/min, 10 min)
#       → 1 trade_date × 2 endpoint = 2 次 (~4s).
_BATCH_CACHE: dict[str, pd.DataFrame] = {}
_BATCH_MIN_DATE: pd.Timestamp | None = None
_BATCH_MAX_DATE: pd.Timestamp | None = None
_BATCH_HIT_COUNT = 0
_BATCH_MISS_COUNT = 0
_VERIFY_MODE = False  # --verify N 设置时为 True, fast-path 强制 skip


def _warn_once(msg: str, key: str = '') -> None:
    """避免并发线程里同一消息打很多次。key 可以分多种警告独立去重。"""
    k = key or msg
    if k not in _WARN_SEEN:
        print(f'  [diag] {msg}')
        _WARN_SEEN.add(k)


def _fetch_a_share_via_efinance(ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """A 股走 efinance。返回标准列 schema 的 DataFrame，或 None。

    路由顺序：Tushare 主 → efinance 备。这个函数只在 Tushare 拿不到时才会被调用。
    """
    try:
        import efinance as ef
    except ImportError:
        _warn_once(
            'efinance 未安装（仅作 Tushare 失败时的兜底，不影响主流程）。'
            '想启用 efinance 兜底：pip install efinance'
        )
        return None

    # efinance 不要 .SH/.SZ 后缀，只要 6 位代码
    code = ts_code.split('.')[0]
    df = ef.stock.get_quote_history(
        code,
        beg=start.replace('-', ''),
        end=end.replace('-', ''),
        fqt=0,  # 不复权（与 sh_quant schema 对齐）
    )
    if df is None or len(df) == 0:
        return None
    # 标准化列名 → sh_quant schema
    df = df.rename(
        columns={
            '日期': 'trade_date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'vol',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg',
            '涨跌额': 'change',
        }
    )
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['ts_code'] = ts_code
    df['pre_close'] = df['close'].shift(1)
    # efinance 不直接给 adj_factor，先填 1.0；后续 Tushare 跑批时回填
    df['adj_factor'] = 1.0

    cols = [
        'trade_date',
        'ts_code',
        'open',
        'high',
        'low',
        'close',
        'pre_close',
        'change',
        'pct_chg',
        'vol',
        'amount',
        'adj_factor',
    ]
    return df[[c for c in cols if c in df.columns]]


def _latest_trade_date(today: pd.Timestamp) -> str | None:
    """问 Tushare 拿最近一个交易日 (YYYYMMDD).

    不依赖 trade_cal (有些积分级别返空), 直接对最近 5 个日历日各试 pro.daily()
    取一个 sample ticker, 第一个返非空的就是最近交易日.
    """
    try:
        import tushare as ts
    except ImportError:
        return None
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        return None
    ts.set_token(token)
    pro = ts.pro_api()

    # 倒序遍历最近 7 个日历日, 跳过周末和未来日期
    for back in range(0, 7):
        cand = today - pd.Timedelta(days=back)
        if cand.weekday() >= 5:  # 周六周日
            continue
        td_str = cand.strftime('%Y%m%d')
        try:
            sample = pro.daily(trade_date=td_str)
            if sample is not None and not sample.empty:
                return td_str
        except Exception:
            continue
    return None


def _prefetch_tushare_batch(today: pd.Timestamp) -> int:
    """按 trade_date 批量预拉最近 1 个交易日的全 A 股 daily + adj_factor.

    daily cron 的核心场景: 给每只股票 append 今天的 bar. 一个 endpoint 一次拿
    全市场 ~5500 行, 比 5000+ 次 per-ticker 调用快 ~150x.

    返回缓存的 ts_code 数量; 任何失败 (无 token / API 挂 / 积分不够) 返回 0,
    主流程自动降级到 per-ticker.
    """
    global _BATCH_CACHE, _BATCH_MIN_DATE, _BATCH_MAX_DATE

    try:
        import tushare as ts
    except ImportError:
        return 0
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        return 0
    ts.set_token(token)
    pro = ts.pro_api()

    td_str = _latest_trade_date(today)
    if td_str is None:
        print('  [batch] 找不到最近交易日, 降级到 per-ticker', flush=True)
        return 0

    print(f'  [batch] 预拉 trade_date={td_str} 全市场 daily + adj_factor', flush=True)
    t_call = time.time()
    try:
        daily = pro.daily(trade_date=td_str)
    except Exception as e:
        print(f'  [batch] daily(trade_date={td_str}) 失败: {e}', flush=True)
        return 0
    if daily is None or daily.empty:
        return 0
    print(f'  [batch] daily: {len(daily)} 行, {time.time() - t_call:.1f}s', flush=True)

    t_call = time.time()
    try:
        adj = pro.adj_factor(trade_date=td_str)
    except Exception as e:
        print(f'  [batch] adj_factor(trade_date={td_str}) 失败 ({e}), 用 1.0 兜底', flush=True)
        adj = None

    if adj is not None and not adj.empty:
        adj = adj[['ts_code', 'trade_date', 'adj_factor']].drop_duplicates(
            ['ts_code', 'trade_date']
        )
        merged = daily.merge(adj, on=['ts_code', 'trade_date'], how='left')
        print(f'  [batch] adj_factor: {len(adj)} 行, {time.time() - t_call:.1f}s', flush=True)
    else:
        merged = daily.copy()
        merged['adj_factor'] = 1.0

    merged['adj_factor'] = merged['adj_factor'].fillna(1.0)
    merged['trade_date'] = pd.to_datetime(merged['trade_date'], format='%Y%m%d')

    by_code: dict[str, pd.DataFrame] = {}
    for code, sub in merged.groupby('ts_code'):
        by_code[str(code)] = sub.sort_values('trade_date').reset_index(drop=True)

    _BATCH_CACHE = by_code
    td_ts = pd.to_datetime(td_str, format='%Y%m%d')
    _BATCH_MIN_DATE = td_ts
    _BATCH_MAX_DATE = td_ts

    return len(by_code)


def _fetch_a_share_via_tushare(ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """A 股走 Tushare（带 adj_factor）。用作 fallback 或定期完整复权刷新。

    Fast path: 如果 _prefetch_tushare_batch 已跑过且 start 落在缓存窗口里, 直接
    返回内存切片, 不调网络.

    Slow path (cache miss / 缓存窗口外 / 全量回填): 原 per-ticker pro.daily +
    pro.adj_factor 接口.
    """
    global _BATCH_HIT_COUNT, _BATCH_MISS_COUNT

    # ---- Fast path: 命中批量缓存 ----
    # 严格语义: 只有 batch 完全覆盖请求范围 [start, end] 时才走 fast-path.
    # 否则 fall-through 到 slow path 由 per-ticker 接口正确处理多天范围.
    # 这避免了 "batch 只有今天 + ticker 缺 5 天 → merge 后中间留空洞" 的 bug.
    if _VERIFY_MODE:
        pass  # --verify 强制走 slow path 做源头校验
    elif _BATCH_MIN_DATE is not None and _BATCH_MAX_DATE is not None:
        try:
            start_dt = pd.Timestamp(start)
            end_dt = pd.Timestamp(end)
            # 必须 batch 范围 ⊇ 请求范围 才能完整服务
            if start_dt >= _BATCH_MIN_DATE and end_dt <= _BATCH_MAX_DATE:
                cached = _BATCH_CACHE.get(ts_code)
                if cached is not None and not cached.empty:
                    mask = (cached['trade_date'] >= start_dt) & (cached['trade_date'] <= end_dt)
                    sliced = cached.loc[mask].copy()
                    if not sliced.empty:
                        _BATCH_HIT_COUNT += 1
                        return sliced
                # ts_code 不在缓存 (停牌/退市/新股), 落 miss 后 fall-through
                _BATCH_MISS_COUNT += 1
        except Exception:
            pass  # 任何 cache 路径异常 → 静默 fallback 到 per-ticker

    # ---- Slow path: 原 per-ticker 接口 ----
    try:
        import tushare as ts
        from dotenv import load_dotenv
    except ImportError:
        return None

    load_dotenv(PROJECT_ROOT / '.env')
    token = os.getenv('TUSHARE_TOKEN')
    if not token or token == 'your_tushare_token_here':
        return None

    ts.set_token(token)
    pro = ts.pro_api()

    s = start.replace('-', '')
    e = end.replace('-', '')

    daily = pro.daily(ts_code=ts_code, start_date=s, end_date=e)
    if daily is None or daily.empty:
        return None
    adj = pro.adj_factor(ts_code=ts_code, start_date=s, end_date=e)

    daily = daily.sort_values('trade_date').reset_index(drop=True)
    if adj is not None and not adj.empty:
        adj = adj[['trade_date', 'adj_factor']].drop_duplicates('trade_date')
        daily = daily.merge(adj, on='trade_date', how='left')
        daily['adj_factor'] = daily['adj_factor'].ffill().bfill().fillna(1.0)
    else:
        daily['adj_factor'] = 1.0

    daily['trade_date'] = pd.to_datetime(daily['trade_date'], format='%Y%m%d')
    return daily


def _fetch_us_via_fmp(ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """美股走 FMP /stable/historical-price-eod/full。

    重要：FMP 把 v3/v4 endpoint 整组迁移到 /stable/ 了（参考 Billionaire/AGENTS.md
    TASK-036 修复记录）。Starter 档对 /stable/* 完全开放。
    付费档可拉 30+ 年历史。
    """
    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        _warn_once(
            'FMP_API_KEY 未配置！美股 OHLC 主源不可用。请把 key 填到 sh_quant/.env 的 FMP_API_KEY=',
            key='fmp_key_missing',
        )
        return None
    try:
        import requests
    except ImportError:
        return None

    code = ts_code[:-3] if ts_code.endswith('.US') else ts_code
    # /stable/historical-price-eod/full —— 2024+ FMP 标准接口
    url = (
        f'https://financialmodelingprep.com/stable/historical-price-eod/full'
        f'?symbol={code}&from={start}&to={end}&apikey={api_key}'
    )
    try:
        r = requests.get(url, timeout=30)
    except requests.exceptions.RequestException as e:
        _warn_once(f'FMP 网络错误: {e}', key='fmp_net_err')
        return None
    if r.status_code != 200:
        _warn_once(
            f'FMP API {r.status_code}: {r.text[:200]}',
            key=f'fmp_http_{r.status_code}',
        )
        return None

    data = r.json()
    # /stable 返回结构是直接的数组，不再包 'historical' key
    historical = data if isinstance(data, list) else data.get('historical', [])
    if not historical:
        return None

    rows = []
    for d in historical:
        close = d.get('close', 0)
        vol = d.get('volume', 0)
        adj_close = d.get('adjClose')
        rows.append(
            {
                'trade_date': pd.to_datetime(d.get('date')),
                'ts_code': ts_code,
                'open': d.get('open'),
                'high': d.get('high'),
                'low': d.get('low'),
                'close': close,
                'vol': vol,
                'amount': close * vol if close and vol else 0,
                # FMP 给 adjClose，反推 adj_factor = adjClose / close
                'adj_factor': adj_close / close if close and adj_close else 1.0,
            }
        )
    df = pd.DataFrame(rows).sort_values('trade_date').reset_index(drop=True)
    df['pre_close'] = df['close'].shift(1)
    df['change'] = df['close'] - df['pre_close']
    df['pct_chg'] = (df['change'] / df['pre_close']) * 100
    return df


def _fetch_us_via_yfinance(ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """美股走 yfinance（最后兜底，付费 key 用户一般不会走到这里）。"""
    try:
        import yfinance as yf
    except ImportError:
        _warn_once(
            'yfinance 未安装（Polygon/FMP 都失败时才会用到），可选安装：pip install yfinance',
            key='yfinance_missing',
        )
        return None

    # ts_code 是 'NVDA.US' 或裸 'NVDA'，yfinance 用裸代码
    code = ts_code[:-3] if ts_code.endswith('.US') else ts_code

    # yfinance 的 end 是 exclusive，+1 天才能拿到当天
    end_inclusive = (datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    df = yf.Ticker(code).history(start=start, end=end_inclusive, auto_adjust=False)
    if df is None or len(df) == 0:
        return None
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    df = df.reset_index()
    df = df.rename(
        columns={
            'Date': 'trade_date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'vol',
        }
    )

    df['ts_code'] = ts_code
    df['pre_close'] = df['close'].shift(1)
    df['change'] = df['close'] - df['pre_close']
    df['pct_chg'] = (df['change'] / df['pre_close']) * 100
    df['amount'] = df['close'] * df['vol']  # yfinance 不给成交额，估算

    # adj_factor 反推：Adj Close / Close
    if 'Adj Close' in df.columns:
        df['adj_factor'] = df['Adj Close'] / df['close']
        df['adj_factor'] = df['adj_factor'].fillna(1.0)
    else:
        df['adj_factor'] = 1.0

    cols = [
        'trade_date',
        'ts_code',
        'open',
        'high',
        'low',
        'close',
        'pre_close',
        'change',
        'pct_chg',
        'vol',
        'amount',
        'adj_factor',
    ]
    return df[cols]


def _fetch_us_via_polygon(ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """美股走 Polygon（美股 OHLC 主源）。"""
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        _warn_once(
            '⚠️ POLYGON_API_KEY 未配置！美股 OHLC 主源不可用。'
            '请把 key 填到 sh_quant/.env 的 POLYGON_API_KEY=',
            key='polygon_key_missing',
        )
        return None
    try:
        import requests
    except ImportError:
        return None

    code = ts_code[:-3] if ts_code.endswith('.US') else ts_code
    # Polygon 用 . 分隔多类股票（BRK.B），雅虎/FMP 用 -（BRK-B）。统一转一下
    polygon_code = code.replace('-', '.')
    url = (
        f'https://api.polygon.io/v2/aggs/ticker/{polygon_code}/range/1/day/'
        f'{start}/{end}?adjusted=false&sort=asc&limit=5000&apiKey={api_key}'
    )
    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        return None
    data = r.json().get('results', [])
    if not data:
        return None

    rows = []
    for d in data:
        rows.append(
            {
                'trade_date': datetime.fromtimestamp(d['t'] / 1000),
                'ts_code': ts_code,
                'open': d['o'],
                'high': d['h'],
                'low': d['l'],
                'close': d['c'],
                'vol': d['v'],
                'amount': d.get('vw', d['c']) * d['v'],
                'adj_factor': 1.0,  # Polygon 不直接给，单独接口
            }
        )
    df = pd.DataFrame(rows)
    df['pre_close'] = df['close'].shift(1)
    df['change'] = df['close'] - df['pre_close']
    df['pct_chg'] = (df['change'] / df['pre_close']) * 100
    return df


# ---------- 路由 ----------


def fetch_one(ts_code: str, start: str, end: str) -> tuple[pd.DataFrame | None, str]:
    """
    根据 ts_code 后缀路由到对应数据源，主源失败自动 fallback。
    返回 (DataFrame|None, 实际成功使用的 vendor 名)。
    """
    market = parse_market(ts_code)

    if market == 'cn_a':
        # Tushare 主（数据清洗 + adj_factor 完整 + 与现有 schema 一致）
        df = _fetch_a_share_via_tushare(ts_code, start, end)
        if df is not None and len(df) > 0:
            return df, 'tushare'
        # efinance 备（如果 Tushare 配额/网络出问题时兜底）
        df = _fetch_a_share_via_efinance(ts_code, start, end)
        if df is not None and len(df) > 0:
            return df, 'efinance'
        return None, 'none'

    if market == 'us':
        # FMP 主：付费档给 30+ 年历史，历史深度对齐 A 股
        df = _fetch_us_via_fmp(ts_code, start, end)
        if df is not None and len(df) > 0:
            return df, 'fmp'
        # Polygon 备：Stocks Starter 5 年滚动，但数据质量极佳，做兜底 + 校验
        df = _fetch_us_via_polygon(ts_code, start, end)
        if df is not None and len(df) > 0:
            return df, 'polygon'
        # yfinance 最后兜底（一般不会走到）
        df = _fetch_us_via_yfinance(ts_code, start, end)
        if df is not None and len(df) > 0:
            return df, 'yfinance'
        return None, 'none'

    if market == 'cn_hk':
        # 港股待集成富途
        return None, 'hk_not_implemented'

    return None, f'unknown_market({ts_code})'


# ---------- 单只 ticker 增量更新 ----------


def _last_trading_day_approx(today: pd.Timestamp) -> pd.Timestamp:
    """粗略算"最近一个**已经收盘**的交易日"，考虑周末 + 当日是否过了收盘时间。

    判断逻辑:
      - 周末: 返回最近的工作日 (周六→周五, 周日→周五)
      - 工作日且当前时间已过 17:00 中国时间: 今天本身就算最近交易日
        (A 股 15:00 收盘 / 港股 16:00 收盘, 加 1h buffer 给 vendor 入库)
      - 工作日但当前时间 < 17:00: 今天数据还没 ready, 返回昨天/上周五
      - 周一早上: 上周五

    边界 (节假日跑批) 会让"上次更新到上一工作日"的股票被误判为需要 fetch,
    Tushare/efinance 会返空, 脚本 fallback 到 'empty' status, 不阻塞流程.

    NOTE: 时区用 pandas 本地, 假设跑批机器在中国时区. 美股口径凌晨 4-5 点收
    (CN tz), 早上 8 点 cron 时美股昨天数据已经 ready, last_td 还是返回昨天,
    符合预期 (美股 5/13 收盘 → 5/14 凌晨入库 → 5/14 早 8 点 cron 拉到 5/13).
    """
    today = today.normalize()
    wd = today.weekday()  # Monday=0
    if wd == 5:  # Saturday
        return today - pd.Timedelta(days=1)
    if wd == 6:  # Sunday
        return today - pd.Timedelta(days=2)
    after_close = pd.Timestamp.now().hour >= 17
    if wd == 0:  # Monday
        return today if after_close else today - pd.Timedelta(days=3)
    # Tuesday-Friday
    return today if after_close else today - pd.Timedelta(days=1)


def _peek_last_date_fast(fp: Path) -> pd.Timestamp | None:
    """O(1) 从 parquet metadata 取 trade_date 的 max，不读 data。

    pyarrow 写 parquet 时默认给每个 row group 写 column statistics（min/max/...）。
    读 metadata 是常数时间 ~0.5ms，对比读完整 200KB parquet 的 10-30ms 快一个量级。
    取不到 statistics 时 fallback 到只读 trade_date 一列。
    """
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(fp)
        schema = pf.schema_arrow
        if 'trade_date' not in schema.names:
            return None
        col_idx = schema.names.index('trade_date')
        max_val = None
        for rg_idx in range(pf.num_row_groups):
            stats = pf.metadata.row_group(rg_idx).column(col_idx).statistics
            if stats is None or not stats.has_max_value:
                # 没 stats，退到读列
                df = pd.read_parquet(fp, columns=['trade_date'])
                return pd.to_datetime(df['trade_date']).max().normalize()
            v = stats.max
            if max_val is None or v > max_val:
                max_val = v
        if max_val is None:
            return None
        return pd.Timestamp(max_val).normalize()
    except Exception:
        # 任何意外都退到完整读 (慢但稳)
        try:
            df = pd.read_parquet(fp, columns=['trade_date'])
            return pd.to_datetime(df['trade_date']).max().normalize()
        except Exception:
            return None


def update_one(ts_code: str, lookback_days: int, cache_dir: Path, force: bool) -> dict:
    """
    增量更新一只 ticker。
    返回 {ticker, status, vendor, new_rows, total_rows, latest, error?}。
    """
    fp = cache_dir / f'{ts_code}.parquet'
    today = pd.Timestamp.today().normalize()
    last_td = _last_trading_day_approx(today)
    end = today.strftime('%Y-%m-%d')

    # 快速 fresh 检查：只读 metadata，不读数据
    peek_last = None
    if fp.exists() and not force:
        peek_last = _peek_last_date_fast(fp)
        if peek_last is not None and peek_last >= last_td:
            return {
                'ticker': ts_code,
                'status': 'fresh',
                'vendor': '-',
                'new_rows': 0,
                'total_rows': -1,  # 没读 data 不知道行数，省 IO
                'latest': peek_last.strftime('%Y-%m-%d'),
            }

    # 退市/historical-universe 检测: 只对 A 股 + batch 加载时生效
    #   场景 (a): 有 parquet 但 90+ 天没新数据 + 不在 batch → 退市
    #   场景 (b): 无 parquet (拉过几次都拉不到) + 不在 batch → universe 残留
    #             (e.g. 000003.SZ PT 金田 A 2002 年就退了)
    # 这两种都跳过 slow path 减无谓 API call.
    # 真正长期停牌的股票, 复牌当天会进 batch → 这里不会误判.
    if (
        not force
        and _BATCH_MIN_DATE is not None
        and parse_market(ts_code) == 'cn_a'
        and ts_code not in _BATCH_CACHE
        and (peek_last is None or peek_last < today - pd.Timedelta(days=90))
    ):
        return {
            'ticker': ts_code,
            'status': 'delisted',
            'vendor': '-',
            'new_rows': 0,
            'total_rows': -1,
            'latest': (peek_last.strftime('%Y-%m-%d') if peek_last is not None else '-'),
        }

    # 走到这里说明要 fetch — 现在才读完整 parquet
    # start 语义: 拉最近 lookback 个交易日 (calendar 范围, vendor 自动过滤周末)
    # lookback=1 → start=today (1 天); lookback=7 → start=6天前 (~7 天)
    # 注: 若 lookback=1 但本地 last_cached 比 1 天前还老 (停牌/catch-up), 中间天会
    # 留空洞. 用户用 --lookback N 显式控制要不要追历史
    old: pd.DataFrame | None = None
    if fp.exists() and not force:
        try:
            old = pd.read_parquet(fp)
            old['trade_date'] = pd.to_datetime(old['trade_date'])
        except Exception:
            old = None
    if force:
        start = '2015-01-01'
    else:
        start_dt = today - pd.Timedelta(days=max(lookback_days - 1, 0))
        start = start_dt.strftime('%Y-%m-%d')

    # 拉
    df_new, vendor = fetch_one(ts_code, start, end)
    if df_new is None or len(df_new) == 0:
        return {
            'ticker': ts_code,
            'status': 'empty',
            'vendor': vendor,
            'new_rows': 0,
            'total_rows': 0,
            'error': f'no data from any vendor (lookback={lookback_days}d)',
        }

    # 合并去重
    merged = pd.concat([old, df_new], ignore_index=True) if old is not None else df_new

    merged = (
        merged.drop_duplicates(subset='trade_date', keep='last')
        .sort_values('trade_date')
        .reset_index(drop=True)
    )
    after = len(merged)
    new_rows = after - (len(old) if old is not None else 0)

    # 跳过无谓 IO：如果数据完全没变（行数同 + 值同），不重写文件。
    # 注意：7 天 overlap 可能让源头静默修正老值，必须比较值而非只看行数。
    if old is not None and not _data_changed(merged, old):
        return {
            'ticker': ts_code,
            'status': 'unchanged',
            'vendor': vendor,
            'new_rows': 0,
            'total_rows': after,
            'latest': merged['trade_date'].max().strftime('%Y-%m-%d'),
        }

    # 写回
    cache_dir.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(fp, index=False, compression='snappy')

    return {
        'ticker': ts_code,
        'status': 'ok',
        'vendor': vendor,
        'new_rows': new_rows,
        'total_rows': after,
        'latest': merged['trade_date'].max().strftime('%Y-%m-%d'),
    }


# 用于比较 merged vs old 是否有实质变化（捕获源头静默修正）
_COMPARE_COLS = ('open', 'high', 'low', 'close', 'vol', 'amount', 'adj_factor')


def _data_changed(merged: pd.DataFrame, old: pd.DataFrame) -> bool:
    """merged 相对 old 有变化（行数不同 / 关键列值不同）就返回 True。"""
    if len(merged) != len(old):
        return True
    old_sorted = (
        old.drop_duplicates(subset='trade_date', keep='last')
        .sort_values('trade_date')
        .reset_index(drop=True)
    )
    cols = [c for c in _COMPARE_COLS if c in merged.columns and c in old_sorted.columns]
    if not cols:
        # 没有可比较的数值列，保守起见认为变了
        return True
    import numpy as np

    a = merged[cols].to_numpy(dtype=float)
    b = old_sorted[cols].to_numpy(dtype=float)
    # 用 isclose 处理浮点舍入差异
    return not np.allclose(a, b, equal_nan=True, rtol=1e-9, atol=1e-9)


# ---------- 批量入口 ----------


def collect_tickers(args, cache_dir: Path) -> list[str]:
    """决定要更新哪些 ticker。

    优先级：
      --tickers 显式列表 > --file 文件 > 默认（universe ∪ cache）
    """
    tickers: list[str] = []

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
    elif args.file:
        with open(args.file, encoding='utf-8') as f:
            tickers = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    else:
        # 默认：union(所有 universe/*.parquet 的 ts_code, stocks/ 已缓存)
        ts_set: set[str] = set()

        universe_dir = PROJECT_ROOT / 'data_cache' / 'universe'
        if universe_dir.exists():
            print('从 universe 文件加载 ticker:')
            for uni_fp in sorted(universe_dir.glob('*.parquet')):
                try:
                    df = pd.read_parquet(uni_fp)
                    if 'ts_code' in df.columns:
                        new = set(df['ts_code'].tolist())
                        ts_set.update(new)
                        print(f'  + {uni_fp.name}: {len(new)} 只')
                except Exception as e:
                    print(f'  ! 读 {uni_fp.name} 失败: {e}')

        # 再合并已缓存但 universe 没收录的（如 --tickers 临时拉的美股）
        if cache_dir.exists():
            cached = {fp.stem for fp in cache_dir.glob('*.parquet') if not fp.stem.startswith('_')}
            ad_hoc = cached - ts_set
            if ad_hoc:
                print(f'  + 已缓存但不在 universe: {len(ad_hoc)} 只 (可能是 --tickers 临时拉过的)')
            ts_set.update(cached)

        if not ts_set:
            raise SystemExit(
                '没有 ticker 可更新。先跑：\n'
                '  python scripts/pull_universe.py    # 生成 A 股清单\n'
                '或用 --tickers / --file 显式指定。'
            )

        tickers = sorted(ts_set)

    # market 过滤
    if args.market:
        markets = set(args.market.split(','))

        def keep(t: str) -> bool:
            m = parse_market(t)
            if 'cn' in markets and m == 'cn_a':
                return True
            if 'us' in markets and m == 'us':
                return True
            return bool('hk' in markets and m == 'cn_hk')

        tickers = [t for t in tickers if keep(t)]

    return tickers


def main() -> int:
    # 统一在入口加载 .env，让所有 fetcher 都能看到 POLYGON_API_KEY / FMP_API_KEY /
    # TUSHARE_TOKEN，不依赖某个 fetcher 顺手加载
    try:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / '.env')
    except ImportError:
        pass  # 没装 python-dotenv 也不阻塞，os.getenv 仍能读 shell export 的变量

    ap = argparse.ArgumentParser(
        description=(
            'A 股 + 美股 日线增量更新器。不传 --tickers/--file 时，自动用 universe + 缓存的并集'
        ),
    )
    src = ap.add_mutually_exclusive_group(required=False)
    src.add_argument('--tickers', help='逗号分隔的 ts_code 列表，如 600519.SH,NVDA.US')
    src.add_argument('--file', help='ticker 列表文件，每行一个')
    ap.add_argument('--market', help='只更新特定市场 (cn/us/hk)，逗号分隔')
    ap.add_argument(
        '--lookback',
        type=int,
        default=1,
        help=(
            '拉最近 N 个交易日 (默认 1 只拉今天; --lookback 7 '
            '拉最近 7 天用于 catch-up). lookback=1 走 fast-path, '
            'lookback>1 自动走 per-ticker slow path'
        ),
    )
    ap.add_argument(
        '--verify',
        type=int,
        default=0,
        metavar='N',
        help=(
            '源头校验模式: 跳过 batch fast-path, per-ticker '
            '重拉最近 N 天用于检测 Tushare 数据回填. 0 = 不校验 '
            '(默认). 周末手动跑 --verify 7 做一次源头对账'
        ),
    )
    ap.add_argument('--workers', type=int, default=5, help='并发线程数')
    ap.add_argument('--force', action='store_true', help='强制全量重拉（破缓存）')
    ap.add_argument(
        '--cache-dir',
        default=str(DEFAULT_CACHE_DIR),
        help='数据目录（默认 sh_quant/data_cache/stocks/）',
    )
    ap.add_argument(
        '--verbose', action='store_true', help='打印所有 ticker 状态（默认跳过 fresh 的刷屏行）'
    )
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir).expanduser()
    tickers = collect_tickers(args, cache_dir)

    if not tickers:
        print('没有 ticker 要更新（试试 --all-cached 或 --tickers）')
        return 2

    print(f'更新 {len(tickers)} 只 ticker → {cache_dir}')
    print(
        f'回退 {args.lookback} 天, 并发 {args.workers}, '
        f'market={args.market or "all"}, force={args.force}'
    )
    print('-' * 70)

    # --verify N: 强制 per-ticker, lookback 改成 N 天用于源头校验
    global _VERIFY_MODE
    if args.verify:
        _VERIFY_MODE = True
        args.lookback = max(args.lookback, args.verify)

    # 批量预拉 Tushare 开关 (单 trade_date, 2 个 API call ~4s):
    #   --force / --verify: 强制走 per-ticker, batch 帮不上
    #   CN ticker < MIN: ticker 少时直接 per-ticker 反而快 (batch 2 call 也要 4s)
    #   全 fresh: 没有 stale ticker 时跳过 (预扫 parquet metadata)
    BATCH_MIN_TICKERS = 20
    cn_tickers = [t for t in tickers if parse_market(t) == 'cn_a']
    cn_count = len(cn_tickers)
    skip_batch = bool(args.force or args.verify)
    if not skip_batch and cn_count >= BATCH_MIN_TICKERS:
        # 预扫: 计算 stale 数 (peek_last < last_td 的). 全 fresh 时不浪费 batch
        today_norm = pd.Timestamp.today().normalize()
        last_td_pre = _last_trading_day_approx(today_norm)
        n_stale = 0
        for t in cn_tickers:
            fp = cache_dir / f'{t}.parquet'
            if not fp.exists():
                n_stale += 1
                continue
            peek = _peek_last_date_fast(fp)
            if peek is None or peek < last_td_pre:
                n_stale += 1
        if n_stale == 0:
            print(
                f'  [batch] 所有 {cn_count} 只 CN ticker 已 fresh '
                f'(last_td={last_td_pre.strftime("%Y-%m-%d")}), 跳过 batch'
            )
            print('-' * 70)
        else:
            print(f'  [batch] {n_stale}/{cn_count} CN ticker 待更新, 启动 batch 预拉')
            t_pre = time.time()
            n_cached = _prefetch_tushare_batch(today_norm)
            if n_cached:
                print(
                    f'  [batch] Tushare 预拉 {n_cached} 只 A 股 @ '
                    f'{_BATCH_MIN_DATE.strftime("%Y-%m-%d")}, '
                    f'用时 {time.time() - t_pre:.1f}s'
                )
            else:
                print('  [batch] Tushare 预拉未启用 (无 token / API 失败), 走原 per-ticker 路径')
            print('-' * 70)
    elif args.verify:
        print(
            f'  [batch] --verify {args.verify}, 跳过 batch 走 per-ticker 校验最近 {args.verify} 天'
        )
        print('-' * 70)
    elif cn_count and cn_count < BATCH_MIN_TICKERS:
        print(
            f'  [batch] CN ticker {cn_count} < {BATCH_MIN_TICKERS}, '
            '跳过 batch (小批次直接 per-ticker)'
        )
        print('-' * 70)

    t0 = time.time()
    results = []
    fresh_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(update_one, t, args.lookback, cache_dir, args.force): t for t in tickers
        }
        width = len(str(len(tickers)))
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {
                    'ticker': t,
                    'status': 'error',
                    'vendor': '?',
                    'new_rows': 0,
                    'total_rows': 0,
                    'error': str(e),
                }
            results.append(r)

            # fresh / delisted 是噪音, 默认不打印 (终端 I/O 是瓶颈)
            # 每 500 个进度刷一行心跳, 让用户知道还在跑
            if r['status'] == 'fresh':
                fresh_count += 1
                if not args.verbose and fresh_count % 500 == 0:
                    print(f'  [{i:>{width}}/{len(tickers)}] · (已跳过 {fresh_count} 个 fresh)')
                if not args.verbose:
                    continue
            if r['status'] == 'delisted' and not args.verbose:
                continue

            status_tag = {
                'ok': '✓',
                'unchanged': '=',
                'fresh': '·',
                'empty': '○',
                'error': '✗',
                'delisted': '☠',
            }.get(r['status'], '?')
            if r['status'] == 'ok':
                extra = (
                    f'  latest={r.get("latest", "-")}, +{r["new_rows"]} new, vendor={r["vendor"]}'
                )
            elif r['status'] == 'unchanged':
                extra = f'  latest={r.get("latest", "-")}, no change (skipped write)'
            elif r['status'] == 'fresh':
                extra = f'  latest={r.get("latest", "-")}, fresh (skipped fetch, metadata-only)'
            elif r['status'] == 'delisted':
                extra = (
                    f'  latest={r.get("latest", "-")}, 疑似退市 '
                    f'(>90d 无新数据且不在 batch 里, skipped)'
                )
            else:
                extra = f'  ERR: {r.get("error", "?")}'
            print(f'  [{i:>{width}}/{len(tickers)}] {status_tag} {t:<14}{extra}')

    elapsed = time.time() - t0
    ok = sum(1 for r in results if r['status'] == 'ok')
    unchanged = sum(1 for r in results if r['status'] == 'unchanged')
    fresh = sum(1 for r in results if r['status'] == 'fresh')
    delisted = sum(1 for r in results if r['status'] == 'delisted')
    new_rows_total = sum(r['new_rows'] for r in results if r['status'] == 'ok')
    print('-' * 70)
    summary = f'完成: {ok} 更新'
    if unchanged:
        summary += f' / {unchanged} 无变化(跳过写)'
    if fresh:
        summary += f' / {fresh} 已最新(跳过 fetch)'
    if delisted:
        summary += f' / {delisted} 疑似退市(跳过)'
    summary += f', +{new_rows_total} 新行, 用时 {elapsed:.1f}s'
    print(summary)

    # 批量缓存命中率: hit = 走 fast path 不调网络的 ticker 次数,
    # miss = 落在窗口里但 ts_code 不在 batch 结果 (停牌/退市/新股) → fall-through
    if _BATCH_HIT_COUNT or _BATCH_MISS_COUNT:
        total = _BATCH_HIT_COUNT + _BATCH_MISS_COUNT
        pct = 100 * _BATCH_HIT_COUNT // max(total, 1)
        print(
            f'  [batch] Tushare cache hit: {_BATCH_HIT_COUNT}/{total} ({pct}%), '
            f'miss (fall-through to per-ticker): {_BATCH_MISS_COUNT}'
        )

    # 真正"失败/空"：只看 empty 和 error，不算 fresh/unchanged（它们是成功的）
    failed = [r for r in results if r['status'] in ('empty', 'error')]
    if failed:
        print(f'\n失败/空 {len(failed)} 只:')
        for r in failed[:20]:
            print(f'  {r["ticker"]}: {r["status"]} - {r.get("error", "-")}')
        if len(failed) > 20:
            print(f'  ... 还有 {len(failed) - 20} 只')

    # 退出码: 0 全成功, 1 部分成功, 2 全失败
    if ok == len(results):
        return 0
    if ok == 0:
        return 2
    return 1


if __name__ == '__main__':
    sys.exit(main())
