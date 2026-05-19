"""一次性拉核心宏观经济序列 → data_cache/macro/<series>.parquet。

为什么需要这个
─────────────
项目此前只有行情 / 财务 / 资讯，没有宏观底座。研究"策略适合的市场环境"
（ROADMAP 每个策略 5 问之一）时，需要把行情叠到 CPI/PPI/M2/PMI/社融/利率
这些宏观节奏上看。这个脚本把常用宏观序列一次性落盘，notebook 里直接读
parquet，不用每次现拉。也是边搭边学宏观的脚手架。

数据源
──────
Tushare 宏观接口（基础会员够用的优先；个别要积分的失败就跳过、不阻断其他）：

    cn_gdp   季度  GDP 及三产构成 + 同比
    cn_cpi   月度  CPI（全国/城市/农村，当月同比/环比/累计）
    cn_ppi   月度  PPI（工业生产者出厂价，多分项同比）
    cn_m     月度  货币供应 M0/M1/M2 及同比
    cn_pmi   月度  采购经理指数（制造业/非制造业/综合）
    cn_sf    月度  社会融资规模（增量/存量）—— 可能要积分，拿不到自动跳过
    shibor   日度  上海银行间同业拆放利率（ON ~ 1Y 期限）
    us_tycr  日度  美国国债收益率曲线（1M ~ 30Y）—— 看外部利率对 A/H 的传导

每个序列除原始列外，统一补一列 `date`（pandas datetime，取区间期末日：
季度末 / 月末 / 当日），方便 notebook 直接画图、和行情按日期 join。
原始日期列（quarter / month / date）保留不动。

依赖
────
tushare / pandas / pyarrow / python-dotenv（都在 requirements.txt）

环境
────
项目根 .env 里需要 TUSHARE_TOKEN。worktree（.claude/worktrees/*）里没 .env
时自动回落主仓库根的 .env。没 token 直接报错退出，不静默继续。

用法（先 source .venv/bin/activate）
────────────────────────────────────
    # worktree 里跑用这个绝对路径（用户默认在主仓目录，cd 不到 worktree）
    python /Users/helm/Documents/Code/sh_quant/.claude/worktrees/\
loving-pasteur-c87edd/scripts/pull_macro.py

    # 主仓 merge 后日常跑
    python scripts/pull_macro.py                       # 拉全部序列
    python scripts/pull_macro.py --only cn_cpi,cn_m    # 只拉指定序列
    python scripts/pull_macro.py --start 20100101      # 日度序列起始（默认 20100101）

输出位置
────────
data_cache/macro/cn_gdp.parquet
data_cache/macro/cn_cpi.parquet
...
data_cache/macro/_series.parquet   序列清单 + 行数 + 日期范围（速查）
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
CACHE_DIR = PROJECT_ROOT / 'data_cache' / 'macro'

PERMISSION_KEYWORDS = ('40203', '权限', '积分', 'permission', 'points')


class PermissionSkip(RuntimeError):
    """该序列权限/积分不足。只跳过这一个序列，不中断其他。"""


# ─── 序列登记表 ──────────────────────────────────────────────────────────
# method    : pro 上的接口名
# date_col  : 原始日期列名
# kind      : 'quarter' | 'month' | 'day'  —— 决定如何归一到 `date`
# ranged    : True 走 start_date/end_date（日度大序列），False 一次全量（小）
SERIES = [
    {'name': 'cn_gdp', 'method': 'cn_gdp', 'date_col': 'quarter', 'kind': 'quarter',
     'ranged': False},
    {'name': 'cn_cpi', 'method': 'cn_cpi', 'date_col': 'month', 'kind': 'month',
     'ranged': False},
    {'name': 'cn_ppi', 'method': 'cn_ppi', 'date_col': 'month', 'kind': 'month',
     'ranged': False},
    {'name': 'cn_m', 'method': 'cn_m', 'date_col': 'month', 'kind': 'month',
     'ranged': False},
    {'name': 'cn_pmi', 'method': 'cn_pmi', 'date_col': 'MONTH', 'kind': 'month',
     'ranged': False},
    # 社融接口名是 sf_month（不是 cn_sf）；输出文件仍叫 cn_sf 便于记忆
    {'name': 'cn_sf', 'method': 'sf_month', 'date_col': 'month', 'kind': 'month',
     'ranged': False},
    {'name': 'shibor', 'method': 'shibor', 'date_col': 'date', 'kind': 'day',
     'ranged': True},
    {'name': 'us_tycr', 'method': 'us_tycr', 'date_col': 'date', 'kind': 'day',
     'ranged': True},
]


# ─── .env 加载（worktree 无 .env 回落主仓库根）────────────────────────────
def load_token() -> str:
    try:
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh` 或 `pip install python-dotenv`。')

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
        sys.exit('TUSHARE_TOKEN not found in .env. cp .env.example .env，再填你的真 token。')
    return token


# ─── 日期归一：原始列 → `date`（区间期末时间戳）─────────────────────────
def add_date_column(df: pd.DataFrame, date_col: str, kind: str) -> pd.DataFrame:
    raw = df[date_col].astype(str).str.strip()
    if kind == 'quarter':
        # '2023Q1' → 该季度末
        per = pd.PeriodIndex(raw.str.replace('Q', '-Q', regex=False), freq='Q')
        date = per.to_timestamp(how='end').normalize()
    elif kind == 'month':
        # 'YYYYMM' → 该月末
        per = pd.PeriodIndex(pd.to_datetime(raw, format='%Y%m'), freq='M')
        date = per.to_timestamp(how='end').normalize()
    else:  # day: 'YYYYMMDD'
        date = pd.to_datetime(raw, format='%Y%m%d')
    out = df.copy()
    out['date'] = pd.Series(date, index=out.index)
    return out


# ─── 单序列拉取 ──────────────────────────────────────────────────────────
def fetch_series(pro, spec: dict, start: str, end: str) -> pd.DataFrame:
    method = getattr(pro, spec['method'])
    try:
        if spec['ranged']:
            # Tushare 单次最多返回 2000 行（且只给最近的），日度序列拉多年
            # 会被截断。按年分段拉再拼，绕开 2000 行上限。
            y0, y1 = int(start[:4]), int(end[:4])
            parts = []
            for yr in range(y0, y1 + 1):
                s = max(start, f'{yr}0101')
                e = min(end, f'{yr}1231')
                chunk = method(start_date=s, end_date=e)
                if chunk is not None and not chunk.empty:
                    parts.append(chunk)
            df = pd.concat(parts, ignore_index=True) if parts else None
        else:
            df = method()
    except Exception as e:  # noqa: BLE001
        if any(s in str(e) for s in PERMISSION_KEYWORDS):
            raise PermissionSkip(str(e)) from e
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    df = add_date_column(df, spec['date_col'], spec['kind'])
    df = (
        df.drop_duplicates('date', keep='last')
        .sort_values('date')
        .reset_index(drop=True)
    )
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description='拉核心宏观序列 → data_cache/macro/')
    ap.add_argument(
        '--only',
        help='逗号分隔，只拉这些序列（如 cn_cpi,cn_m,shibor）。默认全部。',
    )
    ap.add_argument('--start', default='20100101', help='日度序列起始 YYYYMMDD（默认 20100101）')
    ap.add_argument('--end', default='', help='日度序列结束 YYYYMMDD（默认今天）')
    ap.add_argument('--sleep', type=float, default=0.4, help='序列间隔秒数（避开速率限）')
    args = ap.parse_args()

    end = args.end or pd.Timestamp.now().strftime('%Y%m%d')

    wanted = None
    if args.only:
        wanted = {s.strip() for s in args.only.split(',') if s.strip()}
        unknown = wanted - {s['name'] for s in SERIES}
        if unknown:
            sys.exit(f'未知序列: {sorted(unknown)}。可选: {[s["name"] for s in SERIES]}')

    try:
        import tushare as ts
    except ImportError:
        sys.exit('tushare 没装。先 `bash setup.sh`。')

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts.set_token(load_token())
    pro = ts.pro_api()

    specs = [s for s in SERIES if wanted is None or s['name'] in wanted]
    print(f'拉 {len(specs)} 个宏观序列 → {CACHE_DIR.relative_to(PROJECT_ROOT)}/')
    print(f'日度区间 {args.start} ~ {end}')
    print('-' * 70)

    summary: list[dict] = []
    skipped: list[tuple[str, str]] = []
    n = len(specs)
    for i, spec in enumerate(specs, 1):
        name = spec['name']
        try:
            df = fetch_series(pro, spec, args.start, end)
        except PermissionSkip as e:
            print(f'  [{i:>2}/{n}] skip  {name:<8} 权限/积分不足 -> {str(e)[:80]}')
            skipped.append((name, str(e)))
            time.sleep(args.sleep)
            continue
        except Exception as e:  # noqa: BLE001
            print(f'  [{i:>2}/{n}] FAIL  {name:<8} -> {e}')
            skipped.append((name, str(e)))
            time.sleep(args.sleep)
            continue

        if df.empty:
            print(f'  [{i:>2}/{n}] empty {name:<8} 接口返回空')
            skipped.append((name, 'empty'))
            time.sleep(args.sleep)
            continue

        out = CACHE_DIR / f'{name}.parquet'
        df.to_parquet(out, index=False)
        d0 = df['date'].min().date()
        d1 = df['date'].max().date()
        print(f'  [{i:>2}/{n}] ok    {name:<8} {len(df):>5} 行  {d0} ~ {d1}')
        summary.append({
            'series': name,
            'rows': len(df),
            'date_min': df['date'].min(),
            'date_max': df['date'].max(),
        })
        time.sleep(args.sleep)

    if summary:
        pd.DataFrame(summary).to_parquet(CACHE_DIR / '_series.parquet', index=False)

    print('-' * 70)
    print(f'完成: {len(summary)} 成功 / {len(skipped)} 跳过')
    if skipped:
        print('\n跳过明细（权限/积分/空 — 拿不到先用能拿到的，notebook 里注明）：')
        for name, err in skipped:
            print(f'  {name}: {err[:100]}')

    # 全部跳过才算失败；部分跳过按"先用能拿到的近似"约定，返回 0
    return 0 if summary else 1


if __name__ == '__main__':
    sys.exit(main())
