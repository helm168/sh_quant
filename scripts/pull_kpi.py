"""拉 core_kpi.yaml 里 auto-fetchable 的 KPI 时间序列 → <data_root>/kpi/。

为什么是这套契约（写给未来 Billionaire 消费侧）
──────────────────────────────────
跟 macro 同套思路：生产侧（本脚本）必须逐字对齐 UI 那边的 parquet 契约，
否则 "核心关切" tab 全部读不到。Billionaire 的 KPI 面板（还未实现，本脚本
就是约定它该长什么样）将通过 ~/.market_data 软链 + DuckDB 中间件读这两类
parquet：

    <data_root>/kpi/<kpi_id>.parquet     时间序列，每 KPI 一份
        date        datetime64           观测 as-of 日（月度 KPI 用月末）
        value       float                该 KPI 核心读数（YoY %、指数点、稼动率...
                                          语义看 _meta 的 unit 字段）

    <data_root>/kpi/_meta.parquet        19 KPI 元数据，UI 渲染整个 tab 用
        kpi_id, name, tickers (list<str>), category, cadence, polarity, unit,
        baseline, lo, hi, source_kind, source_ref, thesis,
        last_value (float, NaN=无时序), last_as_of (datetime, NaT=无时序)
        — manual KPI 没时间序列文件 → last_* 为 NaN，UI 据 source.kind+陈旧度
          显示「人工填，X 天未更新」

为什么 _meta 每次覆盖重算
    core_kpi.yaml 是单一真相（option A，见 memory project_core_kpi_system）。
    thesis/expected 等会跟着 yaml 改而变；时序数据另有 <kpi_id>.parquet 增量追加。

data_root 路径
    _data_root() = $SH_QUANT_DATA_DIR 或 ~/.market_data，与 pull_macro.py /
    pull_us_daily_basic.py 一致。**不**用 PROJECT_ROOT/data_cache —— 否则在
    worktree 里跑会写到 worktree 自己的 data_cache，UI 看不见（macro 当初的坑）。

依赖：requests / pandas / pyarrow（都在 requirements.txt）；tsmc fetcher 无需 token。

用法（在项目根，激活 venv 后）
    python scripts/pull_kpi.py                  # 全部 auto fetcher + 重建 meta
    python scripts/pull_kpi.py --only tsmc_monthly_rev_yoy
    python scripts/pull_kpi.py --meta-only      # 改了 yaml 后只重刷元数据
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from utils.core_kpi import auto_fetchable, get_kpis  # noqa: E402

HEADERS = {'User-Agent': 'Mozilla/5.0 (sh_quant pull_kpi)'}
TIMEOUT = 15


def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


CACHE_DIR = _data_root() / 'kpi'


def _roc_ym_to_month_end(roc_ym: str) -> pd.Timestamp:
    """民国年月 '11504' → 2026-04-30（月末，作为月度 KPI 的 as-of date）。"""
    roc, mm = int(roc_ym[:-2]), int(roc_ym[-2:])
    return pd.Timestamp(year=roc + 1911, month=mm, day=1) + pd.offsets.MonthEnd(0)


# ── fetchers（一个 OK KPI 一个函数，直接走完，无抽象）──────────────────
def fetch_tsmc_monthly_rev_yoy() -> pd.DataFrame:
    """TWSE OpenAPI t187ap05_L 上市全量月营收，按公司代号 2330 取最新月。"""
    url = 'https://openapi.twse.com.tw/v1/opendata/t187ap05_L'
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    rows = r.json()
    tsmc = next((d for d in rows if d.get('公司代號') == '2330'), None)
    if tsmc is None:
        raise RuntimeError('TWSE t187ap05_L 未返回 2330（端点结构可能已变）')
    return pd.DataFrame([{
        'date': _roc_ym_to_month_end(tsmc['資料年月']),
        'value': float(tsmc['營業收入-去年同月增減(%)']),
    }])


FETCHERS = {
    'tsmc_monthly_rev_yoy': fetch_tsmc_monthly_rev_yoy,
}


def write_series(kpi_id: str, df_new: pd.DataFrame) -> Path:
    """合并已有 parquet + 按 date 去重保新值 → 写回。"""
    fp = CACHE_DIR / f'{kpi_id}.parquet'
    merged = (pd.concat([pd.read_parquet(fp), df_new], ignore_index=True)
              if fp.exists() else df_new)
    merged['date'] = pd.to_datetime(merged['date'])
    merged = (merged.drop_duplicates(subset='date', keep='last')
                    .sort_values('date')
                    .reset_index(drop=True))
    fp.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(fp, index=False)
    return fp


def build_meta() -> pd.DataFrame:
    """扫 yaml 全部 KPI + 各自时间序列最后一行 → 一行一 KPI 的元数据 DataFrame。"""
    rows = []
    for kid, d in get_kpis().items():
        exp = d.get('expected') or {}
        ts_fp = CACHE_DIR / f'{kid}.parquet'
        last_value, last_as_of = None, None
        if ts_fp.exists():
            ts = pd.read_parquet(ts_fp).sort_values('date')
            if not ts.empty:
                last_value = float(ts['value'].iloc[-1])
                last_as_of = ts['date'].iloc[-1]
        rows.append({
            'kpi_id': kid,
            'name': d['name'],
            'tickers': list(d['tickers']),
            'category': d['category'],
            'cadence': d['cadence'],
            'polarity': d['polarity'],
            'unit': d.get('unit') or '',
            'baseline': float(exp['baseline']) if 'baseline' in exp else None,
            'lo': float(exp['lo']) if 'lo' in exp else None,
            'hi': float(exp['hi']) if 'hi' in exp else None,
            'source_kind': d['source']['kind'],
            'source_ref': d['source']['ref'],
            'thesis': d['thesis'],
            'last_value': last_value,
            'last_as_of': last_as_of,
        })
    df = pd.DataFrame(rows)
    # 显式 dtype：避免 manual KPI 全 None 让列变 object
    df['last_value'] = pd.to_numeric(df['last_value'], errors='coerce')
    df['last_as_of'] = pd.to_datetime(df['last_as_of'], errors='coerce')
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description='拉 auto-fetchable KPI 时间序列 + 重建元数据')
    parser.add_argument('--only', metavar='KPI_ID', help='只拉某个 kpi_id（必须是 auto-fetchable）')
    parser.add_argument('--meta-only', action='store_true', help='不拉时序，仅重建 _meta.parquet')
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f'data_root = {_data_root()}')
    print(f'kpi_dir   = {CACHE_DIR}\n')

    if args.meta_only:
        targets: list[str] = []
    elif args.only:
        if args.only not in auto_fetchable():
            print(f'{args.only!r} 不在 auto-fetchable: {auto_fetchable()}')
            return 2
        targets = [args.only]
    else:
        targets = auto_fetchable()

    failed: list[str] = []
    for kid in targets:
        if kid not in FETCHERS:
            print(f'  ⏭  {kid:24s} 标 auto 但本脚本暂无 fetcher（probe 仍 NEEDS_WORK）')
            continue
        try:
            df = FETCHERS[kid]()
            fp = write_series(kid, df)
            n = len(pd.read_parquet(fp))
            last = df.iloc[-1]
            print(f'  ✓ {kid:24s} {n} 行累计，最新 {last["date"].date()} = {last["value"]:.4f}')
        except Exception as e:  # noqa: BLE001
            print(f'  ✗ {kid:24s} 失败: {e!r}')
            failed.append(kid)

    meta = build_meta()
    meta_fp = CACHE_DIR / '_meta.parquet'
    meta.to_parquet(meta_fp, index=False)
    auto_n = int((meta['source_kind'] != 'manual').sum())
    has_data_n = int(meta['last_value'].notna().sum())
    print(f'\n  ✓ _meta.parquet: {len(meta)} KPI '
          f'（{auto_n} auto / {len(meta) - auto_n} manual，{has_data_n} 有时序）')

    if failed:
        print(f'\n失败 {len(failed)} 项: {failed}')
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
