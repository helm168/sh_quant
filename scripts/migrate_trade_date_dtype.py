"""一次性迁移：把 HK str trade_date 转 datetime64；US 非午夜时间戳 normalize。

背景：
  - HK 缓存 (scripts/pull_hk_futu.py 落) trade_date 是 'YYYY-MM-DD' string，
    与 A/US (datetime64) 不一致。update_daily concat 时退化成 object 列。
  - US 缓存有 ~14/300 文件 trade_date 含 12:00:00（FMP/yfinance TZ 偏移），
    `==` 日期 join 会漏行。

源端修复已合：
  - scripts/pull_hk_futu.py 改用 .dt.normalize()
  - scripts/update_daily.py FMP/yfinance 路径都加 .normalize()

本脚本扫 data_cache/stocks/*.HK.parquet 和 *.US.parquet，按需重写为正确 dtype。
干跑：默认仅打印计划；--apply 实际写盘。

用法：
  python /Users/helm/Documents/Code/sh_quant/.claude/worktrees/intelligent-shirley-c0e20e/scripts/migrate_trade_date_dtype.py        # dry-run
  python ... --apply        # 实际写盘
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

STOCKS = Path('/Users/helm/Documents/Code/sh_quant/data_cache/stocks')


def needs_fix(df: pd.DataFrame) -> tuple[bool, str]:
    s = df['trade_date']
    if s.dtype == object or str(s.dtype) == 'str':
        return True, 'str→datetime64'
    if s.dtype.kind == 'M':
        if (pd.to_datetime(s).dt.hour != 0).any():
            return True, 'non-midnight→normalize'
    return False, ''


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='实际写盘；默认 dry-run')
    args = ap.parse_args()

    files = sorted(list(STOCKS.glob('*.HK.parquet')) + list(STOCKS.glob('*.US.parquet')))
    print(f'扫描 {len(files)} 个文件 (HK + US)…')

    plan: list[tuple[Path, str]] = []
    for fp in files:
        df = pd.read_parquet(fp, columns=['trade_date'])
        ok = needs_fix(df)
        if ok[0]:
            plan.append((fp, ok[1]))

    if not plan:
        print('无需修复 ✓')
        return 0

    # 分类计数
    by_kind: dict[str, int] = {}
    for _, kind in plan:
        by_kind[kind] = by_kind.get(kind, 0) + 1
    print()
    print('需修复:')
    for kind, n in by_kind.items():
        print(f'  {kind:<25} {n} 份')

    if not args.apply:
        print()
        print('Dry-run。加 --apply 实际写盘。前 5 个样本:')
        for fp, kind in plan[:5]:
            print(f'  [{kind}] {fp.name}')
        return 0

    print()
    print('写盘中…')
    n_ok = 0
    n_fail = 0
    for fp, kind in plan:
        try:
            df = pd.read_parquet(fp)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
            df.to_parquet(fp, index=False, compression='snappy')
            n_ok += 1
            if n_ok % 200 == 0:
                print(f'  …{n_ok}/{len(plan)}')
        except Exception as e:  # noqa: BLE001
            print(f'  FAIL {fp.name}: {e}')
            n_fail += 1

    print()
    print(f'完成: {n_ok} 成功 / {n_fail} 失败 / {len(plan)} 总计')
    return 0 if n_fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
