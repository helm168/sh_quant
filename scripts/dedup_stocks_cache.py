"""一次性清理 data_cache/stocks/*.parquet 的重复行.

历史 bug (或并发写) 在个别 parquet 里同一 (ts_code, trade_date) 落了多份, 触发
update_daily.py 的 _data_changed broadcast 错误. 本脚本扫一遍, 把含 dup 的文件
按 (ts_code, trade_date) keep='last' 重写.

用法:
    # 干跑 (只报告, 不动文件)
    python scripts/dedup_stocks_cache.py --dry-run

    # 真改
    python scripts/dedup_stocks_cache.py

    # 指定其它目录
    python scripts/dedup_stocks_cache.py --cache-dir /path/to/stocks
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = PROJECT_ROOT / 'data_cache' / 'stocks'


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cache-dir', default=str(DEFAULT_CACHE_DIR))
    ap.add_argument('--dry-run', action='store_true', help='只扫不改')
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir).expanduser()
    if not cache_dir.exists():
        print(f'目录不存在: {cache_dir}', file=sys.stderr)
        return 1

    files = sorted(cache_dir.glob('*.parquet'))
    print(f'扫描 {len(files)} 个 parquet → {cache_dir}')
    print(f'mode: {"DRY-RUN (不写盘)" if args.dry_run else "真改 (keep=last)"}')
    print('-' * 70)

    fixed = 0
    skipped = 0
    errors = 0
    total_dup_rows = 0

    for i, fp in enumerate(files, 1):
        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            print(f'  [{i:>5}/{len(files)}] ✗ {fp.name}: 读失败 {e}')
            errors += 1
            continue

        if 'trade_date' not in df.columns or 'ts_code' not in df.columns:
            skipped += 1
            continue

        n_before = len(df)
        deduped = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
        n_after = len(deduped)
        n_dup = n_before - n_after

        if n_dup == 0:
            skipped += 1
            continue

        total_dup_rows += n_dup
        dup_dates = (
            df[df.duplicated(subset=['ts_code', 'trade_date'], keep=False)]
            ['trade_date']
            .drop_duplicates()
            .sort_values()
            .tolist()
        )
        dup_dates_str = ', '.join(str(d)[:10] for d in dup_dates[:5])
        if len(dup_dates) > 5:
            dup_dates_str += f' ... (+{len(dup_dates) - 5} more)'

        print(f'  [{i:>5}/{len(files)}] {fp.stem:<14} dup={n_dup:>3} on [{dup_dates_str}]')

        if not args.dry_run:
            deduped_sorted = deduped.sort_values('trade_date').reset_index(drop=True)
            try:
                deduped_sorted.to_parquet(fp, index=False, compression='snappy')
                fixed += 1
            except Exception as e:
                print(f'    ✗ 写盘失败: {e}')
                errors += 1

    print('-' * 70)
    print(
        f'扫完: {len(files)} 文件, {skipped} 干净跳过, '
        f'{fixed if not args.dry_run else "需修"}={(fixed if not args.dry_run else len(files) - skipped - errors)} '
        f'共 {total_dup_rows} 行 dup, {errors} 报错'
    )
    if args.dry_run:
        print('  (--dry-run 没改文件, 去掉 flag 再跑一次真改)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
