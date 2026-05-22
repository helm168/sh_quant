"""SFC Aggregated Reportable Short Positions — 全史回填 + 半月度增量。

数据来源：香港证监会 SFC 每两周发布的「合格证券空头持仓汇总」CSV。
覆盖 2012-09-07 至今，每份是该报告期收盘的逐只股票申报空头持仓存量。

为什么走 SFC 不走 HKEX：
HKEX 的卖空 HTM 是当日交易**流量**（snapshot 无 archive）。SFC 的 SPR
是**存量**（每两周发一次完整 archive），口径上跟 spec 2.9 一致，且历史
完整。两者口径不同别混（详见 macroPanel 文案）。

落盘两层：
  1) 明细层 <data_root>/hk_short_position/<YYYYMMDD>.parquet
     每份 CSV 完整解析，per-stock 列。供未来个股因子研究 / 行业聚合用。
  2) macro 派生 <data_root>/macro/hk_short_position.parquet
     列：date / total_position_hkd_yi（亿 HKD）/ n_reporters
     单位与契约对齐（pull_macro.py 的 hk_short_position builder 直读），
     不要改这两个列名/单位，否则 pull_macro 拒绝。

增量：明细层按文件存在跳过；macro 层每次都 rebuild（成本可忽略）。

用法：
    python scripts/pull_sfc_short_positions.py             # 全史 + 增量
    python scripts/pull_sfc_short_positions.py --limit 5   # 只拉最近 5 份(冒烟测试)
    python scripts/pull_sfc_short_positions.py --rebuild-macro   # 不拉新文件，仅基于已落明细重建 macro 派生
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as ex:
    sys.exit(f'{ex.name} 没装。pip install requests beautifulsoup4')


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPR_LANDING_URL = (
    'https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/'
    'Aggregated-reportable-short-positions-of-specified-shares'
)
UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'
)
HDR = {'User-Agent': UA, 'Accept': '*/*'}

# 数据根（与 pull_macro.py / Billionaire getDataRoot 对齐）
def _data_root() -> Path:
    override = os.environ.get('SH_QUANT_DATA_DIR')
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / '.market_data'


RAW_DIR = _data_root() / 'hk_short_position'
MACRO_DIR = _data_root() / 'macro'
MACRO_OUT = MACRO_DIR / 'hk_short_position.parquet'

# SFC URL 形如：.../spr/2026/05/08/Short_Position_Reporting_Aggregated_Data_20260508.csv?rev=...
CSV_DATE_RE = re.compile(r'Short_Position_Reporting_Aggregated_Data_(\d{8})\.csv', re.I)


def fetch_landing() -> str:
    print(f'GET {SPR_LANDING_URL}')
    r = requests.get(SPR_LANDING_URL, headers=HDR, timeout=30)
    r.raise_for_status()
    print(f'  ok, {len(r.content):,} bytes')
    return r.text


def extract_csv_links(html: str) -> list[tuple[str, str]]:
    """从 SFC 页面 HTML 抽 (report_date_str, url) 列表，按日期降序去重。"""
    soup = BeautifulSoup(html, 'html.parser')
    seen: dict[str, str] = {}
    for a in soup.find_all('a', href=True):
        h = a['href']
        m = CSV_DATE_RE.search(h)
        if not m:
            continue
        d = m.group(1)
        if not h.startswith('http'):
            h = 'https://www.sfc.hk' + h
        # 同一 date 出现多次取第一个（带 rev=hash 的版本）
        seen.setdefault(d, h)
    return sorted(seen.items(), reverse=True)  # 降序：最新优先


def parse_csv_bytes(content: bytes, report_date: str) -> pd.DataFrame:
    """SFC CSV 列名带空格 + 含 HK$ 等特殊字符，按关键字 fallback 识别。"""
    # SFC 用 UTF-8 with BOM 较常见；不行再 latin1
    for enc in ('utf-8-sig', 'utf-8', 'latin1'):
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f'{report_date}: CSV 编码识别失败')

    # 找列：code / name / shares / hkd
    cols_l = {c.strip().lower(): c for c in df.columns}

    def find(*needles) -> str | None:
        for low, orig in cols_l.items():
            if all(n in low for n in needles):
                return orig
        return None

    code_col = find('stock', 'code')
    name_col = find('stock', 'name') or find('name')
    shares_col = find('shares') or find('quantity')
    hkd_col = find('hk') or find('value')   # 含 'HK$' 或 'HKD' 或 'value'
    if not (code_col and shares_col and hkd_col):
        raise RuntimeError(
            f'{report_date}: SFC CSV 列名识别失败，看到 {list(df.columns)}'
        )

    out = pd.DataFrame({
        'report_date': pd.to_datetime(report_date, format='%Y%m%d'),
        'code': df[code_col].astype(str).str.strip().str.zfill(5),
        'name': df[name_col].astype(str).str.strip() if name_col else '',
        'short_shares': pd.to_numeric(
            df[shares_col].astype(str).str.replace(',', ''), errors='coerce'
        ),
        'short_hkd': pd.to_numeric(
            df[hkd_col].astype(str).str.replace(',', ''), errors='coerce'
        ),
    })
    out = out.dropna(subset=['short_hkd']).reset_index(drop=True)
    return out


def download_one(url: str, report_date: str) -> pd.DataFrame:
    print(f'  GET {url[:100]}{"..." if len(url) > 100 else ""}')
    r = requests.get(url, headers=HDR, timeout=30)
    r.raise_for_status()
    df = parse_csv_bytes(r.content, report_date)
    return df


def rebuild_macro_series() -> pd.DataFrame:
    """扫 hk_short_position/ 下所有明细，aggregate 出 macro 派生时序。"""
    if not RAW_DIR.exists():
        return pd.DataFrame()
    rows = []
    for f in sorted(RAW_DIR.glob('*.parquet')):
        df = pd.read_parquet(f, columns=['report_date', 'short_hkd'])
        if df.empty:
            continue
        rows.append({
            'date': df['report_date'].iloc[0],
            # 直接出契约单位「亿 HKD」，避免 pull_macro builder 再做转换
            'total_position_hkd_yi': float(df['short_hkd'].sum()) / 1e8,
            'n_reporters': int(len(df)),
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description='Pull SFC aggregated short positions')
    ap.add_argument('--limit', type=int, default=0,
                    help='只拉最近 N 份（冒烟测试用）；0=全量')
    ap.add_argument('--rebuild-macro', action='store_true',
                    help='不拉新 CSV，仅基于已落明细重建 macro 派生')
    ap.add_argument('--sleep', type=float, default=0.4,
                    help='文件间隔秒（防 SFC 限流）')
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MACRO_DIR.mkdir(parents=True, exist_ok=True)

    if not args.rebuild_macro:
        html = fetch_landing()
        links = extract_csv_links(html)
        print(f'页面解析出 {len(links)} 份 CSV 链接（去重，最新到最旧）')

        if args.limit:
            links = links[:args.limit]
            print(f'--limit={args.limit}，只拉 {len(links)} 份')

        # 已落盘的 date 跳过
        existing = {f.stem for f in RAW_DIR.glob('*.parquet')}
        pending = [(d, u) for d, u in links if d not in existing]
        print(f'已落盘 {len(existing)} 份，本次需拉 {len(pending)} 份')

        ok: list[str] = []
        failed: list[tuple[str, str]] = []
        for i, (date_str, url) in enumerate(pending, 1):
            print(f'\n[{i}/{len(pending)}] report_date={date_str}')
            try:
                df = download_one(url, date_str)
            except Exception as ex:  # noqa: BLE001
                print(f'  ✗ FAIL: {type(ex).__name__}: {str(ex)[:150]}')
                failed.append((date_str, str(ex)))
                time.sleep(args.sleep)
                continue
            out_fp = RAW_DIR / f'{date_str}.parquet'
            df.to_parquet(out_fp, index=False)
            tot = df['short_hkd'].sum() / 1e8
            print(f'  ok  n={len(df):>4} stocks  total={tot:>10,.0f} 亿 HKD')
            ok.append(date_str)
            time.sleep(args.sleep)

        print('\n' + '-' * 70)
        print(f'明细层拉取: {len(ok)} 新增 / {len(failed)} 失败 / '
              f'{len(existing)} 已有 → {RAW_DIR}')
        if failed:
            print('\n失败明细：')
            for d, err in failed:
                print(f'  {d}: {err[:150]}')

    # 重建 macro 派生（每次都 rebuild，成本很低）
    macro = rebuild_macro_series()
    if macro.empty:
        print('未生成 macro 派生（无明细文件）')
        return 1
    macro.to_parquet(MACRO_OUT, index=False)
    print(f'\nmacro 派生: {len(macro)} 行 {macro["date"].min().date()} ~ '
          f'{macro["date"].max().date()} → {MACRO_OUT}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
