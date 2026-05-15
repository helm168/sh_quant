"""生成美股标的池清单，写到 data_cache/universe/us.parquet。

跟 pull_universe.py（A 股）配套。两个 universe 文件 update_daily.py 默认会自动
union，所以不用单独传参就能一起拉 OHLC。

为什么 $1B 是合理底线
─────────────────────
- A 股 100 亿 RMB ≈ $1.4B USD，对齐用户实操底线
- 排除 micro-cap / penny stock（流动性差、数据噪声大）
- ~3000-4000 只覆盖 SP500 + Russell 1000 头部 + 大部分中盘股

为什么 FMP 而非 Polygon screener
───────────────────────────────
FMP /stable/company-screener 一次调用就能按市值 / 国家 / 交易所 / sector filter，
而 Polygon /v3/reference/tickers 没有市值字段，要先全量拿再二次筛。
Billionaire/AGENTS.md 确认 FMP STARTER 对 /stable/company-screener PASS。

依赖
────
FMP_API_KEY（.env 里，付费档 US 全套已经验证过）

用法（先 source .venv/bin/activate）
──────────────────────────────────
    python scripts/pull_us_universe.py                     # 默认 $1B+，US 全交易所
    python scripts/pull_us_universe.py --min-mv 5          # 阈值改成 $5B
    python scripts/pull_us_universe.py --exchanges NYSE,NASDAQ
    python scripts/pull_us_universe.py --include-etf       # 默认排除 ETF/FUND
    python scripts/pull_us_universe.py --include-adr       # 默认排除 ADR

输出
────
    data_cache/universe/us.parquet
        列: ts_code (XXX.US), symbol, name, exchange, market_cap (亿美元),
            sector, industry, country, beta, price, volume, is_actively_trading,
            is_etf, is_fund, is_adr, ipo_date, snapshot_date

下一步
──────
    python scripts/update_daily.py --workers 10
    # update_daily 会自动 union 所有 universe/*.parquet 的 ts_code，
    # 包括刚生成的 us.parquet，一次性拉所有股票 OHLC
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR_UNIVERSE = PROJECT_ROOT / 'data_cache' / 'universe'
OUT_FILE = CACHE_DIR_UNIVERSE / 'us.parquet'

FMP_BASE = 'https://financialmodelingprep.com/stable'


def load_key() -> str:
    """从 .env 读 FMP_API_KEY。"""
    try:
        from dotenv import load_dotenv
    except ImportError:
        sys.exit('python-dotenv 没装。先 `bash setup.sh`。')
    load_dotenv(PROJECT_ROOT / '.env')
    key = os.getenv('FMP_API_KEY')
    if not key:
        sys.exit('FMP_API_KEY not found in .env。')
    return key


def fetch_screener(
    key: str,
    min_mv_billion: float,
    exchanges: list[str] | None,
    include_etf: bool,
    include_fund: bool,
    include_adr: bool,
) -> pd.DataFrame:
    """调 FMP company-screener。返回原始 DataFrame。

    FMP screener 一次最多返回 ~10000 行，按市值倒序，所以默认就拿到大盘股。
    """
    # FMP 单位是美元，我们用户友好显示用亿美元，但参数要换回美元
    min_mv = int(min_mv_billion * 1e8)  # 亿美元 → 美元
    params = {
        'marketCapMoreThan': min_mv,
        'isActivelyTrading': 'true',
        'country': 'US',
        'limit': 10000,
        'apikey': key,
    }
    if exchanges:
        params['exchange'] = ','.join(exchanges)

    url = f'{FMP_BASE}/company-screener'
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        sys.exit(f'FMP screener {r.status_code}: {r.text[:300]}')

    data = r.json()
    if not data:
        sys.exit('FMP screener 返回空。检查 key 和参数。')

    df = pd.DataFrame(data)

    # FMP 字段名规范化
    # 原始字段: symbol, companyName, marketCap, sector, industry, beta, price,
    #           lastAnnualDividend, volume, exchange, exchangeShortName, country,
    #           isEtf, isFund, isActivelyTrading
    # 注：FMP 同时返回 `exchange`(完整名, e.g. 'NASDAQ Global Select') 和
    # `exchangeShortName`(简称 'NASDAQ')。我们用简称做 canonical 'exchange'，
    # 所以先 drop 原 exchange 再 rename，避免 to_parquet 报 duplicate columns
    if 'exchange' in df.columns and 'exchangeShortName' in df.columns:
        df = df.drop(columns=['exchange'])

    rename = {
        'companyName': 'name',
        'exchangeShortName': 'exchange',
        'marketCap': 'market_cap_raw',
        'isEtf': 'is_etf',
        'isFund': 'is_fund',
        'isActivelyTrading': 'is_actively_trading',
    }
    df = df.rename(columns=rename)

    # FMP 单位是美元，转亿美元方便看
    if 'market_cap_raw' in df.columns:
        df['market_cap'] = (df['market_cap_raw'].astype(float) / 1e8).round(2)

    # ADR 判定：FMP 没直接给 isAdr，靠 symbol 模式（含 -ADR 或 sector 提示）
    # 简化：暂时不剔除 ADR，靠用户在 notebook 里按 sector/country 过滤
    df['is_adr'] = False  # 占位

    # 过滤 ETF / Fund
    # 注意：FMP 这些字段可能返回 int (0/1) 而非 bool，必须显式 astype(bool)
    # 否则 ~ 是位运算（~1=-2、~0=-1），Pandas 会拿负数当列索引报 KeyError
    if not include_etf and 'is_etf' in df.columns:
        before = len(df)
        df = df[~df['is_etf'].fillna(False).astype(bool)]
        print(f'  排除 ETF: {len(df)} 只 (剔除 {before - len(df)} 只)')
    if not include_fund and 'is_fund' in df.columns:
        before = len(df)
        df = df[~df['is_fund'].fillna(False).astype(bool)]
        print(f'  排除 Fund: {len(df)} 只 (剔除 {before - len(df)} 只)')

    return df.reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description='生成美股标的池（FMP screener）',
    )
    ap.add_argument('--min-mv', type=float, default=10.0, help='最低市值（亿美元，默认 10 = $1B）')
    ap.add_argument(
        '--exchanges', default='NYSE,NASDAQ,AMEX', help='交易所，逗号分隔（默认 NYSE/NASDAQ/AMEX）'
    )
    ap.add_argument('--include-etf', action='store_true', help='包含 ETF（默认排除）')
    ap.add_argument('--include-fund', action='store_true', help='包含 Fund（默认排除）')
    ap.add_argument(
        '--include-adr', action='store_true', help='包含 ADR（默认……暂时也保留，靠用户过滤）'
    )
    ap.add_argument('--out', default=str(OUT_FILE), help='输出 parquet 路径')
    args = ap.parse_args()

    key = load_key()

    print(
        f'拉 FMP company-screener (US, marketCap > '
        f'${args.min_mv} 亿美元 = ${args.min_mv / 10:.1f}B)...'
    )
    print(f'  交易所: {args.exchanges}')
    print()

    exchanges = [e.strip() for e in args.exchanges.split(',') if e.strip()]
    df = fetch_screener(
        key,
        min_mv_billion=args.min_mv,
        exchanges=exchanges,
        include_etf=args.include_etf,
        include_fund=args.include_fund,
        include_adr=args.include_adr,
    )

    # 加 ts_code 列（sh_quant 约定：<symbol>.US）
    df['ts_code'] = df['symbol'] + '.US'
    df['snapshot_date'] = pd.Timestamp.today().normalize()

    # 保留有用列
    keep_cols = [
        c
        for c in [
            'ts_code',
            'symbol',
            'name',
            'exchange',
            'sector',
            'industry',
            'country',
            'market_cap',
            'beta',
            'price',
            'volume',
            'is_actively_trading',
            'is_etf',
            'is_fund',
            'is_adr',
            'snapshot_date',
        ]
        if c in df.columns
    ]
    out = df[keep_cols].copy()

    # 写出
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)

    print(f'\n✓ {len(out)} 只标的写入 {out_path.relative_to(PROJECT_ROOT)}')

    # 分布
    print('\n池子组成:')
    if 'exchange' in out.columns:
        print(' 按交易所:')
        print(out['exchange'].value_counts().head(5).to_string())
    if 'sector' in out.columns:
        print('\n 按 sector (Top 10):')
        print(out['sector'].value_counts().head(10).to_string())
    if 'market_cap' in out.columns:
        print(
            f'\n 市值范围: ${out["market_cap"].min():.1f} 亿 → '
            f'${out["market_cap"].max() / 100:.1f} 万亿'
        )
        print(f' 中位市值: ${out["market_cap"].median():.1f} 亿')

    print('\n下一步拉物理日线:')
    print('  python scripts/update_daily.py --workers 10')
    print('  （自动 union us.parquet + cn_a.parquet 一起拉）')


if __name__ == '__main__':
    main()
