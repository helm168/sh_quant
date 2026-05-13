"""
验证 efinance 在不同市场的历史深度。

回答的具体问题：
  - efinance 能拉到 A 股多久的日线？2005? 2010? 2015?
  - efinance 能拉到港股多久的日线？10 年够不够？
  - efinance 能拉到美股多久的日线？
  - 不复权 / 前复权 / 后复权三档都能拿吗？

这个脚本决定：港股数据底座到底用 efinance 还是必须上富途。

用法:
    source .venv/bin/activate
    pip install efinance  # 如果没装
    python scripts/test_efinance_depth.py
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

try:
    import efinance as ef
except ImportError:
    raise SystemExit('efinance 没装，先 pip install efinance')

import pandas as pd


# 跨市场样本：每个市场挑 1-2 只大盘股
TARGETS = [
    # (中文名, efinance 代码, 市场)
    ('贵州茅台',       '600519', 'A 股'),
    ('平安银行',       '000001', 'A 股'),
    ('腾讯控股',       '00700',  '港股'),
    ('中芯国际 HK',    '00981',  '港股'),
    ('阿里巴巴 HK',    '09988',  '港股'),
    ('英伟达',         'NVDA',   '美股'),
    ('苹果',           'AAPL',   '美股'),
]

# 测试 4 个起点：从最理想（2005）到最保底（2020）
START_DATES = ['20050101', '20100101', '20150101', '20200101']
END_DATE = datetime.now().strftime('%Y%m%d')

# 复权类型：efinance get_quote_history 的 fqt 参数
# 0 = 不复权, 1 = 前复权, 2 = 后复权
FQT_TYPES = [
    (0, '不复权'),
    (1, '前复权'),
    (2, '后复权'),
]


def fmt_date(s: str) -> str:
    """20150101 → 2015-01-01"""
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}'


def test_depth(code: str, market: str) -> dict:
    """
    对一只 ticker 测：
      - 从每个 START_DATES 试拉，看 efinance 返回的最早日期
      - 拿到的最早日期决定该市场的 efinance "可用历史深度"
    """
    result = {
        'code': code,
        'market': market,
        'reachable_earliest': None,
        'rows_total': 0,
        'errors': [],
    }

    # 直接用最早的 START 拉，看 efinance 给到哪
    try:
        df = ef.stock.get_quote_history(code, beg=START_DATES[0], end=END_DATE, fqt=1)
        if df is None or len(df) == 0:
            result['errors'].append(f'beg={START_DATES[0]} 返回空')
            return result

        df = df.rename(columns={'日期': 'date'})
        df['date'] = pd.to_datetime(df['date'])
        result['reachable_earliest'] = df['date'].min().strftime('%Y-%m-%d')
        result['reachable_latest'] = df['date'].max().strftime('%Y-%m-%d')
        result['rows_total'] = len(df)
    except Exception as e:
        result['errors'].append(f'{type(e).__name__}: {e}')

    return result


def test_fqt_support(code: str, market: str) -> dict:
    """测三种复权类型都能不能拿。"""
    out = {}
    for fqt, name in FQT_TYPES:
        try:
            df = ef.stock.get_quote_history(code, beg='20240101', end='20240131', fqt=fqt)
            if df is None or len(df) == 0:
                out[name] = '无数据'
            else:
                out[name] = f'{len(df)} rows, last close={df["收盘"].iloc[-1]}'
        except Exception as e:
            out[name] = f'报错 {type(e).__name__}'
    return out


def main() -> None:
    print('=' * 80)
    print('efinance 历史深度测试')
    print(f'测试时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'参考起点候选: {", ".join(fmt_date(d) for d in START_DATES)}')
    print('=' * 80)

    depth_results = []
    print('\n【第一部分】历史深度（从 2005-01-01 试拉，看实际能拿到多早）\n')
    print(f'{"标的":<18}{"代码":<10}{"市场":<8}{"最早日期":<14}{"最晚日期":<14}{"总行数":>8}')
    print('-' * 80)
    for name, code, market in TARGETS:
        r = test_depth(code, market)
        depth_results.append((name, market, r))
        if r['reachable_earliest']:
            print(
                f'{name:<18}{code:<10}{market:<8}'
                f'{r["reachable_earliest"]:<14}'
                f'{r["reachable_latest"]:<14}'
                f'{r["rows_total"]:>8}'
            )
        else:
            print(f'{name:<18}{code:<10}{market:<8}失败: {r["errors"]}')

    print('\n' + '=' * 80)
    print('\n【第二部分】复权类型支持（用 2024-01 一个月数据验证）\n')
    print(f'{"标的":<18}{"代码":<10}{"不复权":<25}{"前复权":<25}{"后复权":<25}')
    print('-' * 100)
    for name, code, market in TARGETS:
        fqt_r = test_fqt_support(code, market)
        print(
            f'{name:<18}{code:<10}'
            f'{fqt_r.get("不复权", "?"):<25}'
            f'{fqt_r.get("前复权", "?"):<25}'
            f'{fqt_r.get("后复权", "?"):<25}'
        )

    # 按市场汇总最深历史
    print('\n' + '=' * 80)
    print('\n【结论】按市场汇总 efinance 最深历史:\n')
    market_depth: dict = {}
    for name, market, r in depth_results:
        if not r['reachable_earliest']:
            continue
        ear = r['reachable_earliest']
        if market not in market_depth or ear < market_depth[market]:
            market_depth[market] = ear

    for market, earliest in sorted(market_depth.items()):
        years = datetime.now().year - int(earliest[:4])
        verdict = '完全够用' if years >= 10 else ('勉强够用' if years >= 5 else '不够')
        print(f'  {market}: 最早 {earliest}, 约 {years} 年历史  → {verdict}')

    print('\n判读指南:')
    print('  - A 股能拉到 2005 之前 → 完全够用，sh_quant 现状（2015 起）已够')
    print('  - 港股能拉到 2015 之前 → 完全够用，可以放弃富途')
    print('  - 港股只能拉 3-5 年   → 半够用，搭配富途校验')
    print('  - 港股只能拉 1 年以内 → 必须上富途')
    print('=' * 80)


if __name__ == '__main__':
    main()
