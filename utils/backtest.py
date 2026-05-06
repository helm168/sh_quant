"""回测引擎：输入 df + signal 列，输出净值/回撤/交易/指标。

第 1 月 - 周 2 待实现：
    - backtest(df, signal_col='signal', price_col='close') -> dict
    - 处理 NaN、首日、最后一日的边界
    - 同时计算 strategy 与 benchmark
"""


# TODO(week2): implement backtest()
def backtest(df, signal_col: str = 'signal', price_col: str = 'close') -> dict:
    raise NotImplementedError('week2: backtest engine not implemented yet')
