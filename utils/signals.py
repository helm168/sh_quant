"""信号库。

第 1 月 - 周 4 待实现（至少 5 个）：
    signal_double_ma, signal_bollinger_breakout, signal_bollinger_revert,
    signal_rsi, signal_n_day_high, signal_macd ...
"""

# TODO(week4): implement signals
def signal_double_ma(df, fast: int = 5, slow: int = 20):
    raise NotImplementedError

def signal_bollinger_breakout(df, n: int = 20, k: float = 2.0):
    raise NotImplementedError

def signal_bollinger_revert(df, n: int = 20, k: float = 2.0):
    raise NotImplementedError

def signal_rsi(df, n: int = 14, oversold: int = 30, overbought: int = 70):
    raise NotImplementedError

def signal_n_day_high(df, n: int = 20):
    raise NotImplementedError
