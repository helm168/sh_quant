"""绩效指标。

第 1 月 - 周 2 待实现：
    total_return, annual_return, max_drawdown, sharpe, calmar, win_rate, trade_count
"""

# TODO(week2): implement metrics
def total_return(equity):
    raise NotImplementedError

def annual_return(equity, periods: int = 252):
    raise NotImplementedError

def max_drawdown(equity):
    raise NotImplementedError

def sharpe(returns, rf: float = 0.0, periods: int = 252):
    raise NotImplementedError

def calmar(equity):
    raise NotImplementedError

def win_rate(returns):
    raise NotImplementedError

def trade_count(signal):
    raise NotImplementedError
