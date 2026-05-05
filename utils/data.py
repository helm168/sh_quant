"""数据层：行情拉取 + 本地 parquet 缓存。

第 1 月 - 周 1 待实现：
    - load_daily(ts_code, start, end, adj='qfq') -> pd.DataFrame
    - 命中缓存读本地，否则拉接口并落盘
    - trade_date 排序、列名标准化
"""

# TODO(week1): implement load_daily()
def load_daily(ts_code: str, start: str, end: str, adj: str = "qfq"):
    raise NotImplementedError("week1: data layer not implemented yet")
