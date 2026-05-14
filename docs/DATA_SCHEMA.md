# Market Data Schema

> 三个项目（sh_quant / TradingAgents / Billionaire）共享的数据底座约定。
>
> **数据所有者**：sh_quant（pull / 增量更新 / 复权计算）。
> **消费者**：TradingAgents（agent 分析）、Billionaire（前端展示）、sh_quant 自己（研究）。
>
> 物理位置：本仓库 `data_cache/` （目前），未来迁移到 `~/.market_data/` 作为跨项目共享目录（详见 §6）。

---

## 1. 顶层目录结构

```
~/.market_data/                  # 或本项目内 data_cache/ 软链到这里
├── stocks/                      # 股票日线（覆盖 A 股 / 港股 / 美股）
│   ├── 600519.SH.parquet        # A 股沪市
│   ├── 000001.SZ.parquet        # A 股深市
│   ├── 688981.SH.parquet        # 科创板（仍是 .SH 后缀）
│   ├── 300750.SZ.parquet        # 创业板（仍是 .SZ 后缀）
│   ├── 830799.BJ.parquet        # 北交所
│   ├── 00700.HK.parquet         # 港股，代码 5 位补零
│   ├── 00981.HK.parquet
│   └── NVDA.US.parquet          # 美股，字母代码 + .US
├── etf/                         # 场内基金（ETF / LOF）日线
│   ├── _etfs.parquet            # 元数据（管理人 / 上市日 / 状态）
│   ├── 510300.SH.parquet
│   └── 159915.SZ.parquet
├── sw_l1/                       # 申万一级行业 31 个
│   ├── _industries.parquet      # 元数据（行业代码 → 名称 → parent）
│   └── 801010.SI.parquet
├── sw_l2/                       # 申万二级行业 124 个
│   ├── _industries.parquet
│   └── 801011.SI.parquet
├── universe/                    # ticker 全集元数据（新增）
│   ├── cn_a.parquet             # 当前可交易 A 股清单
│   ├── cn_hk.parquet            # 港股清单
│   ├── us.parquet               # 美股清单
│   └── delisted.parquet         # 退市股清单（解 survivorship bias）
└── DATA_SCHEMA.md               # 本文件
```

### 设计要点

**所有文件用 `<ts_code>.parquet` 命名**——`ts_code` = `<本地代码>.<市场后缀>`，沿用 Tushare 风格，扩展到港股 / 美股。这样不分子目录（`stocks/` 一层平铺），但每个文件名自带市场标识，永远不会冲突。

**adj_factor 和 OHLC 同表存**——不分开放 `adj/` 目录。复权由读取层按需计算（`utils/data.py:load_daily(adj='qfq'|'hfq'|None)`），缓存只存原始事实。

**主题 / 行业分类元数据走 yaml**——`config/themes.yaml` 是单源真相，不冗余存 parquet。

---

## 2. ts_code 后缀约定

| 后缀 | 市场 | 代码格式 | 示例 |
|---|---|---|---|
| `.SH` | 上交所主板 / 科创板 | 6 位数字（600/601/603/605/688） | `600519.SH` |
| `.SZ` | 深交所主板 / 中小板 / 创业板 | 6 位数字（000/001/002/003/300） | `000001.SZ` |
| `.BJ` | 北交所 | 6 位数字（4/8 开头） | `830799.BJ` |
| `.HK` | 港交所 | 5 位数字（补前导零） | `00700.HK` |
| `.US` | 美股（NYSE / NASDAQ / AMEX 不区分） | 大写字母，连字符保留 | `NVDA.US`、`BRK-B.US` |
| `.SI` | 申万行业指数 | 6 位数字（801/8501 开头） | `801010.SI` |

不区分 NYSE / NASDAQ 是因为研究/回测层面用不到——需要交易所信息时去 `universe/us.parquet` 查 `exchange` 列即可。

### 格式转换辅助函数

不同下游需要不同格式。读取层提供 `ticker_format()`：

```python
# utils/ticker.py（待实现）
def ticker_format(ts_code: str, fmt: str) -> str:
    """
    输入 SH_Quant 风格 ts_code（如 '600519.SH'），输出指定格式。

    fmt:
      'sh_quant'  → 原样 '600519.SH'                  （本项目缓存的命名）
      'yfinance'  → '600519.SS'（注意 SH → SS）/ '0700.HK'（注意去前导零）/ 'NVDA'
      'efinance'  → '600519' / '00700' / 'NVDA'        （efinance 用裸码）
      'tushare'   → '600519.SH' / '00700.HK' / 'NVDA.US'  （和 sh_quant 一致）
      'display'   → '600519 (SH)' / '00700 (HK)'      （展示）
    """
```

---

## 3. parquet 列约定

### 3.1 个股 / 行业 / ETF 日线（通用列）

| 列名 | 类型 | 含义 | 必有？ |
|---|---|---|---|
| `trade_date` | `pd.Timestamp` | 交易日 | ✓ 必有 |
| `ts_code` | `str` | 见 §2 | ✓ 必有 |
| `open` | `float` | 不复权开盘价 | ✓ 必有 |
| `high` | `float` | 不复权最高 | ✓ 必有 |
| `low` | `float` | 不复权最低 | ✓ 必有 |
| `close` | `float` | 不复权收盘 | ✓ 必有 |
| `pre_close` | `float` | 前收盘 | 可选 |
| `change` | `float` | 涨跌额 | 可选 |
| `pct_chg` | `float` | 涨跌幅（百分点，不是小数：`5.0` 而非 `0.05`） | 可选 |
| `vol` | `float` | 成交量。A 股单位"手"，港股/美股单位"股" | ✓ 必有 |
| `amount` | `float` | 成交额。A 股单位"千元"，港股 HKD 千元，美股 USD 千元 | 可选 |
| `adj_factor` | `float` | 复权因子（不复权时为 1.0） | ✓ 必有（个股/ETF） |

**关键不变量**：
- `open ≤ high`、`low ≤ close ≤ high`、`open / high / low / close > 0`
- `trade_date` 单调递增，无重复
- `adj_factor` 单调非降（前低后高，分红/拆股事件后跳变）
- 行业指数 `sw_l1/*.parquet` 不需要 `adj_factor`（指数本身无复权）

### 3.2 universe 元数据列（`universe/*.parquet`）

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | `str` | 主键 |
| `name` | `str` | 中文/英文名 |
| `market` | `str` | `cn_a` / `cn_hk` / `us` / `cn_bj` |
| `exchange` | `str` | 具体交易所，如 `SSE` / `SZSE` / `BSE` / `HKEX` / `NYSE` / `NASDAQ` |
| `list_date` | `pd.Timestamp` | 上市日 |
| `delist_date` | `pd.Timestamp \| NaT` | 退市日（NaT = 仍在交易） |
| `status` | `str` | `L` 上市 / `D` 退市 / `P` 暂停 |
| `industry_l1` | `str \| None` | 申万一级行业代码（如 `801010.SI`），仅 A 股 |
| `industry_l2` | `str \| None` | 申万二级行业代码 |
| `currency` | `str` | `CNY` / `HKD` / `USD` |

### 3.3 退市股清单（`universe/delisted.parquet`）

和 `universe/cn_a.parquet` 同结构，但只装 `status='D'` 的退市股。同时它们的历史日线写入 `stocks/<ts_code>.parquet`（同主目录，靠 `status` 字段区分）。

### 3.4 ⚠️ 成交量与成交额单位陷阱（消费者必读，踩过多次）

**核心结论：永远不要靠 `vol` 算成交额，永远用 `amount` 字段。**

#### 为什么 `vol` 的"手"会骗人

"手"是 vendor-specific 的概念，**不同数据源叫"手"但语义不一定相同**：

| 数据源 | "手" 的定义 | 说明 |
|--------|------------|------|
| **Tushare daily** | **1 手 = 100 股** | parquet 里 `vol` 字段就是这个口径 |
| 富途 | 1 手 = 1 股 | 它界面"343.55 万手"实际指 343.55 万股 |
| 东方财富 | 各接口不一致 | 部分给"股"部分给"手" |
| 交易所规则 | 普通 A 股二级 1 手 = 100 股 | 跟 vendor 单位不一定对齐 |
| 科创板申购 | 1 签 = 200 股 | 申购规则，跟二级交易"手"无关 |

同一份成交量数据，每家把内部约定都叫"手"，但语义不同——所以 `close × vol × N` 这种算法**N 取多少都可能错**。

#### 实测数据（688498.SH 源杰科技 2026-05-13）

| 算法 | 数值 | 跟公开成交额比 |
|------|------|----------------|
| `amount × 1000`（千元→元） | **57.89 亿** | ✓ ratio 1.0000 |
| `close × vol × 100`（假设 1手=100股） | 58.68 亿 | 略偏 1.4%（close 不是 VWAP） |
| `close × vol × 200`（误传"科创板 1手=200股"） | 117.36 亿 | ✗ 翻倍 |
| `close × vol`（忘了乘） | 0.59 亿 | ✗ 错 100× |

**`amount × 1000` 永远是 vendor 直供的真实成交金额**，跟一手几股完全无关，准确度 100%。

#### 消费者代码规则

```python
# ❌ 千万别这么算成交额 — 跟"手"概念耦合, 不同源结果不同
turnover = close * vol * 100    # 你以为 1手=100 但 vendor 可能不是

# ✓ 正确做法 — 用 amount 字段, ×1000 转成元
turnover = amount * 1000        # A 股 / HKD / USD 一律千元单位

# ✓ 美股 / 港股 走 vendor 直接给的成交额字段时可省 ×1000
#   (各 vendor 单位有差, 入库时统一规范化到"千元")
```

#### 老的隐式约定（重要历史背景）

`Billionaire/src/services/tushare/tushareApi.ts` 老代码 line 563-564, 673-674 早就处理过：

```typescript
volume: (r.vol ?? 0) * 100,                                  // vol '手' × 100 转'股'
turnover: r.amount != null ? r.amount * 1_000 : undefined,   // amount '千元' × 1000 转'元'
```

**所以 `PriceSeries.volume` 在 remote 通道是"股"单位**。Local 通道（middleware 直读 parquet）必须做同样的 `× 100` 转换才能保持约定一致。

不做转换的话，下游 `computeAvgTurnover20` 算出来 ADV 少 100x，1亿+ 等流动性 filter 把高价/高成交股票（茅台、大族激光、宁德）全部错杀。这个 bug 2026-05-14 才被发现并修复。

---

## 4. 复权计算（消费者必读）

缓存里**永远存不复权价 + adj_factor**。前复权 / 后复权由读取层即时计算：

```python
# utils/data.py（已存在，按本约定实现 / 维护）
def load_daily(ts_code: str, adj: str | None = None) -> pd.DataFrame:
    """
    Args:
        adj: None → 返回 raw OHLC（不复权）
             'qfq' → 前复权（以最近一日为基准，研究/画图首选）
             'hfq' → 后复权（以最早一日为基准，回测首选，可保 reproducibility）

    Returns:
        DataFrame with columns ['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        如果 adj 不为 None，OHLC 已经被替换为对应复权后的值。
    """
    df = pd.read_parquet(_path(ts_code))
    if adj is None:
        return df

    af = df['adj_factor']
    base = af.iloc[-1] if adj == 'qfq' else af.iloc[0]
    factor = af / base
    for col in ('open', 'high', 'low', 'close'):
        df[col] = df[col] * factor
    return df
```

**为什么不缓存前复权**：qfq 的基准是"最后一日"，每次新增交易日基准都漂移，缓存的历史前复权价会随时间变。回测如果直接读缓存的 qfq close，**不同时间跑出来的结果不一致**，无法 reproducibility。

**为什么 hfq 也不缓存**：虽然 hfq 是稳定的（基准最早一日），但既然存 raw + factor 已经够用，多缓存一份只是浪费空间，且要双重更新。

---

## 5. 增量更新规则

由 `scripts/update_daily.py` 维护，每天收盘后跑一次：

**回退窗口**：每只 ticker **拉最近 7 个交易日**（不是 1 天），按 `trade_date` 去重合并后写回。理由：跨周末/节假日不会漏、源头偶尔回填修正能被捕获、跑批失败重试自动覆盖。

**幂等**：重复跑同一只股票同一天结果完全相同（依赖 `drop_duplicates('trade_date', keep='last')`）。

**adj_factor 更新**：分红/拆股事件触发时，源头会返回**全段历史**的新 adj_factor 序列。增量合并时**用新值覆盖老值**（`keep='last'` 自然实现）。

**港股 / 美股扩展**：efinance 作为主数据源（不限速、无 token），Tushare 仍是 A 股主力。详细 vendor 选择参见 `scripts/update_daily.py` 实现。

---

## 6. 跨项目使用

### 6.1 物理位置

**短期**（现在）：`<sh_quant>/data_cache/`
**长期**（执行 Task 15 之后）：实体在 `~/.market_data/`，原位置软链：
```bash
~/.market_data/
└── (实际数据)
<sh_quant>/data_cache  →  ~/.market_data  (软链)
```
其他项目读取时直接走 `~/.market_data/`，sh_quant 自己保持 `data_cache/` 路径不变（notebook 代码不用动）。

### 6.2 环境变量

读取层统一支持 `SH_QUANT_DATA_DIR` 环境变量（默认 `~/.market_data`）：

```python
# sh_quant/config/config.py
DATA_DIR = Path(os.getenv('SH_QUANT_DATA_DIR', '~/.market_data')).expanduser()
```

```python
# tradingagents/dataflows/efinance_stock.py（Task 16 待加）
LOCAL_DATA_DIR = Path(os.getenv('SH_QUANT_DATA_DIR', '~/.market_data')).expanduser()
```

### 6.3 Python 项目读取

```python
import pandas as pd
from pathlib import Path

DATA = Path('~/.market_data').expanduser()

# 读 A 股
df = pd.read_parquet(DATA / 'stocks' / '600519.SH.parquet')

# 读港股
df = pd.read_parquet(DATA / 'stocks' / '00981.HK.parquet')

# 读所有 ETF 元数据
etfs = pd.read_parquet(DATA / 'etf' / '_etfs.parquet')

# 读申万一级行业全集
l1 = pd.read_parquet(DATA / 'sw_l1' / '_industries.parquet')
```

### 6.4 Node 项目读取（Billionaire）

Node 读 parquet 推荐 [`parquetjs-lite`](https://www.npmjs.com/package/parquetjs-lite) 或 [`apache-arrow`](https://www.npmjs.com/package/apache-arrow)。或者由 sh_quant 提供一个 `scripts/export_json.py` 把指定子集导出成 JSON：

```bash
# 给 Billionaire 用
python scripts/export_json.py --tickers 600519.SH,000001.SZ --out billionaire/data/quotes.json
```

### 6.5 写入权限

**只有 sh_quant 写**——其他项目永远只读。任何项目想新增数据，都要通过 sh_quant 的 pull 脚本进入。这避免了多写者冲突，也让数据来源可追溯。

---

## 7. 版本与演进

- 本 schema 视为 **v1**。未来如果改字段需要：
  - 加新列 → minor 改动，向后兼容
  - 改列名 / 改语义 → major 改动，需要更新本文件版本号并通知所有 consumer
- 任何 schema 变更先改本文件，再改实现。**文档先行**。

---

## 8. FAQ

**Q：为什么不直接用 Tushare 的原生 ts_code 命名，而要扩展？**
A：本来就是用 Tushare 原生命名（`<code>.SH/.SZ/.SI`），只是扩展了 `.HK/.US/.BJ` 三个后缀让多市场统一处理。

**Q：为什么 adj_factor 和 OHLC 同表？**
A：单文件 IO 更快、读取无需 merge、原子写入。代价是 adj_factor 每行重复存一遍（200KB 文件里大约多占 20KB），完全可以接受。

**Q：港股代码到底是 4 位还是 5 位？**
A：港交所官方是 **5 位补零**（`00700`）。yfinance 用 4 位（`0700.HK`），efinance 用 5 位（`00700`）。本项目**文件名用 5 位**，调用 yfinance 时由 `ticker_format()` 转 4 位。

**Q：美股要区分 NYSE / NASDAQ 吗？**
A：文件名层不区分（都是 `.US`）。需要交易所信息时查 `universe/us.parquet` 的 `exchange` 列。

**Q：能存分钟线/tick 吗？**
A：本 schema **仅日线**。分钟线/tick 量级完全不同（单股可能 GB 级），需要单独设计分区数据集（`stocks_min/600519.SH/year=2026/data.parquet`），届时再起 schema v2。
