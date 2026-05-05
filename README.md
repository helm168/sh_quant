# sh_quant

个人量化研究工具链 + 6 个月学习路线。**目标**：把"研究新策略"的成本从几小时压到几分钟；衡量标准是「跑通 + 理解」，不是「赚钱」。

## 目录结构

```
sh_quant/
├── notebooks/        # 每个研究问题一个 notebook
├── utils/            # 复用工具（data / backtest / metrics / plot / signals）
├── data_cache/       # 本地 parquet 缓存，不进 git
├── outputs/          # 图、临时 csv、报告产物
├── reports/          # 月报、复盘文档
├── docs/             # 投资方法论、风控规则等长文档
├── requirements.txt
├── setup.sh
├── Makefile
├── ROADMAP.md        # 6 个月路线图（原文）
└── TASKS.md          # 周粒度可勾选 checklist
```

## 快速开始

> 当前所有 `utils/*.py` 都是占位 stub，按 `ROADMAP.md` 的进度逐周实现。

```bash
cd sh_quant
bash setup.sh                # 建 .venv、装依赖、注册 jupyter kernel
source .venv/bin/activate    # 激活
cp .env.example .env         # 填入 TUSHARE_TOKEN
jupyter lab                  # 启动
```

在 notebook 里选 kernel **"Python (sh_quant)"** 即可。

## 路线图概要

| 月 | 主题 | 关键交付 |
|----|------|---------|
| 1 | 工具链建设 | data / backtest / metrics / plot / signals 五个模块；5 行代码完成新策略回测 |
| 2 | 经典策略巡礼 | 8+ 策略回测 + "市场感"总结 |
| 3 | 把自己的方法量化化 | 投资说明书 + 规则量化 + 自我归因 |
| 4 | 风控系统 | 仓位/止损/分散/回撤控制 |
| 5 | 多标的 + 因子 | 横截面、样本外测试（亲历过拟合） |
| 6 | 综合复盘 + 实盘准备 | 综合策略 + 压力测试 + 6 月复盘 |

详见 [ROADMAP.md](ROADMAP.md)，可勾选清单见 [TASKS.md](TASKS.md)。

## 几条原则

1. 每个 notebook 顶部写：研究问题、结论、关键参数。
2. 过程 > 结果。建研究能力，不是找圣杯。
3. 诚实面对失败的回测。失败的研究也是研究。
4. 工具够用就行，有需求再改。
5. 每月写一次月报，放在 `reports/` 下。
6. 周末/晚上 5–10 小时/周，长跑节奏，不冲刺。
