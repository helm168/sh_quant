# sh_quant 项目 — Agent 工作笔记

> 本文件记录长期有效的项目事实 + AI agent 协作约定，避免每次对话重新对齐。
> 事实过期（数据源变化 / 工具流程调整）时人工更新。
>
> Cursor / Aider / Codex CLI 等基于 [agents.md](https://agents.md/) 约定的工具
> 会自动读取本文件。Claude Code / Cowork 通过项目根的 `CLAUDE.md` 里的
> `@AGENTS.md` 引用读取相同内容，无需重复维护。

---

## 项目定位

个人量化研究工具链 + 6 个月学习路线（详见 `ROADMAP.md` / `TASKS.md`）。

- **目标**：把"研究新策略"的成本从几小时压到几分钟。
- **衡量标准**：「跑通 + 理解」，不是「赚钱」。
- **节奏**：周末/晚上 5–10 小时/周，长跑节奏。

## 技术栈与目录约定

```
sh_quant/
├── notebooks/        # 每个研究问题一个 .ipynb（探索 / 实验 / 写结论）
├── utils/            # 复用工具库（data / backtest / metrics / plot / signals）
├── data_cache/       # 本地 parquet 缓存，不进 git
├── outputs/          # 图、临时 csv、报告产物
├── reports/          # 月报、复盘文档
├── docs/             # 投资方法论、风控规则等长文档
├── requirements.txt
├── setup.sh / Makefile
├── ROADMAP.md / TASKS.md
└── AGENTS.md / CLAUDE.md
```

**代码分层（重要）**：

- 稳定 / 可复用的逻辑沉淀进 `utils/*.py`，notebook 里 `from utils.X import Y` 调用。
- 一次性的探索 / 实验 / 画图 / 写结论放在 `notebooks/*.ipynb`。
- 同一段代码在两个 notebook 里出现第二次时，立刻搬进 `utils/`。
- 每个 notebook 顶部的 markdown cell 必写：研究问题 / 关键参数 / 结论。3 个月后回头看不会懵。

## 数据源

- 主要数据源：**Tushare**（基础付费会员）。Token 放在项目根 `.env` 的 `TUSHARE_TOKEN`，**已 gitignore**。
- 不依赖任何 VIP 接口（如 `*_vip`）。如果某些数据拿不到，先用能拿到的近似，并在 notebook 里注明妥协。
- 行情数据本地 parquet 缓存（`data_cache/`），命中读本地、未命中拉接口落盘。详细行为见 `utils/data.py`。
- 缓存文件不进 git（`.gitignore` 已配）；数据是大文件且可重建。

## 环境

- Python ≥ 3.10，项目级 venv (`.venv/`)。
- 一键 bootstrap：`bash setup.sh` → 建 venv + 装依赖 + 注册 Jupyter kernel `Python (sh_quant)`。
- 之后 `source .venv/bin/activate && jupyter lab`，notebook 里选 `Python (sh_quant)` kernel。
- 锁版本：需要 reproduce 时跑 `make freeze` 生成 `requirements.lock.txt`。
- 不要全局装包；不要在 notebook 里 `!pip install`（会污染环境且 CI 不可重放）。

---

## Git 提交约定

### 作者身份

跟随仓库 / 全局 git config，不要在 commit 里硬编码作者。当前 local config：

```
user.name  = helm168
user.email = sunhao_1988@msn.cn
```

### Commit message 格式

按 [Conventional Commits](https://www.conventionalcommits.org/) 写：

```
<type>(<scope>): <subject>
```

| 前缀       | 用途                                 | 示例                                                  |
| ---------- | ------------------------------------ | ----------------------------------------------------- |
| `feat:`    | 新功能 / 新模块                      | `feat(data): implement load_daily with parquet cache` |
| `fix:`     | bug 修复                             | `fix(backtest): handle NaN on first trading day`      |
| `docs:`    | 文档（README / ROADMAP / docs/）     | `docs: add week-1 retro to reports/2025-XX_review.md` |
| `refactor:`| 重构（不改外部行为）                 | `refactor(metrics): split sharpe into helpers`        |
| `chore:`   | 杂项（依赖、配置、.gitignore）       | `chore: bump pyarrow to 16.1`                         |
| `test:`    | 加 / 改测试                          | `test(metrics): cover max_drawdown edge cases`        |
| `perf:`    | 性能优化                             | `perf(data): switch parquet engine to pyarrow`        |

scope 用模块名（`data` / `backtest` / `metrics` / `plot` / `signals` / `notebook` ...），可省。subject 用祈使句、英文小写、不超过 72 字符。

### 敏感文件

提交前 `git status` 自检：**任何 token / api key / .env / 真实账号信息一律不进 git**。`.gitignore` 已覆盖常见情况，但每次 add 之前再扫一眼。

---

## Agent 行为准则

跟 TASKS.md 互补 — TASKS.md 管「单个 task 怎么验证」，这里管「动手前 / 动手中」的态度。偏保守一面，简单任务用判断力别死磕。

### 写代码之前

- **假设要明说**。多个解读都说得通时，先列出来让用户选，不要静默挑一个。
- **听不懂就停**。写出"哪里不清楚"，不要硬猜继续编。
- **更简单的方案要主动提**。用户没问到但你看到，就说。

### 简单优先

- 没要求的"灵活性 / 可配置 / 抽象"一律不写。
- 单次使用的代码不抽接口。
- 不可能发生的错误不要做兜底。
- 200 行能解决的不写 500 行。写完问自己「senior engineer 会觉得过度设计么？」

### 外科手术式改动

- 不要顺手改"看着不顺眼"的相邻代码（formatting / 注释 / 命名）。
- 跟现有风格走，即使你的写法更好。
- 看到不相关的死代码 — 提一句，不要删。
- 自己改动产生的孤儿（新没人引用的 import / 变量）自己擦干净；项目里早就有的死代码，用户不要求就别删。
- 测试：每行 diff 都能直接对应到用户的请求。

### 目标驱动

模糊任务先转成可验证目标：

- 「实现 backtest」→「拿历史已知策略跑一遍，结果跟手算一致（误差 < 0.01%）」
- 「修 bug」→「写 reproduce 测试 + 让它过」
- 「重构 X」→「前后跑同一组 notebook，关键指标完全一致」

多步任务给一份 plan，每步附「验证：<怎么知道做完了>」。强成功条件让 agent 自己迭代；弱条件（"跑通就行"）一定要回头问。单 task 的具体验证 block 见 `TASKS.md`。

### 量化研究特有

- **诚实面对失败的回测**。90% 的策略不行是常态。结果不好就如实写，不要调参 p-hacking 出"好看"的曲线。
- **过拟合警觉**：在样本内调参 → 样本外验证；不要看了测试集再回头改训练集参数。
- **每个策略 5 问**（参考 `ROADMAP.md` 第 2 月）必须在 notebook 里写明确：市场逻辑 / 适合的市场环境 / 弱点 / 手续费敏感度 / 适合的标的。

---

## 当前进度

参见 `TASKS.md` 顶部未勾选的最早一项。Roadmap 里所有 `utils/*.py` 当前都是 `NotImplementedError` 占位，按周逐步实现。
