# WealthPilot · 信号消费侧 PRD (v1)

> 状态: Draft · 2026-05-25
> 范围: WealthPilot App 端 — 把 sh_quant 已经产出的「信号」消费起来
> 关联: `docs/signal-engine.md`(sh_quant 信号引擎 PRD), `docs/DATA_SCHEMA.md`
>      (产物物理布局), `~/.market_data/signals/`(数据落点),
>      `config/notify_filter.yaml`(共享过滤器配置)
> 实施: 全在 WealthPilot 仓库 (`/Users/helm/Documents/Code/Billionaire`),
>      sh_quant 这边**零改动**.

---

## 1. 背景

sh_quant 数据层每日收盘后(9:45 / 19:45)跑信号引擎,产出:

```
~/.market_data/signals/cn_latest.json    # CN 当日全量
~/.market_data/signals/cn_2026-05-22.json
~/.market_data/signals/us_latest.json
~/.market_data/signals/hk_latest.json
```

同时 sh_quant `scripts/notify_digest.py` 9:50 / 19:55 跑,**只挑** 「`isNew=true`
且通过用户过滤器 (`config/notify_filter.yaml`) 的信号」**推到飞书**.

**飞书推送是"主动报警"** —— 用户被动接收最重要的几条.
**WealthPilot 是"详情站"** —— 用户想看全貌、调过滤器、翻历史的地方.

两边**共用同一份过滤器** (`~/.market_data/notify_filter.yaml`,详见 §3.3).
WealthPilot 端做的是这个 yaml 的可视化编辑器 + 历史浏览器,**不要在 WP
端重复实现过滤逻辑**——除了即时预览以外.

---

## 2. 目标 / 非目标

### 目标 (v1)

1. 用户在 WealthPilot 里能看到「**信号**」tab,汇总今日 / 过去 N 天的信号.
2. 用户能 fill 自己的**持仓股**列表 → 写回 `notify_filter.yaml` 的 `portfolio` 字段
   → 下次 sh_quant 跑 `notify_digest.py` 就会按新 portfolio 过滤推送.
3. 用户能在 UI 里勾选**机会信号 universe** (themes.yaml 里的若干 theme_id) →
   同样写回 yaml.
4. 用户能勾选**大盘/板块 risk 是否推送**、**个股 risk 是否仅看持仓**.
5. 离线优先 —— 断网仍能展示最新一次产物 (跟既有 `/api/local/*` 通路一致).

### 非目标 (v1 明确不做)

- ❌ 不重新实现信号计算 —— 全部来自 `signals/*.json`.
- ❌ 不重新实现过滤逻辑 —— sh_quant `notify_digest.py:_passes_user_filter()`
   是单一真相;WP 端如果要"即时预览过滤效果",也只是按同套规则计算,**别**塞进
   重新打分.
- ❌ 不做盘中实时刷新 —— 跟 sh_quant 收盘批处理节奏一致,一天 2 次.
- ❌ 不做用户自定义信号规则 (v2,见 PRD §10).
- ❌ 不接入推送 —— 推送由 sh_quant 飞书 bot 负责.

---

## 3. 数据契约 (sh_quant 已实现,WP 只读 / 只写)

### 3.1 信号 JSON (sh_quant 产, WP 读)

物理路径 (跟 macro / events / sector_history 完全同款):

```
~/.market_data/signals/<market>_latest.json     # WP 默认读这个
~/.market_data/signals/<market>_<YYYY-MM-DD>.json   # 按日存档, 翻历史用
```

`market ∈ {cn, us, hk}`. 文件 schema:

```jsonc
{
  "market": "CN",
  "asOfDate": "2026-05-22",
  "generatedAt": "2026-05-22T19:45:32+08:00",
  "universeSize": 2042,        // 当次扫描覆盖的票数 (透明度)
  "signals": [
    {
      "id": "CN-2026-05-22-STK_BREAKDOWN-603919.SH",  // 跨日稳定
      "type": "STK_BREAKDOWN",  // 信号代号, 见 sh_quant signal-engine PRD §5
      "scope": "stock",         // market | sector | stock
      "level": "risk",          // risk | watch | opportunity
      "severity": 74,           // 0-100, 同 level 内排序
      "subject": {
        "kind": "stock",
        "id": "603919.SH",      // company_id (sh_quant 风格 ts_code)
        "ticker": "603919",
        "name": "金徽酒"
      },
      "title": "放量跌破 MA250 (年线)",
      "detail": "收盘跌破 250 日均线 且当日放量 (1.87x), 长期趋势走弱.",
      "metrics": {              // sh_quant 已算好的数字, UI 直接渲染
        "maWindow": 250,
        "latestClose": 19.25,
        "latestMa": 19.65204,
        "volRatio": 1.8747776
      },
      "firstTriggeredDate": "2026-05-22",  // 边沿触发: 信号首次达成的日子
      "isNew": false            // 今天是否首次触发 (vs 持续触发)
    }
  ]
}
```

字段约定:
- `signals` 已经按 `level` (risk → watch → opportunity) + 同 level 内
  `severity` 降序排好,**WP 端不要重排**;要重排可以,但请保留二级 key.
- `id` 跨日稳定 —— 持续触发的信号 id 不变,基于 `firstTriggeredDate`
  + `subject.id`. WP 用 `id` 做去重 / 跳转锚点.
- `metrics` 是给 UI 直接展示的事实数字,语义见 `signal-engine.md` §5
  每条信号的字段表.
- `subject.id` 是 sh_quant 风格 `ts_code` (`<code>.<MARKET>`,如 `603919.SH` /
  `NVDA.US`). WP 内部 ticker 格式如果是 `SH603919` 之类, 需 reverse-map.
  跟 events-parity 同款约定 (见 `Billionaire` 老 memory).

### 3.2 历史 / 多日数据

WP「信号」翻历史 = 读 `signals/<market>_<YYYY-MM-DD>.json`,文件名就是当日日期.
保留多久取决于 sh_quant 是否轮转(目前**不轮转**, 每日产一份 + latest);WP 端不
需要管轮转.

### 3.3 过滤器 yaml (WP 写, sh_quant 读)

物理路径:`~/.market_data/notify_filter.yaml` (或 sh_quant repo 内
`config/notify_filter.yaml` 的副本,**沿用 `_data_root()` 通路最干净**).

⚠️ **路径决议** (上线前 sh_quant 侧需配合):
- 目前 sh_quant `scripts/notify_digest.py` 读的是 `<sh_quant>/config/notify_filter.yaml`
- WP 写到 `~/.market_data/notify_filter.yaml` 更符合 "WP 是消费者 / sh_quant
  仓库不存用户配置" 的分工
- 上线前 sh_quant 改成读 `~/.market_data/notify_filter.yaml`,
  `<sh_quant>/config/notify_filter.yaml` 作为 fallback 模板. **一次性改 5 行**.

Schema:

```yaml
# 用户的持仓股 ts_code 列表 (sh_quant 风格, .SH/.SZ/.HK/.US 后缀)
portfolio:
  - 600519.SH
  - 603986.SH
  - NVDA.US

# 哪些主题里捞机会信号. theme_id 来自 sh_quant config/themes.yaml.
# WP 端如果要做编辑器, 需要先调 sh_quant 提供的 themes 元数据 API
# (sh_quant 这边稍后会加一个 /api/local/themes 列出 theme_id + 名字 + 成员数,
#  跟 macroPanel.config.ts 的模式同款 —— 这是阶段二的事, 先用硬编码列表)
opportunity_themes:
  - ai_compute
  - semi_equipment

# 大盘 / 板块级 risk 信号是否推 (默认 false: 大盘风险面太宽泛)
push_market_risk: false

# 个股 risk 是否只看持仓 (默认 true: 不在持仓的 risk 跟你没切身关系)
risk_portfolio_only: true
```

**字段都是可选** —— 缺字段 = 走默认值. WP 写 yaml 时**只动用户改过的字段**,
不要把所有字段都 dump 一遍(避免改一个动一片).

### 3.4 sh_quant 暴露的元数据 (阶段二,本 PRD 不阻塞)

为让 WP 端 theme 选择器有"主题 id → 显示名 → 成员数"的下拉,sh_quant 后续
加 `/api/local/themes` 或导出 `~/.market_data/themes/_meta.json`. **v1 阶段
WP 直接硬编码 `ai_compute / semi_equipment / ...` 的英文 id 当占位即可**,
能跑通就行,显示名后做.

---

## 4. WP 端 — 要做的 4 件事

### 4.1 「持仓」按钮 (收藏 / Watchlist 功能旁)

- 在现有「收藏」按钮旁加一个「**持仓**」按钮(同样的样式,不同的颜色 / 图标).
- 状态存 zustand + dexie (跟收藏同套).
- 用户点「加入持仓」→ 该股票 `ts_code` 加进本地 portfolio set.
- **持仓变更立即同步写回 yaml** (见 4.2).
- 视觉上区分:收藏 = 关注,持仓 = 真金白银. 收藏 ≠ 持仓,**互不影响**.

### 4.2 写回 yaml 的 middleware route

新增 `PUT /api/local/notify-filter` —— 接收 partial yaml, merge 到现有
`~/.market_data/notify_filter.yaml`. 用 `js-yaml` (已有依赖) 读写.

```typescript
// 请求体 (partial — 只传要改的字段)
{
  portfolio?: string[];
  opportunity_themes?: string[];
  push_market_risk?: boolean;
  risk_portfolio_only?: boolean;
}

// 响应
{ success: true, updatedFields: ['portfolio'] }
```

并发安全:**先读 → 改字段 → 写回**,文件锁可以用 `fs.writeFileSync({ flag: 'wx' })`
+ retry,或者干脆懒一点用 atomic rename. 单用户单实例,冲突基本不会发生.

注意:
- 留空数组 (`portfolio: []`) **不要**当成"删除字段",要老老实实写 `[]` 进 yaml.
- yaml 顶部的注释 (`# ── portfolio: ...`) 想保留就用 `yaml.parseDocument`
  保 AST,不想保用 `yaml.parse + yaml.stringify` 也行(注释丢了,sh_quant 这边
  会重新加).

### 4.3 「信号」tab

- 在 `ScreenerTabBar` 加「信号」tab (跟 趋势 / 价值 / 事件 / 云图 并列).
  这一个 tab 同时承担三件事:**当日汇总展示** + **历史翻看** + **过滤器编辑入口**.
  (老版 PRD 把"信号 tab"和"消息中心"分成两个, 现已合并 —— 用户视角它们是
  同一件事.)

- 布局 (粗草图):

```
┌─[ 信号 ]─────────────────────────────────────────┐
│ asOf 2026-05-22 · CN 92 · US 27 · HK 17 · 新触发 3   │
│ [⚙️ 设置过滤器]  [📅 历史]  [👁 全部 / 只看新]         │
├──────────────────────────────────────────────────────┤
│ 🔴 风险 (3)                                          │
│ ┌──────────────────────────────────────────────────┐ │
│ │ [NEW] 603919.SH 金徽酒 · 放量跌破 MA250 (年线)    │ │
│ │       severity 74 · 收盘 19.25 距 MA250 -2.0%    │ │
│ │       2026-05-22 触发  [跳详情 →]                │ │
│ └──────────────────────────────────────────────────┘ │
│ ...                                                  │
│ 🟡 关注 (21) ▶ 展开                                  │
│ 🟢 机会 (68) ▶ 展开                                  │
└──────────────────────────────────────────────────────┘
```

- 每条卡片:level 色条 + title + detail + 关键 metrics 数字 + `isNew` 角标 +
  点击 → `onSelectCompany(subject.id)` 打开右侧详情(已有通路).
- 同一只票多信号 → 合并成一张卡片 (`subject.id` 一致, 卡内列多条).
- **默认按 portfolio + opportunity_themes 过滤**;顶部 toggle「全部 / 只看新」
  切换. 即时预览过滤后的条数.
- 空态:"今日无新触发信号" (是好事, 不是错误).
- staleness:`asOfDate` 落后于当前日 (节假日除外) 时给提示,复用云图
  `cloudDateLag` 思路.

### 4.4 过滤器编辑器 (modal / drawer)

`[⚙️ 设置过滤器]` 点开:

1. **Portfolio** 编辑 (4.1 的列表展示,可删除单个 ticker)
2. **Opportunity themes** 多选 checkbox (v1 硬编码 `ai_compute / semi_equipment /
   ai_data_center_power / ev_battery / ...`,从 themes.yaml 摘要)
3. 两个 toggle:`push_market_risk` / `risk_portfolio_only`
4. 「保存」→ `PUT /api/local/notify-filter`,提示"下次 sh_quant 跑批 (19:45)
   后生效"

---

## 5. 数据通路 (Repository 三层)

跟既有 `PriceRepository` / `EventRepository` 完全同构:

```
useRepositories() {
  ...
  signals: new SignalRepository({
    memory: signalsCache,        // Map<market, payload>, TTL 5 min
    local: signalsLocalProvider, // GET /api/local/signals?market=CN[&date=2026-05-21]
    remote: null,                // v1 不做远端兜底
  }),
  notifyFilter: new NotifyFilterRepository({
    memory: filterCache,
    local: filterLocalProvider,  // GET/PUT /api/local/notify-filter
    remote: null,
  }),
}
```

新增 middleware:
- `GET  /api/local/signals?market=<cn|us|hk>[&date=YYYY-MM-DD]` — 读单日 JSON
- `GET  /api/local/signals/history?market=<m>&days=<N>` — 列过去 N 天的产物
  汇总(也可以 client 端连续调上面那个,首选不做 history endpoint,YAGNI)
- `GET  /api/local/notify-filter` — 读当前 yaml
- `PUT  /api/local/notify-filter` — 写回 partial yaml

---

## 6. 验收

1. **数据通路**:三市场 `signals/<m>_latest.json` 任意编辑后, WP 「信号」
   tab 刷新可见.
2. **持仓往返**:UI 点「加入持仓」→ yaml 出现该 ts_code → 下次 sh_quant
   `python scripts/notify_digest.py` 跑,该股票的 risk 信号会被推到飞书
   (可手动构造一条 STK_BREAKDOWN 验证).
3. **theme 多选生效**:勾掉 `semi_equipment` → 该主题内 stock 的 opportunity
   信号在 WP 列表 + 飞书都不再出现.
4. **本地优先**:`local` 模式断网, tab 仍能展示最新一次产物.
5. **纯展示**:WP 端代码无任何 signal 阈值或计算逻辑.`grep` 一下 `severity`
   / `MA250` / `streakDays` 应当只有展示代码 (字符串 / 取值), 没有判断逻辑.
6. **合规**:产物 + UI 全文无买卖建议措辞.
7. `tsc` + `vitest` 全绿;`pnpm dev` 三市场 + 信号 + 过滤器编辑器 smoke 通过.

---

## 7. 不在范围 / v2 候选

- 多用户(目前单用户单机,yaml 文件级隔离够用)
- WP 端推送 (锁屏 / web push) —— 飞书 bot 已经解决
- 信号订阅粒度细到单条信号类型 (v2,跟 yaml 加 `subscribed_types: [...]`)
- 用户自定义信号规则 / 阈值
- 信号回测面板 (sh_quant 那边的 v2,跟 WP 关系不大)

---

## 8. 关键假设 (实施前请确认)

1. **WP 已有 `/api/local/*` middleware 框架**,且能跑 Node fs.读写 yaml.
2. **WP 已有 `onSelectCompany(ts_code)` 通路**(events tab 已经在用).
3. **持仓 ≠ 收藏** —— 是新增功能,不复用收藏的状态.
4. **portfolio 是 ts_code 列表**(sh_quant 风格,`.SH` / `.SZ` / `.HK` / `.US`),
   不是 WP 内部 ticker 格式.WP 端做格式转换 (跟 events-parity 同款方式).

---

## 9. 联调清单

- [ ] sh_quant 改 `notify_digest.py` 读 `~/.market_data/notify_filter.yaml`
  (fallback `<repo>/config/notify_filter.yaml`),一次性 5 行改动.
- [ ] WP 端实现 §4.1-§4.4.
- [ ] 双方对一遍 ts_code 后缀约定 (§3.1 最后一段),避免又出 `SH600519` vs
  `600519.SH` 的格式 mismatch (events-parity 已踩过坑).
- [ ] 真实数据下端到端跑一遍:用户 UI 加持仓 → 改 yaml → 等 sh_quant 下个
  19:45 cron → 飞书出卡片. 拍照留证.
