# EVI Strategy 模板 — 实现说明

> 这是 EVI 估值策略模板的**端到端实现文档**，覆盖前端 + 后端 + skill 三层。
> 模板系统通用能力请看 [`docs/template-system.md`](../../../../../../docs/template-system.md)。

## 1. 定位

EVI（Evolving Valuation Intelligence）= 持久化的公司分析 + 估值跟踪系统。

一个 entry = 一家公司，提供：

- 一次性的产业调研 + 4 维定性分析 + 多方法估值（SOTP / 整体）
- 持续的监控 automation（财报 / 公告 / 竞品事件）
- 用户对话指出问题时的增量重估闭环（修改材料 → 重算 → 写 changelog → 刷看板）

当前版本：**v3.7.0**

---

## 2. 三层架构

```
┌─ Frontend ───────────────────────────────────────────────────┐
│  web/src/pages/Templates/evi/                                 │
│    ├── EviDashboard.tsx     总览（每行一家公司）              │
│    └── EviReportPanel.tsx   单个 entry 的 Tab 视图            │
│                             ↑ 读 entry.payload + entry.summary │
└──────────────────────────────────────────────────────────────┘
                              ↑ REST API
┌─ Backend ────────────────────────────────────────────────────┐
│  src/server/templates/manifests/evi_strategy.py               │
│    ├── _EVI_PROMPT                  初始 user message         │
│    ├── _EVI_AGENT_MD                注入 agent.md             │
│    ├── _EVI_SKILL_WHITELIST         38 个 skill               │
│    ├── _EVI_EXPECTED_FILES          10 项产物（4 req + 6 opt）│
│    ├── _EVI_FINALIZE                FinalizeSpec              │
│    └── release_notes                3.1.0 → 3.7.0             │
└──────────────────────────────────────────────────────────────┘
                              ↑ 框架自动调
┌─ Sandbox (skills) ───────────────────────────────────────────┐
│  skills/evi-*/                                                │
│    ├── evi-data-orchestrator    产业调研编排                  │
│    ├── evi-quality-analysis     4 维定性分析                  │
│    ├── evi-valuation-analysis   估值总控                      │
│    ├── evi-valuation-{dcf,ps,peg,ddm,comps}                   │
│    ├── evi-toolkit              共享脚本（fetch / persist）   │
│    └── ... (共 19 个 evi-* skill)                             │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. 后端配置（evi_strategy.py）

### 3.1 白名单（38 个 skill）

参见 [`src/server/templates/manifests/evi_strategy.py`](../../../../../../src/server/templates/manifests/evi_strategy.py) 的 `_EVI_SKILL_WHITELIST`：

| 分组 | 数量 | 说明 |
|---|---|---|
| EVI 自有（`evi-*`） | 19 | 模板本身的实现，全保留 |
| 通用基础设施 | 10 | `pdf` / `docx` / `xlsx` / `pptx` / `web-scraping` / `automation` / `inline-widget` / `interactive-dashboard` / `user-profile` / `self-improve` |
| 通用分析互补能力 | 9 | `competitive-analysis` / `sector-overview` / `earnings-analysis` / `earnings-preview` / `catalyst-calendar` / `morning-note` / `thesis-tracker` / `idea-generation` / `x-api` |

**排除的 skill（与 evi-\* 实现冲突）**：

| 排除 | 因为 EVI 有 |
|---|---|
| `dcf-model` | `evi-valuation-dcf` |
| `comps-analysis` | `evi-valuation-comps` |
| `3-statements` | EVI 走 FMP 直接取数 |
| `check-model` / `check-deck` | EVI 是自产，不审查别人 |
| `model-update` | `evi-revaluation-updater` |
| `initiating-coverage` | `reports/final.md` 即首次覆盖报告 |

### 3.2 Finalize 清单（10 项产物）

| rel_path | 级别 | 缺失行为 |
|---|---|---|
| `facets.json` | required | 阻塞，回喂消息要求 Agent 补 |
| `reports/final.md` | required | 阻塞 |
| `reports/changelog.md` | required | 阻塞 |
| `base/CHECKLIST.json` | required | 阻塞 |
| `quality.json` | optional | 写空骨架 placeholder |
| `reports/quality.md` | optional | 写"本次未生成"placeholder |
| `reports/company_overview.md` | optional | placeholder |
| `reports/reverse_valuation.md` | optional | placeholder |
| `information/indexed_facts.json` | optional | 写 `{"facts":[]}` |
| `business_segments.json` | optional | 写 single_segment 空骨架 |

**持久化脚本**：`.agents/skills/evi-toolkit/scripts/persist_evi_report.py`

**重试策略**：缺 required 时最多让 Agent 再跑 1 轮（`max_retries=1`），用尽后 entry 置 `failed`。

### 3.3 触发时机

Agent 跑完 generator drain 后，`TemplateOrchestrator._run_agent` 自动调框架 finalize runner。**Agent / SKILL.md / prompt 都不再要求手动调持久化脚本**。

## 4. 前端实现

### 4.1 EviDashboard.tsx — 总览

每行一个 entry，列展示：

- 公司名 + 股票代码
- 估值结论：base 价 + upside% + 判断标签（低估 / 合理 / 高估）
- 分部数（`n_segments`）
- 监控开放任务数（`monitor_open_tasks`）
- 数据质量徽章（`checklist_overall`：ok / partial / blocked）
- 状态徽章（来自框架的 `entry.status`）
- 操作：打开聊天 / 重跑 / 删除

**数据源**：`entry.summary`（由 `persist_evi_report.py` 写入 DB 的 JSONB）。

### 4.2 EviReportPanel.tsx — 单 entry 详情

#### Tab 结构（动态）

按 `facets.structure_type` 自适应：

**single_segment**（公司只有一个业务）：

```
1. 估值结论  ← facets + final.md + 整体估值卡 + 定性分析卡
2. 定性分析  ← reports/quality.md + quality.json 4 维度评级
3. 公司产业调研  ← reports/company_overview.md
4. 更新记录  ← reports/changelog.md
5. 自动化任务  ← monitor automation 列表 + reports/monitor.md
6. 数据收集  ← base/CHECKLIST.json + information/indexed_facts.json
```

**multi_segment**（公司有多个业务）：

```
1. 估值结论
2. 定性分析
3. 分部1（如"地产开发"）  ← segments/{seg}.md + segments/{seg}_valuation.md
4. 分部2
...
N+1. 更新记录
N+2. 自动化任务
N+3. 数据收集
```

#### 关键数据契约

| 前端字段 | 来源 sandbox 文件 |
|---|---|
| 估值卡片（fair_value / current_price / judgment） | `facets.json` |
| 4 维度定性评级 | `facets.quality` + `quality.json` |
| 分部贡献 | `facets.segments[]` |
| 数据质量徽章 | `base/CHECKLIST.json` |
| 各 Tab 的 markdown 正文 | `reports/*.md`（按文件名分类） |
| 监控任务 | `entry.payload.automations`（runner 写入） |

#### 报告分类逻辑

`classifyReports()` 把 `entry.payload.reports[]` 数组按文件名映射到分类：

```ts
{
  bySegment: { [seg_id]: { research: ReportEntry, valuation: ReportEntry } },
  companyOverview: ReportEntry | null,
  companyQuality: ReportEntry | null,
  final: ReportEntry | null,
  changelog: ReportEntry | null,
  reverseValuation: ReportEntry | null,
  monitorLog: ReportEntry | null,
}
```

### 4.3 状态显示

`status` 在 ReportPanel 顶部以 banner 形式展示，所有状态都进入 panel（不只是 `completed`）：

| status | banner 表现 |
|---|---|
| `pending` | "正在创建工作区..." 浅灰 |
| `analyzing` | "Agent 正在分析（含 finalize 自动重试）..." 蓝色加 spinner |
| `completed` | 隐藏 banner，直接展示报告 |
| `partial` | 黄色 "部分产物缺失（已自动占位），可继续完善" |
| `failed` | 红色 "分析失败：<error_message>"，提供"重新跑"按钮 |

---

## 5. 数据流端到端

```
[1] 用户在 TemplateHome 点"创建 EVI entry"
        ↓
[2] POST /api/v1/templates/evi-strategy/entries
    │ body: { entry_key: "0700.HK", display_name: "腾讯", params: {market:"hk"} }
        ↓
[3] orchestrator.instantiate
    ├─ 创建 workspace + sandbox（按白名单只装 38 个 skill）
    ├─ INSERT entry row (status=pending)
    ├─ seed agent.md + memory.md
    └─ 后台 asyncio.create_task(_run_agent_safe)
        ↓
[4] _run_agent_safe → _run_agent
    │ 1. status=analyzing
    │ 2. 发 initial_prompt 给 agent
    │ 3. async for in astream_ptc_workflow(...)  ← Agent 在 sandbox 跑全流程
        ↓
[5] Agent 在 sandbox 内：
    │ - Read evi-data-orchestrator/SKILL.md
    │ - 产业调研 + 4 维定性分析 → reports/quality.md / quality.json
    │ - Read evi-valuation-analysis/SKILL.md
    │ - 估值（路由 → 假设 → 多方法 → SOTP）→ facets.json
    │ - 写 reports/final.md + changelog.md
        ↓
[6] generator drain 完 → _run_finalize
    ├─ scan: data/0700_HK/ 下 10 项产物
    ├─ 全 required 齐 + 部分 optional 缺 → 写 placeholder + 调 persist_script (status=partial)
    │  → entry.status = partial
    ├─ 全齐 → 调 persist_script (status=completed)
    │  → entry.status = completed
    └─ required 缺 → 把缺件提示作为 user message 喂回同一 thread
       → Agent 再跑一轮补缺件 → runner 再 scan → 自动 finalize
        ↓
[7] persist_evi_report.py POST /_internal/entries/{id}/finalize
    │ body: { summary, payload, status }
        ↓
[8] DB 更新 → 前端 SSE 收到 entry update → 重新拉数据 → 渲染
```

---

## 6. 改动这块代码时的关注点

| 改什么 | 注意 |
|---|---|
| 加新的 EVI skill | 加到白名单 `_EVI_SKILL_WHITELIST`，否则 sandbox 看不到 |
| 加新产物文件 | 同时加到 `_EVI_EXPECTED_FILES`，否则不会被自动检查 |
| 改 finalize 决策 | 改 `src/server/templates/finalize/runner.py`（**通用层**），不要在 evi_strategy.py 里写逻辑 |
| 改前端 Tab 顺序 | 改 `EviReportPanel.tsx` 的 `buildTabs()`；single/multi segment 两条路径都要改 |
| 改 facets.json schema | 同步改 `persist_evi_report.py` 写入 + `EviReportPanel.tsx` 读取 + `EviDashboard.tsx` 用到的字段 |
| Agent 不再手动调持久化 | 不需要再在 SKILL.md / prompt 里写"任务结束必须调脚本" |
| 调试 finalize 卡住 | 跑 `python3 .agents/skills/evi-toolkit/scripts/wakeup_check.py --data-dir data/<sym> --check-only` 看缺什么 |

---

## 7. 版本演进速查

| 版本 | 核心变化 |
|---|---|
| 3.1.0 | 单一 prompt + 更新记录闭环 |
| 3.2.0 | 新增 evi-market-sizing（TAM/SAM/SOM + Bottom-Up） |
| 3.3.0 | agent.md 重构（结构化工作流） |
| 3.4.0 | PS / Comps 估值脚本化 + MCP broken pipe 修复 |
| 3.5.0 | 新增 4 维定性分析（合并原 Sirius 价值分析） |
| 3.6.0 | sandbox skill 白名单机制（隔离冲突 skill） |
| 3.6.1 | wakeup_check.py 门卫脚本（软约束，仍依赖 Agent 自觉） |
| **3.7.0** | **Finalize 框架化：清单挪到模板层，runner 自动接管 + 缺件回喂消息；白名单扩到 38 个；wakeup_check 退化为开发诊断工具** |
