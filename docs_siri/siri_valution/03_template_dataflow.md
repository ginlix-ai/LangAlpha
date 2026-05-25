# 模板系统数据流与 Agent 配置机制

## 1. 看板展示读哪里的数据？

**完全读数据库（PostgreSQL）**，数据流如下：

```
Agent 在 sandbox 里跑完 D1-D7 分析
  ↓
python3 persist_entry.py --entry-id {entry_id} --data-dir ...
  ↓ 读 sandbox 本地文件：d1.json ~ d7.json + engine_result.json + financial_context.md
  ↓
POST /api/v1/templates/_internal/entries/{entry_id}/finalize
  ↓ HTTP 调用后端内部 API
  ↓
UPDATE template_entries SET payload={...}, summary={...}, status='completed'
  ↓ 写入 PostgreSQL
  ↓
前端每 5 秒轮询 GET /api/v1/templates/sirius-valuation/entries
  ↓ 读 template_entries.summary → 渲染看板表格行
  ↓
进入 workspace 详情页
  ↓ 读 template_entries.payload → SiriusReportPanel 展示 D1-D7
```

**关键点**：sandbox 里的中间文件（d1.json 等）只是运行时中间产物，看板和报告面板展示的全部来自数据库 `template_entries.payload`。即使 sandbox 目录被删除，历史数据依然完整保留在 DB 里。

---

## 2. Agent 如何理解自己属于某个模板？

通过 **`agent.md`** 注入。

每次模型调用时，`WorkspaceContextMiddleware` 把 `agent.md` 的内容注入进 system prompt（作为最后一个 content block）。因此 agent.md 的内容**对 agent 始终可见、始终生效**。

### agent.md 的内容结构（模板 workspace）

```markdown
---
workspace_name: 腾讯科技
description: [Sirius 估值] 0700.HK
template_id: sirius-valuation
entry_id: 020781a3-...        ← 这是关键！agent 知道写哪一行数据库记录
entry_key: 0700.HK
---

# 腾讯科技 — Sirius 估值

## 模板说明
... persist_entry.py 的完整命令（含 entry_id）...

## 分析规则（用户可配置）  ← 这一节是用户自定义区
### 当前配置
- 估值方法权重：使用默认
- D7 定性调整偏好：遵循知识指南

## Thread Index / Key Findings / File Index
```

这意味着：
- Agent 知道 `entry_id` → 知道要更新哪条数据库记录
- Agent 知道 `persist_entry.py` 的完整调用命令
- Agent 知道"修改完就要重新持久化"

---

## 3. 用户修改分析规则的完整工作流

### 场景：用户想修改 D7 估值方法权重

**在 workspace 聊天页直接说**：
```
用户：我想把 DCF 的权重从 35% 改成 50%，PS 权重改成 20%，重新计算估值
```

**Agent 会做什么**（因为看到了 agent.md 的说明）：

1. **编辑 agent.md** — 在"分析规则 → 当前配置"里更新：
   ```
   - 估值方法权重：DCF 50%, PEG 30%, PS 20%（用户自定义）
   ```

2. **修改 d7.json** — 按新权重重新选择敏感性矩阵坐标，生成新的定性调整结论

3. **重新跑 persist_entry.py** — 把新结果写回数据库：
   ```bash
   python3 .agents/skills/sirius-valuation/scripts/persist_entry.py \
     --entry-id 020781a3-... \
     --data-dir .agents/skills/sirius-valuation/data/0700_HK
   ```

4. **看板自动刷新** — 前端每 5 秒轮询，无需手动操作

### 场景：用户想修改 D2 护城河评估标准

```
用户：对于腾讯，我认为微信生态的转换成本被低估了，D2 护城河评级应该是"强"而不是"较强"
```

Agent 会：
1. 更新 d2.json 的 `moat_rating` 字段
2. 更新 agent.md 的分析规则记录这个偏好
3. 根据新的 D2 结论重新跑 D6（综合评估）、D7（定性调整）
4. 跑 persist_entry.py 更新数据库

**这些规则会持久化在 agent.md 里**，下次再进入这个 workspace，agent 依然记得这家公司的个性化配置。

---

## 4. 数据库设计（供参考）

```sql
-- template_entries 表的关键字段
entry_id        UUID            -- 主键，persist_entry.py 引用这个
template_id     VARCHAR(64)     -- 'sirius-valuation'
workspace_id    UUID UNIQUE     -- 1:1 对应 sandbox（CASCADE DELETE）
entry_key       VARCHAR(128)    -- 股票代码，如 '0700.HK'
status          VARCHAR(16)     -- pending / analyzing / completed / failed
summary         JSONB           -- 看板轻量数据（6-8 字段）
payload         JSONB           -- 完整结构化数据（engine_result + D1-D7 + financial_context_md）
params          JSONB           -- 初始化参数（market 等）
```

`payload` 的结构（由 persist_entry.py 构造）：

```json
{
  "engine_result": {
    "classification": {...},
    "wacc": {...},
    "methods": [...],
    "crossValidation": { "weighted_avg": 462, "judgment": "合理", ... }
  },
  "dimensions": {
    "D1": { "score": 8, "title": "...", "metrics": {...}, "key_findings": [...] },
    "D2": { ... },
    ...
    "D7": { "final_recommendation": "...", "adjusted_fair_value": ... }
  },
  "financial_context_md": "## 利润表 (近5年)\n..."
}
```

---

## 5. 前后端数据流总结

```
┌──────────────────────────────────────────────────────────────────────┐
│  浏览器                                                               │
│  /chat/templates/sirius-valuation  ← 看板：读 summary               │
│  /chat/:workspaceId                ← 报告：读 payload                │
└──────────┬───────────────────────────────────┬───────────────────────┘
           │ GET /api/v1/templates/...entries  │ GET .../entries/:id
           ▼                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│  FastAPI Backend                                                      │
│  templates.py router → 读 template_entries 表                        │
│  _internal/entries/{id}/finalize ← sandbox 端脚本回写               │
└──────────┬───────────────────────────────────────────────────────────┘
           │ SQL
           ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PostgreSQL: template_entries                                        │
│  payload JSONB ← persist_entry.py 写入                              │
└──────────────────────────────────────────────────────────────────────┘
           ▲
           │ HTTP POST /finalize
           │
┌──────────────────────────────────────────────────────────────────────┐
│  Sandbox（~/.codebuddy/local-sandboxes/local-XXX/）                  │
│  agent.md          ← 模板上下文 + 规则（用户可配置）                │
│  engine_result.json ← fetch_data.py 产出                            │
│  d1.json ~ d7.json ← Agent LLM 产出                                 │
│  persist_entry.py  ← 读上面文件 → 写数据库                          │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 6. 如何给已有 workspace 回写 agent.md

对于在本功能上线前创建的旧 workspace，可以通过直接编辑 sandbox 目录里的 `agent.md` 文件来补充模板上下文。

新创建的 workspace（通过模板市场 → 新增分析）会自动注入正确的 agent.md，包含：
- `entry_id`（对应数据库记录）
- `persist_entry.py` 的完整调用命令
- 用户可编辑的"分析规则"节

---

## 7. 未来可扩展点

| 功能 | 实现方式 |
|---|---|
| 用户在 Web UI 直接编辑分析规则（不用聊天） | 前端加"规则配置"面板，写入 `template_entries.params` 并更新 agent.md |
| 定期自动重跑（如每周五更新看板数据） | 用 Automations 框架注册 cron，在 workspace 里发"重新分析"消息 |
| 多家公司批量分析 | 白名单循环调用 `/api/v1/templates/{id}/entries`（已支持） |
| D7 估值方法权重可视化调整 | 看板页加滑块，写回 DB，触发 agent 重算 |
