---
name: evi-monitor
description: "EVI 持续监控：通过 automation skill 注册定时任务，发现新材料后驱动 Phase1 调研更新 → Phase2 重估。回到完整 Monitor → Update Research → Revaluate 链路。"
---

# EVI Monitor — 持续监控与重估驱动

## 1. 职责

> 估值不是一次性。建立分析体系后必须**持续跟踪**：发现新材料 → 更新产业调研 → 驱动估值更新。

通过 `automation` skill 注册定时任务（推荐每周一次），形成完整的监控-更新闭环。

---

## 2. 监控-更新闭环（核心）

```
┌─────────────────────────────────────────────────────────┐
│            完整监控更新链路（automation 驱动）              │
│                                                         │
│  Step 1: automation 定时触发（如每周一 9 AM）             │
│            ↓                                            │
│  Step 2: evi-monitor 扫描新材料                          │
│            ├─ 新财报/电话会/公告                         │
│            ├─ 新研报与共识变化                           │
│            ├─ 行业新闻 / 政策变化                        │
│            ├─ 产品级数据（榜单/出货）                     │
│            └─ rerate_triggers 中的指标变化               │
│            ↓                                            │
│  Step 3: 判断影响范围                                     │
│            ├─ 仅 informational → 只记录，不重估           │
│            └─ 影响假设 → 进入 Step 4                     │
│            ↓                                            │
│  Step 4: 更新 Phase 1 产业调研                           │
│            ├─ 影响公司层 → 更新 company_overview.md       │
│            └─ 影响某分部 → 更新 segments/{seg_id}.md     │
│            ↓ 追加新 fact 到 indexed_facts.json           │
│  Step 5: 驱动 Phase 2 重估                               │
│            └─ evi-revaluation-updater                   │
│                ├─ 比对旧/新 facts                        │
│                ├─ 定位受影响 assumptions                  │
│                ├─ 重跑对应 segment × method             │
│                ├─ 重新汇总 SOTP（如适用）                 │
│                └─ 更新 facets.json + 看板                │
│            ↓                                            │
│  Step 6: 记录到 memory.md（变更日志）                     │
│            ↓                                            │
│  Step 7: 通知用户（可选 Slack delivery）                  │
└─────────────────────────────────────────────────────────┘
```

---

## 3. 设置定时监控（用户首次估值完成后）

> ⚠️ **支持注册多个监控**，每个监控对应一个独立的 automation 任务。
> 完成 Phase 2 后，**主动询问用户**："你希望跟踪哪些方面？"，根据回答**分别注册**多个 monitor。

### 3.1 推荐的 4 类监控（用户场景驱动）

| 类型 | type 字段 | 适用场景 | 频率建议 |
|---|---|---|---|
| **指标阈值监控** | `metric` | 监控 reverse_valuation 输出的 rerate_triggers，越线即重估 | 每周 |
| **事件型监控** | `event` | 公司财报/电话会/公告/发布会等事件 | 每日扫一遍 |
| **产业链监控** | `industry` | 上下游关键数据：车厂月销、机器人量产新闻 | 每周 / 每月 |
| **竞品监控** | `competitor` | Peer 业绩、新品发布、估值倍数变化 | 每周 |

### 3.2 注册示例（多个监控并存）

```python
# 监控 1：指标阈值（自动派生自 rerate_triggers）
create_automation(
    name="速腾聚创-指标阈值监控",
    instruction=(
        "对 2498.HK 执行 evi-monitor，重点检查 facets.json.rerate_triggers 中各指标的当前值。"
        "如有指标突破阈值，立即调用 evi-revaluation-updater 重估对应分部。"
    ),
    schedule="0 9 * * 1",
    thread="persistent",
    metadata={
        "type": "metric",
        "evi_symbol": "2498.HK",
        "evi_monitor_kind": "rerate_triggers",
    }
)

# 监控 2：事件型（财报/电话会/公告）
create_automation(
    name="速腾聚创-事件监控",
    instruction=(
        "对 2498.HK 扫描最近一周新发布的财报、电话会、HKEx 公告（用 evi_download_knowledge.py）。"
        "若发现新材料：解析 → 抽取关键事实 → 追加到 indexed_facts.json → 调用 evi-revaluation-updater。"
    ),
    schedule="0 9 * * *",          # 每日扫描
    thread="persistent",
    metadata={
        "type": "event",
        "evi_symbol": "2498.HK",
        "evi_monitor_kind": "new_filings",
    }
)

# 监控 3：产业链（车厂月销 / 机器人量产新闻）
create_automation(
    name="速腾聚创-产业链监控",
    instruction=(
        "WebSearch 中国车厂上月新能源乘用车批发数据（比亚迪/理想/小鹏/小米SU7等），"
        "以及人形机器人量产新闻（特斯拉Optimus/Figure/优必选/宇树）。"
        "整理后追加到 reports/monitor.md，对预期销量影响超过 ±10% 时调用 revaluation-updater。"
    ),
    schedule="0 10 5 * *",         # 每月 5 号（车厂当月销量披露后）
    thread="persistent",
    metadata={
        "type": "industry",
        "evi_symbol": "2498.HK",
        "evi_monitor_kind": "supply_chain",
    }
)

# 监控 4：竞品（peer 业绩）
create_automation(
    name="速腾聚创-竞品监控",
    instruction=(
        "扫描 peers（禾赛科技 HSAI、Innoviz INVZ、Luminar LAZR）的最新季报、出货数据、估值倍数。"
        "对比速腾的 PS / EV-Sales / 出货市场份额，更新 valuation/group/peer_comp.json，"
        "若 peer 估值中位数偏离 ±20%，触发 Comps 方法的重估。"
    ),
    schedule="0 9 * * 1",
    thread="persistent",
    metadata={
        "type": "competitor",
        "evi_symbol": "2498.HK",
        "evi_monitor_kind": "peer_tracking",
        "peers": ["HSAI", "INVZ", "LAZR"],
    }
)
```

### 3.3 注册成功后回写 facets.json

每注册一个 automation，就要在 `facets.json.monitor.monitors[]` 追加一条：

```json
{
  "monitor": {
    "monitors": [
      {
        "id": "auto_a1b2c3",
        "name": "速腾聚创-指标阈值监控",
        "type": "metric",
        "description": "rerate_triggers 越线告警",
        "schedule": "0 9 * * 1",
        "automation_id": "auto_a1b2c3",
        "trigger_count": 0,
        "status": "active"
      },
      ...
    ]
  }
}
```

每次 monitor 触发时累加 `trigger_count`，并写 `last_triggered_at` 和 `last_impact`（最近一次发现了什么）——前端"自动化任务"Tab 会读这些字段展示。

### 3.1 不同时机的 schedule 建议

| 频率 | 适用 | cron |
|---|---|---|
| 每日 | 价格波动敏感 + 持续关注 | `0 9 * * 1-5` |
| 每周 | 标准跟踪频率 | `0 9 * * 1` |
| 每月 | 长期持有 + 低关注度 | `0 9 1 * *` |
| 财报前 | 业绩前预热 | `0 8 30 4,7,10 *`（季度财报前 1 天） |

### 3.2 价格触发监控（补充定时）

除定时外，也可设价格触发：

```python
create_automation(
    name="{company} 价格异动监控",
    instruction=(
        "{company} 价格异动超过 5%。立即调用 evi-monitor 检查："
        "1. 是否有新材料（公告/新闻）"
        "2. 反向估值的隐含参数是否需要更新"
        "3. 如必要，调用 evi-revaluation-updater"
    ),
    trigger_type="price",
    trigger_config={
        "symbol": "{symbol}",
        "conditions": [
            {"type": "pct_change_above", "value": 5, "reference": "previous_close"},
            {"type": "pct_change_below", "value": -5, "reference": "previous_close"},
        ],
        "retrigger": {"mode": "recurring", "cooldown_seconds": 14400},
    },
)
```

### 3.3 rerate_triggers 触发（关键指标偏离）

Phase 2 的 reverse_valuation.json 输出了 `rerate_triggers`，例如：

```jsonc
{
  "rerate_triggers": [
    {"metric": "cloud_revenue_yoy", "threshold_down": 18, "threshold_up": 26}
  ]
}
```

每次扫描时，evi-monitor 必须检查这些关键指标——一旦偏离阈值，即使没有新材料也要触发重估。

---

## 4. evi-monitor 扫描流程（脚本化）

```bash
# Step 1: 增量下载（>last_checked_at）
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \
    --symbol {symbol} --market {market} \
    --since {last_checked_at} \
    --data-dir data/{symbol_dir}

# 下载新材料（财报/公告/研报/电话会）
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \
    --symbol {symbol} --market {market} --all --years 1 \
    --data-dir data/{symbol_dir}
```

子 agent 完成：
- WebSearch 行业新闻 + 产品级数据
- 检查 rerate_triggers 中的指标当前值
- 登记新材料

---

## 5. 输出格式

### 5.1 monitor/new_materials.json

```jsonc
{
  "schema_version": 2,
  "monitor_run_id": "monitor_{symbol}_2026-05-21",
  "checked_at": "2026-05-21",
  "last_checked_at": "2026-05-14",
  "watch_scope": ["financial_reports","earnings_calls","research_reports","industry_news","product_metrics","rerate_triggers"],
  "automation_id": "auto_xxx",
  
  "new_materials": [
    {
      "material_id": "mat_2026Q1_call",
      "source_type": "earnings_call",
      "title": "2026Q1 业绩电话会",
      "source_file": "base/transcripts/raw/2026Q1.md",
      "discovered_at": "2026-05-21",
      "related_segments": ["games","cloud"],   // 影响哪些分部
      "preliminary_severity": "medium",
      "preliminary_note": "管理层提到游戏增速将放缓，云 AI 收入超预期",
      "status": "new"
    }
  ],
  
  "trigger_checks": [   // rerate_triggers 检查结果
    {
      "metric": "cloud_revenue_yoy",
      "current_value": 16.5,
      "threshold_down": 18.0,
      "threshold_up": 26.0,
      "status": "breached_down",
      "severity": "high",
      "interpretation": "云增速跌破市场隐含 18% 阈值"
    }
  ],
  
  "actions": [
    {
      "type": "update_phase1_research",
      "target_segments": ["games","cloud"],
      "material_ids": ["mat_2026Q1_call"]
    },
    {
      "type": "trigger_revaluation",
      "target_skill": "evi-revaluation-updater",
      "reason": "rerate_trigger breached + new material with medium severity"
    }
  ]
}
```

### 5.2 reports/monitor.md（增量追加，不重写）

```markdown
## 2026-05-21 监控扫描

**触发方式**：automation `auto_xxx`（每周一 9 AM）
**扫描范围**：6 项
**上次扫描**：2026-05-14（7 天前）

### 新材料发现（2 项）
| 时间 | 类型 | 标题 | 严重性 | 影响分部 | 摘要 |
|---|---|---|---|---|---|
| 2026-05-20 | earnings_call | 2026Q1 电话会 | 🟡 medium | games, cloud | 游戏增速放缓 + 云 AI 超预期 |
| 2026-05-19 | research_report | GS 上调云增速预测 | 🟢 low | cloud | 25% → 28% |

### 关键指标检查（rerate_triggers）
| 指标 | 当前 | 隐含 | 阈值 | 状态 |
|---|---|---|---|---|
| 云收入增速 (YoY) | 16.5% | 22% | 18%-26% | 🔴 breached_down |
| EBIT Margin | 33.2% | 33% | 30%-35% | ✓ aligned |

### 触发动作
- ✅ 调用 evi-revaluation-updater（rerate_trigger breached + 新材料 medium）
- 📝 更新 reports/segments/cloud.md 和 games.md（追加新 facts）

---
```

---

## 6. 操作约束

- 只**发现并登记**，不直接修改 indexed_facts，不重算估值
- 严格控制 token：电话会/研报全文不解析，只摘要 + 评级 + 关键数字
- 每个新材料必须能定位到 `related_segments`
- rerate_triggers 检查是**强制**的（即使没新材料也要查）
- severity 升序：informational → low → medium → high
  - high / medium → 必须立刻调 evi-revaluation-updater
  - low → 累积 3 项后再触发重估
  - informational → 只记录

---

## 7. 与 automation skill 的对接

evi-monitor **不直接管理调度**，调度由 LangAlpha 平台的 automation skill 负责：

| 职责 | Skill |
|---|---|
| 创建/管理定时/触发任务 | `automation`（平台原生） |
| 监控扫描的具体逻辑 | `evi-monitor` |
| 发现变化后的重估 | `evi-revaluation-updater` |

automation 触发时，agent 应：
1. 读 instruction（已包含执行什么）
2. 调用 evi-monitor 执行扫描
3. 根据结果决定是否调用 evi-revaluation-updater

---

## 8. 与 evi-revaluation-updater 的衔接

evi-monitor 输出的 `actions` 字段直接驱动后续 skill：

```jsonc
"actions": [
  {"type":"update_phase1_research","target_segments":["cloud"],"material_ids":["mat_xxx"]},
  {"type":"trigger_revaluation","target_skill":"evi-revaluation-updater"}
]
```

agent 看到 `trigger_revaluation` action → 立即调用 evi-revaluation-updater。
