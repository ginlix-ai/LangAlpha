---
name: evi-revaluation-updater
description: "EVI 重估更新器：在 evi-monitor 发现新材料后，更新 Phase 1 产业调研 → 定位受影响假设 → 触发 Phase 2 估值方法重算 → 更新 facets.json + memory.md。完整的 monitor → update → revaluate 闭环的最后一环。"
---

# EVI Revaluation Updater — 重估更新器

## 1. 职责

> 把 evi-monitor 发现的新信息**转化为估值变化**，完成完整闭环：
>
> ```
> Monitor → Update Phase 1 Research → Identify Affected → Recompute Phase 2 → Update Dashboard
> ```

---

## 2. 输入

```text
data/{symbol_dir}/monitor/new_materials.json     ← evi-monitor 输出
data/{symbol_dir}/information/indexed_facts.json (旧版)
data/{symbol_dir}/business_segments.json
data/{symbol_dir}/valuation/{segment_id}/assumption_ledger.json
data/{symbol_dir}/valuation_method_matrix.json
data/{symbol_dir}/reports/company_overview.md
data/{symbol_dir}/reports/segments/{seg_id}.md   (multi_segment)
.agents/workspace/memory/memory.md
```

---

## 3. 工作流

### Step 1: 更新 Phase 1 产业调研

针对 `new_materials.json` 中的 `actions[].update_phase1_research`：

```
对每个 target_segment：
  打开 reports/segments/{seg_id}.md（或 company_overview.md）
  在相关章节追加新发现：
    例：在"## 8. 增长驱动与风险"末尾追加
        "**2026-05-21 更新**：管理层在 Q1 电话会确认 AI 商业化加速 [^new_42]"
  在 ## Facts Index 末尾追加新 fact 条目
  
跑 format_facts.py 更新 indexed_facts.json
```

**核心原则**：
- ❌ 不修改旧 fact_id（保留历史）
- ✅ 只追加新 fact（新 fact_id）
- ✅ 在报告章节中标注更新日期
- ✅ 用 footnote `[^new_N]` 链接到新 fact

### Step 2: 定位受影响假设（写 information_delta）

对比旧/新 facts，识别哪些 assumption 受影响：

```jsonc
// monitor/information_delta.json
{
  "information_delta_id": "delta_{symbol}_2026-05-21",
  "trigger_run_id": "monitor_{symbol}_2026-05-21",
  
  "changed_facts": [
    {
      "old_fact_id": "fact_cloud_revenue_growth_001",
      "new_fact_id": "fact_cloud_revenue_growth_009",
      "change_type": "value_updated | reason_updated | risk_added",
      "old_value": "云增速 22%",
      "new_value": "云增速 16.5%（Q1 实际）",
      "affected_segments": ["cloud"],
      "affected_assumptions": [
        "assump_cloud_growth_2026_base",
        "assump_cloud_growth_2027_base"
      ],
      "severity": "high"
    }
  ],
  
  "trigger_revaluation": true,
  "valuation_methods_to_rerun": {
    "cloud": ["DCF","EV/Sales","Comps"]
  }
}
```

### Step 3: 写重估任务队列

```jsonc
// monitor/revaluation_tasks.json
{
  "tasks": [
    {
      "id": "task_001",
      "segment_id": "cloud",
      "method": "DCF",
      "reason": "云增速假设变化",
      "delta_id": "delta_xxx",
      "status": "pending"
    },
    {
      "id": "task_002",
      "segment_id": "cloud",
      "method": "EV/Sales",
      "reason": "forward_revenue 变化",
      "delta_id": "delta_xxx",
      "status": "pending"
    },
    {
      "id": "task_003",
      "segment_id": "group",
      "method": "SOTP",
      "reason": "cloud 重估后需重新汇总",
      "depends_on": ["task_001","task_002"],
      "status": "pending"
    }
  ]
}
```

### Step 4: 打 snapshot（重要：保留历史估值）

```bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
cp data/{symbol_dir}/valuation/group/final_company_valuation.json \
   data/{symbol_dir}/valuation/group/history/${TIMESTAMP}_final.json
cp data/{symbol_dir}/facets.json \
   data/{symbol_dir}/valuation/group/history/${TIMESTAMP}_facets.json
```

### Step 5: 更新假设账本

对每个受影响 assumption：
1. 读 `valuation/{segment_id}/assumption_ledger.json`
2. 找到 `assumption_id` 对应的条目
3. 更新 `value` + 在 `change_history` 数组末尾追加：
   ```jsonc
   {
     "ts": "2026-05-21T10:30:00Z",
     "old_value": 22.0,
     "new_value": 16.5,
     "trigger_delta_id": "delta_xxx",
     "trigger_fact_id": "fact_cloud_revenue_growth_009"
   }
   ```
4. 同步更新 growth_bridge.json / margin_bridge.json（如适用）

### Step 6: 重算受影响方法

按 revaluation_tasks.json 的 dependency 顺序执行：

```bash
# 例：DCF 重算
python3 .agents/skills/evi-valuation-dcf/scripts/dcf_calc.py \
    --data-dir data/{symbol_dir} --segment cloud

# 其它方法：参考各 SKILL.md 重写 *_result.json

# 然后 segment 聚合
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --segment cloud
```

如果触及 `rerate_triggers`，反向估值也要重跑：

```bash
# 重跑 evi-reverse-valuation，更新 rerate_triggers
```

### Step 7: SOTP 重新汇总

```bash
# multi_segment：跑总汇总
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --group
```

更新：
- `valuation/group/final_company_valuation.json`
- `facets.json`

### Step 8: 重写报告的"估值变化记录"段落

在 `reports/valuation_summary.md`（multi_segment）或 `reports/valuation.md`（single_segment）末尾追加：

```markdown
## 估值变化记录

### 2026-05-21 — 因云增速 Q1 实际放缓重估

**触发**：
- 来源：evi-monitor 周度扫描（automation_id=auto_xxx）
- 关键发现：2026Q1 实际云增速 16.5% < 市场隐含 18%
- 触发 fact：fact_cloud_revenue_growth_009 [^new_42]

**调整**：
| 假设 | 旧值 | 新值 | 触发原因 |
|---|---|---|---|
| cloud.revenue_growth_2026_base | 22.0% | 18.0% | Q1 实际拐点 |
| cloud.revenue_growth_2027_base | 19.0% | 16.0% | 增速衰减惯性 |

**估值变化**：
| 维度 | 旧 | 新 | 变化 |
|---|---|---|---|
| cloud segment Base EV | 2,232K | 1,950K | -12.6% |
| 集团 SOTP Base 每股 | 685.6 | 658.4 HKD | -4.0% |
| 判断 | 低估 | 低估 | 不变 |
```

### Step 9: 持久化（更新看板）

```bash
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \
    --entry-id {entry_id} --data-dir data/{symbol_dir} \
    --display-name "{display_name}" --symbol "{symbol}" --market "{market}"
```

### Step 10: 写 memory.md（强制 — 变更日志）

往 `.agents/workspace/memory/memory.md` 末尾追加：

```markdown
### 2026-05-21 重估

- **触发**：evi-monitor 周度扫描发现 Q1 云增速放缓 + 跌破 rerate_trigger 阈值
- **automation_id**：auto_xxx
- **delta_id**：delta_xxx
- **影响**：cloud segment 增速假设
- **新值**：cloud.revenue_growth_2026 22% → 18%
- **估值变化**：base 685.6 → 658.4 HKD/股 (-4.0%)
- **判断变化**：低估 → 低估（不变）
- **操作记录**：dcf_calc → aggregate(cloud) → reverse_valuation → aggregate(group) → persist_evi_report
- **快照**：valuation/group/history/20260521_103000_final.json
```

### Step 11: 关闭任务

把 `revaluation_tasks.json` 中处理过的任务 `status` 改为 `done`，并记录 `completed_at`。

### Step 12: 通知用户

如配置了 delivery（Slack）：

```
📊 {company} 估值更新

触发：周度监控发现 Q1 云增速 16.5% < 市场隐含 18%

调整：
- cloud 增速 22% → 18%

估值：
- Base 685.6 → 658.4 HKD (-4.0%)
- 判断：低估 → 低估（不变）

详见看板。
```

---

## 4. 操作约束

- **不修改旧 fact_id**；只追加（保留可审计历史）
- 每条 delta 必须能映射到至少 1 个 affected_assumption；映射不到 → severity=informational，不触发重估
- 重算前**必须**打 snapshot（错了能回滚）
- memory.md 是强制写的（变更日志，用户和未来 agent 都要看）
- 与 automation skill 配合：从 monitor 自动驱动到这里
- multi_segment 模式：必须在最后跑 group 级 SOTP 重新汇总
