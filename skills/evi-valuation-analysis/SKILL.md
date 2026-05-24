---
name: evi-valuation-analysis
description: "EVI Phase 2 总控：基于 Phase 1 产业调研做估值。自适应整体估值 vs SOTP，支持反向请求 Phase 1 补数据的迭代闭环。输出整体估值 + 各分部估值（如适用）。"
---

# EVI Valuation Analysis — Phase 2：估值分析

> **你是 Phase 2 的总控**。读取 Phase 1 的产业调研报告，根据公司结构（单一 vs 多分部）执行整体估值或 SOTP 估值，并在数据不足时**反向请求 Phase 1 补充**。

---

## 1. 一句话职责

> 基于 Phase 1 的产业调研 → 自适应估值（整体估值 or SOTP）→ 数据不足时反向请求 Phase 1 补充 → 迭代到收敛 → 输出最终估值 + facets.json + persist(completed)。

---

## 2. 核心理念：估值结构匹配业务结构

```
读 business_segments.json → structure_type
   │
   ├─ single_segment（小公司/单一业务）
   │     └─ 整体估值（一份估值报告）
   │           - DCF / PE Band / EV/Sales
   │           - 不做 SOTP
   │
   └─ multi_segment（多业务线公司）
         └─ SOTP 估值
               - 每个分部独立估值（基于该分部的 segments/{seg_id}.md）
               - 集团层：合并 + 反向估值 + 持续派息（DDM if applicable）
               - 输出 estimates / segment 共 N+1 份报告
```

---

## 3. 前置检查

```
读 base/CHECKLIST.json:
  if overall == "blocked":
    STOP. 告知"Phase 1 数据未就绪，请先完成产业调研。"

读 reports/company_overview.md:
  if 不存在: STOP. "Phase 1 总报告缺失"

读 business_segments.json:
  if structure_type == "multi_segment":
    检查 reports/segments/*.md 是否齐全（每个分部都要有）
    若缺失 → 反向请求 Phase 1 补
```

---

## 4. 数据闭环：Phase 2 ⇄ Phase 1 迭代

> 这是新设计的核心。Phase 2 不是单向消费 Phase 1，而是可以反向请求补数据。

```
Phase 2 进行中
   ↓
   发现数据不足（如某分部的 peer 倍数缺失）
   ↓
   写 monitor/phase1_gap_request.json：
   {
     "request_id": "req_001",
     "segment_id": "cloud",
     "gap_type": "peer_multiples_missing",
     "specific_need": "需要 AWS / Azure 最新 EV/Sales 倍数",
     "blocking_for": ["evi-valuation-ps cloud segment"]
   }
   ↓
   实例化子 agent 调用 evi-information-search 补数据
   ↓
   补完后追加到 indexed_facts.json + 对应 segment 报告
   ↓
   Phase 2 继续，重新计算
```

每个分部的估值 skill 在缺数据时**必须用此机制**，而不是用模拟数据填坑。

---

## 5. 编排方案

### 5.1 单一估值模式（structure_type == single_segment）

```
┌──────────────────────────────────────────────────┐
│       evi-valuation-analysis (单一估值模式)        │
│                                                  │
│  Phase 2.1: 估值方法路由                          │
│    └─ evi-valuation-router → 选 1-3 个方法       │
│                                                  │
│  Phase 2.2: 假设构建                              │
│    └─ evi-assumption-builder（公司层）            │
│                                                  │
│  Phase 2.3: 多方法估值（并发）                    │
│    ├─ DCF（如适用）                              │
│    ├─ PE Band / Comps                           │
│    └─ EV/Sales（如未盈利）                       │
│                                                  │
│  Phase 2.4: 反向估值（可选）                      │
│    └─ evi-reverse-valuation                     │
│                                                  │
│  Phase 2.5: 整体汇总                              │
│    └─ 写 reports/valuation.md（一份）             │
│                                                  │
│  Phase 2.6: facets.json + persist(completed)     │
└──────────────────────────────────────────────────┘
```

### 5.2 SOTP 估值模式（structure_type == multi_segment）

```
┌────────────────────────────────────────────────────────────────┐
│           evi-valuation-analysis (SOTP 估值模式)                  │
│                                                                │
│  Phase 2.1: 估值方法路由（每分部独立）                            │
│    └─ evi-valuation-router → 每个 segment 的方法                │
│                                                                │
│  Phase 2.2: 假设构建（每分部并发）                                │
│    └─ evi-assumption-builder × N segments                     │
│                                                                │
│  Phase 2.3: 多方法估值（segment × method 并发）                  │
│    ├─ segment 1: DCF + EV/Sales + Comps                       │
│    ├─ segment 2: DCF + Comps + PEG                            │
│    ├─ segment 3: PS + Comps                                   │
│    └─ ...（按 valuation_method_matrix）                        │
│                                                                │
│  Phase 2.4: 数据闭环检查                                         │
│    ├─ 任何 segment 数据不足 → 反向请求 Phase 1                  │
│    └─ 等补完 → 重新跑该 segment                                 │
│                                                                │
│  Phase 2.5: 反向估值（集团层）                                    │
│    └─ evi-reverse-valuation                                    │
│                                                                │
│  Phase 2.6: SOTP 汇总                                           │
│    ├─ 各 segment 加权汇总                                        │
│    ├─ 集团调整（净债 / 现金 / 投资资产 / 控股折价）                │
│    └─ 写 reports/valuation_summary.md（总） +                  │
│        reports/segments/{seg_id}_valuation.md（每分部）         │
│                                                                │
│  Phase 2.7: facets.json + persist(completed)                   │
└────────────────────────────────────────────────────────────────┘
```

---

## 6. Phase 2.1 — 估值方法路由

调用 `evi-valuation-router`：

输入：
- `business_segments.json`
- `base/financials/indicators/key_metrics.json`
- 各分部的 segments/{seg_id}.md（multi_segment）或 company_overview.md（single_segment）

输出 `valuation_method_matrix.json`：

```jsonc
{
  "structure_type": "multi_segment",
  "matrix": [
    {
      "segment_id": "cloud",
      "methods": [
        {"method":"DCF",        "role":"primary",     "data_needs":[...]},
        {"method":"EV/Sales",   "role":"cross_check", "data_needs":["peer_ev_sales"]},
        {"method":"Comps",      "role":"cross_check"}
      ]
    },
    {
      "segment_id": "games",
      "methods": [
        {"method":"DCF",        "role":"primary"},
        {"method":"PEG",        "role":"cross_check"},
        {"method":"Comps",      "role":"cross_check"}
      ]
    }
  ],
  "group_methods": ["SOTP", "DDM", "Reverse"]   // multi_segment 才有
}
```

---

## 7. Phase 2.2 — 假设构建

调用 `evi-assumption-builder`，对每个分部（multi_segment）或公司（single_segment）：

输入（来自 Phase 1）：
- `reports/segments/{seg_id}.md` 或 `reports/company_overview.md`
- `information/indexed_facts.json`
- `business_segments.json`
- `valuation_method_matrix.json`
- `base/financials/indicators/*`

输出每个 segment：
- `valuation/{segment_id}/assumption_ledger.json`
- `valuation/{segment_id}/growth_bridge.json`
- `valuation/{segment_id}/margin_bridge.json`
- `valuation/{segment_id}/risk_adjustment.json`
- `reports/segments/{seg_id}_assumptions.md`（人类可读）

集团层：
- `valuation/group/assumption_ledger.json`（含 WACC、税率、永续增长等）

---

## 8. Phase 2.3 — 多方法估值（segment × method 并发）

按 valuation_method_matrix 并发执行：

```
multi_segment 模式（如腾讯）:
├── cloud 分部（并发）:
│   ├── DCF:       evi-valuation-dcf
│   ├── EV/Sales:  evi-valuation-ps
│   └── Comps:     evi-valuation-comps
├── games 分部（并发）:
│   ├── DCF
│   ├── PEG
│   └── Comps
└── ...

single_segment 模式（小公司）:
└── 公司层（并发）:
    ├── DCF
    ├── PE Band / Comps
    └── EV/Sales（如未盈利）
```

每个 segment 完成后聚合：

```bash
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --segment {seg_id}
```

→ `valuation/{segment_id}/final_segment_valuation.json`

---

## 9. Phase 2.4 — 数据闭环检查（关键创新）

每个估值 skill 完成后，检查输出 status：

```
if result.status == "missing_inputs" or "insufficient_peers" or ...:
   1. 写 monitor/phase1_gap_request.json
   2. 实例化子 agent 调 evi-information-search 补数据
   3. 等子 agent 完成 → format_facts.py 更新事实库
   4. 重跑该 segment 的对应方法
   5. 最多 3 轮迭代
```

**典型缺口与补救**：

| 缺口 | 补救动作 |
|---|---|
| Peer 倍数不足 3 家 | WebSearch 同行业公司，从 FMP 拉倍数 |
| 行业增速 consensus 缺失 | WebSearch 行业研报 |
| 分部利润率历史缺失 | 重新解析财报附注 |
| 管理层指引未抽取 | 重新解析最新电话会 |

---

## 10. Phase 2.5 — 反向估值（集团层）

调用 `evi-reverse-valuation`：

- single_segment：基于公司整体 DCF 反推
- multi_segment：基于 SOTP 函数反推

输出 `valuation/group/reverse_valuation.json` + `reports/reverse_valuation.md`。

**关键产出**：触发重估的指标清单（写入 `rerate_triggers`），供 evi-monitor 使用。

---

## 11. Phase 2.6 — SOTP 汇总

调用 `evi-valuation-orchestrator`：

### 11.1 multi_segment 模式

```
1. 各 segment 用权重合并多方法 → final_segment_valuation
2. SOTP 加总：
   group_EV = Σ segment_EV
            + 投资资产（上市折价 20-25% / 非上市折价 50-60%）
            + 净现金
            - 控股公司折价
3. group_EV → 每股价值 (Bear/Base/Bull)
4. 写 valuation/group/final_company_valuation.json
5. 写 reports/valuation_summary.md（总）
6. 写 reports/segments/{seg_id}_valuation.md（每分部）
7. 写 facets.json
```

### 11.2 single_segment 模式

```
1. 多方法权重合并 → 最终估值
2. 写 valuation/group/final_company_valuation.json
3. 写 reports/valuation.md（一份）
4. 写 facets.json
```

---

## 12. 报告产出（最终交付）

### 12.1 multi_segment 模式

```
reports/
├── company_overview.md         ← Phase 1 总报告（已有）
├── segments/
│   ├── cloud.md                ← Phase 1 分部调研（已有）
│   ├── cloud_valuation.md      ← Phase 2 分部估值（新）
│   ├── games.md
│   ├── games_valuation.md
│   └── ...
├── valuation_summary.md        ← Phase 2 估值总报告（SOTP）
├── reverse_valuation.md
└── final.md                    ← 最终结论（一句话 + 推荐）
```

### 12.2 single_segment 模式

```
reports/
├── company_overview.md         ← Phase 1 总报告
├── valuation.md                ← Phase 2 估值（一份）
├── reverse_valuation.md
└── final.md                    ← 最终结论
```

---

## 13. facets.json 结构（看板核心）

```jsonc
{
  "company_name": "腾讯科技",
  "structure_type": "multi_segment",   // 看板用这个决定怎么展示
  "currency_unit": "HKD per share",
  "fair_value": {"bear": 577.5, "base": 685.6, "bull": 859.3},
  "current_price": 441.0,
  "upside_pct": 55.5,
  "judgment": "低估",
  "n_segments": 4,
  
  // multi_segment 才有：每个 segment 的估值快照（看板按这个动态展示）
  "segments": [
    {
      "segment_id": "vas",
      "name": "增值服务",
      "fair_value_share": {"bear": 250, "base": 295, "bull": 345},
      "contribution_pct_base": 43.0,
      "primary_method": "DCF",
      "confidence": 0.75
    },
    {
      "segment_id": "marketing_services",
      "name": "营销服务",
      "fair_value_share": {"bear": 95, "base": 115, "bull": 140},
      "contribution_pct_base": 16.8,
      "primary_method": "DCF + Comps",
      "confidence": 0.7
    }
    // ... N 个 segments
  ],
  
  "key_drivers": ["AI 商业化", "游戏稳态利润率"],
  "key_risks": ["监管", "海外宏观"],
  
  // 给 monitor 用
  "rerate_triggers": [
    {"metric": "cloud_revenue_yoy", "threshold_down": 18, "threshold_up": 26}
  ]
}
```

---

## 14. 持久化

```bash
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \
    --entry-id {entry_id} --data-dir data/{symbol_dir} \
    --display-name "{display_name}" --symbol "{symbol}" --market "{market}" \
    --status completed
```

---

## 15. 汇报

告知用户：
- 估值结构：整体估值 / SOTP（N 分部）
- 最终估值（Bear/Base/Bull + 判断 + Upside）
- 各分部估值贡献（multi_segment）
- 方法一致性（CV）
- 反向估值解读（市场已 price-in 多少）
- 触发重估指标清单
- 提示"可注册 Automation 持续监控"

---

## 16. 失败处理

- 某 segment 估值方法失败 + 3 轮迭代仍缺数据 → 跳过该方法，用其它方法继续
- 某 segment 完全失败 → SOTP 中标 partial，并在 valuation_summary.md 说明
- 所有 segment 都失败 → persist failed
- 数据闭环死循环（同一缺口补不上）→ 标 partial + 在报告说明
