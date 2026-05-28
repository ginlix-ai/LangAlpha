---
name: evi-valuation-analysis
description: "EVI Phase 2 总控（planning-only）：基于 Phase 1 调研，编排估值流程并分派给子 skill。本 skill 不做计算，所有 DCF/PS/Comps 数字必须由对应子 skill 的脚本产出。"
---

# EVI Valuation Analysis — Phase 2 总控

> ⚠️ **本 skill 是 planning（编排者），不是 calculator（执行者）**。
> 你的工作是按 DAG 顺序调度子 skill，不是自己写 Python 算 DCF。

---

## 1. 一句话职责

> 读 Phase 1 调研产物 → 按公司结构（single / multi segment）选估值路径 → 调度子 skill 完成 1) 路由 2) 假设 3) 多方法估值 4) 反向 5) 汇总 → 产出 facets.json + reports/final.md。

**不要做的事**：

- ❌ 不要自己写 Python 算 DCF / PS / Comps（必须用 evi-valuation-{dcf,ps,comps}/scripts 里的脚本）
- ❌ 不要"为了快"跳过子 skill 直接写最终结论（结论必须基于子 skill 的产物 JSON）
- ❌ 不要在本文件 §14 之后做任何持久化操作（framework 自动接管）

---

## 2. 输入门禁（先检查再开工）

```
读 base/CHECKLIST.json
   if overall == "blocked":
     STOP. 告知用户 "Phase 1 数据未就绪，请先完成产业调研。"

读 reports/company_overview.md（或 segments/*.md）
   if 不存在: STOP.

读 business_segments.json
   if structure_type == "multi_segment":
     检查每个 segment 都有 reports/segments/{seg_id}.md
     若缺失 → 反向请求 Phase 1 补（见 §8）
```

---

## 3. 权责矩阵（**理解后再开工**）

| 子 skill | 类型 | 是否有脚本 | 你的动作 | 产物 |
|---|---|---|---|---|
| `evi-valuation-router` | **planning** | ❌ | Read → 按规则推理 → 写 JSON | `valuation_method_matrix.json` |
| `evi-assumption-builder` | **planning** | ❌ | Read → 综合 Phase 1 数据 → 写 JSON + md | `assumption_ledger.json` / `growth_bridge.json` / `margin_bridge.json` |
| `evi-valuation-dcf` | **executor** | ✅ `dcf_calc.py` | Read → **必须** Bash 跑脚本 | `dcf_result.json` |
| `evi-valuation-ps` | **executor** | ✅ `ps_calc.py` | Read → **必须** Bash 跑脚本 | `ps_result.json` |
| `evi-valuation-comps` | **executor** | ✅ `comps_calc.py` | Read → **必须** Bash 跑脚本 | `comps_result.json` |
| `evi-valuation-peg` | **planning** | ❌ | Read → 推理 → 写 JSON | `peg_result.json` |
| `evi-valuation-ddm` | **planning** | ❌ | Read → 推理 → 写 JSON | `ddm_result.json` |
| `evi-reverse-valuation` | **planning** | ❌ | Read → 反推 → 写 JSON + md | `reverse_valuation.json` / `reports/reverse_valuation.md` |
| `evi-valuation-orchestrator` | **executor** | ✅ `aggregate.py` | Read → **必须** Bash 跑脚本汇总 | `final_segment_valuation.json` / `final_company_valuation.json` / `facets.json` |

**铁律**：标"executor"的 skill 必须用脚本。**严禁自己重写 DCF/PS/Comps/aggregate 的逻辑**——脚本已经处理了 WACC、TTM、time alignment、敏感性等坑，重写一遍只会出错。

---

## 4. 执行 DAG

### 4.1 single_segment 模式

```
                    Step 0: 输入门禁
                          │
                          ▼
                    Step 1: Router
                    Read evi-valuation-router/SKILL.md
                    → valuation_method_matrix.json (公司层)
                          │
                          ▼
                    Step 2: Assumptions
                    Read evi-assumption-builder/SKILL.md
                    → valuation/group/assumption_ledger.json
                    → valuation/group/{growth,margin,risk}_bridge.json
                          │
                          ▼
              ┌───────────┴───────────┐
              │                       │
         Step 3a (必脚本)         Step 3b (planning 方法)
         DCF / PS / Comps         PEG / DDM (如适用)
         Bash dcf_calc.py         Read SKILL.md → 写 *_result.json
         Bash ps_calc.py
         Bash comps_calc.py
              │                       │
              └───────────┬───────────┘
                          ▼
                    Step 4: Aggregate
                    Bash evi-valuation-orchestrator/scripts/aggregate.py
                    → valuation/group/final_company_valuation.json
                          │
                          ▼
                    Step 5: Reverse Valuation
                    Read evi-reverse-valuation/SKILL.md
                    → reports/reverse_valuation.md
                    → 抽取 rerate_triggers
                          │
                          ▼
                    Step 6: Final report + facets
                    写 reports/final.md（手写，结论优先）
                    Bash aggregate.py --emit-facets
                    → facets.json（含 rerate_triggers）
                          │
                          ▼
                    （结束。framework 自动 finalize）
```

### 4.2 multi_segment 模式（SOTP）

```
                    Step 0: 输入门禁
                          │
                          ▼
                    Step 1: Router (per segment)
                    每个 segment 都跑一次 router 推理
                    → valuation_method_matrix.json（含 N 个 segments + group_methods）
                          │
                          ▼
                    Step 2: Assumptions
                    每个 segment 并发：
                    ├─ segment_1: assumption-builder → valuation/seg_1/*
                    ├─ segment_2: assumption-builder → valuation/seg_2/*
                    └─ ... (N 个)
                    集团层：assumption-builder → valuation/group/* (WACC等)
                          │
                          ▼
                    Step 3: 多方法估值（segment × method 并发）
                    每个 (segment, method) 一个并发任务：
                    ├─ (seg_1, DCF)   → Bash dcf_calc.py --segment seg_1
                    ├─ (seg_1, PS)    → Bash ps_calc.py --segment seg_1
                    ├─ (seg_1, Comps) → Bash comps_calc.py --segment seg_1
                    ├─ (seg_2, DCF)   → Bash dcf_calc.py --segment seg_2
                    ├─ (seg_2, PEG)   → planning（Read SKILL.md → 写 peg_result.json）
                    └─ ...
                          │
                          ▼
                    Step 4: Per-segment aggregate
                    每个 segment：
                    Bash evi-valuation-orchestrator/scripts/aggregate.py --segment {seg}
                    → valuation/{seg}/final_segment_valuation.json
                          │
                          ▼
                    Step 5: Group aggregate (SOTP)
                    Bash aggregate.py --group
                    → valuation/group/final_company_valuation.json
                      含：Σ segment_EV + 投资资产 +/- 控股折价 + 净现金
                          │
                          ▼
                    Step 6: Reverse Valuation (集团层)
                    Read evi-reverse-valuation/SKILL.md
                    → reports/reverse_valuation.md
                    → 抽取 rerate_triggers
                          │
                          ▼
                    Step 7: 写报告 + facets
                    ├─ 写 reports/segments/{seg}_valuation.md（每分部一篇）
                    ├─ 写 reports/final.md（集团层结论 + SOTP 表 + 推荐）
                    └─ Bash aggregate.py --emit-facets
                       → facets.json（含 segments[] + rerate_triggers）
                          │
                          ▼
                    （结束。framework 自动 finalize）
```

### 4.3 数据闭环（Step 3 内每个估值方法都可触发）

任一估值脚本返回 `status == "missing_inputs"`：

```
1. 写 monitor/phase1_gap_request.json
2. 调 evi-information-search 补数据（最多 3 轮迭代）
3. 跑 evi-toolkit/scripts/format_facts.py 更新事实库
4. 重跑该 (segment, method)
5. 仍补不上 → 该方法标 "skipped"，其它方法继续
```

---

## 5. Step 1 —— Router 详解

**目标**：决定每个 segment（或公司）适用哪些估值方法、谁是 primary、谁是 cross_check。

**怎么做**：

```
1. Read .agents/skills/evi-valuation-router/SKILL.md
2. 按 router 给的判定规则（盈利状况 / 行业 / 业务模式）打 method 组合
3. 对每个 segment 写 1 行：
   {segment_id, methods:[{method, role, data_needs}]}
4. 写 valuation_method_matrix.json
```

**典型组合**：

| 业务画像 | primary | cross_check |
|---|---|---|
| 成熟现金牛（盈利稳定） | DCF | Comps + PE Band |
| 高增长 SaaS / 平台 | DCF | EV/Sales（PS） + Comps |
| 未盈利成长股 | EV/Sales（PS） | Comps |
| 周期股 | Comps（Mid-cycle） | DCF（去周期） |
| 派息稳态金融 | DDM | DCF |
| 投资性资产（如腾讯投资组合） | NAV / 上市公允价值 - 折价 | — |

---

## 6. Step 2 —— Assumptions 详解

**目标**：每个 segment 准备好估值方法所需的输入（增长率、毛利率、WACC、风险加项等）。

**怎么做**：

```
1. Read .agents/skills/evi-assumption-builder/SKILL.md
2. 综合输入：
   - reports/segments/{seg}.md（Phase 1 调研，含 quality.json.assumption_hints）
   - information/indexed_facts.json（结构化事实）
   - business_segments.json
   - valuation_method_matrix.json
   - base/financials/indicators/key_metrics.json
3. 对每个 segment 写：
   - valuation/{seg}/assumption_ledger.json   ← 所有数字 + 来源链路
   - valuation/{seg}/growth_bridge.json       ← 5-10 年增长率拆解
   - valuation/{seg}/margin_bridge.json       ← 利润率走廊
   - valuation/{seg}/risk_adjustment.json     ← WACC 风险加项
   - reports/segments/{seg}_assumptions.md    ← 人类可读
4. 集团层写 valuation/group/assumption_ledger.json（WACC、税率、永续增长 g）
```

**质量门**：增长率必须有 market-sizing（TAM/SAM/SOM）支撑或历史趋势 + 内生增长 g=ROIC×再投资率交叉验证。**禁止拍脑袋**。

---

## 7. Step 3 —— 多方法估值

### 7.1 必脚本方法（DCF / PS / Comps）

每个 (segment, method) 一次：

```bash
python3 .agents/skills/evi-valuation-dcf/scripts/dcf_calc.py \
    --data-dir data/{symbol_dir} --segment {seg_id}
# → valuation/{seg}/dcf_result.json

python3 .agents/skills/evi-valuation-ps/scripts/ps_calc.py \
    --data-dir data/{symbol_dir} --segment {seg_id} --peers TICK1,TICK2,TICK3
# → valuation/{seg}/ps_result.json

python3 .agents/skills/evi-valuation-comps/scripts/comps_calc.py \
    --data-dir data/{symbol_dir} --segment {seg_id} --peers TICK1,TICK2,TICK3
# → valuation/{seg}/comps_result.json
```

**严禁手算**。这些脚本已经处理了：

- TTM 时间对齐（peer 倍数必须取 FMP keyMetrics-TTM，不是实时报价）
- 敏感性 Tornado / 三场景
- 输入校验 + status="missing_inputs" 反馈

如果你看着脚本不存在 / 看着 stderr 报错，**先 Read 对应 SKILL.md 的 §脚本用法**，而不是自己写一份。

### 7.2 Planning 方法（PEG / DDM）

没有脚本，是因为输入很简单：

```
Read .agents/skills/evi-valuation-peg/SKILL.md
→ 用预测 EPS × 行业 PEG → 写 peg_result.json

Read .agents/skills/evi-valuation-ddm/SKILL.md
→ Gordon / 三阶段 DDM → 写 ddm_result.json
```

JSON schema 与 dcf_result.json 对齐（{bear, base, bull, method, confidence, status}），保证 Step 4 的 aggregate.py 能消费。

---

## 8. Step 4-5 —— Aggregate + Reverse

### 8.1 Per-segment aggregate（multi_segment）

```bash
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --segment {seg_id}
# → valuation/{seg}/final_segment_valuation.json
```

权重规则：primary=0.5，cross_check 平分剩 0.5；偏离 base >30% 砍半。

### 8.2 Group aggregate

```bash
# single_segment：直接合并多方法
python3 aggregate.py --data-dir ... --group

# multi_segment：SOTP
python3 aggregate.py --data-dir ... --group --sotp
```

### 8.3 Reverse Valuation

```
Read .agents/skills/evi-reverse-valuation/SKILL.md
→ 写 valuation/group/reverse_valuation.json
→ 写 reports/reverse_valuation.md
→ 抽 rerate_triggers，回填 facets.json
```

---

## 9. Step 6/7 —— 报告 + facets

### 9.1 reports/final.md（必写）

**结论优先 + 总分结构**。开头先给：

```markdown
## 结论

**判断**：低估 | 合理 | 高估
**Base 公允价**：HKD XXX（Bear: XXX / Bull: XXX）
**当前价**：HKD XXX
**Upside**：+XX% / -XX%

**核心逻辑**（3 句话内）：...

**关键风险**：...

**重估触发条件**：（前 3 条）...
```

下面再展开：估值方法对照、SOTP 表（multi_segment）、敏感性、与市场分歧。

### 9.2 facets.json

**不要手写**，由 aggregate.py 生成：

```bash
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --emit-facets
```

facets schema 见 [§13](#13-facetsjson-结构) 例子。

---

## 10. 数据闭环失败处理

| 缺口 | 补救动作 |
|---|---|
| Peer 倍数不足 3 家 | WebSearch 同行业公司，Bash `evi-toolkit/scripts/evi_fetch_data.py --peers ...` 拉倍数 |
| 行业增速 consensus 缺 | WebSearch 行业研报 → 补 indexed_facts.json |
| 分部利润率历史缺 | 重新解析 base/filings/ 下的财报附注 |
| 管理层指引未抽取 | 重 parse 最新电话会 |
| 3 轮仍补不上 | 该方法标 status="skipped"，其它方法继续。最终汇总用 partial。 |

---

## 11. 报告产出（最终交付）

### 11.1 multi_segment

```
reports/
├── company_overview.md         ← Phase 1（已有）
├── quality.md                  ← Phase 1（已有）
├── segments/
│   ├── {seg}.md                ← Phase 1（已有）
│   ├── {seg}_assumptions.md    ← Step 2（新）
│   └── {seg}_valuation.md      ← Step 7（新）
├── reverse_valuation.md         ← Step 5（新）
└── final.md                     ← Step 7（新，最终结论）
```

### 11.2 single_segment

```
reports/
├── company_overview.md
├── quality.md
├── reverse_valuation.md
└── final.md
```

---

## 12. facets.json 结构（看板核心）

```jsonc
{
  "company_name": "腾讯科技",
  "structure_type": "multi_segment",
  "currency_unit": "HKD per share",
  "fair_value": {"bear": 577.5, "base": 685.6, "bull": 859.3},
  "current_price": 441.0,
  "upside_pct": 55.5,
  "judgment": "低估",
  "n_segments": 4,

  "segments": [
    {
      "segment_id": "vas",
      "name": "增值服务",
      "fair_value_share": {"bear": 250, "base": 295, "bull": 345},
      "contribution_pct_base": 43.0,
      "primary_method": "DCF",
      "confidence": 0.75
    }
    // ... N 个
  ],

  "key_drivers": ["AI 商业化", "游戏稳态利润率"],
  "key_risks": ["监管", "海外宏观"],

  "rerate_triggers": [
    {"metric": "cloud_revenue_yoy", "threshold_down": 18, "threshold_up": 26}
  ],

  // Phase 1 quality 快照（在 Phase 1 已经写入，Phase 2 不要覆盖）
  "quality": { ... }
}
```

---

## 13. 持久化（一句话）

**不需要你做。** Framework 在你结束本轮后自动扫产物 + 调 persist + 推 DB。如果缺 required 文件，会以 user message 形式重新告诉你缺什么、怎么补。

> 只要你产出了 §11 列的报告 + §12 的 facets.json + 写了 changelog 摘要，就可以正常结束本轮。

> 开发者诊断（不是必经路径）：`python3 .agents/skills/evi-toolkit/scripts/wakeup_check.py --data-dir data/{symbol_dir} --check-only`

---

## 14. 汇报（结束本轮前）

告知用户：

- 估值结构：整体估值 / SOTP（N 分部）
- 每个估值方法用一段话说核心假设、逻辑、结论（**总分结构**，不要直接陷入数字细节）
- 最终估值（Bear/Base/Bull + 判断 + Upside）
- 各分部估值贡献（multi_segment）
- 方法一致性（CV：标准差/均值）
- 反向估值解读（市场已 price-in 了多少）
- 触发重估指标清单（前 3 条）
- 提示"可注册 Automation 持续监控"

---

## 15. 失败处理

- 某 (segment, method) 失败 + 3 轮闭环仍缺数据 → 跳过该方法
- 某 segment 全部方法失败 → SOTP 标 partial，在 final.md 单独说明
- 所有 segment 失败 → framework 会基于产物清单自动 partial / failed
- 数据闭环死循环（同一缺口反复补不上）→ 标 partial + 报告说明

---

## 附录 A：常见错误清单

| ❌ 错误行为 | ✅ 正确行为 |
|---|---|
| "Phase 2 复杂，我直接写一份完整的 DCF 脚本" | Bash 现成的 dcf_calc.py |
| 自己根据 P/E 推一个估值价 | 走 evi-valuation-comps 的 comps_calc.py（用 FMP TTM 倍数） |
| 跳过 router/assumption-builder 直接写 final.md | 必须先有 method_matrix.json + assumption_ledger.json |
| 在本 skill 调 persist_evi_report.py | framework 自动接管，不需要 |
| facets.json 手写 | 用 aggregate.py --emit-facets |
| Peer 倍数自己估一个 | comps_calc.py 必须传 --peers 让脚本去 FMP 拉 |
