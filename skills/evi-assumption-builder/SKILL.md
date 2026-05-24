---
name: evi-assumption-builder
description: "EVI 估值假设构建：把 indexed_facts 转化为分部级的增长率、利润率、WACC、风险折价等假设。主输出 reports/assumptions.md + valuation/{segment}/assumption_ledger.json (DCF 计算脚本会读)。"
---

# EVI Assumption Builder

## 职责

估值前的最关键一步：**把信息变成假设**。
拿到所有带索引的事实后，自主分析原因，形成可被估值方法直接消费的结构化假设账本。

> ❗ 每条假设必须 **绑定 fact_refs / display_refs**，否则下游估值不能使用。

## 输入

```text
data/{symbol_dir}/information/indexed_facts.json
data/{symbol_dir}/business_segments.json
data/{symbol_dir}/valuation_method_matrix.json
data/{symbol_dir}/base/financials/indicators/key_metrics.json
data/{symbol_dir}/base/validation/fmp_reconcile.json
data/{symbol_dir}/reports/segments.md     ← 段落画像
```

## 主输出（人类可读）

`data/{symbol_dir}/reports/assumptions.md`

```markdown
# 假设账本 — {display_name}

## 1. 集团层假设
| 变量 | 取值 | 依据 | 引用 |
|---|---|---|---|
| WACC | 9.0% | β=1.05, ERP=6%, Rf=2.7% | [1] |
| 永续增长 | 2.5% | 中长期 GDP 名义增速 | [2] |
| 税率 | 22% | 5 年加权有效税率 | [3] |

## 2. 各分部假设

### 2.1 cloud（云业务）
**收入增长（三场景）**

| 年份 | Bear | Base | Bull | 主要驱动 / 引用 |
|---|---|---|---|---|
| 2026E | 18% | 22% | 28% | 国央企续约 + AI 算力需求 [4][5] |
| 2027E | 15% | 19% | 25% | ... |
| 2028E | 12% | 16% | 22% | ... |
| 2029E | 10% | 14% | 19% | 增长收敛到行业均值 [6] |
| 2030E | 8%  | 12% | 16% | ... |

**EBIT margin 演化**：
- 2025 实际 11%，2026 升至 13/15/17%（bear/base/bull），驱动：高毛利 AI 收入占比 [7]、低毛利项目收缩 [8]

**风险折价**：
- WACC 溢价：+50bps（执行风险）
- terminal_growth：2.5%
- execution_risk_factor：0.95

### 2.2 games（游戏）
...

## 3. 假设之间的耦合关系
（如：游戏增速放缓如果同时叠加云业务支出加速 → 需要把对应组合写进 bear/bull）

## 4. 不能确定的假设
列出 5 项最敏感、最不确定的假设，建议看板用敏感性矩阵展示

---

## Facts Index

[1] fact_id=fact_assump_001 | segment=group | reliability=high
    text: 公司当前杠杆率（D/E）约 0.18，β 历史均值 1.05。
    source: base/fmp/profile.json
```

## 旁路结构化产物

每个 segment 一组（DCF 计算脚本必需）：

```text
data/{symbol_dir}/valuation/{segment_id}/assumption_ledger.json
data/{symbol_dir}/valuation/{segment_id}/growth_bridge.json
data/{symbol_dir}/valuation/{segment_id}/margin_bridge.json
data/{symbol_dir}/valuation/{segment_id}/risk_adjustment.json
```

集团：

```text
data/{symbol_dir}/valuation/group/assumption_ledger.json
```

### 文件 schema 见 `evi-valuation-dcf/SKILL.md`（DCF 脚本的输入合同）

简表：

```jsonc
// growth_bridge.json
{
  "segment_id":"cloud",
  "rows":[
    {"year":"2025A","revenue":100000},
    {"year":"2026E","revenue":{"bear":118000,"base":122000,"bull":128000}}
  ]
}

// margin_bridge.json
{
  "segment_id":"cloud",
  "rows":[
    {"year":"2026E","ebit_margin":{"bear":13.0,"base":15.0,"bull":17.0},
     "capex_to_rev_pct":{"bear":8.0,"base":7.0,"bull":6.5}}
  ]
}

// risk_adjustment.json
{"wacc_premium_bps":50,"terminal_growth_pct":2.5,"execution_risk_factor":0.95}

// assumption_ledger.json — 摘要型，便于审计；DCF 脚本会回填 key_assumptions
{
  "segment_id":"cloud",
  "currency":"RMB million",
  "assumptions":[
    {"assumption_id":"a1","variable":"revenue_growth_2026E_base","value":22.0,"unit":"%","fact_refs":["fact_cloud_001"],"display_refs":[4]}
  ]
}
```

## 硬规则

- 管理层话术不能直接映射成增长率（**禁止**："管理层说双位数 → 假设 15%"）。
- 三场景（bear / base / bull）都必须存在。
- 每条假设至少 1 个 fact_refs / display_refs。
- 低可靠性事实不能单独支撑核心估值假设；至少需 1 条 medium+ 或 2 条 low。
- 集团层 assumption_ledger 必须含：`wacc_pct` / `tax_rate_pct` / 公司层永续增长率。
