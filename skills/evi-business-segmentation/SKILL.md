---
name: evi-business-segmentation
description: "EVI 业务分部识别：基于披露口径与 MD&A，识别业务线、披露范围与数据缺口，输出 reports/segments.md（人类可读）+ business_segments.json（下游消费）。"
---

# EVI Business Segmentation

## 职责

识别公司的业务线、披露口径与数据缺口，让 `evi-valuation-router` 决定每个分部的估值方法。

## 输入

```text
data/{symbol_dir}/base/catalog.json
data/{symbol_dir}/base/financials/segments/segment_data.json
data/{symbol_dir}/base/financials/mdna/*.md
data/{symbol_dir}/reports/data.md                  ← 已有数据画像
data/{symbol_dir}/information/indexed_facts.json   (可选)
```

## 主输出（人类可读）

`data/{symbol_dir}/reports/segments.md`

```markdown
# 业务分部识别 — {display_name}

## 1. 披露口径概览
（说明公司报表中的一级分部、二级分部，引用最新年报/季报中的披露表格）

## 2. 各业务分部画像
### 2.1 增值服务 — 网络游戏
- 收入：2023=172000 → 2024=180000 → 2025=195000 → 2026Q1=53000（同比 +12%）[1]
- 利润：EBIT margin 维持 30%+ [2]
- 主要驱动：国内长青游戏 + 海外发行 + 新品周期
- 风险：版号节奏 / 用户老化
- 候选估值方法：DCF (primary), PEG (cross_check)

### 2.2 金融科技及企业服务 — 云业务
...

## 3. 数据缺口
- 国际游戏分部毛利率未单独披露
- 云业务的"AI 高毛利收入"占比仅在电话会模糊提及

## 4. 推荐分部颗粒度
（决定每个 segment_id 的拆分粒度——保守原则：与披露一致）

---

## Facts Index

[1] fact_id=fact_segments_001 | segment=games | reliability=high | topic=segment_revenue
    text: ...
    source: doc_2026Q1#segment_breakdown
```

## 旁路结构化产物（下游消费）

`data/{symbol_dir}/business_segments.json`：

```jsonc
{
  "schema_version": 1,
  "symbol":   "{symbol}",
  "company":  "公司名称",
  "fiscal_year": "2025",
  "segments": [
    {
      "segment_id":   "cloud",
      "name":         "云业务（金融科技及企业服务子项）",
      "reported_under": "金融科技及企业服务",
      "revenue_disclosure": "yes_grouped",
      "profit_disclosure":  "grouped_with_others",
      "asset_disclosure":   "no",
      "capex_disclosure":   "yes",
      "data_gaps":          ["segment_ebit_margin","standalone_assets"],
      "candidate_methods":  ["DCF", "PS", "Comps"],
      "drivers":            ["enterprise_demand","ai_workload","product_mix"],
      "key_kpis":           ["paying_customers","arr","gross_margin"],
      "fact_refs":          ["fact_segments_001","fact_segments_004"]
    }
  ]
}
```

## 操作约束

- 与披露口径一致——不要凭空二级拆分。
- `candidate_methods` 是**初步建议**；最终由 `evi-valuation-router` 锁定。
- 每个 segment 至少 1 条 `fact_refs`（指向 reports/segments.md 的 Facts Index 编号）。
