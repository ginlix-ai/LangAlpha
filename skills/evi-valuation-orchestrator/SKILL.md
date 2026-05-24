---
name: evi-valuation-orchestrator
description: "EVI 最终估值汇总：消费每分部的 DCF/PS/PEG/Comps/DDM 结果与 reverse_valuation，按权重输出 final_segment_valuation 和 final_company_valuation。同时写 reports/final.md（投研结论）和 facets.json（看板字段）；最后调 persist_evi_report.py 写库。"
---

# EVI Valuation Orchestrator

## 职责

汇总每个分部的多种估值结果，输出**分部最终估值** + **集团 SOTP 估值**，并完成持久化。
**也是流水线最后一步**——把 reports/*.md 全部产出汇总到看板。

## 输入

```text
data/{symbol_dir}/valuation/{segment_id}/*_result.json
data/{symbol_dir}/valuation/group/reverse_valuation.json
data/{symbol_dir}/valuation/group/assumption_ledger.json
data/{symbol_dir}/business_segments.json
data/{symbol_dir}/valuation_method_matrix.json
data/{symbol_dir}/reports/*.md
data/{symbol_dir}/base/CHECKLIST.json
data/{symbol_dir}/information/indexed_facts.json
```

## Step 1 — 给每个 segment 选权重并合并

权重选择规则（缺省）：
- primary 方法权重 0.5；cross_check 平均瓜分剩余 0.5
- cross_check 与 primary 偏离 > 30% → 该 cross_check 权重砍半
- confidence < 0.4 → 权重砍半

调用脚本：

```bash
python3 .agents/skills/evi-valuation-orchestrator/scripts/aggregate.py \
    --data-dir data/{symbol_dir} --segment cloud
```

输出：`valuation/{segment_id}/final_segment_valuation.json`

## Step 2 — 集团 SOTP

把所有 segment 的 final_values 按本币换算后求和；扣减总部成本、净债、少数股东权益、加上现金。

输出：`data/{symbol_dir}/valuation/group/final_company_valuation.json`

```jsonc
{
  "schema_version": 1,
  "valuation_date": "2026-05-21",
  "currency": "HKD",
  "by_segment": [...],
  "consolidation_adjustments": [...],
  "final_values": { "bear":..., "base":..., "bull":... },
  "shares_outstanding_m": 9180.0,
  "fair_value_per_share": { "bear":..., "base":..., "bull":... },
  "current_price": 439.0,
  "upside_pct": 56.2,         // 必须是 number（非 dict）
  "judgment": "高估 | 合理 | 低估",
  "confidence": 0.72
}
```

> ⚠️ **`upside_pct` 必须是 number**（一个 base case 的标量）。如果想给三场景空间，命名成 `upside_pct_scenarios:{bear,base,bull}`。

## Step 3 — facets.json（看板专用）

写 `data/{symbol_dir}/facets.json`：

```jsonc
{
  "company_name":   "腾讯科技",
  "currency_unit":  "HKD per share",
  "fair_value":     { "bear":577.5, "base":685.6, "bull":859.3 },
  "current_price":  439.0,
  "upside_pct":     56.2,
  "judgment":       "低估",
  "n_segments":     7,
  "key_drivers":    ["云AI增长","游戏稳态利润率"],
  "key_risks":      ["监管","海外宏观"]
}
```

> 这是看板真正读的"快照"。命名稳定、字段简单，**前端就指望这一个文件**。

## Step 4 — reports/final.md

```markdown
# 最终估值结论 — {display_name}

## 1. 一句话结论
当前价 439 HKD 较 base case 685.6 HKD 折价 36%，**判断：低估**（置信 0.72）。

## 2. 估值范围
| 场景 | 公允价值 (HKD/股) | 隐含上行 |
|---|---|---|
| Bear | 577.5 | +31.5% |
| Base | 685.6 | +56.2% |
| Bull | 859.3 | +95.7% |

## 3. 分部贡献（base case，百万元 → HKD）
（柱状图替代：用文字展示每个分部的占比）

## 4. 关键驱动 vs 关键风险
- 驱动：[列出最重要 3 项，引用 fact 编号]
- 风险：[列出最重要 3 项]

## 5. 触发重估的关键事件
- 季度 earnings、监管发布、AI 商业化披露
- 任一驱动假设偏离 > 20% → 触发 evi-revaluation-updater

## 6. 与 reverse 估值对比
（市场隐含 vs 我们的 base case；解读差异）
```

## Step 5 — 持久化

```bash
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \
    --entry-id {entry_id} \
    --data-dir data/{symbol_dir} \
    --display-name "{display_name}" \
    --symbol "{entry_key}" \
    --market "{market}"
```

> persist_evi_report.py 会自动收集所有 reports/*.md + facets.json + checklist.json + 旧版 valuation/ 文件，组装成 v2 payload 写库。

## 操作约束

- 每个 segment 至少 1 个 method 的 status=ok。否则该 segment 标 `partial`。
- 最终 final_values 三场景缺一不可。
- `executive_summary` / `final.md` 必须基于事实引用，至少 2 个 fact_refs。
- 完成后**必须**重新跑一次 build_checklist + format_facts，确保看板拿到的索引是最新的：

```bash
python3 .agents/skills/evi-toolkit/scripts/format_facts.py    --data-dir data/{symbol_dir}
python3 .agents/skills/evi-toolkit/scripts/build_checklist.py --data-dir data/{symbol_dir} --required-periods 6
```
