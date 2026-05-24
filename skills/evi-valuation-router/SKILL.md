---
name: evi-valuation-router
description: "EVI 估值方法路由：基于业务特征选择 DCF/PS/PEG/DDM/Comps，输出 reports/valuation_router.md + valuation_method_matrix.json。"
---

# EVI Valuation Router

## 职责

按业务分部的成熟度、可见性、披露完整度、盈利特征，决定**主估值方法 (primary) + 交叉验证方法 (cross_check)**，并显式声明每种方法所需的数据。
避免下游"先估值后找数据"的反工程。

## 输入

```text
data/{symbol_dir}/business_segments.json
data/{symbol_dir}/base/financials/indicators/key_metrics.json
data/{symbol_dir}/information/indexed_facts.json (可选)
```

## 主输出

`data/{symbol_dir}/reports/valuation_router.md`

```markdown
# 估值方法路由 — {display_name}

## 1. 全公司方法组合
- 集团：SOTP（每分部独立估值后汇总） + DDM（持续派息部分）+ Reverse（市场隐含预期）

## 2. 各分部方法选择
### 2.1 cloud（云业务）
- **Primary：DCF**（已盈利 + 收入清晰可建模）
- **Cross-check：PS**（行业 peer 倍数）+ **Comps**
- 数据需求：
  - segment_revenue_history（≥6 期）
  - growth_bridge_inputs
  - segment_margin_estimate
  - peer_ps_multiples（≥3 家 peer）
  - wacc_inputs

### 2.2 games（游戏）
...

## 3. 例外说明
（哪些 segment 因披露不足只能用 group 级方法）

## 4. 集团层
- DDM：用于反映持续派息政策
- Reverse：从市值反推市场对增长率/利润率/WACC 的隐含预期

---

## Facts Index

[1] fact_id=fact_router_001 | segment=cloud | reliability=high
    text: 云业务已实现持续盈利，2025 经营利润率约 11%。
    source: doc_2025_annual#segment_disclosure
```

## 旁路结构化产物

`data/{symbol_dir}/valuation_method_matrix.json`：

```jsonc
{
  "schema_version": 1,
  "symbol": "{symbol}",
  "matrix": [
    {
      "segment_id": "cloud",
      "methods": [
        {"method":"DCF",   "role":"primary",
         "data_needs":["segment_revenue_history","growth_bridge_inputs","segment_margin_estimate","wacc_inputs","terminal_growth_basis"]},
        {"method":"PS",    "role":"cross_check",
         "data_needs":["forward_revenue","peer_ps_multiples","revenue_quality"]}
      ]
    }
  ],
  "group_methods": ["SOTP","DDM","Reverse"]
}
```

## 默认选择规则

| 业务特征 | Primary | Cross-check |
|---|---|---|
| 高增长、未盈利、收入清晰 | PS | DCF（场景化） |
| 高增长、已盈利、利润可预测 | DCF | PEG, Comps |
| 成熟稳定、现金流稳定 | DCF | DDM, Comps |
| 现金奶牛 + 持续派息 | DDM | DCF |
| 周期股 / 资本密集 | EV/EBITDA Comps | DCF（横跨周期） |
| 多业务集团 | SOTP（每段独立） | Reverse |

## 硬规则

- 每个分部至少 1 个 primary + 1 个 cross_check。
- `data_needs` 是**完整字段名清单**，给下游 `evi-assumption-builder` 当输入合同。
- 若 segment 的 `revenue_disclosure: "no"`，**禁止** PS / Comps 当 primary，改用 group 级 SOTP / Reverse。
