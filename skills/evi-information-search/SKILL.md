---
name: evi-information-search
description: "EVI 信息搜集：基于 base/catalog + 业务分部，自动覆盖财报/电话会/研报/行业/产品级数据。主输出 reports/facts.md（含 Facts Index 段落），由 format_facts.py 自动抽成 information/indexed_facts.json。"
---

# EVI Information Search

## 核心职责

把对估值有用的所有信息**结构化**成带索引编号、带可靠性分层、带 segment 关联的事实库。
**不是**让用户提问后再去搜，而是根据公司、业务、估值方法**主动覆盖**重要信息。

## 输入

```text
data/{symbol_dir}/base/catalog.json
data/{symbol_dir}/base/CHECKLIST.json                          ← 看缺口
data/{symbol_dir}/base/financials/segments/segment_data.json
data/{symbol_dir}/base/financials/indicators/key_metrics.json
data/{symbol_dir}/business_segments.json     (若已有)
data/{symbol_dir}/valuation_method_matrix.json (若已有)
```

## 主输出（人类可读报告）

`data/{symbol_dir}/reports/facts.md`

```markdown
# 事实索引说明 — {display_name}

## 1. 搜索范围
- 本地材料：base/catalog.json 中的财报、研报、电话会
- 外部材料：[列出本次新搜到的来源]

## 2. 覆盖度（按业务分部）
| 分部 | 已收集事实数 | 高可靠性 | 备注 |
|---|---|---|---|
| games   | 12 | 75% | 含 2 篇研报、4 期电话会 |
| cloud   | 14 | 71% | 含 GS 报告 |
| ...

## 3. 关键事实摘要（按主题）
（按 topic 聚合，每条事实都带 [N] 引用编号；不要重复罗列，只挑最关键的 5-10 条）

### 3.1 收入与增长
腾讯云 2026Q1 收入同比 +21%，主要来自国央企客户大单 [3]。研报上调 2026 全年增速预测至 25-28% [11]。

### 3.2 利润率与成本
游戏业务毛利率持续高位（>62%），但销售费用同比+18%，说明依赖营销驱动 [5][6]。

### 3.3 风险与不确定性
监管风险：未成年人保护新规可能影响青少年用户付费 [9]。

## 4. 信息缺口
- 海外游戏分部缺乏区域级毛利率披露
- AI Agent 商业化进度无第三方验证

## 5. 来源可靠性约定
| 等级 | 类型 |
|---|---|
| high   | 财报、公告、电话会原文、公司投资者材料 |
| medium | 卖方研报、行业报告、第三方数据库 |
| low    | 新闻、论坛、未经验证网页 |

---

## Facts Index

[1] fact_id=fact_cloud_001 | segment=cloud | reliability=high | topic=cloud_revenue_growth | valid_for=DCF,PS
    text: 腾讯云 2026Q1 收入同比 +21%，环比+5%。
    source: doc_2026Q1#mdna_fbs
    quote: "金融科技及企业服务分部本季收入达 549 亿元，其中云业务延续高增长 ..."

[2] fact_id=fact_cloud_002 | segment=cloud | reliability=medium | topic=cloud_revenue_growth | valid_for=DCF,PS,Comps
    text: GS 2026-04-30 报告将腾讯云 2026 全年增速预测从 18% 上调至 25%。
    source: research/2026-04-30_goldman_sachs.pdf
    url: https://...

...
```

## 旁路结构化产物

`data/{symbol_dir}/information/indexed_facts.json` —— 由 `format_facts.py` 从 reports/*.md 自动抽取，**Agent 不直接编辑**。

`data/{symbol_dir}/information/search_plan.json`（可选，便于审计 / Monitor 复用）：

```jsonc
{
  "by_segment": {
    "cloud": ["revenue_history","margin_evolution","peers_ps_multiples","industry_growth"],
    "games": ["pipeline_titles","grossing_charts","regulation_risk","peers_pe"]
  },
  "external_sources_used": ["FMP","SerpAPI:research:goldman","WebSearch:Sensor Tower"],
  "version": 1
}
```

## 执行流程

1. 读 `business_segments.json` + `valuation_method_matrix.json`，按"业务 × 方法"列出**最少必需事实清单**（写入 `search_plan.json`）。
2. 优先用本地 base 材料覆盖；覆盖不上的再外搜。
3. 把所有发现写入 reports/facts.md（含 Facts Index）。
4. 跑：

```bash
python3 .agents/skills/evi-toolkit/scripts/format_facts.py --data-dir data/{symbol_dir}
```

5. 如果有新落地的本地材料，登记到 catalog：

```bash
python3 .agents/skills/evi-toolkit/scripts/update_catalog.py --data-dir data/{symbol_dir} --rebuild
```

## 硬规则

- 每个分部至少 1 条 high 可靠性事实。
- 涉及估值假设的每个关键参数（增长率、利润率、终端增长）都要有 ≥1 条 medium+ 事实背书。
- 增量调用时（被 `evi-monitor` 触发）：**不修改旧 fact_id**，只在 reports/facts.md 末尾追加新条目，再跑 format_facts.py。
