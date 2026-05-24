# 定性分析结构化参数输出 Schema（v2.0）

> 定义定性分析模块输出的标准化参数。下游投资策略（龟龟、烟蒂等）通过读取该参数表获取定性结论，
> 各策略可在自己的 interface 文件中定义额外的映射规则。
>
> **v2.0 重大变更**：结构化 json 从分析报告中**提取**而来（而非直接输出）。
> Agent 先写报告（`reports/d{N}_report.md`），报告中用脚注内联引用，
> 最后从报告提取结构化数据写入 `structured/d{N}.json`。
> References 新增 `section` 字段，指向报告中的章节锚点，支持前端跳转。

---

## 参数定义

### D1：商业模式与资本特征

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| business_model_clarity | enum | 清晰且已验证 / 清晰但未充分验证 / 模糊 | 商业模式是否可用一句话概括并经历周期验证 |
| capital_intensity | enum | capital-light / capital-hungry | 维持盈利规模的持续性资本消耗 |
| collection_mode | enum | 先款后货 / 订阅预收 / 先货后款 / 垫资回收 | 典型交易的现金流时间线 |
| cash_impact | enum | 正贡献 / 中性 / 负担 | 收款模式对现金状况的影响 |

### D2：竞争优势与护城河

**D2.1 行业地图**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| market_structure | enum | 垄断 / 寡头 / 垄断竞争 / 充分竞争 | 行业竞争格局 |
| market_cr4 | float / null | 百分比 | 前4名市场份额合计（数据可得时填写） |
| entry_barrier | enum | 高 / 中 / 低 | 行业进入壁垒综合评估 |

**D2.2 量化验证**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| roe_5y_avg | float | 百分比 | 5年平均ROE |
| moat_existence | enum | 存在 / 可能存在 / 不存在 | 基于量化数据的竞争优势存在性判断 |
| moat_evidence_strength | enum | 强证据 / 中等证据 / 弱证据 | 量化证据的强度 |

**D2.3 护城河来源（双框架）**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| moat_type | string | "[非技术] xxx + [技术] xxx" | 框架A：双层护城河描述（技术层可为"不适用"） |
| moat_framework_primary | enum | A / B | 主分析框架：A=双层(科技)，B=Greenwald(传统) |
| supply_side_rating | enum | 强 / 较强 / 中等 / 弱 / 不适用 | 框架B：供给侧优势（成本、资源、垂直整合） |
| demand_side_rating | enum | 强 / 较强 / 中等 / 弱 / 不适用 | 框架B：需求侧优势（品牌、转换成本、网络效应） |
| scale_economy_rating | enum | 强 / 较强 / 中等 / 弱 / 不适用 | 框架B：规模经济效益 |
| moat_flywheel | bool | true / false | 是否形成复合护城河飞轮 |

**D2.4 虚假优势辨析**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| false_advantages | list | [string] | 被排除的"虚假优势"列表及理由 |

**D2.5 竞争对手对比**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| competitors | list | [{name, ticker}] | 主要竞争对手列表 |
| competitor_ranking | string | 自由文本 | 目标公司 vs 对手的综合排名 |
| advantage_gap_sustainability | enum | 高 / 中 / 低 | 优势差距可持续性 |

**D2.6 可持续性与综合**

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| pricing_power | enum | 强 / 中 / 弱 | 主动提价能力 |
| human_capital_dep | enum | 系统型 / 人才型 | 竞争优势是否已沉淀为系统性能力 |
| moat_sustainability | enum | 高可持续 / 中等可持续 / 低可持续 | 护城河可持续性判断 |
| moat_rating | enum | 强 / 较强 / 中 / 弱 | 护城河综合评价 |
| moat_monitor_kpis | list | [{kpi, current, threshold}] | 未来3-5年护城河健康度跟踪指标 |

### D3：外部环境

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| cyclicality | enum | 强周期 / 弱周期 / 非周期 | 收入和盈利波动幅度 |
| cycle_position | enum | 底部 / 中段 / 顶部 / 不适用 | 当前周期位置（仅强周期适用） |
| regulatory_risk | enum | 低 / 中 / 高 | 监管与政策风险 |
| industry_keywords | list | [string] | 行业监控关键词 |

### D4：管理层与治理

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| governance_flags | list | [{flag_name: status}] | 治理风险逐项标志 |
| management_rating | enum | 优秀 / 合格 / 损害价值 / 观察期 | 管理层综合评价 |
| capital_allocation_record | string | 自由文本 | 资本配置历史简要评价 |
| related_party_risk | enum | 低 / 中 / 高 | 关联交易风险 |

### D5：MD&A 解读

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| mda_credibility | enum | 高 / 中 / 低 | MD&A 可信度 |
| mda_impact | enum | 正面 / 中性 / 负面 | MD&A 对投资判断的影响 |
| mda_forward_guidance | enum | 有量化 / 仅方向性 / 无 | 前瞻性指引质量 |
| distribution_signal | string | 自由文本 | 从 MD&A 提取的分红/回购意向摘要 |

### D6：控股结构（条件触发）

| 参数 | 类型 | 值域 | 说明 |
|------|------|------|------|
| holding_structure | bool | true / false | 是否适用控股结构分析 |
| sotp_value_mm | float / null | 百万元 | SOTP 估值（不适用时为 null） |
| sotp_discount_pct | float / null | 百分比 | 控股折价率（不适用时为 null） |

---

## 使用说明

### 独立运行模式
参数表附在完整分析报告末尾，供阅读者快速定位结论。

### 被策略调用模式
下游策略通过读取参数表获取定性结论，不需要解析报告正文。各策略在自己的 interface 文件中定义映射规则，例如：

- **龟龟策略**：`strategies/turtle/references/factor_interface.md` 定义 output_schema → 龟龟因子参数的映射
- **烟蒂策略**：`strategies/cigarbutt/references/cigarbutt_interface.md` 定义 output_schema → 烟蒂支柱评分的映射

### 版本兼容
- Schema 版本号：v2.0
- v1.2 → v2.0 变更：
  - 结构化 json 从**直接输出**改为从**报告提取**
  - references 新增 `section` 字段（报告章节锚点）
  - 文件路径变更：`data/{symbol}/d{N}.json` → `data/{symbol}/structured/d{N}.json`
  - 新增报告文件：`data/{symbol}/reports/d{N}_report.md`
  - 移除顶层 `analysis` 字段（完整分析在报告中）
- v1.1 → v1.2 变更：所有维度新增 `references` 字段
- v1.0 → v1.1 变更：D2 新增行业地图等多项参数
- 新增参数向后兼容（下游策略忽略未知参数）
- 删除/改名参数需同步更新所有策略的 interface 文件

---

## 溯源引用（References）— v2.0

> 金融分析要求可溯源：每个核心结论必须能追溯到原始数据来源。
> v2.0 中，references 从报告脚注提取，新增 `section` 字段支持前端跳转到报告章节。

### 每个维度输出必须包含 `references` 字段

```json
{
  "score": 8,
  "title": "D1 商业模式与资本特征",
  "metrics": { ... },
  "summary": "...",
  "key_findings": [ "..." ],
  "risks": [ "..." ],
  "confidence": 0.85,
  "references": [
    {
      "id": "ref_d1_01",
      "type": "data",
      "title": "FMP 利润表 2021-2025",
      "source": "raw/incomeStatement.json",
      "section": "收入质量与核心盈利拆解",
      "detail": "近5年营收从5601亿增长到7323亿，CAGR 6.93%",
      "cited_by": ["key_findings[0]", "metrics.core_revenue_growth_pct"]
    },
    {
      "id": "ref_d1_02",
      "type": "calculation",
      "title": "核心经营利润增速计算",
      "source": "analysis",
      "section": "收入质量与核心盈利拆解",
      "detail": "剥离投资收益和汇兑损益后：2023=-15.2%, 2024=+16.3%, 2025=+14.6%",
      "logic": "核心经营利润 = 税前利润 - 投资收益 - 公允价值变动 - 汇兑损益\n增速 = (本期-上期)/|上期|",
      "cited_by": ["metrics.core_op_profit_growth_pct", "key_findings[1]"]
    },
    {
      "id": "ref_d1_03",
      "type": "external",
      "title": "年报 MD&A - 收入拆分",
      "source": "https://www.hkexnews.hk/...",
      "section": "商业模式可视化描述",
      "detail": "增值服务收入占比从65%降至50%，金融科技占比升至35%",
      "cited_by": ["summary"]
    }
  ]
}
```

### Reference 字段定义

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| id | string | ✅ | 唯一标识，格式 `ref_{dimension}_{seq}`，如 `ref_d1_01` |
| type | enum | ✅ | `data` / `calculation` / `external` / `knowledge` / `user_memo` |
| title | string | ✅ | 简短标题（前端悬浮框显示） |
| source | string | ✅ | 来源路径（见下方说明） |
| section | string | ✅ | **报告中的 H2 标题**（去掉序号前缀），前端用于跳转定位 |
| detail | string | ✅ | 引用摘要（1-3 句话） |
| logic | string | ❌ | 计算逻辑（type=calculation 时填写，可多行公式） |
| cited_by | list[string] | ✅ | 被哪些结论引用：`key_findings[0]` / `metrics.xxx` / `score` / `risks[1]` |

### Reference type 说明

| type | 何时使用 | source 示例 |
|------|---------|------------|
| `data` | 直接引用原始数据文件 | `raw/incomeStatement.json`, `raw/keyMetrics.json` |
| `calculation` | 涉及推导计算逻辑 | `analysis`（分析正文中的计算过程） |
| `external` | 引用外部网页/报告 | `https://...`, `SEC 10-K FY2024` |
| `knowledge` | 引用框架知识文档 | `knowledge/d1_business_model.md` |
| `user_memo` | 引用用户上传的备忘录 | `memo://xxx`（平台 memo store 的 key） |

### 最低引用要求

| 结论类型 | 最低引用数 | 示例 |
|---------|-----------|------|
| score（维度打分） | 至少 2 个 ref | 打分依据至少需要 data + calculation |
| key_findings 每条 | 至少 1 个 ref | 每个核心发现要说数据从哪来 |
| metrics 中的量化值 | 至少 1 个 ref | 如 core_revenue_growth_pct 要引用利润表 |
| risks 每条 | 至少 1 个 ref | 风险判断也要有依据 |

### 如何引用 sandbox 中的文件

```
source 字段使用相对于 data/{symbol}/ 的路径：
  "raw/incomeStatement.json"     → data/0700_HK/raw/incomeStatement.json
  "engine_result.json"           → data/0700_HK/engine_result.json
  "financial_context.md"         → data/0700_HK/financial_context.md

对于知识文档：
  "knowledge/valuation_methods.md"  → .agents/skills/sirius-valuation/knowledge/valuation_methods.md
```

---

*通用定性分析模块 v1.2 | 结构化参数输出 Schema*
