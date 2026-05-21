---
name: sirius-valuation
description: "Sirius 七维度价值分析（D1~D7）：商业模式、护城河、外部环境、管理层、MD&A、综合评估、估值修正。报告优先，结构化数据从报告提取。"
---

# Sirius 价值分析

## 核心原则

1. **报告是第一产出物**：每个维度先输出分析报告（md），这是分析的底稿
2. **结构化是二次加工**：从报告中提取结构化 json，给看板和策略消费
3. **内联溯源**：报告中每个判断带脚注引用，结构化数据通过 `section` 字段链接回报告章节

---

## 执行流程

### Phase 1：数据获取

```bash
scripts/fetch_data.py --symbol {symbol} --market {market}
```
- 读取 `data/{symbol}/financial_context.md`
- 读取 `data/{symbol}/engine_result.json`

### Phase 2：维度分析（输出报告）

对每个维度（D1-D7），执行两步：

#### Step 1：写分析报告

按 DAG 顺序执行（D1-D5 可并行 → D6 → D7）。

读取对应的知识文档（`knowledge/d{N}_xxx.md`），文档中定义了：
- 这个维度要回答的**核心问题**
- 每个问题的**背景上下文**和**为什么重要**
- 需要关注的**数据来源**

Agent 基于问题清单 + 财务数据，**自主推导分析逻辑链**，输出一篇完整的分析报告。

**报告写作要求**（通用，此处统一说明一次）：
- 用 H2 标题组织，每个标题对应一个核心问题
- 每个判断/观点用脚注 `[^ref_id]` 标记数据来源或推导逻辑
- 末尾 `## References` 区域定义所有脚注
- 需有"评分与逻辑链"章节，写出分数和推导过程
- 需有"核心发现"和"风险提示"章节
- 格式规范详见 `knowledge/report_format.md`

输出路径：`data/{symbol}/reports/d{N}_report.md`

#### Step 2：结构化提取（所有 Step1 完成后并行）

**D1-D7 全部报告写完后**，统一对 7 篇报告并行提取结构化 json。

读取报告，提取结构化 json 写入 `data/{symbol}/structured/d{N}.json`。

提取规则（通用工具逻辑）：
- `score` → 从"评分与逻辑链"章节
- `metrics` → 从报告中的表格/加粗指标
- `key_findings` → 从"核心发现"列表
- `risks` → 从"风险提示"列表
- `summary` → 1-2 句概括
- `references` → 从脚注解析，每个 ref 附加 `section` 字段（所在 H2 标题）

Schema 定义：`knowledge/output_schema.md`

### Phase 3：持久化

```bash
python3 scripts/persist_entry.py --entry-id {entry_id} --data-dir data/{symbol_dir}
```

---

## 执行 DAG

```
Phase 1: fetch_data.py
    ↓
Phase 2 - Step1（写报告）:
  D1 Step1 ─┐
  D2 Step1 ─┤
  D3 Step1 ─┼─→ D6 Step1 → D7 Step1
  D4 Step1 ─┤
  D5 Step1 ─┘
    ↓
Phase 2 - Step2（结构化提取，全部 Step1 完成后并行）:
  D1~D7 Step2 并行执行
    ↓
Phase 3: persist_entry.py
```

---

## 文件目录结构

```
data/{symbol}/
├── financial_context.md
├── engine_result.json
├── raw/                          # FMP 原始数据
├── reports/                      # Phase 2 Step 1 产出
│   ├── d1_report.md
│   ├── d2_report.md
│   ├── d3_report.md
│   ├── d4_report.md
│   ├── d5_report.md
│   ├── d6_report.md
│   └── d7_report.md
└── structured/                   # Phase 2 Step 2 产出
    ├── d1.json
    ├── d2.json
    ├── d3.json
    ├── d4.json
    ├── d5.json
    ├── d6.json
    └── d7.json
```

---

## 知识文档

### 维度分析指南（每个文档只提问题 + 背景）
- `knowledge/d1_business_model.md` — D1 商业模式与资本特征
- `knowledge/d2_moat.md` — D2 竞争优势与护城河
- `knowledge/d3_environment.md` — D3 外部环境
- `knowledge/d4_management.md` — D4 管理层与公司治理
- `knowledge/d5_forward_guidance.md` — D5 MD&A 解读与前瞻
- `knowledge/d6_comprehensive.md` — D6 综合评估与投资论点
- `knowledge/d7_qualitative_adjustment.md` — D7 定性调整与估值修正

### 通用参考
- `knowledge/report_format.md` — 报告格式与脚注规范
- `knowledge/output_schema.md` — 结构化参数 Schema（v2.0）
- `knowledge/valuation_methods.md` — 6 种估值方法完整公式
- `knowledge/classification_rules.md` — 公司分类规则
- `knowledge/framework_guide.md` — Greenwald 框架与评级标准
- `knowledge/judgment_examples.md` — 判断锚点与 Logic Chain 示例
- `knowledge/valuation_examples.md` — 4 个估值计算案例
