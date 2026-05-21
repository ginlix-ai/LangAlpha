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

## 知识库下载与管理（独立 Skill）

### 功能概述

一站式下载和管理股票分析所需的所有知识数据：
- **财报 PDF**：港股（HKEx 披露易）/ A股（巨潮资讯）/ 美股（SEC 10-K）
- **公司公告**：盈利预告、内幕消息、股权变动等
- **研究报告**：国内外专业投研机构（按日期-机构命名）
- **电话会纪要**：FMP Earnings Call Transcript

### 下载全部数据

```bash
python3 scripts/download_knowledge.py --symbol {symbol} --market {market} --all
```

### 仅下载某类数据

```bash
# 财报
python3 scripts/download_knowledge.py --symbol {symbol} --market {market} --financials

# 公告
python3 scripts/download_knowledge.py --symbol {symbol} --market {market} --announcements

# 研报（国内外投研机构）
python3 scripts/download_knowledge.py --symbol {symbol} --market {market} --research

# 电话会纪要
python3 scripts/download_knowledge.py --symbol {symbol} --market {market} --transcripts
```

### 目录管理

```bash
# 查看数据目录
python3 scripts/manage_knowledge.py --symbol {symbol} --action list

# 搜索
python3 scripts/manage_knowledge.py --symbol {symbol} --action search --query "goldman"

# 统计
python3 scripts/manage_knowledge.py --symbol {symbol} --action stats

# 导出可读的 Markdown 索引
python3 scripts/manage_knowledge.py --symbol {symbol} --action export-md

# 验证文件完整性
python3 scripts/manage_knowledge.py --symbol {symbol} --action verify

# 删除
python3 scripts/manage_knowledge.py --symbol {symbol} --action delete --category research --filename "xxx.pdf"
```

### 输出目录结构

下载的数据保存在 `data/{symbol}/knowledge/` 下：

```
data/{symbol}/knowledge/
├── catalog.json                    # 数据目录索引（自动维护）
├── INDEX.md                        # 可读目录清单（export-md 生成）
├── financials/                     # 财报 PDF
│   ├── 2024-annual-report.pdf
│   └── 2024-H1-interim-report.pdf
├── announcements/                  # 公告
│   ├── 2024-12-01_profit-warning.pdf
│   └── ...
├── research/                       # 研报（日期_机构 命名）
│   ├── 2024-12-15_Goldman-Sachs.pdf
│   ├── 2024-11-20_中金公司.pdf
│   └── ...
└── transcripts/                    # 电话会纪要
    ├── 2024-Q3-earnings-call.md
    └── ...
```

### 数据来源覆盖

| 市场 | 财报 | 公告 | 研报 | 电话会 |
|------|------|------|------|--------|
| 港股 | HKEx 披露易 | HKEx | FMP + SerpAPI | FMP |
| A股 | 巨潮资讯 | 巨潮资讯 | 东方财富 + FMP | FMP |
| 美股 | FMP/SEC (10-K) | SEC | FMP + SerpAPI | FMP |

### 与分析流程的集成

知识库数据在 D1-D7 维度分析中作为额外参考材料使用：
- D1（商业模式）：参考年报业务描述章节
- D3（外部环境）：参考公告中的监管政策变化
- D4（管理层）：参考电话会纪要中的管理层发言
- D5（MD&A）：直接对应年报 MD&A 章节 + 电话会 Q&A
- D6/D7：综合参考研报中其他机构的估值判断

---

## 文件目录结构

```
data/{symbol}/
├── financial_context.md
├── engine_result.json
├── raw/                          # FMP 原始数据
├── knowledge/                    # 知识库数据（download_knowledge.py 产出）
│   ├── catalog.json
│   ├── INDEX.md
│   ├── financials/
│   ├── announcements/
│   ├── research/
│   └── transcripts/
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
