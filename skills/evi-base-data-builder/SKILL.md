---
name: evi-base-data-builder
description: "EVI 项目第一步（Task 1）：使用多 Agent 并发建立公司基础数据库。含 CHECKLIST 门禁——有 blocking 缺失时禁止进入估值，必须 loop back。严禁模拟数据。"
---

# EVI Base Data Builder

> **职责**：EVI 流水线的 **Task 1**（独立任务）。**只做数据收集 / 解析 / 校验**。
> - **不做估值、不做事实抽取。**
> - **Task 1 结束条件：CHECKLIST.json.summary.overall ≠ "blocked"。**
> - 数据没准备好 → 绝对不能进 Task 2（估值）。

---

## 🚨 铁律（违反任何一条 = 任务失败）

1. **严禁模拟数据**：
   - `reliability: "simulated"` 禁止出现在任何 fact / metric / 假设中
   - 所有数字必须能追溯到具体来源（FMP JSON / 财报原文 / 研报引用）
   - 不确定的数据 → 标 `"status": "unverified"` 并触发子 agent 去验证

2. **CHECKLIST 门禁**：
   - Phase 1 结束后跑 `build_checklist.py`；如果 `overall == "blocked"` → **禁止结束 Task 1**
   - 必须 loop back：找到缺失项 → 调子 agent 补数据 → 重跑 checklist → 直到不再 blocked
   - 允许最终状态为 `"partial"`（非 blocking 缺失可以带着进 Task 2），但 `"blocked"` 绝不可以

3. **FMP 数据是基线，财报用来补充和校验**：
   - key_metrics.json **必须从 FMP 的 6 张表计算得出**（不是从 MD&A "估计"）
   - 用 MD&A 来解释**变化原因**（`change_reason`），但数值以 FMP 为准
   - 如果 FMP 与财报有差异 → 写 `fmp_reconcile.json` 记录差异，以财报为准并标注

4. **PDF 必须验证**：
   - 下载后解析第一页 + 目录页，确认是**年度/中期报告**而不是 ESG / 股东通知 / 通函
   - 验证关键词：`profit` / `loss` / `revenue` / `income` / `balance sheet` / `利润` / `收入` / `资产负债`
   - 不含 → 删掉，重新搜正确的年报/业绩公告

---

## 🔀 多 Agent 并发执行模型

> 本项目**原生支持多 Agent 并发。数据收集天然可以并行——不要串行一个个来，不然会消耗上下文，增加延迟。**

### 并发架构

```
Task 1 (主 Agent) ── 编排 + CHECKLIST gate
  │
  ├── 子 Agent A：FMP 数据拉取 + key_metrics 计算
  ├── 子 Agent B：财报 PDF 下载 + 验证 + 解析 + MD&A 抽取
  ├── 子 Agent C：研报搜索 + 下载 + 摘要
  ├── 子 Agent D：电话会纪要获取
  ├── 子 Agent E：可比公司数据 (peers profile + keyMetrics)
  └── 子 Agent F：行业 / 产品级外部数据搜索 (WebSearch)
```

### 操作方式

每个子 agent 用**独立的 message 会话**在同一个 workspace 中执行（工作区支持多个并行 thread/task）。

子 agent 完成后把结果写入约定路径，主 agent 检查：
- 子 Agent A 完成 → `base/fmp/*.json` + `base/financials/indicators/key_metrics.json` 存在
- 子 Agent B 完成 → `base/financials/raw/*.pdf` + `base/financials/parsed/*.md` + `base/financials/mdna/*.md` 存在
- 子 Agent C 完成 → `base/research/raw/*` 存在（或标注"市场无可用研报"）
- 子 Agent D 完成 → `base/transcripts/raw/*` 存在
- 子 Agent E 完成 → `base/peers/` 下每个 peer 有 `profile.json` + `keyMetrics.json`
- 子 Agent F 完成 → `base/external/` 下有搜索结果 markdown

### 子 Agent 任务模板

**子 Agent A — FMP 数据 + 指标计算**：
```
1. 运行 evi_fetch_data.py --symbol {symbol} --market {market} --data-dir data/{symbol_dir}
2. 读 base/fmp/incomeStatement.json + balanceSheet.json + cashFlow.json + keyMetrics.json + ratios.json
3. 计算 key_metrics.json（8 个核心指标 × 全部可用期数），公式：
   - ROE = netIncome / totalStockholdersEquity
   - ROIC = NOPAT / (totalEquity + totalDebt - cash)
   - gross_margin = grossProfit / revenue × 100
   - operating_margin = operatingIncome / revenue × 100
   - rd_ratio = researchAndDevelopmentExpenses / revenue × 100
   - fcf = operatingCashFlow - capitalExpenditure
   - debt_to_ebitda = (shortTermDebt + longTermDebt) / ebitda
   - interest_coverage = ebitda / interestExpense
4. 每期每个指标计算 change_yoy_pct（如果有上期数据）
5. fmp_cross_check: 标 "matched"（因为就是从 FMP 算的）
6. 把结果写 base/financials/indicators/key_metrics.json
```

**子 Agent B — 财报下载 + 解析**：
```
1. 运行 evi_download_knowledge.py --symbol {symbol} --market {market} --financials --years 4 --data-dir data/{symbol_dir}
2. 检查下载的 PDF：对每个 PDF 运行 parse_pdf.py，看前 5 页是否含"revenue/利润/income statement/balance sheet"
   - 如果不含 → 删掉，标为"非财报"
   - 如果只有 ESG / 通知 → WebSearch 搜正确的年报 URL 重新下载
3. 对通过验证的 PDF（最近 3 期）完成 parse_pdf + extract_mdna
4. 用 mdna 补充 key_metrics.json 的 change_reason 字段（引用原文）
```

**子 Agent C — 研报搜索**：
```
1. WebSearch "公司名 + 投行名 + target price + 2025/2026"
2. 搜到的研报标题 + 评级 + 目标价 + 关键假设摘要 → 写 base/research/raw/{date}_{institution}.md
3. 至少搜 3 家不同机构的研报
```

**子 Agent D — 电话会**：
```
1. 运行 evi_download_knowledge.py --symbol {symbol} --market {market} --transcripts --years 2 --data-dir data/{symbol_dir}
2. 如果 FMP 无数据 → WebSearch "公司名 + earnings call transcript + Q4 2025"
```

**子 Agent E — 可比公司**：
```
1. 确定 peer set（同行业、类似商业模式、类似阶段）：
   - 激光雷达行业：禾赛科技(HSAI)、Luminar(LAZR)、Ouster(OUST)、Innoviz(INVZ)
   - 广义自动驾驶：Mobileye(MBLY)
2. 对每个 peer 跑 evi_fetch_data.py --symbol {peer} --data-dir data/{symbol_dir}/base/peers/{peer}/
3. 或者直接用 FMP API 拉 peer 的 profile + keyMetrics + ratios
4. 汇总成 base/peers/peer_summary.json（含 PE/PS/EV-Revenue/Growth 等）
```

**子 Agent F — 外部搜索**：
```
1. WebSearch 搜行业数据：
   - "global LiDAR market size 2025 2026 growth"
   - "ADAS penetration rate China 2025 forecast"
   - "{company} order backlog pipeline 2026"
   - "{company} 出货量 2025 季度"
2. 每条搜索结果写成一个 markdown fact → base/external/{topic}.md
```

---

## 标准执行流程（主 Agent 视角）

### Step 1 — 初始化 + 派发

```bash
python3 .agents/skills/evi-toolkit/scripts/init_project.py --symbol {symbol} --market {market}
```

然后**同时派发子 Agent A-F**。不要等 A 完了再跑 B。

### Step 2 — 等待 + 检查

所有子 agent 完成后（或超时后），主 agent 检查各个输出路径是否存在：
- 如果某个子 agent 失败 → 读它的报错 → 决定是重试还是标注缺失
- 特别关注子 Agent B 的 PDF 验证结果

### Step 3 — Segment 抽取

> 在子 Agent B 的 mdna 产出 + 子 Agent A 的 FMP 数据基础上，主 Agent 自己做：

- 读 `base/financials/mdna/*.md` + `base/fmp/incomeStatement.json`（看是否有 segment 收入披露）
- 写 `base/financials/segments/segment_data.json`
- 如果公司**没有分部披露**（2498.HK 在上市初期可能只有总收入分产品线）→ 用收入拆分替代

### Step 4 — FMP 校验

对比 FMP 数据 vs MD&A 提到的数字：
- 差异 < 5% → matched
- 差异 5-15% → warning（可能是会计准则差异）
- 差异 > 15% → mismatch（需要解释，可能是 FMP 数据口径问题）

写 `base/validation/fmp_reconcile.json`

### Step 5 — 重建 catalog + CHECKLIST

```bash
python3 .agents/skills/evi-toolkit/scripts/update_catalog.py --data-dir data/{symbol_dir} --rebuild
python3 .agents/skills/evi-toolkit/scripts/build_checklist.py --data-dir data/{symbol_dir} --required-periods 6
```

### Step 6 — 🚨 CHECKLIST 门禁

读 `base/CHECKLIST.json`：

```python
if summary.overall == "blocked":
    # 找到 blocking_missing 项
    # 调子 agent 针对性补数据
    # 重跑 Step 5
    # 循环直到 overall != "blocked"（最多 3 轮）
elif summary.overall == "partial":
    # 可以继续；在 reports/data.md 中显式说明 partial 原因
```

如果 3 轮后仍 blocked → 持久化为 `partial` 状态，在 reports/data.md 中清晰列出缺什么 + 为什么拿不到。

### Step 7 — 写 reports/data.md + run format_facts.py

data.md 格式见本 SKILL 下方 §报告模板。**所有数字必须有来源**。

```bash
python3 .agents/skills/evi-toolkit/scripts/format_facts.py --data-dir data/{symbol_dir}
```

### Step 8 — 写 base/INDEX.md

### Step 9 — 持久化（partial — 只有 data 阶段完成）

```bash
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \
    --entry-id {entry_id} --data-dir data/{symbol_dir} \
    --display-name "{display_name}" --symbol "{entry_key}" --market "{market}" \
    --status partial
```

> Task 1 到此结束。用户在看板上能看到 CHECKLIST 概况 + data.md 报告。
> Task 2（估值）由第二条消息触发。

---

## 报告模板（reports/data.md）

```markdown
# 数据收集与基础数据库 — {display_name}（{symbol}）

## 1. 公司画像
（来自 FMP profile；不要编造）

## 2. 数据完整性 CHECKLIST 摘要
- 整体状态：✅/⚠️/❌
- blocking 缺失：（无 / 列出）
- 本次数据源：FMP API + 港交所年报 + WebSearch 研报 + WebSearch 行业数据

## 3. 关键指标 6 期一览（来自 FMP 计算，不是模拟）
| 指标 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 拐点/解读 |
（数据来自 key_metrics.json，change_reason 来自 MD&A 或 FMP 趋势分析）

## 4. 业务分部数据（来自年报 segment disclosure）
（如公司不单独披露 → 说明"公司未披露独立分部数据，改用产品线收入拆分"）

## 5. 可比公司一览
| 公司 | 代码 | 市值 | PS | PE | Revenue Growth | Gross Margin | 来源 |
（来自子 Agent E 的 peer_summary.json）

## 6. MD&A 核心解读
（来自实际财报解析，不是模拟；每条引用 [N]）

## 7. 数据缺口 & 下一步动作
- [ ] 缺什么
- [ ] 用户需要关注什么

---

## Facts Index

[1] fact_id=fact_data_001 | segment=group | reliability=high | topic=revenue_actual
    text: 公司 2025 全年收入 1,891M RMB（FMP incomeStatement），同比 +14.7%。
    source: base/fmp/incomeStatement.json#2025-12-31

[2] fact_id=fact_data_002 | segment=group | reliability=high | topic=gross_margin_actual
    text: 2025 毛利率 26.5%（FMP ratios），较 2024 年 17.2% 提升 9.3pp。
    source: base/fmp/ratios.json#2025

...
```

---

## 可比公司数据目录

```
data/{symbol_dir}/base/peers/
├── HSAI/
│   ├── profile.json
│   └── keyMetrics.json
├── LAZR/
│   ├── profile.json
│   └── keyMetrics.json
├── OUST/
│   ├── profile.json
│   └── keyMetrics.json
└── peer_summary.json     ← 汇总表（PS / PE / Revenue CAGR / Gross Margin）
```

---

## 与 Task 2 的衔接

Task 1 完成后，用户或 Automation 触发 Task 2（第二条消息），prompt 类似：
```
继续 Task 2：基于 data/{symbol_dir}/ 中的基础数据库执行估值分析（Phase 2-4）。
```

Task 2 的 Agent 会读到 Task 1 已建好的：
- `base/CHECKLIST.json`（确认 overall != blocked）
- `base/financials/indicators/key_metrics.json`（真实 FMP 数据）
- `base/financials/segments/segment_data.json`
- `base/peers/peer_summary.json`
- `information/indexed_facts.json`（已有真实引用的 facts）
- `reports/data.md`

然后按 Phase 2-4 的 14 个 skill 流水线执行估值。
