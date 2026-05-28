---
name: evi-quality-analysis
description: "EVI 定性分析模块：基于已下载的财报/MD&A/电话会/财务数据，对商业模式、竞争优势/护城河、管理层与治理、MD&A 与前瞻指引四个维度做结构化定性判断。轻量、复用已有数据，输出 reports/quality.md + quality.json。"
---

# EVI Quality Analysis — 定性分析（4 维度）

> **定位**：EVI 估值流程中的"定性垫脚石"。在 Phase 1 产业调研完成、已经拿到财报 + MD&A + 电话会 + FMP 数据后调用一次，输出结构化定性判断，给 Phase 2 估值假设和前端看板做支撑。
>
> **不做什么**：不重做产业调研（已由 evi-data-orchestrator 完成）、不做估值（Phase 2）、不写长篇大论。**少而精**——每个维度回答 4-5 个核心问题，提炼出**单一判断 + 评级 + 3-5 个证据** 即可。

---

## 1. 一句话职责

> 复用 `data/{symbol_dir}/` 下已有的 FMP / MD&A / 电话会 / 研报数据，按 4 个维度（商业模式 / 护城河 / 管理层 / MD&A 前瞻）做轻量定性判断 → 输出一篇 `reports/quality.md` + 一份结构化 `quality.json`。

---

## 2. 触发时机

```
evi-data-orchestrator 完成 Phase 1（产业调研报告齐全）
        ↓
   调用 evi-quality-analysis（本 skill）
        ↓
evi-valuation-analysis 启动 Phase 2（估值时引用 quality 结论调假设）
```

也支持**用户主动触发**（"重看一遍护城河"、"管理层 D 评分降一档"），此时只重写对应维度。

---

## 3. 数据来源（全部复用，不下载新东西）

| 来源 | 路径 | 主要用途 |
|---|---|---|
| FMP 三表 + 关键比率 | `base/financials/financial_context.md`、`base/financials/indicators/*.json` | D1 收入/利润质量、D2 ROE 序列、D4 商誉/审计 |
| MD&A | `base/financials/mdna/*.md` | D5 全部、D4 言行一致 |
| 电话会纪要 | `base/transcripts/raw/*.md` | D5 前瞻指引、D4 管理层口径 |
| 研报摘要 | `base/research/raw/*` | D2 行业格局、D4 第三方观点交叉验证 |
| 已写好的产业调研 | `reports/company_overview.md`、`reports/segments/*.md` | 行业上下文/分部信息（避免重复调研） |
| Web/Agent 知识 | — | D2 竞争对手、D4 治理事件等公开信息 |

> ⚠️ **不要重新跑数据下载**。如果发现某项数据缺失，写入 `quality.json.data_gaps` 并继续；除非该缺失阻塞核心判断，才回头补。

---

## 4. 4 个维度的核心问题（精简版）

每个维度限定 **4-5 个问题**，**每个问题答 1-3 句话**，外加 1 段"评级与逻辑链"。控制单维度在 **300-600 字**，整篇 `quality.md` 不超过 **2500 字**。

### Q-Frame B1 · 商业模式与资本特征（business_model）

1. **怎么赚钱**：一句话本质（不是业务罗列）。是否经过周期验证？
2. **收入质量**：核心主营 vs 一次性/关联交易/低毛利收入占比；增长是否"注水"？
3. **利润驱动**：毛利率 / 费用率 / 非经营性贡献，剥离后核心经营利润增速？
4. **资本强度**：Capex/折旧、近 5 年 ROIC/ROE 趋势，重资产 or 轻资产？
5. **现金质量**：OCF/NI 比率、应收应付变化、收款模式。

**评级（1-10）**：商业模式清晰度(±N) + 收入质量(±N) + 利润质量(±N) + 资本效率(±N) + 现金能力(±N) = **X/10**

---

### Q-Frame B2 · 竞争优势与护城河（moat）

1. **行业格局**：垄断/寡头/竞争？关键壁垒类型（资金/技术/许可/品牌/规模）？
2. **量化证据**：5 年 ROE 是否超越行业 + 波动率；毛利率/净利率相对优势可量化？
3. **护城河来源**：写**因果链**而非贴标签（规模经济 / 网络效应 / 转换成本 / 无形资产 / 成本优势 / 数据飞轮）。
4. **虚假护城河检查**：知名度≠定价权 / 卓越运营可被模仿 / 政策牌照依赖 / 周期顶部。
5. **未来 3 年监控指标**：2-3 个 KPI + 失效条件。

**评级（1-10）**：行业壁垒(±N) + 量化证据(±N) + 来源清晰度(±N) + 可持续性(±N) + 对手差距(±N) = **X/10**

---

### Q-Frame B3 · 管理层与公司治理（management）

1. **治理红旗**：审计意见 / 审计师变更 / 处罚前科 / 大股东质押+减持 / 关联交易性质。
2. **管理层能力**：核心团队任期、过去 3 年承诺兑现率、资本配置去向（投资/并购/分红）的结果。
3. **言行一致**：去年 MD&A 设的目标实现多少？对失败是否坦诚？分红/回购是否兑现？
4. **综合评级**：优秀 / 合格 / 损害价值 / 观察期。

**评级（1-10）**：治理红旗(±N) + 执行能力(±N) + 言行一致(±N) + 资本配置(±N) = **X/10**

---

### Q-Frame B4 · MD&A 解读与前瞻指引（forward_guidance）

1. **指引可信度**：过去 3 年前瞻指引偏差；是否系统性"画饼"；MD&A 长度/质量趋势。
2. **解释 vs 独立判断**：管理层说的增长来源 与 D1 分析是否一致？不一致点说明什么？
3. **报表外信息**：战略调整 / 在建项目 / 签约客户 / 技术突破 / 管理层"今年没说什么"。
4. **未来 1-2 年关键判断点**：定量指引 + 验证窗口（哪个季度可验证）。
5. **被低估的风险**：风险章节措辞变化 / 与外部环境对照 / 诉讼调查 / 高管离职信号。

**评级（1-10）**：MD&A 可信度(±N) + 信息增量(±N) + 前瞻清晰度(±N) + 隐含风险(±N) = **X/10**

---

## 5. 报告产出 `reports/quality.md`

固定结构（与 `quality.json` 字段对齐，方便前端按章节抽取）：

```markdown
# {公司名} 定性分析

> 复用产业调研数据的 4 维度定性判断。给估值假设与监控做支撑。

## 1. 总览

| 维度 | 评级 | 一句话判断 |
|---|---|---|
| 商业模式与资本特征 | X/10 | ... |
| 竞争优势与护城河 | X/10 | ... |
| 管理层与公司治理 | X/10 | ... |
| MD&A 与前瞻指引 | X/10 | ... |

**关键投资支柱**（来自 4 个维度的最强 evidence）：
- ✅ ...
- ✅ ...

**关键风险信号**（来自 4 个维度的最弱 evidence / red flag）：
- ⚠️ ...
- ⚠️ ...

## 2. 商业模式与资本特征

### Q1 怎么赚钱
{1-2 句}[^1]

### Q2 收入质量
{1-2 句}[^2]

### Q3 利润驱动
{1-2 句}[^3]

### Q4 资本强度
{1-2 句}[^4]

### Q5 现金质量
{1-2 句}[^5]

### 评级与逻辑链
- 评级：**X/10**
- 推导：商业模式清晰度(+N) + 收入质量(±N) + 利润质量(±N) + 资本效率(±N) + 现金能力(±N) = X
- 一句话定调：{...}

---

## 3. 竞争优势与护城河
（同上模板，省略）

## 4. 管理层与公司治理
（同上模板，省略）

## 5. MD&A 与前瞻指引
（同上模板，省略）

## 6. 对估值假设的影响

> 这一段是给 Phase 2 evi-assumption-builder 看的。

| 假设 | 对应维度 | 调整建议 |
|---|---|---|
| 长期增长率 g | B1 商业模式 / B4 前瞻 | {建议提高/保持/下调，理由} |
| 稳态毛利率 | B2 护城河 | {...} |
| WACC 风险溢价 | B3 治理 / B4 风险 | {建议+1pp 或不调} |
| 退出倍数 | B2 护城河可持续性 | {...} |

## 7. 监控建议

> 这一段是给 evi-monitor 看的。每个维度推荐 1-2 个量化追踪指标。

| 维度 | 监控指标 | 阈值 / 触发条件 |
|---|---|---|
| B1 现金质量 | OCF/NI | <0.8 连续 2 季度 |
| B2 护城河 | ROE 滚动 5 年 | <行业平均 |
| B3 治理 | 审计师变更 / 大股东减持 | 任意发生即触发 |
| B4 前瞻 | 季度业绩 vs 指引偏差 | >15% |

---

## References
[^1]: source: base/financials/financial_context.md#收入构成
[^2]: source: base/financials/mdna/2024-mdna.md#收入分析
...
```

> ⚠️ 每个判断必须有 `[^N]` 脚注指向数据来源，否则前端的「引用问 AI」按钮会让用户看到光秃秃的判断。

---

## 6. 结构化产出 `data/{symbol_dir}/quality.json`

```jsonc
{
  "schema_version": "quality-1.0",
  "generated_at": "2026-05-27T15:00:00Z",
  "company": {
    "symbol": "0700.HK",
    "display_name": "腾讯科技"
  },

  // 4 个维度逐一展开
  "dimensions": {
    "business_model": {
      "score": 8,
      "verdict": "高质量平台经济，已经过完整周期验证",
      "evidence": [
        {"text": "5 年平均 ROE 18%，低谷期仍 12%", "source": "base/financials/financial_context.md"},
        {"text": "OCF/NI 长期 >1.1，现金回收能力强", "source": "..."}
      ],
      "risks": [
        "广告 + 金融板块对宏观敏感"
      ],
      "logic_chain": "商业模式清晰度(+2) + 收入质量(+2) + 利润质量(+1) + 资本效率(+2) + 现金能力(+1) = 8"
    },
    "moat": {
      "score": 9,
      "verdict": "强网络效应 + 生态锁定，护城河深",
      "evidence": [...],
      "risks": [...],
      "logic_chain": "...",
      "kpi_to_monitor": [
        {"metric": "DAU", "threshold_down": "环比 -3%"},
        {"metric": "ARPU", "threshold_down": "YoY -5%"}
      ]
    },
    "management": {
      "score": 7,
      "verdict": "合格——资本配置稳健，无重大红旗",
      "evidence": [...],
      "risks": [...],
      "logic_chain": "...",
      "red_flags": []
    },
    "forward_guidance": {
      "score": 7,
      "verdict": "MD&A 信息密度高，过去 3 年指引平均偏差 <8%",
      "evidence": [...],
      "key_judgement_points": [
        {"item": "AI 业务商业化", "verify_window": "2026Q4"},
        {"item": "云业务利润率拐点", "verify_window": "2026Q2"}
      ],
      "underrated_risks": [...],
      "logic_chain": "..."
    }
  },

  // 跨维度结论汇总（前端看板用）
  "summary": {
    "overall_score": 7.75,                 // 4 项算术平均，可加权
    "investment_pillars": [                // 关键投资支柱（最强 evidence 抽取）
      "B2 护城河深：网络效应 + 生态闭环",
      "B1 现金能力强：OCF/NI 长期 >1"
    ],
    "key_risk_signals": [                  // 关键风险信号
      "B3 大股东减持节奏",
      "B4 海外市场监管政策不确定"
    ]
  },

  // 给 evi-assumption-builder 的桥接
  "assumption_hints": {
    "growth_rate": {"adjust": "hold", "reason": "护城河支撑稳态增速"},
    "steady_state_margin": {"adjust": "+1pp", "reason": "规模效应未释放完毕"},
    "wacc_risk_premium": {"adjust": "+0.5pp", "reason": "海外业务监管不确定性"},
    "exit_multiple": {"adjust": "hold"}
  },

  // 给 evi-monitor 的桥接
  "monitor_suggestions": [
    {"dim": "B1", "metric": "OCF/NI", "threshold": "<0.8 连续 2 季度", "freq": "quarterly"},
    {"dim": "B2", "metric": "DAU", "threshold": "环比 -3%", "freq": "monthly"},
    {"dim": "B3", "metric": "审计师变更", "threshold": "任意", "freq": "event"},
    {"dim": "B4", "metric": "业绩 vs 指引", "threshold": "偏差 >15%", "freq": "quarterly"}
  ],

  // 数据缺口（被跳过的问题）
  "data_gaps": [
    {"dim": "B3", "question": "Q1 治理红旗 - 关联交易明细", "reason": "财报附注未披露，需 web_search 公司公告"}
  ]
}
```

> 字段约定：
> - `dimensions.{key}.score` 必填，1-10 整数
> - `dimensions.{key}.verdict` 一句话总判断（≤30 字）
> - `dimensions.{key}.evidence` 至少 2 条带 source 的事实
> - `summary.overall_score` 默认算术平均，如要加权请在 `summary.weights` 里写明

---

## 7. 执行步骤（Agent 操作流）

```
1. Read evi-quality-analysis/SKILL.md（本文件）
2. 检查前置数据：
   - reports/company_overview.md ✓
   - base/financials/financial_context.md ✓
   - base/financials/mdna/*.md（至少 1 期）
   - base/transcripts/raw/*.md（至少 1 期，港美股；A 股可空）
   缺失阻塞项 → 提示用户/反向请求 Phase 1
3. 顺序写 4 个维度（无 DAG，互相轻度参考）：
   for dim in [business_model, moat, management, forward_guidance]:
       根据本文档 Q-Frame，结合数据写 quality.md 对应章节 + 评级
4. 写完 4 个维度后：
   - 抽取 summary.investment_pillars / key_risk_signals
   - 写 § 6 对估值假设的影响
   - 写 § 7 监控建议
5. 从 quality.md 提取 quality.json（结构见上）
6. 更新 facets.json：把 quality 关键字段塞进去（见 §8）
7. 在 reports/changelog.md 顶部追加一条记录
```

> 不需要专门的脚本——直接用 `write_file` / `edit_file` 工具写。提取 `quality.json` 时如果想自动化，可以用 `python3 -c` 一行脚本从 markdown 抽，但**不强制**，Agent 直接写 json 也可以。

---

## 8. 与 facets.json 的接口

`facets.json` 顶层增加 `quality` 字段：

```jsonc
{
  // ... 已有字段（fair_value / segments / 等）

  "quality": {
    "overall_score": 7.75,
    "scores": {
      "business_model": 8,
      "moat": 9,
      "management": 7,
      "forward_guidance": 7
    },
    "verdicts": {
      "business_model": "高质量平台经济，已经过完整周期验证",
      "moat": "强网络效应 + 生态锁定，护城河深",
      "management": "合格——资本配置稳健，无重大红旗",
      "forward_guidance": "MD&A 信息密度高，过去 3 年指引平均偏差 <8%"
    },
    "investment_pillars": [...],
    "key_risk_signals": [...],
    "report_path": "reports/quality.md"
  }
}
```

**前端约定**：`EviReportPanel` 在估值结论 Tab 渲染一个「定性分析卡」（4 项评级 + 投资支柱 + 风险信号），点击维度名跳转到 `reports/quality.md` 对应章节。

---

## 9. 与其它 EVI skill 的协作

| Skill | 协作关系 |
|---|---|
| `evi-data-orchestrator` | **上游**。完成产业调研 + 数据下载后触发本 skill |
| `evi-assumption-builder` | **下游**。读 `quality.json.assumption_hints` 调整估值假设 |
| `evi-reverse-valuation` | **下游**。把 `quality.json.dimensions.moat.kpi_to_monitor` 合并到 rerate_triggers |
| `evi-monitor` | **下游**。读 `quality.json.monitor_suggestions` 注册监控 |
| `evi-revaluation-updater` | **触发回调**。监控发现治理红旗 / 业绩偏差 → 重跑对应维度（不必重跑全部 4 维） |

---

## 10. 增量更新模式（最常用）

用户在对话中说"D2 评级太乐观了 / 管理层 Q3 失误你没考虑"：

```
1. 定位是哪个维度（business_model / moat / management / forward_guidance）
2. 只重写 reports/quality.md 中对应 § 章节 + § 1 总览中的对应行
3. 重新提取 quality.json 的对应 dimension（其它维度保持）
4. 更新 facets.json 中 quality 块
5. 写 changelog.md
6. 跑 persist_evi_report.py 推到看板
```

> **铁律：严禁回复"好的我知道了"后不做任何操作**——必须落地到 `quality.md` + `quality.json` + `facets.json` + `changelog.md`。

---

## 11. 失败处理

| 情况 | 处理 |
|---|---|
| MD&A 缺失（PDF 解析失败） | 该维度仍写，但在 evidence 中写"MD&A 缺失"，evidence 改用电话会/研报，对应分数 -1 |
| 电话会缺失（A 股常见） | forward_guidance 维度可降级为"基于年报 MD&A 单一来源"，分数 -1 |
| 治理事件无法核实 | 写入 data_gaps，management 维度按已有信息打分 |
| 全部前置数据缺失 | 反向请求 evi-data-orchestrator 补；不在缺数据时凭空打分 |

---

## 12. 一句话铁律

> **少而精**：每个维度 4-5 个问题、300-600 字、每条 evidence 必带 source、最终输出 1 篇 md + 1 个 json。
