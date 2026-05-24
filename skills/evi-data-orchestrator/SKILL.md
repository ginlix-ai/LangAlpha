---
name: evi-data-orchestrator
description: "EVI Phase 1 总控：产业调研（总-分结构）。先做业务分部识别 → 决定是单一报告还是 SOTP 结构 → 输出公司总报告 + N 个分部子报告 + 索引事实库。"
---

# EVI Data Orchestrator — Phase 1：产业调研

> **你是 Phase 1 的总控**。Phase 1 的核心交付物是一套**总-分结构的产业调研报告**——先识别公司是否需要 SOTP 拆分，再按需输出整体报告 + 各分部子报告。

---

## 1. 一句话职责

> 自动化获取数据 → 决定结构（整体 or 总+分部）→ 并发产出公司总报告 + 各分部子报告 + 索引事实库 → 为 Phase 2 估值打底。

---

## 2. 核心设计：自适应总-分结构

```
Step 1.1: 识别公司业务线（基于财报分部数据 + MD&A）
            ↓
       N = 业务分部数
            ↓
   ┌────────┴────────┐
   │                 │
N ≤ 1（单一业务）   N ≥ 2（多分部）
   │                 │
   ▼                 ▼
单一报告模式        总-分模式
（整体估值）        （整体 + N 个分部 → SOTP）
```

### 2.1 单一业务模式（小公司 / 单一业务线）

输出 1 份完整报告：

```
reports/
├── company_overview.md  ← 公司全景：行业 + 公司 + 财务 + 估值锚
└── data_index.md        ← 数据收集索引（紧凑摘要）
```

→ Phase 2 用整体估值（不做 SOTP）

### 2.2 总-分模式（多业务线公司）

输出 1 份总报告 + N 份分部子报告：

```
reports/
├── company_overview.md          ← 总报告：公司全景 + 各分部摘要 + 整合视角
├── segments/
│   ├── {seg_id_1}.md            ← 分部 1：腾讯云
│   ├── {seg_id_2}.md            ← 分部 2：游戏
│   ├── {seg_id_3}.md            ← 分部 3：广告
│   └── ...                       ← N 个分部
└── data_index.md                ← 数据收集索引
```

→ Phase 2 用 SOTP 估值（每个分部独立估值后加总）

---

## 3. 铁律

1. 严禁模拟数据（reliability:simulated 不允许）
2. FMP 是数值基线（自动计算所有可计算指标）
3. **Phase 1 的核心交付是产业调研报告**（人类可读、深度分析）
4. **总-分结构由分部数 N 决定**，不强行拆分也不强行合并
5. 所有事实必须有真实来源 → 写入 indexed_facts.json
6. 财报/电话会/研报：港美股复用 FMP；A 股 FMP 没有的用 WebSearch
7. 财报/公告下载复用 sirius-valuation 的脚本

---

## 4. 编排方案

```
┌─────────────────────────────────────────────────────────────────┐
│                evi-data-orchestrator (Phase 1 主控)               │
│                                                                 │
│  Step 1: 自动化数据获取（脚本）                                   │
│    └─ FMP 全套 + TTM + 增长率 + 电话会（港美股）                  │
│                                                                 │
│  Step 2: 业务分部识别 → 决定结构                                  │
│    ├─ 调用 evi-business-segmentation                            │
│    ├─ 输出 business_segments.json（N 个分部）                    │
│    └─ if N==1 → 单一报告模式 / if N≥2 → 总-分模式                │
│                                                                 │
│  Step 3: 并发采集补充材料                                         │
│    ├─ [A] 财报 PDF 下载 + 解析（复用 sirius-valuation）          │
│    ├─ [B] 研报搜索（保留分析师完整分析）                          │
│    ├─ [C] 行业数据搜索（市场规模/政策/竞争）                      │
│    ├─ [D] 各分部产品数据（按 segment 并发）                       │
│    └─ [E] A 股电话会补充（如 FMP 无）                            │
│                                                                 │
│  Step 4: 整合产业调研报告                                         │
│    ├─ 总报告：reports/company_overview.md                        │
│    └─ 分部报告（如 N≥2）：reports/segments/{seg_id}.md           │
│                                                                 │
│  Step 5: 提取结构化事实库                                         │
│    └─ format_facts.py → information/indexed_facts.json          │
│                                                                 │
│  Step 6: CHECKLIST 质检 + persist(partial)                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. Step 1 — 自动化数据获取（脚本完成）

> ⚠️ **必须完整跑完这 4 个脚本**。如果跳过 evi_download_knowledge 会导致 CHECKLIST 卡在
> "原始财报 PDF 0 期" 等阻塞项，Phase 2 估值无据可循。

```bash
# 1) 初始化项目目录
python3 .agents/skills/evi-toolkit/scripts/init_project.py \
    --symbol {symbol} --market {market}

# 2) FMP 财务数据 + TTM + 增长率（自动算）
python3 .agents/skills/evi-toolkit/scripts/evi_fetch_data.py \
    --symbol {symbol} --market {market} --data-dir data/{symbol_dir} \
    --quarterly --ttm --growth-rates

# 3) 财报 PDF + 公告（核心！港股=HKEx / A股=巨潮 / 美股=SEC）
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \
    --symbol {symbol} --market {market} --financials --announcements --years 4 \
    --data-dir data/{symbol_dir}

# 4) 电话会纪要（港美股=FMP / A股需 WebSearch 兜底）
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \
    --symbol {symbol} --market {market} --transcripts --years 2 \
    --data-dir data/{symbol_dir}
```

自动产出：
- `base/fmp/*.json`（年报+季报）
- `base/financials/indicators/{key_metrics, ttm_metrics, growth_rates}.json`
- `knowledge/financials/*.pdf`（年报/中期/季报，按市场自动适配）
- `knowledge/announcements/*.pdf`（公告/业绩预告等）
- `knowledge/transcripts/*.md`（电话会纪要，港美股 FMP）
- `knowledge/_dl_catalog.json`（下载索引）

> ⚠️ **如果 knowledge/financials/ 是空的**，说明下载失败。检查：
>   1. 港股：股票代码格式（如 `2498.HK`，不是 `02498.HK`）
>   2. A 股：代码后缀（`.SS`/`.SZ`）
>   3. 网络/API 可达性（`curl -s https://www1.hkexnews.hk/search/prefix.do?...`）
> 不能用 web_search 凑信息代替 PDF 下载——PDF 是事实索引的根基。

---

## 6. Step 2 — 业务分部识别（决定结构的关键步骤）

调用 `evi-business-segmentation`：

读取：
- `base/financials/segments/segment_data.json`（财报披露）
- `base/financials/mdna/*.md`
- `base/transcripts/raw/*.md`

输出：`business_segments.json`：

```jsonc
{
  "company_name": "腾讯科技",
  "structure_type": "multi_segment",   // single_segment | multi_segment
  "segments": [
    {
      "segment_id": "vas",
      "name": "增值服务（VAS）",
      "revenue_share_pct": 55.3,
      "sub_segments": ["games_domestic", "games_overseas", "social_network"],
      "candidate_methods": ["DCF", "Comps"]
    },
    {
      "segment_id": "marketing_services",
      "name": "营销服务",
      "revenue_share_pct": 21.5,
      "candidate_methods": ["DCF", "Comps"]
    },
    {
      "segment_id": "fintech",
      "name": "金融科技与企业服务",
      "revenue_share_pct": 14.7,
      "sub_segments": ["fintech", "cloud"],
      "candidate_methods": ["DCF", "PS"]
    }
    // ... 可以是 N 个，前端会按这里的实际数量展示
  ],
  "n_segments": 4,
  "decision_rationale": "公司财报披露 4 个一级分部，各分部业务模式差异大（游戏 vs 金融科技），需用 SOTP"
}
```

**结构决策规则**：

| N | structure_type | 报告产出 | 估值方法 |
|---|---|---|---|
| 1 | single_segment | 仅 company_overview.md | 整体估值（DCF/PS/PEG 任选） |
| 2-3 | multi_segment | company_overview.md + 2-3 segments | 简化 SOTP |
| 4-10 | multi_segment | company_overview.md + N segments | 完整 SOTP |
| > 10 | multi_segment + 二级聚合 | 总报告 + N 一级 + 关键二级 | 一级 SOTP（二级合到一级） |

---

## 7. Step 3 — 并发采集补充材料（按结构类型分发）

### 7.1 公司层并发任务（永远跑）

| 任务 | 内容 | 输出 |
|---|---|---|
| **A** 财报下载+解析 | 复用 sirius-valuation/download_knowledge.py | base/financials/parsed/* + mdna/* |
| **B** 研报搜索 | 保留分析师完整分析（不只摘结论） | base/research/raw/* |
| **C** 行业数据 | 行业规模/政策/竞争格局 | base/external/industry_*.md |

### 7.2 分部层并发任务（仅 multi_segment 模式）

对每个 segment 并发：

| 任务 | 内容 | 输出 |
|---|---|---|
| **D-{seg}** 分部产品数据 | 该分部的产品/客户/出货/榜单等 KPI | base/external/segment_{seg_id}_*.md |
| **E-{seg}** 分部 peers | 该分部的可比公司（不同分部 peer 集合不同） | base/peers/{seg_id}/peer_summary.json |

> ⚠️ 不同分部的 peer 完全不同！腾讯云的 peer 是阿里云/AWS，腾讯游戏的 peer 是网易/EA。

### 7.3 财报子任务的关键步骤

下载完成后：
1. PDF → Markdown（parse_pdf.py）
2. 验证（含 revenue/profit/balance sheet 才保留）
3. 提取 MD&A
4. **以 FMP 指标为 key**，从 MD&A 找变化原因（"为什么毛利率升了"）写入 `key_metrics.change_reason`
5. **提取分部级数据**（FMP 没有的）：每个 segment 的 revenue / EBIT / capex / KPI

### 7.4 研报必须保留的内容

每篇研报摘要必须有：
- 核心论点与分析逻辑（**不是只摘结论**）
- 关键假设（增速/毛利率/WACC，含数字+依据）
- 估值方法与推导
- 风险提示

---

## 8. Step 4 — 整合产业调研报告

### 8.1 公司总报告 `reports/company_overview.md`

```markdown
# {公司名} 产业调研总报告

## 目录
- [1. 一句话定位](#1-一句话定位)
- [2. 行业全景](#2-行业全景)
- [3. 公司全景](#3-公司全景)
- [4. 财务总览](#4-财务总览)
- [5. 业务分部概览](#5-业务分部概览)  ← multi_segment 才有
- [6. 管理层与治理](#6-管理层与治理)
- [7. 市场观点汇总](#7-市场观点汇总)
- [8. 估值锚点](#8-估值锚点)
- [9. 数据缺口与不确定性](#9-数据缺口与不确定性)
- [Facts Index](#facts-index)

## 1. 一句话定位
{公司}是{行业}{细分}领域的{龙头/挑战者/...}，主营{核心业务}，2025 收入 {X}，估值锚点 {PE_TTM}/{PS_TTM}。

## 2. 行业全景
- 市场规模与增速：{...} [^1]
- 竞争格局：{Top 5 + 市占率} [^2]
- 政策/监管：{...} [^3]
- 技术趋势：{AI / 国产化 / ...} [^4]

## 3. 公司全景
- 业务模式：{B2B/B2C/平台}
- 商业模式：{收入构成}
- 护城河：{品牌/技术/网络效应/规模/生态} 评估
- 历史里程碑

## 4. 财务总览（用表格）
| 指标 | 2023A | 2024A | 2025A | TTM | YoY% |
|---|---|---|---|---|---|
| Revenue | ... | ... | ... | ... | ... |
| Gross Margin | ... | ... | ... | ... | ... |
| EBIT Margin | ... | ... | ... | ... | ... |
| Net Margin | ... | ... | ... | ... | ... |
| ROE | ... | ... | ... | ... | ... |
| FCF | ... | ... | ... | ... | ... |

**TTM 估值倍数**：PE_TTM / PS_TTM / EV/EBITDA_TTM

## 5. 业务分部概览（仅 multi_segment）
| 分部 | 收入占比 | 毛利率 | 增速 | 估值方法 | 详细报告 |
|---|---|---|---|---|---|
| 腾讯云 | 14% | 22% | +18% | DCF + EV/Sales | [→ 详见](segments/cloud.md) |
| 游戏 | 55% | 60% | +12% | DCF + Comps | [→ 详见](segments/games.md) |
| ... | ... | ... | ... | ... | ... |

## 6. 管理层与治理
- 创始人 / CEO 背景
- 股权结构
- 关键人事变动

## 7. 市场观点汇总
**研报**：
- 高盛 2026-04 目标价 700 HKD，关键假设：云增速 25%、游戏稳态利润率 35% [^10]
- 摩根士丹利 2026-03 目标价 650 HKD，关键差异点：云增速更保守 18% [^11]

**电话会**（最近一次）：
- 管理层指引 2026 收入双位数增长 [^12]
- 重点提到 AI 商业化进展 [^13]

## 8. 估值锚点
- 当前价格：HKD 441
- 历史 PE 中位：18x（当前 22x）
- 行业平均 PS：5x（当前 6.2x）
- DDM 价值下限（基于股息）：~110 HKD

## 9. 数据缺口与不确定性
- ❌ 云业务的 GMV / 客户数未单独披露 → Phase 2 估值需用代理变量
- ⚠️ 游戏海外收入分国家数据缺失 → 需用流水榜单估算
- ⚠️ AI 商业化收入占比未单独披露 → 需用电话会管理层指引推断

---

## Facts Index
[1] fact_id=fact_001 | reliability=high
    text: 中国云计算市场 2025 规模 8500 亿，CAGR 25%。
    source: IDC 2025Q4 报告 (base/external/industry_cloud.md)

[2] ...
```

### 8.2 分部子报告 `reports/segments/{seg_id}.md`

每个分部一份独立报告，结构如下：

```markdown
# {分部名} 分部调研

## 目录
- [1. 业务定位](#1-业务定位)
- [2. 行业子赛道](#2-行业子赛道)
- [3. 业务模式与变现](#3-业务模式与变现)
- [4. 关键运营数据](#4-关键运营数据)
- [5. 财务表现](#5-财务表现)
- [6. 竞争格局](#6-竞争格局)
- [7. Peers 对比](#7-peers-对比)
- [8. 增长驱动与风险](#8-增长驱动与风险)
- [9. 估值方法预选](#9-估值方法预选)
- [Facts Index](#facts-index)

## 1. 业务定位
{这个分部在公司中的角色，行业位置}

## 2. 行业子赛道
- 子赛道规模：{...}
- 增速：{...}
- 关键玩家：{...}

## 3. 业务模式与变现
- 收入模型（订阅/广告/抽成）
- 客户结构（B2B/B2C，集中度）
- 定价能力

## 4. 关键运营数据
| 指标 | 2023A | 2024A | 2025A | TTM |
|---|---|---|---|---|
| 收入 | ... | ... | ... | ... |
| 毛利率 | ... | ... | ... | ... |
| 用户数/客户数 | ... | ... | ... | ... |
| ARPU | ... | ... | ... | ... |
| 行业相关 KPI | ... | ... | ... | ... |

## 5. 财务表现
（图表 + 解读，引用 [N]）

## 6. 竞争格局
（市占率 + 主要对手 + 差异化）

## 7. Peers 对比
| Peer | 收入规模 | 增速 | 毛利率 | EV/Sales | PE |
|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... |

## 8. 增长驱动与风险
**核心驱动**：
- {Driver 1} [^N]
- {Driver 2}

**核心风险**：
- {Risk 1}
- {Risk 2}

## 9. 估值方法预选
- 推荐 Primary：DCF（理由：盈利稳定 + 收入可见度高）
- 推荐 Cross-check：EV/Sales + Comps
- DDM 不适用（不单独派息）

---

## Facts Index
[N] fact_id=... | reliability=high
    text: ...
    source: ...
```

---

## 9. Step 5 — 提取结构化事实库

```bash
python3 .agents/skills/evi-toolkit/scripts/format_facts.py \
    --data-dir data/{symbol_dir}
```

从所有 reports/*.md（含 segments/*）抽取 fact，写入 `information/indexed_facts.json`：

```jsonc
{
  "facts": [
    {
      "fact_id": "fact_cloud_001",
      "segment_id": "cloud",          // 或 "group" 表示公司级
      "topic": "revenue_growth",
      "text": "2025 云业务收入 1080 亿，YoY +18%",
      "value": 108000,
      "unit": "RMB million",
      "reliability": "high",
      "source": "doc_2025_annual#segment_cloud",
      "report_section": "reports/segments/cloud.md#5-财务表现"
    }
  ]
}
```

---

## 10. Step 6 — CHECKLIST + 持久化

```bash
python3 .agents/skills/evi-toolkit/scripts/build_checklist.py \
    --data-dir data/{symbol_dir} --required-periods 6

python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \
    --entry-id {entry_id} --data-dir data/{symbol_dir} \
    --display-name "{display_name}" --symbol "{symbol}" --market "{market}" \
    --status partial
```

---

## 11. Step 7 — 汇报

告知用户：
- 公司结构判断：单一业务 / N 分部
- 总报告大小 + 分部报告数量
- 数据收集状态（FMP 几期 / 财报几份 / 研报几家 / 电话会几期）
- CHECKLIST 状态
- 关键数据缺口
- 提示"发送「继续估值分析」触发 Phase 2"

---

## 12. 与 sirius-valuation 的复用关系

| 功能 | 使用脚本 |
|---|---|
| 财报/公告下载 | evi-toolkit/scripts/evi_download_knowledge.py --financials --announcements |
| 知识库管理 | sirius-valuation/scripts/manage_knowledge.py |
| FMP 数据 + TTM + 增长率 | evi-toolkit/scripts/evi_fetch_data.py |
| 电话会下载 | evi-toolkit/scripts/evi_download_knowledge.py --transcripts |

---

## 13. 失败处理

- 财报下载 0 → 用 WebSearch 找官方 IR 网站补
- 某分部数据严重缺失 → 在该 segment 报告中明确标注"数据缺口"，Phase 2 会反向请求补充
- 3 轮 loop back 后仍 blocked → persist partial + 在总报告"数据缺口"段说明
- **绝不用模拟数据填坑**
