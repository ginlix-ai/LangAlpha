"""EVI Strategy template — 自适应估值（整体 / SOTP）+ 持续监控驱动重估。

Pipeline (all skills under skills/evi-*):

  Phase 1 — 产业调研（evi-data-orchestrator 总控）
    1. 自动化数据获取（FMP + TTM + 增长率 + 电话会）
    2. 业务分部识别（决定 single_segment / multi_segment）
    3. 并发采集补充材料（财报解析/研报/peers/行业数据）
    4. 整合产业调研报告：
       - single_segment → reports/company_overview.md
       - multi_segment  → reports/company_overview.md + reports/segments/{seg_id}.md × N
    5. 提取索引事实库 indexed_facts.json
    6. CHECKLIST 质检 + persist(partial)

  Phase 2 — 估值分析（evi-valuation-analysis 总控）
    1. 估值方法路由（按业务特征选方法）
    2. 假设构建（增长桥/利润桥/风险调整）
    3. 多方法估值（segment × method 并发；single_segment 时只有公司层）
    4. 数据闭环检查（缺数据时反向请求 Phase 1 补充）
    5. 反向估值（输出 rerate_triggers）
    6. 整体汇总（single_segment）或 SOTP 汇总（multi_segment）
    7. 写 facets.json + persist(completed)

  Phase 3 — 持续跟踪（automation skill 注册定时任务）
    1. evi-monitor 扫描新材料 + 检查 rerate_triggers
    2. evi-revaluation-updater 更新 Phase 1 调研 + 重算 Phase 2 估值
    3. 写 memory.md 变更日志
"""

from typing import Any

from src.server.models.template import TemplateField, TemplateManifest
from src.server.templates.registry import TemplateDefinition


_MARKET_LABELS = {
    "hk": "港股 (HK)",
    "us": "美股 (US)",
    "cn": "A 股 (CN)",
}


_EVI_PROMPT = (
    "对 {display_name}{symbol_clause}（市场：{market_label}）执行 EVI 估值策略的 **Phase 1：产业调研**。\n"
    "\n"
    "Read `.agents/skills/evi-data-orchestrator/SKILL.md`，按其编排方案执行。\n"
    "\n"
    "{fetch_step}\n"
    "\n"
    "完成后用户可以发「继续估值分析」触发 Phase 2（调用 `evi-valuation-analysis` skill）。\n"
)


_EVI_AGENT_MD = """\
---
workspace_name: {workspace_name}
description: [{template_name}] {entry_key}
template_id: evi-strategy
entry_id: {entry_id}
entry_key: {entry_key}
---

# {workspace_name} — {template_name}

## 模板思路（一句话）

> 估值不是一次性。本模板让你给 {display_name} 建立**自适应估值体系**——
> 单一业务用整体估值，多业务用 SOTP（按真实分部数动态展示），
> 配合持续监控驱动的重估闭环，所有结论以人类可读 markdown 报告呈现。

---

## 五条铁律

1. **报告优先 + 表格化**：每个 Phase 的主交付物是 `reports/*.md`。
   - 有引用 `[N]`、附 `## Facts Index`。
   - 能用表格的数据必须用表格。
   - 强调分析过程：驱动因子 → 假设依据 → 数值计算 → 结论。
2. **严禁模拟数据**：`reliability:simulated` 绝不允许。所有数字必须追溯到 FMP / 财报 / WebSearch。
3. **CHECKLIST 门禁**：build_checklist.py 输出 blocked → 禁止进入下一步，必须 loop back 补数据。
4. **修改记录走 memory.md**：所有偏好、规则、估值变化追加到 `.agents/workspace/memory/memory.md`。
5. **多 Agent 并发**：数据采集、各分部估值天然可并行——用子 agent 同时执行。

---

## 核心设计：估值结构匹配业务结构

```
Phase 1: 业务分部识别（基于财报披露）
   └─ N = 业务分部数

   ┌──────────────┴──────────────┐
   │                             │
N == 1                       N >= 2
（小公司/单一业务）            （多业务公司，如腾讯）
   │                             │
   ▼                             ▼
单一报告                      总-分结构
└─ company_overview.md        ├─ company_overview.md（公司总报告）
                              └─ segments/
                                   ├─ {{seg_1}}.md  ← 如腾讯云
                                   ├─ {{seg_2}}.md  ← 如游戏
                                   └─ ...           ← N 个分部

整体估值                      SOTP 估值（各分部独立估值后加总）
```

---

## 任务拆分（两个 Phase）

| Phase | 内容 | 主交付物 |
|---|---|---|
| **Phase 1**（首条消息） | 产业调研：数据采集 + 总报告 + 分部报告 | reports/company_overview.md（+ segments/*.md）+ indexed_facts.json |
| **Phase 2**（"继续估值分析"） | 估值分析：路由 + 假设 + 多方法估值 + SOTP | reports/final.md + facets.json |
| **Phase 3**（自动） | 监控驱动重估（automation 定时触发） | 持续更新 facets.json + memory.md |

**关键创新**：Phase 2 在估值过程中如果发现数据不足，可以**反向请求 Phase 1 补数据**，迭代闭环。

---

## 持续跟踪闭环

```
Phase 2 完成后 → 通过 automation skill 注册定时监控
        ↓
定时触发（如每周一 9 AM）
        ↓
evi-monitor 扫描新材料 + 检查 rerate_triggers
        ↓
发现变化 → evi-revaluation-updater
        ├─ 更新 Phase 1 调研报告（追加新 facts）
        ├─ 重算受影响分部 × 方法
        ├─ SOTP 重新汇总
        └─ 更新 facets.json + memory.md
        ↓
看板自动刷新
```

注册命令（Phase 2 完成后建议）：

```python
# 调用 automation skill 的 create_automation
create_automation(
    name="EVI {display_name} 周度监控",
    instruction="对 {entry_key} 执行 evi-monitor，发现变化则调用 evi-revaluation-updater",
    schedule="0 9 * * 1",  # 每周一 9 AM
    thread="persistent",
)
```

---

## 多 Agent 并发协议

### 何时使用
- 多个数据源并行（FMP / 财报 / 研报 / peers）
- 多个 segment 的产业调研并行写
- 多个 segment × method 的估值并行算

### 协议
- 子 agent 独立执行（平台原生支持）
- 主 agent 等待所有子 agent 完成后再聚合
- 子 agent 失败 → 主 agent 决定重试或标注缺失
- 不要让子 agent 互相依赖

---

## 项目目录约定

```
data/{symbol_dir}/
├── reports/                          ← 主交付物
│   ├── company_overview.md           公司总报告（永远有）
│   ├── segments/                     ← multi_segment 才有
│   │   ├── {{seg_id_1}}.md           分部产业调研
│   │   ├── {{seg_id_1}}_valuation.md 分部估值
│   │   ├── {{seg_id_2}}.md
│   │   └── ...
│   ├── valuation.md                  整体估值（single_segment）
│   ├── valuation_summary.md          SOTP 汇总（multi_segment）
│   ├── reverse_valuation.md          反向估值
│   ├── final.md                      ⭐ 最终结论
│   ├── data.md                       数据索引
│   └── monitor.md                    监控记录
│
├── facets.json                       ← ⭐ 看板 source of truth
├── business_segments.json            ← 决定 single/multi_segment
├── valuation_method_matrix.json
│
├── base/                             基础数据库（FMP / 财报 / 研报 / 电话会 / Peers）
├── information/indexed_facts.json    索引事实库
├── valuation/{{group,segment_id}}/   估值结果与假设账本
└── monitor/                          持续监控产物
```

---

## 前端 Tab（按真实分部数动态）

```
Tab 顺序：
  ① 估值结论        → final.md / valuation*.md / reverse_valuation.md
  ② 分部 1（如腾讯云） → segments/cloud.md + cloud_valuation.md
  ③ 分部 2（如游戏）   → segments/games.md + games_valuation.md
  ④ ...              → 实际几个分部就几个 Tab
  ⑤ 自动化任务       → monitor 状态 + automation + rerate_triggers
  ⑥ 数据收集         → CHECKLIST + 数据索引
```

> single_segment 模式：分部 Tab 退化为"产业调研"Tab。

---

## Skill 速查表

| Skill | 职责 |
|---|---|
| **evi-data-orchestrator** | Phase 1 总控（产业调研编排） |
| **evi-business-segmentation** | 识别业务分部，决定 single/multi_segment |
| **evi-information-search** | 信息搜集（按估值需要） |
| **evi-valuation-analysis** | Phase 2 总控（估值分析编排） |
| **evi-valuation-router** | 选估值方法 |
| **evi-assumption-builder** | 构建假设（增长桥/利润桥/风险） |
| **evi-valuation-dcf / ps / peg / ddm / comps** | 各估值方法（每个 SKILL.md 含完整方法论） |
| **evi-reverse-valuation** | 反向估值 + 输出 rerate_triggers |
| **evi-valuation-orchestrator** | SOTP 汇总 + facets.json |
| **evi-monitor** | 持续监控 |
| **evi-revaluation-updater** | 更新调研 + 重算估值 |
| **automation**（平台） | 注册定时/价格触发任务 |
| **sirius-valuation** | 财报/公告/研报下载（被 EVI 复用） |
| **evi-toolkit** | 共享脚本（FMP / TTM / 增长率 / CHECKLIST / persist） |

---

## 核心命令速查

### 数据获取（自动化）
```bash
python3 .agents/skills/evi-toolkit/scripts/evi_fetch_data.py \\
    --symbol {entry_key} --market {market} --data-dir data/{symbol_dir} \\
    --quarterly --ttm --growth-rates
```

### 财报/公告下载
```bash
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \\
    --symbol {entry_key} --market {market} --financials --announcements --years 4 \\
    --data-dir data/{symbol_dir}
```

### 估值方法（每分部独立）
```bash
python3 .agents/skills/evi-valuation-dcf/scripts/dcf_calc.py \\
    --data-dir data/{symbol_dir} --segment <seg_id>
```

### 看板更新（修改估值结论后必须跑）
```bash
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \\
  --entry-id {entry_id} \\
  --data-dir data/{symbol_dir} \\
  --display-name "{display_name}" \\
  --symbol "{entry_key}" \\
  --market "{market}"
```

---

## 注意事项

1. **upside_pct 必须是 number**（标量），不是 dict。
2. **facets.json 是看板 source of truth**——重要数字必须写入。
3. **structure_type 字段**（single_segment / multi_segment）决定前端 Tab 形式。
4. **facets.segments 数组**控制有几个分部 Tab。每个 segment 必须有 `segment_id` + `name`。
5. **rerate_triggers 是监控的核心**：reverse_valuation 必须输出，monitor 必须检查。
6. **总-分对应**：腾讯云、游戏、广告等每个业务一个分部 Tab，估值就是分部估值之和。
7. **fact 编号在每个 report 内连续**：[1] [2] 是 report 内引用，fact_id 全局唯一。
8. **修改记录走 memory.md**，不要写在 agent.md。
9. **报告必须用表格 + 写清推导**，不能只有结论。
10. **季报数据用 FMP**（period=quarter），不强求 PDF。
11. **港美股电话会 FMP 直取**；A 股需 WebSearch。
12. **研报必须保留分析过程**，不能只摘结论/目标价。
13. **复用 sirius-valuation** 的财报/公告下载脚本。
14. **Phase 2 → Phase 1 反向请求**：缺数据时实例化子 agent 调 evi-information-search 补，再迭代估值。

---

## Thread Index

## Key Findings

## File Index
"""


def _enrich_evi_params(
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    has_symbol = bool(entry_key) and not entry_key.startswith("auto_")
    market_raw = (params.get("market") or "").strip()

    extra: dict[str, Any] = {}
    extra["symbol_clause"] = f"（代码 {entry_key}）" if has_symbol else ""
    extra["market_label"] = _MARKET_LABELS.get(market_raw, "由你结合上下文判断")

    if has_symbol:
        market_arg = f" --market {market_raw}" if market_raw else ""
        extra["fetch_step"] = (
            f"Bash 运行 `python3 .agents/skills/evi-toolkit/scripts/init_project.py "
            f"--symbol {entry_key}{market_arg}` 创建项目骨架。"
        )
    else:
        extra["fetch_step"] = (
            f"先用 web_search / SEC 工具查出 {display_name or '该公司'} 的官方股票代码"
            "（含交易所后缀，如 0700.HK / TCEHY / 600519.SS），再 Bash 运行 "
            "`python3 .agents/skills/evi-toolkit/scripts/init_project.py --symbol <代码>`。"
            "如检索失败请把状态置 failed 并写明原因。"
        )

    return extra


# ---------------------------------------------------------------------------
# Seed files: initial memory.md
# ---------------------------------------------------------------------------

_EVI_MEMORY_SEED = """\
# Workspace Memory — {display_name}

> 这是工作区级 memory.md，由 Agent 与用户共同维护。
> 模板说明、目录结构、skill 用法都在 `agent.md`；本文件**只记录变化**。
>
> 路径：`.agents/workspace/memory/memory.md`

## 当前规则配置

- 分析标的：{display_name}（{entry_key}，{market_label}）
- 估值框架：EVI Strategy（自适应整体估值 / SOTP；DCF/PS/PEG/DDM/Comps + Reverse）
- 默认估值权重：primary=0.5；cross_check 平均分剩 0.5；偏离>30% 砍半；confidence<0.4 砍半
- 默认监控范围：financial_reports, earnings_calls, research_reports, industry_news, product_metrics, rerate_triggers
- 默认监控频率：每周一次（待用户在 Automation 中实例化）

> 用户/Agent 修改任一规则时，把修改追加到下面"修改记录"段落，**不要修改"当前规则配置"**——而是在记录里说明新值。

## 修改记录

<!--
追加格式（按时间倒序）：
### YYYY-MM-DD HH:MM
- 触发：…
- 范围：（哪个 segment / 假设 / 权重 / 监控规则）
- 新值：…
- 估值变化：base 从 X 变为 Y
- 操作：（跑了什么脚本）
-->

## 监控触发记录

<!--
每次 evi-monitor 跑完后追加；evi-revaluation-updater 处理后再补"重估完成"记录。
-->
"""


def _build_evi_seed_files(
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
) -> list[tuple[str, str]]:
    """Seed `.agents/workspace/memory/memory.md`。"""
    market_raw = (params.get("market") or "").strip()
    ctx = {
        "entry_key": entry_key,
        "display_name": display_name or entry_key,
        "market_label": _MARKET_LABELS.get(market_raw, "由你结合上下文判断"),
    }
    body = _EVI_MEMORY_SEED.format(**ctx)
    return [(".agents/workspace/memory/memory.md", body)]


EVI_STRATEGY = TemplateDefinition(
    manifest=TemplateManifest(
        id="evi-strategy",
        name="EVI 估值策略",
        description=(
            "自适应估值 + 持续监控的完整投研体系。"
            "小公司用整体估值；多业务公司用 SOTP（按真实分部数动态展示）。"
            "Phase 1 产业调研建立公司画像 + 索引事实库；"
            "Phase 2 基于调研做估值（DCF/PS/PEG/DDM/Comps + Reverse），"
            "并通过 automation 定时监控新材料触发自动重估。"
        ),
        icon="layers",
        version="3.0.0",
        estimated_minutes=25,
        fields=[
            TemplateField(
                name="display_name",
                label="公司名称",
                type="text",
                required=True,
                placeholder="例如 腾讯科技",
            ),
            TemplateField(
                name="entry_key",
                label="股票代码（可选）",
                type="text",
                required=False,
                placeholder="例如 0700.HK / TCEHY / 600519.SS（留空 Agent 自动检索）",
            ),
            TemplateField(
                name="market",
                label="市场（可选）",
                type="select",
                required=False,
                options=[
                    {"value": "", "label": "自动判断"},
                    {"value": "hk", "label": "港股 (HK)"},
                    {"value": "us", "label": "美股 (US)"},
                    {"value": "cn", "label": "A 股 (CN)"},
                ],
            ),
        ],
    ),
    initial_prompt_template=_EVI_PROMPT,
    agent_md_template=_EVI_AGENT_MD,
    workspace_name_builder=lambda key, name, params: (
        f"{name or key}" + (f"（{key}）" if (name and key and name != key) else "")
    ),
    params_enricher=_enrich_evi_params,
    seed_files_builder=_build_evi_seed_files,
)
