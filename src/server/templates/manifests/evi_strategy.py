"""EVI Strategy template — 自适应估值（整体 / SOTP）+ 持续监控驱动重估。

v3.1: 合并为单一 prompt（不再分 Phase 1 / Phase 2），精简 agent.md。
"""

from typing import Any

from src.server.models.template import TemplateField, TemplateManifest
from src.server.templates.registry import TemplateDefinition


_MARKET_LABELS = {
    "hk": "港股 (HK)",
    "us": "美股 (US)",
    "cn": "A 股 (CN)",
}


# ---------------------------------------------------------------------------
# Initial Prompt（单一 prompt，不再分两条消息）
# ---------------------------------------------------------------------------

_EVI_PROMPT = (
    "对 {display_name}{symbol_clause}（市场：{market_label}）执行 **EVI 完整估值分析**。\n"
    "\n"
    "Read `.agents/skills/evi-data-orchestrator/SKILL.md`，按其编排方案执行产业调研；\n"
    "调研完成后**立即**继续 Read `.agents/skills/evi-valuation-analysis/SKILL.md`，执行估值分析。\n"
    "不需要等待用户确认，一次性跑完调研+估值全流程。\n"
    "\n"
    "{fetch_step}\n"
    "\n"
    "**全流程**：\n"
    "1. 数据获取 + 产业调研（company_overview + 分部报告）\n"
    "2. 估值分析（路由 + 假设 + 多方法 + SOTP/整体 + 反向估值）\n"
    "3. 输出 facets.json + persist + 建议注册监控\n"
    "\n"
    "完成后在 `reports/changelog.md` 记录本次分析摘要。\n"
)


# ---------------------------------------------------------------------------
# Agent.md — 大幅精简，不再重复 skill 内容
# ---------------------------------------------------------------------------

_EVI_AGENT_MD = """\
---
workspace_name: {workspace_name}
description: [{template_name}] {entry_key}
template_id: evi-strategy
entry_id: {entry_id}
entry_key: {entry_key}
---

# {workspace_name} — {template_name}

> 本 Agent 给 {display_name} 建立自适应估值体系，持续跟踪并自动重估。

---

## 核心行为准则

### 1. 首次分析（一次性跑完）

读 `evi-data-orchestrator/SKILL.md` 做产业调研 → 读 `evi-valuation-analysis/SKILL.md` 做估值分析。
**不等用户说"继续"，一次性完成全流程。**

### 2. 用户/监控提出问题时 → 更新材料 → 修改估值（最关键）

⚠️ **这是你最常遇到的场景，优先级最高**。

当用户说了以下任何一种：
- "这个数据不对"
- "XX 消息影响估值"
- "帮我分析一下这条新闻"
- "更新一下最新季报"
- 或 automation 自动收到了新材料

**你的标准动作链**（不需要用户逐步指导）：
```
① 理解变化：弄清什么变了（哪个事实/数据/假设受影响）
② 更新材料：
   - 修改对应的 reports/*.md（调研报告）
   - 更新 information/indexed_facts.json（新增/修正 fact）
   - 如需要：重新跑脚本拉最新数据
③ 修改估值：
   - 判断影响哪个分部 × 哪个假设
   - 重算受影响的估值方法（重跑 dcf_calc.py 等）
   - 更新 valuation/{segment}/assumption_ledger.json
   - 重新汇总 SOTP / 整体估值
④ 更新交付物：
   - 重写 reports/final.md（结论）
   - 更新 facets.json
   - 跑 persist_evi_report.py 刷新看板
⑤ 写更新记录：
   - 在 reports/changelog.md 顶部追加本次变更纪要
   - 格式见下方 §changelog 格式
```

**绝对不要回复"好的我知道了"就完事。必须执行到 ⑤ 才算完成。**

### 3. 完成任何任务后 → 写更新记录

无论是首次分析、用户指正、还是监控自动触发的重估，**完成后都必须在 `reports/changelog.md` 顶部追加记录**。

#### changelog 格式

```markdown
### YYYY-MM-DD HH:MM — [触发类型]

**触发**：[用户指出/监控发现/首次分析]（一句话说明原因）
**影响范围**：[哪些分部/假设/方法受影响]
**关键变化**：
- XXX 从 A 改为 B（原因：[N]）
- ...
**估值变化**：base 从 X.XX → Y.YY（+Z%）
**操作**：[跑了什么脚本/改了什么文件]
```

---

## 铁律（5 条不可违反）

1. **严禁模拟数据** — 所有数字必须有来源（FMP / 财报 / WebSearch）
2. **报告用表格 + 写清推导** — 不能只有结论
3. **修改估值后必须更新 facets.json** — 看板靠它刷新
4. **完成任务后必须写 changelog.md** — 这是用户跟踪你工作的唯一方式
5. **缺数据时主动补** — 实例化子 agent 跑脚本/搜索，不要让用户催

---

## 项目目录（简版）

```
data/{symbol_dir}/
├── reports/
│   ├── company_overview.md      公司总调研
│   ├── segments/*.md            分部调研 + 分部估值
│   ├── final.md                 ⭐ 最终结论
│   ├── reverse_valuation.md     反向估值
│   ├── changelog.md             ⭐ 更新记录（本 agent 负责维护）
│   └── monitor.md               监控日志
├── facets.json                  ⭐ 看板数据
├── base/                        FMP + 财报 + 研报 + 电话会
├── information/indexed_facts.json
├── valuation/                   估值结果
└── monitor/                     监控产物
```

---

## 核心命令速查

```bash
# 数据获取
python3 .agents/skills/evi-toolkit/scripts/evi_fetch_data.py \\
    --symbol {entry_key} --market {market} --data-dir data/{symbol_dir} \\
    --quarterly --ttm --growth-rates

# 财报下载
python3 .agents/skills/evi-toolkit/scripts/evi_download_knowledge.py \\
    --symbol {entry_key} --market {market} --financials --announcements --years 4 \\
    --data-dir data/{symbol_dir}

# DCF 估值（每分部独立）
python3 .agents/skills/evi-valuation-dcf/scripts/dcf_calc.py \\
    --data-dir data/{symbol_dir} --segment <seg_id>

# 看板刷新（任何改估值之后必跑）
python3 .agents/skills/evi-toolkit/scripts/persist_evi_report.py \\
  --entry-id {entry_id} --data-dir data/{symbol_dir} \\
  --display-name "{display_name}" --symbol "{entry_key}" --market "{market}"
```

---

## Skill 列表（详细方法论在各 SKILL.md 里，此处不重复）

| Skill | 一句话 |
|---|---|
| evi-data-orchestrator | 产业调研总控 |
| evi-valuation-analysis | 估值分析总控 |
| evi-toolkit | 共享脚本库 |
| evi-market-sizing | **市场空间推演**（TAM→SAM→SOM，Bottom-Up 量价推导） |
| evi-assumption-builder | 假设构建（消费 market-sizing 的结果） |
| evi-valuation-dcf/ps/peg/ddm/comps | 各估值方法 |
| evi-reverse-valuation | 反向估值 + rerate_triggers |
| evi-valuation-orchestrator | SOTP 汇总 + facets |
| evi-monitor | 持续监控 |
| evi-revaluation-updater | 增量重估 |

> 各 Skill 的完整指南在 `.agents/skills/evi-*/SKILL.md`，需要时 Read 对应文件即可。
> **不要凭记忆执行**——每次都 Read 最新 SKILL.md。

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
# Seed files
# ---------------------------------------------------------------------------

_EVI_MEMORY_SEED = """\
# Workspace Memory — {display_name}

> 路径：`.agents/workspace/memory/memory.md`
> 只记录用户偏好变化和规则覆盖。不要记录每次分析的变化（那些写 changelog.md）。

## 当前规则配置

- 分析标的：{display_name}（{entry_key}，{market_label}）
- 估值框架：EVI Strategy（自适应整体 / SOTP）
- 默认估值权重：primary=0.5；cross_check 平分 0.5；偏离>30% 砍半
- 默认监控范围：financial_reports, earnings_calls, research_reports, industry_news, product_metrics

## 用户偏好

<!--
用户通过对话明确的偏好，如：
- "我更看重 DCF"
- "机器人业务用 PS，不要 DCF"
- "监控频率改为每日"
-->
"""

_EVI_CHANGELOG_SEED = """\
# 更新记录 — {display_name}

> 每次完成分析/修改/重估后，在此文件**顶部**追加一条记录。
> 这是用户跟踪 AI 工作进展的唯一入口。
> 前端"更新记录"Tab 直接渲染此文件。

---

"""


def _build_evi_seed_files(
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
) -> list[tuple[str, str]]:
    """Seed memory.md + changelog.md。"""
    market_raw = (params.get("market") or "").strip()
    ctx = {
        "entry_key": entry_key,
        "display_name": display_name or entry_key,
        "market_label": _MARKET_LABELS.get(market_raw, "由你结合上下文判断"),
    }
    return [
        (".agents/workspace/memory/memory.md", _EVI_MEMORY_SEED.format(**ctx)),
        # changelog 也 seed 到 data 目录（persist 时会读取）
    ]


EVI_STRATEGY = TemplateDefinition(
    manifest=TemplateManifest(
        id="evi-strategy",
        name="EVI 估值策略",
        description=(
            "自适应估值 + 持续监控的完整投研体系。"
            "单一 prompt 一次性完成产业调研+估值分析；"
            "用户指出问题或监控发现新材料时，自动更新调研→修改估值→写 changelog。"
        ),
        icon="layers",
        version="3.1.0",
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
    release_notes={
        "3.1.0": {
            "summary": "单一 prompt + 更新记录 + 强化材料更新闭环",
            "changes": [
                "合并为单一 prompt：不再分 Phase 1/Phase 2，一次性跑完产业调研+估值",
                "新增「更新记录」功能：每次修改/重估后自动追加到 reports/changelog.md",
                "强化核心行为：用户指出问题 → 更新材料 → 修改估值 → 写 changelog（5 步闭环）",
                "精简 agent.md：不再重复 skill 内容，需要时 Read 最新 SKILL.md",
                "新增 4 类监控支持：指标阈值 / 事件型 / 产业链 / 竞品",
                "修复财报下载：港股 HKEx / A 股巨潮 / 美股 SEC 全面修复",
                "修复 FMP 电话会：V4 batch API 支持多季度拉取",
            ],
            "suggested_actions": [
                {
                    "label": "让 Agent 注册监控",
                    "prompt": "请帮我注册以下监控：1) 指标阈值（rerate_triggers）每周检查；2) 事件型（财报/公告发布）每日扫描；3) 竞品（peer 业绩+估值变化）每周跟踪。",
                },
                {
                    "label": "更新产业调研",
                    "prompt": "请基于最新数据更新 reports/company_overview.md 的产业调研内容，补充 2026Q1 最新数据和行业动态，完成后写 changelog。",
                },
                {
                    "label": "重跑完整估值",
                    "prompt": "请按照最新模板的完整流程重新执行估值分析：Read evi-data-orchestrator/SKILL.md 做调研，然后 Read evi-valuation-analysis/SKILL.md 做估值。完成后写 changelog。",
                },
            ],
        },
    },
)
