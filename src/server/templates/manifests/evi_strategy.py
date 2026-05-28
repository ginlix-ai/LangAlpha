"""EVI Strategy template — 自适应估值（整体 / SOTP）+ 持续监控驱动重估。

v3.1: 合并为单一 prompt（不再分 Phase 1 / Phase 2），精简 agent.md。
v3.7: 把 finalize 清单挪到模板层（声明式），由通用 finalize runner 执行。
"""

from typing import Any

from src.server.models.template import TemplateField, TemplateManifest
from src.server.templates.registry import (
    FinalizeExpected,
    FinalizeSpec,
    TemplateDefinition,
)


_MARKET_LABELS = {
    "hk": "港股 (HK)",
    "us": "美股 (US)",
    "cn": "A 股 (CN)",
}


# ---------------------------------------------------------------------------
# Skill whitelist for EVI workspace sandbox
# ---------------------------------------------------------------------------
# Only these skills get uploaded to the sandbox of an EVI workspace.
# 规则：
#   - 所有 evi-* 都保留（这是模板本身的实现）
#   - 与 evi-* 实现完全重合的通用 skill 排除（避免 Agent 选错）
#   - 其余通用 skill 全部加回 —— 它们是"工具"，不会和 evi 流程冲突，
#     反而能给 Agent 提供额外能力（产业分析、舆情、催化剂日历等）

_EVI_SKILL_WHITELIST: set[str] = {
    # --- EVI core (all kept) ---
    "evi-toolkit",
    "evi-data-orchestrator",
    "evi-base-data-builder",
    "evi-business-segmentation",
    "evi-information-search",
    "evi-market-sizing",
    "evi-assumption-builder",
    "evi-valuation-router",
    "evi-valuation-analysis",
    "evi-valuation-orchestrator",
    "evi-valuation-dcf",
    "evi-valuation-ps",
    "evi-valuation-peg",
    "evi-valuation-ddm",
    "evi-valuation-comps",
    "evi-reverse-valuation",
    "evi-quality-analysis",
    "evi-monitor",
    "evi-revaluation-updater",

    # --- Generic infrastructure (no overlap with EVI flow) ---
    "pdf",                   # parse financial-report PDFs
    "docx",                  # rare: user-supplied Word
    "xlsx",                  # export valuation tables
    "pptx",                  # optional pitch deck export
    "web-scraping",          # A-share earnings call fallback
    "automation",            # register monitoring crons
    "inline-widget",         # render charts in chat
    "interactive-dashboard", # optional interactive valuation dashboard
    "user-profile",          # read user preferences
    "self-improve",          # generic feedback path

    # --- Generic analytical skills (complementary to EVI, do NOT overlap) ---
    "competitive-analysis",  # D2 moat 分析时补充竞品矩阵
    "sector-overview",       # 产业地图，调研阶段补行业格局
    "earnings-analysis",     # 已发布财报解读（evi 里没有专门 skill）
    "earnings-preview",      # 财报前瞻，监控 cron 触发用
    "catalyst-calendar",     # 监控 automation 识别催化剂时间表
    "morning-note",          # 快速生成晨会简报（复用 EVI 已有数据）
    "thesis-tracker",        # 投资观点追踪，配合 changelog 用
    "idea-generation",       # reverse_valuation 借鉴 idea 框架
    "x-api",                 # 港股/A 股舆情补充信息源

    # NOTE: deliberately excluded — overlap with evi-* counterparts:
    #   dcf-model       (与 evi-valuation-dcf 重合)
    #   comps-analysis  (与 evi-valuation-comps 重合)
    #   3-statements    (EVI 走 FMP，不另起三表流程)
    #   check-model     (审查别人的模型，不适用 EVI 自产)
    #   check-deck      (审查别人的 deck)
    #   model-update    (EVI 用 evi-revaluation-updater)
    #   initiating-coverage (EVI 的 reports/final.md 即首次覆盖报告)
}


# ---------------------------------------------------------------------------
# Finalize spec — 声明 EVI 的产物清单 + 持久化脚本
# ---------------------------------------------------------------------------
# 这部分以前硬编码在 skills/evi-toolkit/scripts/wakeup_check.py 里。
# v3.7 起挪到模板层，由通用 finalize runner 在 agent generator drain 完
# 之后自动执行：
#   - required 缺 → 把缺件提示作为 user message 再喂给 Agent 一轮
#   - optional 缺 → 自动占位 + finalize 为 partial
#   - 全齐       → finalize 为 completed
# wakeup_check.py 保留为"开发者手动诊断"入口，不再是必经之路。

_EVI_EXPECTED_FILES: tuple[FinalizeExpected, ...] = (
    # ---- required ----（缺一不可，否则前端展示不完整 / DB 推不上去）
    FinalizeExpected(
        rel_path="facets.json",
        level="required",
        description=(
            "看板核心数据。必须有 fair_value / current_price / judgment 字段。"
            "通常由 evi-valuation-orchestrator 收尾时生成。"
        ),
    ),
    FinalizeExpected(
        rel_path="reports/final.md",
        level="required",
        description=(
            "最终估值结论报告（前端「估值结论」Tab 主体内容）。"
            "缺这个等于一无所有。"
        ),
    ),
    FinalizeExpected(
        rel_path="reports/changelog.md",
        level="required",
        description=(
            "更新记录。即使是首次分析也要写一条『首次分析』摘要。"
        ),
    ),
    FinalizeExpected(
        rel_path="base/CHECKLIST.json",
        level="required",
        description=(
            "数据质量记分卡。跑 `python3 .agents/skills/evi-toolkit/"
            "scripts/build_checklist.py --data-dir <dir>` 生成。"
        ),
    ),

    # ---- optional ----（缺则自动占位空骨架，最终 status=partial）
    FinalizeExpected(
        rel_path="quality.json",
        level="optional",
        description=(
            "4 维度定性分析结构化结论。强烈建议有 — 缺则前端定性卡为空。"
        ),
        placeholder='{"schema_version":"quality-1.0","dimensions":{},"summary":{}}',
    ),
    FinalizeExpected(
        rel_path="reports/quality.md",
        level="optional",
        description="4 维度定性分析报告（商业模式/护城河/管理层/MD&A 前瞻）。",
        placeholder=(
            "# 定性分析\n\n> 本次未生成。"
            "请让 Agent 跑 `evi-quality-analysis` skill 补齐。\n"
        ),
    ),
    FinalizeExpected(
        rel_path="reports/company_overview.md",
        level="optional",
        description="公司整体产业调研（single_segment 模式下尤其需要）。",
        placeholder="# 产业调研总报告\n\n> 本次未生成。\n",
    ),
    FinalizeExpected(
        rel_path="reports/reverse_valuation.md",
        level="optional",
        description="反向估值报告（验证当前股价隐含预期）。",
        placeholder="# 反向估值\n\n> 本次未生成。\n",
    ),
    FinalizeExpected(
        rel_path="information/indexed_facts.json",
        level="optional",
        description=(
            "事实索引。跑 `python3 .agents/skills/evi-toolkit/scripts/"
            "format_facts.py --data-dir <dir>` 生成。"
        ),
        placeholder='{"facts":[]}',
    ),
    FinalizeExpected(
        rel_path="business_segments.json",
        level="optional",
        description="业务分部识别结果。",
        placeholder=(
            '{"structure_type":"single_segment","segments":[],"n_segments":0}'
        ),
    ),
)


def _evi_data_dir(entry_key: str, display_name: str | None, params: dict[str, Any]) -> str:
    """Resolve sandbox-internal data dir from entry params。"""
    symbol_dir = (
        params.get("symbol_dir")
        or _safe_symbol_dir_for_finalize(entry_key)
    )
    return f"data/{symbol_dir}"


def _safe_symbol_dir_for_finalize(symbol: str) -> str:
    """Mirror evi-toolkit/fetch_data: 1357.HK -> 1357_HK。"""
    import re as _re
    return _re.sub(r"[^A-Za-z0-9_]+", "_", (symbol or "unknown")).strip("_") or "unknown"


def _evi_persist_extra_args(
    entry_key: str, display_name: str | None, params: dict[str, Any]
) -> list[str]:
    """Extra CLI args for persist_evi_report.py beyond the framework defaults."""
    args: list[str] = []
    if entry_key:
        args.extend(["--symbol", entry_key])
    market = (params.get("market") or "").strip()
    if market:
        args.extend(["--market", market])
    return args


_EVI_FINALIZE = FinalizeSpec(
    data_dir_builder=_evi_data_dir,
    expected_files=_EVI_EXPECTED_FILES,
    persist_script=".agents/skills/evi-toolkit/scripts/persist_evi_report.py",
    persist_args_builder=_evi_persist_extra_args,
    max_retries=1,  # 主跑 + 1 次补救，避免无限循环
)


# ---------------------------------------------------------------------------
# Initial Prompt（单一 prompt，不再分两条消息）
# ---------------------------------------------------------------------------

_EVI_PROMPT = (
    "对 {display_name}{symbol_clause}（市场：{market_label}）执行 **EVI 完整估值分析**。\n"
    "\n"
    "Read `.agents/skills/evi-data-orchestrator/SKILL.md`，按其编排方案执行产业调研 + 4 维度定性分析；\n"
    "调研完成后**立即**继续 Read `.agents/skills/evi-valuation-analysis/SKILL.md`，执行估值分析。\n"
    "不需要等待用户确认，一次性跑完调研+定性+估值全流程。\n"
    "\n"
    "{fetch_step}\n"
    "\n"
    "**全流程**：\n"
    "1. 数据获取 + 产业调研（company_overview + 分部报告）\n"
    "2. 定性分析（商业模式 / 护城河 / 管理层 / MD&A 前瞻 4 维度评级）→ reports/quality.md\n"
    "3. 估值分析（路由 + 假设 + 多方法 + SOTP/整体 + 反向估值）\n"
    "4. 输出 facets.json + 写 changelog 摘要\n"
    "\n"
    "**⭐ 持久化机制（重要）**：\n"
    "你**不需要手动调** `persist_evi_report.py`。当你跑完所有产物后，系统会自动：\n"
    "1. 扫描 `data/{symbol_dir}/` 下的关键产物（facets.json / reports/final.md / "
    "reports/changelog.md / base/CHECKLIST.json 等）\n"
    "2. 若 required 文件齐全 → 自动 finalize entry 为 completed\n"
    "3. 若有 required 文件缺失 → 系统会**重新给你发一条 user message**列出缺什么、"
    "怎么补，你按提示补齐即可（不要重跑全流程）\n"
    "4. 若仅 optional 缺失 → 系统自动占位 + finalize 为 partial\n"
    "\n"
    "你只需要：**专注产出所有 required 文件**，写 changelog 记录工作摘要，"
    "然后正常结束本次任务即可。\n"
    "\n"
    "如需手动诊断（开发场景），可跑：\n"
    "`python3 .agents/skills/evi-toolkit/scripts/wakeup_check.py "
    "--data-dir data/{symbol_dir} --check-only`\n"
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

# {workspace_name} — EVI 估值策略

## 定位

持久化的公司分析与估值跟踪系统。为 **{display_name}** 建立完整的产业调研→估值建模→持续监控闭环，当新信息出现时自动触发重估。

---

## 工作流

### WF-1 初始化（首次分析，一次性跑完）

```
Read evi-data-orchestrator/SKILL.md → 产业调研
   ↓ 完成后立即继续（不等用户确认）
（产业调研收尾时）触发 evi-quality-analysis → reports/quality.md（4 维度定性）
   ↓
Read evi-valuation-analysis/SKILL.md → 估值分析（消费 quality.json.assumption_hints）
   ↓
输出 facets.json + persist 看板 + 建议注册监控
   ↓
写 changelog.md 记录首次分析
```

### WF-2 迭代更新（最常用，优先级最高）

触发：用户指正 / 新闻事件 / 季报更新 / 监控 automation 推送

```
① 理解变化 — 定位哪个事实/假设受影响
② 更新材料 — 修改 reports/*.md + information/indexed_facts.json
③ 修改估值 — 重算受影响分部（重跑 dcf_calc/ps_calc/comps_calc 脚本）→ 更新 assumption_ledger → 重跑 aggregate.py
④ 刷新 facets.json — 跑 `aggregate.py --emit-facets`（**不要手写 facets**）
⑤ 写 changelog — 在 reports/changelog.md 顶部追加记录
```

**必须执行到 ⑤ 才算完成。严禁回复"好的我知道了"后不做任何操作。**
**持久化由 framework 自动接管**——不需要手动调 persist_evi_report.py。

---

## 铁律

1. **严禁模拟数据** — 所有数字必须有来源（FMP / 财报 / WebSearch）
2. **结论优先** — 先给结论和判断，再展开推导过程（报告用表格+推导链）
3. **修改估值后必须更新 facets.json** — 前端看板靠它刷新
4. **每次任务完成后必须写 changelog.md** — 用户跟踪工作的唯一入口
5. **缺数据时主动补** — 跑脚本/搜索/子 agent，不要让用户催
6. **不需要手动调 persist** — 系统会在你结束本轮后自动扫产物并 finalize；若缺 required 文件，会**重新给你发 user message** 提示缺什么，你按提示补齐即可（不要重跑全流程）

---

## 文件规范

所有产出文件存放在 `data/{symbol_dir}/` 下：

| 路径 | 用途 | 前端展示位置 |
|------|------|-------------|
| `facets.json` | ⭐ 看板核心数据（估值结论/判断/分部贡献/quality 评级） | 估值结论卡片（价格、涨跌幅、判断标签）+ 定性分析卡 |
| `quality.json` | ⭐ 4 维度定性分析结构化结论 | 估值结论 Tab 的「定性分析」卡 |
| `reports/final.md` | ⭐ 最终估值结论报告 | 「估值结论」Tab 正文 |
| `reports/quality.md` | ⭐ 4 维度定性分析报告（商业模式/护城河/管理层/MD&A） | 「估值结论」Tab 附加 + 定性卡跳转目标 |
| `reports/changelog.md` | ⭐ 更新记录（每次操作后追加） | 「更新记录」Tab |
| `reports/company_overview.md` | 公司整体产业调研 | 「产业调研」Tab（single_segment 时） |
| `reports/segments/{{seg_id}}.md` | 分部产业调研 | 对应分部 Tab（调研部分） |
| `reports/segments/{{seg_id}}_valuation.md` | 分部估值报告 | 对应分部 Tab（估值部分） |
| `reports/reverse_valuation.md` | 反向估值报告 | 「估值结论」Tab 附加内容 |
| `reports/monitor.md` | 监控执行日志 | 「自动化任务」Tab |
| `base/CHECKLIST.json` | 数据质量记分卡 | 「数据收集」Tab |
| `base/` | FMP 财务数据 + 财报 + 研报 + 电话会 | — |
| `information/indexed_facts.json` | 结构化事实索引 | 「数据收集」Tab 摘要 |
| `valuation/{{segment}}/` | 各分部估值中间结果 | — |
| `monitor/` | 监控 automation 产物 | 「自动化任务」Tab |

### changelog 格式

```markdown
### YYYY-MM-DD HH:MM — [触发类型]

**触发**：[用户指出/监控发现/首次分析]（一句话说明原因）
**影响范围**：[哪些分部/假设/方法受影响]
**关键变化**：
- XXX 从 A 改为 B（原因）
**估值变化**：base 从 X.XX → Y.YY（+Z%）
**操作**：[跑了什么脚本/改了什么文件]
```

---

## Skills

| Skill | 职责 | 何时调用 |
|-------|------|----------|
| `evi-data-orchestrator` | 产业调研总控（编排数据获取+调研报告） | 初始化 / 重做调研 |
| `evi-quality-analysis` | 4 维度定性分析（商业模式/护城河/管理层/MD&A 前瞻） | 产业调研收尾 / 用户指出某维度需调整 |
| `evi-valuation-analysis` | 估值分析总控（路由+假设+多方法+汇总） | 初始化 / 重估 |
| `evi-toolkit` | 共享脚本库（fetch_data / download / persist） | 贯穿始终 |
| `evi-market-sizing` | 市场空间推演（TAM→SAM→SOM + Bottom-Up 量价） | 构建增长率假设时 |
| `evi-assumption-builder` | 假设构建（消费 market-sizing + quality.assumption_hints） | 估值前 |
| `evi-business-segmentation` | 业务分部拆分 | 调研阶段 |
| `evi-information-search` | 信息搜索（WebSearch / SEC / 研报） | 需要补充数据时 |
| `evi-valuation-dcf` | DCF 估值（含脚本 dcf_calc.py） | 估值阶段 |
| `evi-valuation-ps` | PS 估值 | 估值阶段 |
| `evi-valuation-peg` | PEG 估值 | 估值阶段 |
| `evi-valuation-ddm` | DDM 估值 | 估值阶段 |
| `evi-valuation-comps` | 可比公司估值 | 估值阶段 |
| `evi-valuation-router` | 估值方法路由（选择适合的方法组合） | 估值阶段 |
| `evi-valuation-orchestrator` | SOTP 汇总 + facets.json 生成 | 估值收尾 |
| `evi-reverse-valuation` | 反向估值 + rerate_triggers（吸收 quality.monitor_suggestions） | 估值收尾 |
| `evi-monitor` | 持续监控定义（消费 quality.monitor_suggestions） | 初始化完成后 |
| `evi-revaluation-updater` | 增量重估（监控触发时） | 迭代更新 |

> **每次执行 Skill 前必须 Read 最新 `.agents/skills/evi-*/SKILL.md`，不要凭记忆。**

## File Index

<!-- Agent 在此维护已产出文件的索引，每次新增/修改文件后更新 -->

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
        version="3.7.1",
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
    allowed_skill_names=_EVI_SKILL_WHITELIST,
    finalize_spec=_EVI_FINALIZE,
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
        "3.2.0": {
            "summary": "新增市场空间推演 skill（TAM/SAM/SOM + Bottom-Up 量价推导）",
            "changes": [
                "新增 evi-market-sizing skill：TAM → SAM → SOM 自上而下 + 终端量×渗透率×ASP 自下而上 + 类比法交叉验证",
                "增长率必须有市场空间推演支撑：assumption-builder 会自动调用 market-sizing",
                "收入驱动因子 7 类拆解框架（量/价/渗透率/留存/频次/供给/结构）",
                "增长率三重验证：历史趋势 + 内生增长(g=再投资率×ROIC) + 市场空间约束",
                "竞争格局→利润率可持续性（Morningstar 五类护城河）",
                "Reverse DCF 反推检验：验证当前股价隐含预期是否合理",
            ],
            "suggested_actions": [
                {
                    "label": "对当前公司做市场空间推演",
                    "prompt": "请 Read .agents/skills/evi-market-sizing/SKILL.md，对当前公司的每个业务分部做市场空间推演（TAM/SAM/SOM + Bottom-Up），输出到 reports/segments/ 目录，完成后写 changelog。",
                },
                {
                    "label": "用市场空间重算增长率",
                    "prompt": "基于最新的 market_sizing.json 结果，重新构建各分部的增长率假设（growth_bridge.json），确保增长率有物理量约束支撑而非拍脑袋。完成后更新估值并写 changelog。",
                },
            ],
        },
        "3.3.0": {
            "summary": "Agent.md 重构：结构化工作流 + 文件规范 + 结论优先写作规范",
            "changes": [
                "重构 agent.md 结构：定位 → 工作流 → 铁律 → 文件规范 → Skills → 命令 → 写作规范 → File Index",
                "明确两大工作流：WF-1 初始化（一次性跑完）、WF-2 迭代更新（5 步闭环）",
                "新增文件规范表：每个文件的用途 + 前端展示位置，一目了然",
                "新增 Skills 调用时机说明：何时调用哪个 Skill",
                "新增写作规范：结论优先、数据有源、表格推导、变化量化",
                "新增 File Index 区域：Agent 自动维护已产出文件索引",
            ],
            "suggested_actions": [
                {
                    "label": "让 Agent 按新规范重写报告",
                    "prompt": "请按照最新 agent.md 的写作规范（结论优先、数据有源、表格推导），重新审视并优化 reports/final.md 的结构，确保开头先给结论判断，再展开论证。完成后写 changelog。",
                },
            ],
        },
        "3.4.0": {
            "summary": "PS/Comps 估值脚本化 + MCP broken pipe 修复",
            "changes": [
                "新增 ps_calc.py：EV/Sales 估值全自动，只需输入 --symbol --peers，数据从 FMP TTM 获取（时间对齐）",
                "新增 comps_calc.py：可比公司多倍数交叉（EV/EBITDA + EV/Sales + P/E + P/B），全自动",
                "严禁手动计算倍数：所有 peer 倍数必须来自 FMP keyMetrics-TTM，避免实时 vs 历史数据口径错配",
                "修复 MCP broken pipe：sandbox 内 MCP server 路径错误（/home/workspace → 相对路径）",
                "MCP 自动重启：broken pipe / 超时 / 连接关闭时自动 kill + restart + retry",
                "修复模板后台 Agent BYOK 密钥读取（is_byok=True）",
            ],
            "suggested_actions": [
                {
                    "label": "用脚本重算 PS 估值",
                    "prompt": "请用 ps_calc.py 重新计算当前公司的 EV/Sales 估值。Read .agents/skills/evi-valuation-ps/SKILL.md 看第 8 节脚本用法，选择合适的 peers 执行。完成后更新估值并写 changelog。",
                },
                {
                    "label": "用脚本跑可比公司估值",
                    "prompt": "请用 comps_calc.py 对当前公司做可比公司估值。Read .agents/skills/evi-valuation-comps/SKILL.md 看第 9 节脚本用法，选择 4-5 家 peers 执行。完成后更新估值并写 changelog。",
                },
            ],
        },
        "3.5.0": {
            "summary": "新增 4 维度定性分析（合并原 Sirius 价值分析能力）",
            "changes": [
                "新增 evi-quality-analysis skill：商业模式 / 护城河 / 管理层 / MD&A 前瞻 4 维度定性判断",
                "复用现有数据（FMP / MD&A / 电话会 / 研报），不引入额外下载步骤",
                "产出 reports/quality.md（≤2500 字）+ 结构化 quality.json（4 维度评级 + assumption_hints + monitor_suggestions）",
                "facets.json 新增 quality 字段：4 项评级 + 投资支柱 + 风险信号，前端在「估值结论」Tab 渲染定性分析卡",
                "evi-data-orchestrator Phase 1 收尾自动触发定性分析（在 CHECKLIST 之前）",
                "evi-assumption-builder 升级为消费 quality.json.assumption_hints，让定性结论直接影响估值假设",
                "evi-monitor / evi-reverse-valuation 升级为吸收 quality.json.monitor_suggestions",
                "支持增量更新：用户指出某维度（如「D2 评级太乐观」），只重写对应章节并刷看板",
                "下线独立的 sirius-valuation 模板，相关定性能力全部并入 EVI",
            ],
            "suggested_actions": [
                {
                    "label": "为当前公司做定性分析",
                    "prompt": "请 Read .agents/skills/evi-quality-analysis/SKILL.md，对当前公司执行 4 维度定性分析（商业模式 / 护城河 / 管理层 / MD&A 前瞻）。基于已下载的财报/MD&A/电话会数据，输出 reports/quality.md + quality.json + 更新 facets.json，完成后写 changelog。",
                },
                {
                    "label": "重看护城河评级",
                    "prompt": "我觉得 D2 护城河评级偏乐观，请重新审视并下调。读 reports/quality.md 第 3 节 + 重新检查行业格局/对手差距/虚假护城河，重写该维度章节 + quality.json.dimensions.moat + facets.quality，然后跑 persist 刷看板，写 changelog。",
                },
            ],
        },
        "3.6.0": {
            "summary": "模板 sandbox 隔离：只上传 EVI 相关 skill，避免与通用 skill 冲突",
            "changes": [
                "新增 TemplateDefinition.allowed_skill_names 字段：支持模板声明 sandbox skill 白名单",
                "WorkspaceManager 创建/恢复模板 sandbox 时根据白名单过滤 skill 上传",
                "EVI 白名单：保留全部 evi-* + 通用基础设施（pdf/docx/xlsx/pptx/web-scraping/automation/inline-widget/interactive-dashboard/user-profile/self-improve）",
                "排除冲突 skill：dcf-model / comps-analysis / 3-statements / check-model / model-update / competitive-analysis / sector-overview / initiating-coverage / earnings-analysis / earnings-preview / thesis-tracker / idea-generation / morning-note / catalyst-calendar / check-deck / x-api",
                "现有 entry 重启 workspace 时自动 prune 掉不在白名单的 skill",
                "已物理删除 sirius-valuation 目录（前次保留作备份，本版彻底清理）",
            ],
            "suggested_actions": [
                {
                    "label": "查看当前 sandbox skill 列表",
                    "prompt": "Bash 运行 `ls .agents/skills/` 列出当前 sandbox 中所有已加载的 skill，应该只看到 evi-* + 通用基础设施 skill（pdf/docx/xlsx/pptx/web-scraping/automation 等），不应该再看到 dcf-model/comps-analysis 等通用估值 skill。",
                },
            ],
        },
        "3.6.1": {
            "summary": "门卫脚本 wakeup_check.py：杜绝『跑完了但 entry 停在 analyzing』bug",
            "changes": [
                "新增 evi-toolkit/scripts/wakeup_check.py：流程结束前的强制门卫",
                "扫描 10 项预期产出（facets.json/quality.json/reports/*.md 等），按 required/optional 分级",
                "required 缺失 → 退出码 2，明确告诉 Agent 缺哪个文件、怎么补",
                "optional 缺失 → 自动写占位空文件 + finalize 为 partial",
                "全部齐全 → 自动调 persist_evi_report.py 推 completed 到 DB",
                "每次跑都会在 reports/changelog.md 追加一行 wakeup_check 审计记录",
                "evi-data-orchestrator 的 Phase 1 收尾 + evi-valuation-analysis 的 §14 都改为强制走 wakeup_check.py",
                "EVI agent.md 第 6 条铁律：任何任务结束前必须 wakeup_check（包括用户对话后的修改）",
            ],
            "suggested_actions": [
                {
                    "label": "对当前 entry 跑一次门卫检查",
                    "prompt": "Bash 运行 `python3 .agents/skills/evi-toolkit/scripts/wakeup_check.py --data-dir data/<symbol_dir> --check-only` 看当前产物清单是否齐全。如果有缺失，按 stdout 提示补齐，然后跑完整版（去掉 --check-only）触发 finalize。",
                },
            ],
        },
        "3.7.0": {
            "summary": "Finalize 机制框架化：模板通用能力 + 自动注入消息让 AI 补缺件",
            "changes": [
                "架构分层：finalize 逻辑（扫产物 / 占位 / 调持久化 / 决定状态）"
                "从 evi-toolkit/wakeup_check.py 抽到框架层 src/server/templates/finalize/runner.py，"
                "成为所有模板都能用的通用能力",
                "EVI 模板把产物清单（10 项 required/optional）声明在 evi_strategy.py 的 _EVI_FINALIZE，"
                "其它模板想接入只需填同样的 FinalizeSpec",
                "TemplateOrchestrator._run_agent 在 agent generator drain 完后自动调 finalize runner",
                "缺 required → runner 生成一条结构化提示，作为 user message 通过同一 thread_id 再喂给 Agent，"
                "Agent 看到提示直接补缺件即可（不需要重跑全流程），最多 retry 1 次",
                "Agent 不再需要手动调 wakeup_check / persist_evi_report —— 系统自动接管",
                "wakeup_check.py 保留为开发者手动诊断入口（--check-only 仍可用）",
                "白名单扩容：加回 9 个不与 evi-* 冲突的通用 skill —— competitive-analysis / "
                "sector-overview / earnings-analysis / earnings-preview / catalyst-calendar / "
                "morning-note / thesis-tracker / idea-generation / x-api",
                "白名单仍排除真正冲突的：dcf-model / comps-analysis / 3-statements / "
                "check-model / check-deck / model-update / initiating-coverage",
                "agent.md 第 6 条铁律改为：『不需要手动调 persist，系统自动 finalize；"
                "缺件会推送 user message 提示』",
            ],
            "suggested_actions": [
                {
                    "label": "强制重跑 finalize 检查（当前 entry）",
                    "prompt": "请按当前 data 目录里已有的产物，写一条简短 changelog 摘要表示『模板升级到 3.7.0 完成检查』。系统会在你结束本轮后自动扫描产物并 finalize，无需你手动调任何脚本。如果系统再发消息提示缺件，按提示补齐即可。",
                },
            ],
        },
        "3.7.1": {
            "summary": "Phase 2 总路由权责重构：planning vs executor 明确分界，杜绝 Agent 自己撸 DCF 代码",
            "changes": [
                "evi-valuation-analysis/SKILL.md 整体重写：顶部明确『planning-only』定位，"
                "禁止自己写 DCF/PS/Comps Python 代码",
                "新增 §3 权责矩阵：每个子 skill 标注 planning（写 markdown）vs executor（必须 Bash 跑脚本）",
                "新增 §4 完整 DAG 图：single_segment / multi_segment 两条路径，"
                "每个 step 注明是 Read SKILL.md 还是 Bash 脚本",
                "明确三个必脚本方法及其命令行：dcf_calc.py / ps_calc.py / comps_calc.py，"
                "并列出脚本已经处理的细节（TTM 对齐 / 敏感性 / 输入校验）",
                "facets.json 改为强制由 aggregate.py --emit-facets 生成（不允许手写）",
                "删除 SKILL.md §14 wakeup_check 长篇说明：framework v3.7.0 起已自动接管 finalize",
                "新增附录 A 常见错误清单：『看到复杂就自己写代码』列为第一条",
                "evi-data-orchestrator/SKILL.md §11 同步精简：Phase 1 不再要求手动调 wakeup_check --force",
                "agent.md WF-2 ④ 改为跑 `aggregate.py --emit-facets`，移除 `persist_evi_report.py` 引用",
            ],
            "suggested_actions": [
                {
                    "label": "用新规范重跑 Phase 2 估值",
                    "prompt": "请 Read .agents/skills/evi-valuation-analysis/SKILL.md 看最新的权责矩阵和 DAG，按 step 顺序执行：1) Router 推理 → 2) Assumption-builder 构建假设 → 3) Bash dcf_calc.py / ps_calc.py / comps_calc.py 跑必脚本方法 → 4) Bash aggregate.py 汇总 → 5) 反向估值 → 6) 写 reports/final.md + Bash aggregate.py --emit-facets。严禁自己写 Python 算 DCF/PS/Comps。完成后写 changelog。",
                },
            ],
        },
    },
)
