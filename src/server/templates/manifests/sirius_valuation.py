"""Sirius Valuation template — seven-dimension equity valuation.

Drives the agent through the ``sirius-valuation`` skill (D1-D7 analysis +
quantitative valuation engine), then persists the structured result back to
the dashboard via the internal finalize API.

Template contract (what this file owns):
  - manifest         → public form fields shown in InstantiateDialog
  - initial_prompt_template  → first message sent to the agent
  - agent_md_template        → workspace knowledge file (injected every call)
  - params_enricher  → dynamic prompt clauses for optional fields
"""

from typing import Any

from src.server.models.template import TemplateField, TemplateManifest
from src.server.templates.registry import TemplateDefinition


_MARKET_LABELS = {
    "hk": "港股 (HK)",
    "us": "美股 (US)",
    "cn": "A 股 (CN)",
}


_SIRIUS_PROMPT = (
    "请使用 sirius-valuation skill 对 {display_name}"
    "{symbol_clause}（市场：{market_label}）"
    "执行完整的七维度估值分析（D1~D7）。\n"
    "\n"
    "执行流程要求：\n"
    "1. 首先 Read .agents/skills/sirius-valuation/SKILL.md（触发 skill 加载）。\n"
    "2. {fetch_step}\n"
    "3. 按 DAG 顺序（D1-D5 可并行 → D6 → D7），对每个维度执行两步：\n"
    "   Step 1：读取 knowledge/ 下对应维度文档的问题清单，自主推导分析逻辑链，"
    "用 write_file 将分析报告写入 data/{symbol_dir}/reports/d{{N}}_report.md（必须写文件，不能只输出文本）。\n"
    "   Step 2：从报告中提取结构化数据，用 write_file 写入 data/{symbol_dir}/structured/d{{N}}.json。\n"
    "   ⚠️ 每个维度必须确认文件已写入磁盘，不能只在回复中输出内容。\n"
    "4. D1-D5 全部完成后，继续执行 D6（依赖 D1-D5 报告）→ D7（依赖 D6），同样每个都写报告+结构化。\n"
    "5. 全部 7 个维度完成后，Bash 运行：\n"
    "   `python3 scripts/persist_entry.py --entry-id {entry_id} --data-dir data/{symbol_dir}`\n"
    "6. 最后用 1-2 句话总结估值结论与最大风险点。\n"
    "\n"
    "重要：步骤 3-6 是连续的，D1-D5 子任务完成后不要停止，必须继续执行 D6→D7→persist→总结。\n"
)

# agent.md written into the workspace sandbox on first instantiation.
# Every LLM call in this workspace will see this content (via WorkspaceContextMiddleware).
# Placeholders use the same context as _SIRIUS_PROMPT, plus:
#   {workspace_name}, {template_name} (added by TemplateDefinition._build_ctx)
_SIRIUS_AGENT_MD = """\
---
workspace_name: {workspace_name}
description: [{template_name}] {entry_key}
template_id: sirius-valuation
entry_id: {entry_id}
entry_key: {entry_key}
---

# {workspace_name} — {template_name}

## 模板说明

这个工作区由 **{template_name}** 模板管理。

### 文件结构

```
data/{symbol_dir}/
├── reports/          # 分析报告（底稿，每个维度一篇 md）
├── structured/       # 结构化数据（从报告提取，给看板消费）
├── raw/              # FMP 原始数据
├── engine_result.json
└── financial_context.md
```

### 更新看板命令

```bash
python3 scripts/persist_entry.py \\
  --entry-id {entry_id} \\
  --data-dir data/{symbol_dir}
```

每次修改分析结论后，必须重新运行此命令更新数据库和看板。

## 修改分析 & 更新报告面板

当用户和你讨论后敲定了对某个分析观点的修改：

1. **修改对应维度的报告**（`data/{symbol_dir}/reports/d{{N}}_report.md`）
2. **重新提取该维度的结构化数据**（`data/{symbol_dir}/structured/d{{N}}.json`）
3. **运行 persist_entry.py** 更新看板
4. **建议用户**："报告面板已更新，刷新即可查看最新分析。"

> 💡 如果用户说"我觉得 D3 的监管风险应该更高"或"D2 护城河评级太乐观了"，
> 你应该修改对应报告 → 重新提取 json → 更新数据库 → 告知用户刷新面板。

## 分析规则（用户可配置）

> 用户为这家公司设定的个性化分析偏好。

### 当前配置

- 分析标的：{display_name}（{entry_key}，{market_label}）
- 估值框架：Sirius D1~D7 七维度
- 估值方法权重：使用估值引擎默认权重（如需调整请在此处指定）
- D7 定性调整偏好：遵循知识指南默认逻辑

### 如何修改规则

用户可以直接告诉你：
- "把 D7 的安全边际阈值从 10% 改成 20%"
- "更保守地评估护城河，D2 使用更严格标准"
- "这家公司监管风险应该降权"
- "D3 的评分改成 6 分，我认为 AI 投入回报不确定性更大"

收到指令后：
1. 修改对应维度报告中的相关章节
2. 重新提取该维度的结构化 json
3. 如果改动影响下游维度（如 D2 影响 D6/D7），顺带更新下游
4. 运行 persist_entry.py 更新数据库
5. 更新本文件"当前配置"记录修改历史
6. 告知用户"报告面板已更新"

## Thread Index

## Key Findings

## File Index
"""


def _enrich_sirius_params(
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Build dynamic prompt/agent.md fragments based on which optional fields were provided."""
    has_symbol = bool(entry_key) and not entry_key.startswith("auto_")
    market_raw = (params.get("market") or "").strip()

    extra: dict[str, Any] = {}
    extra["symbol_clause"] = f"（代码 {entry_key}）" if has_symbol else ""
    extra["market_label"] = _MARKET_LABELS.get(market_raw, "由你结合上下文判断")

    if has_symbol:
        market_arg = f" --market {market_raw}" if market_raw else ""
        extra["fetch_step"] = (
            "Bash 运行 `python3 .agents/skills/sirius-valuation/scripts/fetch_data.py "
            f"--symbol {entry_key}{market_arg}`，获取财务数据与估值引擎结果。"
        )
    else:
        extra["fetch_step"] = (
            f"先用 web_search / SEC 工具查出 {display_name or '该公司'} 的官方股票代码"
            "（含交易所后缀，如 1357.HK / AAPL / 600519.SS），"
            "再 Bash 运行 `python3 .agents/skills/sirius-valuation/scripts/fetch_data.py "
            "--symbol <查到的代码>`。注意：如果检索失败，请把状态置为 failed 并在 "
            "`scripts/persist_entry.py --status failed --error-message ...` 中说明原因。"
        )

    return extra


SIRIUS_VALUATION = TemplateDefinition(
    manifest=TemplateManifest(
        id="sirius-valuation",
        name="Sirius 估值",
        description=(
            "七维度（商业模式 / 护城河 / 外部环境 / 管理层 / MD&A / 综合 / 估值修正）"
            "的深度公司估值分析。自动获取财务数据、跑量化估值引擎，给出公允价值与"
            "买入/持有/卖出建议。"
        ),
        icon="trending-up",
        version="1.0.0",
        estimated_minutes=10,
        fields=[
            TemplateField(
                name="display_name",
                label="公司名称",
                type="text",
                required=True,
                placeholder="例如 美图科技",
            ),
            TemplateField(
                name="entry_key",
                label="股票代码（可选）",
                type="text",
                required=False,
                placeholder="例如 1357.HK / AAPL / 600519.SS（留空 Agent 自动检索）",
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
    initial_prompt_template=_SIRIUS_PROMPT,
    agent_md_template=_SIRIUS_AGENT_MD,
    workspace_name_builder=lambda key, name, params: (
        f"{name or key}" + (f"（{key}）" if (name and key and name != key) else "")
    ),
    params_enricher=_enrich_sirius_params,
)
