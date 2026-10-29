"""Built-in subagent definitions.

These are the default subagents shipped with the agent. A definition in
``agent_config.yaml`` with one's name changes only the fields it sets.
"""

from __future__ import annotations

from ptc_agent.agent.subagents.definition import SubagentDefinition

BUILTIN_SUBAGENTS: dict[str, SubagentDefinition] = {
    "research": SubagentDefinition(
        name="research",
        description=(
            "Delegate research to the sub-agent researcher. "
            "Give this researcher one specific topic or question at a time. "
            "The researcher will search the web and provide findings with citations."
        ),
        role_prompt_template="roles/researcher.md.j2",
        tools=["web_search", "filesystem"],
        sections={
            "workspace_paths": False,
            "tool_guide": False,
            "data_processing": False,
            "visualizations": False,
        },
    ),
    "general-purpose": SubagentDefinition(
        name="general-purpose",
        description=(
            "Delegate complex tasks to the general-purpose sub-agent. "
            "This agent has access to all filesystem tools "
            "(read, write, edit, glob, grep, bash) and can execute Python "
            "code with MCP tools. Use for multi-step operations, data "
            "processing, file manipulation, or any task requiring full tool access."
        ),
        role_prompt_template="roles/general.md.j2",
        tools=["execute_code", "filesystem", "bash", "finance", "web_search"],
        memory=True,
        sections={
            "workspace_paths": True,
            "tool_guide": True,
            "data_processing": True,
            "visualizations": True,
        },
    ),
    "data-prep": SubagentDefinition(
        name="data-prep",
        description=(
            "Delegate data fetching and preparation to the data prep sub-agent. "
            "This agent pulls data from MCP financial data servers (price, "
            "fundamentals, macro, options), cleans and transforms it, and saves "
            "structured datasets to files. Use for bulk data collection, "
            "multi-ticker data gathering, or preparing datasets for analysis. "
            "Can run multiple instances in parallel for different data needs."
        ),
        role_prompt_template="roles/data_prep.md.j2",
        tools=["execute_code", "filesystem", "bash", "finance"],
        sections={
            "workspace_paths": True,
            "tool_guide": True,
            "data_processing": True,
            "visualizations": False,
        },
    ),
    "equity-analyst": SubagentDefinition(
        name="equity-analyst",
        description=(
            "Delegate financial analysis tasks to the equity analyst sub-agent. "
            "This agent can search the web, execute Python code with MCP financial "
            "data tools, and build models/charts. Use for company analysis, "
            "valuation, earnings analysis, sector research, or any task requiring "
            "both web research and financial data processing."
        ),
        role_prompt_template="roles/equity_analyst.md.j2",
        tools=["execute_code", "filesystem", "bash", "finance", "web_search"],
        # New analysis is the workspace's own: the Chief of Staff hands it to
        # that workspace's analyst, so the workspace keeps the work and its
        # history.
        roles=frozenset({"analyst"}),
        memory=True,
        sections={
            "workspace_paths": True,
            "tool_guide": True,
            "data_processing": True,
            "visualizations": True,
        },
    ),
    "report-builder": SubagentDefinition(
        name="report-builder",
        description=(
            "Delegate document creation to the report builder sub-agent. "
            "This agent specializes in producing polished DOCX reports, XLSX "
            "models, PPTX presentations, and PDF files. Give it the analysis "
            "results and desired output format. Loads the relevant format "
            "skill (xlsx/docx/pptx/pdf) as its first action."
        ),
        role_prompt_template="roles/report_builder.md.j2",
        tools=["execute_code", "filesystem", "bash"],
        sections={
            "workspace_paths": True,
            "tool_guide": True,
            "data_processing": False,
            "visualizations": True,
        },
    ),
}
