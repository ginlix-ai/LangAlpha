"""Agent package - AI agent implementations using deepagent.

This package provides the PTC (Programmatic Tool Calling) agent pattern:
- Uses deepagent for orchestration and sub-agent delegation
- Integrates Daytona sandbox via DaytonaBackend
- MCP tools accessed through execute_code tool

Structure:
- agent.py: Main PTCAgent using deepagent
- backends/: Custom backends (DaytonaBackend)
- prompts/: Prompt templates (base, research)
- tools/: Custom tools (execute_code, research)
- langchain_tools/: LangChain @tool implementations (Bash, Read, Write, Edit, Glob, Grep)
- subagents/: Sub-agent definitions

Configuration:
- All config classes in ptc_agent.config package
- Programmatic (default): Create AgentConfig directly or use AgentConfig.create()
- File-based: Use load_from_files() from ptc_agent.config
"""

# Re-export from ptc_agent.config (lightweight — no heavy dependencies)
from ptc_agent.config import (
    # Config classes (pure data)
    AgentConfig,
    LLMConfig,
    LLMDefinition,
    # Utilities
    configure_logging,
    ensure_config_dir,
    find_config_file,
    find_project_root,
    # Template generation
    generate_config_template,
    get_config_search_paths,
    # Config path utilities
    get_default_config_dir,
    load_core_from_files,
    load_from_dict,
    # Config loading
    load_from_files,
)

# Heavy imports (PTCAgent, graph, subagents) are deferred to avoid pulling in
# the middleware chain and src.* dependencies at package import time.
_LAZY_IMPORTS = {
    "PTCAgent": ".agent",
    "DaytonaBackend": ".backends",
    "SessionProvider": ".graph",
    "build_ptc_graph": ".graph",
    "build_ptc_graph_with_session": ".graph",
    "SubagentCompiler": ".subagents",
    "SubagentDefinition": ".subagents",
    "SubagentRegistry": ".subagents",
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        import importlib
        module = importlib.import_module(_LAZY_IMPORTS[name], __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Config classes (pure data)
    "AgentConfig",
    "DaytonaBackend",
    "LLMConfig",
    "LLMDefinition",
    # Agent
    "PTCAgent",
    # Graph factory
    "SessionProvider",
    "build_ptc_graph",
    "build_ptc_graph_with_session",
    # Utilities
    "configure_logging",
    "SubagentCompiler",
    "SubagentDefinition",
    "SubagentRegistry",
    "ensure_config_dir",
    "find_config_file",
    "find_project_root",
    # Template generation
    "generate_config_template",
    "get_config_search_paths",
    # Config path utilities
    "get_default_config_dir",
    "load_core_from_files",
    "load_from_dict",
    # Config loaders (optional file-based)
    "load_from_files",
]
