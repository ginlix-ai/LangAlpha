"""Prompt templates for agent operations.

This module provides a Jinja2-based template system for agent prompts.
Templates are stored as .md.j2 files and can be composed using includes.

Time awareness: The loader captures session start time once at initialization.
All prompts automatically receive `date` and `datetime` variables from the session,
ensuring consistent values across all prompts (preserves input cache).

Usage:
    from ptc_agent.agent.prompts import get_loader, init_loader, format_tool_summary

    # Initialize loader at session start (optional - captures time)
    loader = init_loader()

    # Get the main system prompt (date auto-injected from session)
    prompt = loader.get_system_prompt(tool_summary=tool_summary)

    # Get a sub-agent prompt
    prompt = loader.get_subagent_prompt("researcher")

    # Format tool summary for prompts
    summary = format_tool_summary(tools_by_server, mode="summary", server_configs=configs)
"""

from .formatter import (
    format_subagent_summary,
    format_tool_summary,
)
from .loader import (
    PromptLoader,
    get_loader,
    init_loader,
    reset_loader,
)

__all__ = [
    "PromptLoader",
    "format_subagent_summary",
    "format_tool_summary",
    "get_loader",
    "init_loader",
    "reset_loader",
]
