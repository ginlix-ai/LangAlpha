#!/usr/bin/env python3
"""Render system/subagent prompts as they appear at runtime.

Usage examples:

  # PTC system prompt with defaults
  python scripts/utils/render_prompt.py

  # Flash mode
  python scripts/utils/render_prompt.py --mode flash

  # PTC with plan mode + storage enabled
  python scripts/utils/render_prompt.py --plan-mode --storage

  # General-purpose subagent prompt
  python scripts/utils/render_prompt.py --subagent general-purpose

  # Research subagent prompt
  python scripts/utils/render_prompt.py --subagent research

  # Write to file instead of stdout
  python scripts/utils/render_prompt.py -o rendered_prompt.md

  # Count tokens (rough estimate)
  python scripts/utils/render_prompt.py --count-tokens
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

# Add project root to path so we can import from src/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ptc_agent.agent.prompts import (
    format_current_time,
    format_subagent_summary,
    init_loader,
)
from ptc_agent.agent.subagents import SubagentCompiler, SubagentRegistry
from ptc_agent.agent.subagents.builtins import BUILTIN_SUBAGENTS


# ---------------------------------------------------------------------------
# Stub data — realistic placeholders so the rendered prompt reads naturally
# ---------------------------------------------------------------------------

STUB_TOOL_SUMMARY = """\
### financial_data (3 tools)
Financial market data server — historical prices, fundamentals, screening.
- Module: `tools.financial_data`
- Docs: `tools/docs/financial_data/`

### yfinance (5 tools)
Yahoo Finance data — quotes, options, earnings, holders.
- Module: `tools.yfinance`
- Docs: `tools/docs/yfinance/`"""

STUB_SUBAGENTS = [
    {"name": defn.name, "description": defn.description, "tools": defn.tools}
    for defn in BUILTIN_SUBAGENTS.values()
]

STUB_USER_PROFILE = {
    "name": "Demo User",
    "timezone": "America/New_York",
    "locale": "en-US",
    "agent_preference": {
        "proactive_questions": "sometimes",
    },
    "context_files": [
        {
            "name": "portfolio.json",
            "description": "Current stock portfolio holdings",
        },
        {
            "name": "watchlist.json",
            "description": "Watchlist of tracked tickers",
        },
    ],
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Render system/subagent prompts as they appear at runtime.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Template selection
    p.add_argument(
        "--mode",
        choices=["ptc", "flash"],
        default="ptc",
        help="Agent mode: ptc (full sandbox agent) or flash (lightweight). Default: ptc",
    )
    p.add_argument(
        "--subagent",
        default=None,
        help="Render a subagent prompt (e.g., general-purpose, research). Lists available if invalid.",
    )

    # Feature flags
    p.add_argument("--plan-mode", action="store_true", help="Enable plan mode section.")
    p.add_argument(
        "--crawl", action="store_true", help="Enable the site-crawl tool section (WebCrawl/WebMap)."
    )
    p.add_argument(
        "--storage",
        action="store_true",
        help="Enable cloud storage (affects visualizations).",
    )
    p.add_argument(
        "--no-ask-user", action="store_true", help="Disable ask-user guidelines."
    )
    p.add_argument(
        "--no-user-profile", action="store_true", help="Omit user profile section."
    )

    # Variable overrides
    p.add_argument(
        "--thread-id",
        default="a1b2c3d4",
        help="Thread ID (first 8 chars). Default: a1b2c3d4",
    )
    p.add_argument(
        "--timezone",
        default="America/New_York",
        help="User timezone. Default: America/New_York",
    )
    p.add_argument(
        "--tool-summary", default=None, help="Custom tool summary text (default: stub)."
    )
    p.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=3,
        help="Max concurrent sub-agent tasks. Default: 3",
    )
    p.add_argument(
        "--max-task-iterations",
        type=int,
        default=10,
        help="Max task delegation rounds. Default: 10",
    )
    p.add_argument(
        "--max-iterations",
        type=int,
        default=15,
        help="Max iterations for general subagent. Default: 15",
    )

    # Output
    p.add_argument(
        "-o", "--output", default=None, help="Write to file instead of stdout."
    )
    p.add_argument(
        "--count-tokens", action="store_true", help="Print approximate token count."
    )
    p.add_argument(
        "--no-color", action="store_true", help="Suppress ANSI header/footer coloring."
    )

    return p


def render(args: argparse.Namespace) -> str:
    """Render the prompt based on CLI args."""
    now = datetime.now(tz=UTC)
    current_time = format_current_time(now, args.timezone)

    # Init loader (freezes session time)
    loader = init_loader(session_start_time=now)

    tool_summary = args.tool_summary if args.tool_summary else STUB_TOOL_SUMMARY
    subagent_summary = format_subagent_summary(STUB_SUBAGENTS)
    user_profile = None if args.no_user_profile else STUB_USER_PROFILE

    if args.subagent:
        # Subagent prompt via new registry/compiler system
        registry = SubagentRegistry()
        defn = registry.get(args.subagent)
        if defn is None:
            available = ", ".join(sorted(registry.list_all()))
            raise SystemExit(
                f"Unknown subagent '{args.subagent}'. Available: {available}"
            )
        # Build stub tool_sets so rendered prompts include tool lists
        from types import SimpleNamespace

        stub_tool_sets = {
            name: [SimpleNamespace(name=name)]
            for name in [
                "execute_code",
                "bash",
                "filesystem",
                "web_search",
                "finance",
                "think",
                "todo",
            ]
        }
        compiler = SubagentCompiler(
            current_time=current_time,
            thread_id=args.thread_id,
            tool_sets=stub_tool_sets,
            user_profile=user_profile,
        )
        result = compiler.compile(defn)
        return result["system_prompt"]

    if args.mode == "flash":
        # Flash system prompt
        return loader.render(
            "flash_system.md.j2",
            current_time=current_time,
            user_profile=user_profile,
        )

    # PTC system prompt
    return loader.get_system_prompt(
        tool_summary=tool_summary,
        subagent_summary=subagent_summary,
        user_profile=user_profile,
        plan_mode=args.plan_mode,
        crawl_enabled=args.crawl,
        storage_enabled=args.storage,
        ask_user_enabled=not args.no_ask_user,
        current_time=current_time,
        thread_id=args.thread_id,
        max_concurrent_task_units=args.max_concurrent_tasks,
        max_task_iterations=args.max_task_iterations,
        include_examples=True,
        include_anti_patterns=True,
    )


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English."""
    return len(text) // 4


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    result = render(args)

    # Header info
    use_color = not args.no_color and args.output is None and sys.stdout.isatty()
    dim = "\033[2m" if use_color else ""
    reset = "\033[0m" if use_color else ""
    bold = "\033[1m" if use_color else ""

    if args.subagent:
        label = f"subagent:{args.subagent}"
    else:
        label = args.mode

    header = f"{dim}--- Rendered prompt: {bold}{label}{reset}{dim} ---{reset}"
    footer_parts = [f"chars: {len(result):,}"]
    if args.count_tokens:
        footer_parts.append(f"tokens (est): ~{estimate_tokens(result):,}")
    footer = f"{dim}--- {' | '.join(footer_parts)} ---{reset}"

    if args.output:
        Path(args.output).write_text(result, encoding="utf-8")
        # Print stats to stderr even when writing to file
        print(f"Written to {args.output}", file=sys.stderr)
        print(footer, file=sys.stderr)
    else:
        print(header)
        print(result)
        print(footer)


if __name__ == "__main__":
    main()
