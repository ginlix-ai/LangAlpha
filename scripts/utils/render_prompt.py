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
import difflib
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
    guidance_template_vars,
    init_loader,
    resolve_prompt_guidance,
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
    p.add_argument(
        "--guidance",
        choices=["lean", "detailed"],
        default="detailed",
        help=(
            "Prompt scaffolding level to render. At runtime this resolves from "
            "user preference, then config.yaml, then the model's prompt_guidance "
            "in models.json. Default: detailed"
        ),
    )
    p.add_argument(
        "--model",
        default=None,
        help=(
            "Resolve the guidance level from this model's manifest entry "
            "(e.g. claude-opus-5) instead of --guidance."
        ),
    )
    p.add_argument(
        "--diff",
        action="store_true",
        help="Show what lean drops relative to detailed, instead of a prompt.",
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


class _PreviewConfig:
    """The slice of ``AgentConfig`` the compiler's prompt path reads.

    A preview pins one level for every subagent; the runtime resolves one per
    model, off a config this script has no way to build.
    """

    llm = None

    def __init__(self, guidance: str) -> None:
        self._guidance = guidance

    def prompt_guidance_for_role(self, role: str) -> str:
        return self._guidance

    def feature_enabled(self, key: str) -> bool:
        return False

    def client_for_role(self, role: str, *, fallback_to_main: bool = False):
        return None


def resolve_guidance(args: argparse.Namespace) -> str:
    """--model probes the manifest as the runtime would; otherwise --guidance."""
    if args.model:
        return resolve_prompt_guidance(args.model)
    return args.guidance


def render(args: argparse.Namespace) -> str:
    """Render the prompt based on CLI args."""
    now = datetime.now(tz=UTC)
    current_time = format_current_time(now, args.timezone)

    # Init loader (freezes session time)
    loader = init_loader(session_start_time=now)

    tool_summary = args.tool_summary if args.tool_summary else STUB_TOOL_SUMMARY
    subagent_summary = format_subagent_summary(STUB_SUBAGENTS)
    user_profile = None if args.no_user_profile else STUB_USER_PROFILE
    guidance_vars = guidance_template_vars(resolve_guidance(args))

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
            config=_PreviewConfig(guidance_vars["guidance"]),
        )
        result = compiler.compile(defn)
        return result["system_prompt"]

    if args.mode == "flash":
        # Flash system prompt
        return loader.render(
            "flash_system.md.j2",
            current_time=current_time,
            user_profile=user_profile,
            **guidance_vars,
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
        include_examples=True,
        include_anti_patterns=True,
        **guidance_vars,
    )


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English."""
    return len(text) // 4


def render_diff(args: argparse.Namespace, *, use_color: bool) -> str:
    """Show what lean drops relative to detailed.

    Answers the one question inline fences make hard to eyeball: what does the
    lean prompt actually say? Anything reported as added (rather than removed)
    is a drift bug — lean is meant to be a strict subset.
    """
    detailed = render(argparse.Namespace(**{**vars(args), "guidance": "detailed", "model": None}))
    lean = render(argparse.Namespace(**{**vars(args), "guidance": "lean", "model": None}))

    red = "\033[31m" if use_color else ""
    green = "\033[32m" if use_color else ""
    dim = "\033[2m" if use_color else ""
    reset = "\033[0m" if use_color else ""

    lines = []
    added = 0
    for line in difflib.unified_diff(
        detailed.splitlines(), lean.splitlines(), "detailed", "lean", lineterm="", n=1
    ):
        if line.startswith("+") and not line.startswith("+++"):
            added += 1
            lines.append(f"{green}{line}{reset}")
        elif line.startswith("-") and not line.startswith("---"):
            lines.append(f"{red}{line}{reset}")
        else:
            lines.append(f"{dim}{line}{reset}")

    d_tok, l_tok = estimate_tokens(detailed), estimate_tokens(lean)
    saved = d_tok - l_tok
    pct = (saved / d_tok * 100) if d_tok else 0.0
    summary = [
        "",
        f"{dim}detailed: ~{d_tok:,} tok | lean: ~{l_tok:,} tok | "
        f"lean drops ~{saved:,} tok ({pct:.1f}%){reset}",
    ]
    if added:
        summary.append(
            f"{red}WARNING: lean adds {added} line(s) absent from detailed — "
            f"lean must be a strict subset.{reset}"
        )
    return "\n".join(lines + summary)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.diff:
        color = not args.no_color and args.output is None and sys.stdout.isatty()
        out = render_diff(args, use_color=color)
        if args.output:
            Path(args.output).write_text(out, encoding="utf-8")
            print(f"Written to {args.output}", file=sys.stderr)
        else:
            print(out)
        return

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
