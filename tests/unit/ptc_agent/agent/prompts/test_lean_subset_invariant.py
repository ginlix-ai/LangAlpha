"""Lean must be a strict subset of detailed, on every rendered surface.

The whole guidance design rests on there being one template body with the
coaching fenced off — not two texts to keep in sync. This catches the drift
that would reintroduce: text parked in an ``{% else %}``, or a lean-only
rewording that detailed never got.
"""

from collections import Counter
from types import SimpleNamespace

import pytest

from ptc_agent.agent.prompts import guidance_template_vars, init_loader
from ptc_agent.agent.subagents import SubagentCompiler, SubagentRegistry

# Text lean is permitted to introduce, because the steering genuinely inverts
# between tiers rather than merely expanding. Every entry costs the drift
# guarantee for that block, so each needs a comment saying why it inverts.
# Keep this list near-empty.
EITHER_OR_ALLOWLIST: list[str] = []

SUBAGENTS = ["research", "general-purpose", "data-prep", "equity-analyst", "report-builder"]

_STUB_TOOL_SETS = {
    n: [SimpleNamespace(name=n)]
    for n in ["execute_code", "bash", "filesystem", "web_search", "finance", "think", "todo"]
}


class _PinnedConfig:
    """The slice of ``AgentConfig`` the compiler reads, pinned to one level.

    The runtime resolves a level per model in ``resolve_llm_config``; this
    surface only needs both levels rendered from the same definition.
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


def _render_pair(surface: str) -> tuple[str, str]:
    """Return (detailed, lean) for a surface name."""
    loader = init_loader()
    if surface == "ptc":
        kwargs = dict(tool_summary="STUB", subagent_summary="STUB", crawl_enabled=True)
        return (
            loader.get_system_prompt(**guidance_template_vars("detailed"), **kwargs),
            loader.get_system_prompt(**guidance_template_vars("lean"), **kwargs),
        )
    if surface == "flash":
        return (
            loader.render("flash_system.md.j2", tools=[], **guidance_template_vars("detailed")),
            loader.render("flash_system.md.j2", tools=[], **guidance_template_vars("lean")),
        )
    defn = SubagentRegistry().get(surface)
    assert defn is not None, f"unknown subagent {surface}"

    def compile_at(level: str) -> str:
        compiler = SubagentCompiler(
            current_time="2026-01-01", thread_id="t", tool_sets=_STUB_TOOL_SETS,
            config=_PinnedConfig(level),
        )
        return compiler.compile(defn)["system_prompt"]

    return compile_at("detailed"), compile_at("lean")


def _significant(text: str) -> Counter:
    """Non-blank, non-fence lines. Whitespace-only differences around a fence
    are an artifact of Jinja trimming, not content drift."""
    return Counter(line.strip() for line in text.splitlines() if line.strip())


@pytest.mark.parametrize("surface", ["ptc", "flash", *SUBAGENTS])
class TestLeanSubset:
    def test_lean_introduces_no_new_text(self, surface):
        detailed, lean = _render_pair(surface)
        extra = _significant(lean) - _significant(detailed)
        unexplained = [
            line
            for line in extra
            if not any(allowed in line for allowed in EITHER_OR_ALLOWLIST)
        ]
        assert not unexplained, (
            f"{surface}: lean renders text absent from detailed. Either move it "
            f"outside the fence so both tiers get it, or register it in "
            f"EITHER_OR_ALLOWLIST with a note on why the steering inverts:\n"
            + "\n".join(f"  + {line}" for line in sorted(unexplained)[:10])
        )

    def test_lean_is_not_larger(self, surface):
        detailed, lean = _render_pair(surface)
        assert len(lean) <= len(detailed)


def test_at_least_one_surface_actually_differs():
    """Guards against the invariant passing vacuously — if no fence is wired,
    lean == detailed everywhere and the subset check proves nothing."""
    differing = [s for s in ["ptc", "flash", *SUBAGENTS] if len(set(_render_pair(s))) > 1]
    assert differing, "no surface renders lean differently from detailed"
