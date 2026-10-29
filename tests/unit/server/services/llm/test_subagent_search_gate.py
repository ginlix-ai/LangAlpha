"""A subagent's search provider and depth, gated against the plan, and written only where they differ from the user's own."""

from __future__ import annotations

import pytest

from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import ResolvedSubagents
from src.server.services.llm.capabilities import SearchGate, gate_search
from src.server.services.llm.subagents import _gate_search
from src.tools.web.manifest import CAPABILITY_SEARCH, CapabilitySpec, LevelSpec

PLAN = 1


def _cap(levels: list[tuple[str, int]], *, min_tier: int = 0) -> CapabilitySpec:
    return CapabilitySpec(
        verb=CAPABILITY_SEARCH,
        tracking_name="placeholder",
        min_tier=min_tier,
        default_level="standard",
        levels=tuple(
            LevelSpec(name=n, display_name=n, native_params={}, min_tier=t, credits=0.0)
            for n, t in levels
        ),
    )


# alpha is the user's own provider. Every tier above PLAN is off the plan.
MANIFEST = {
    "alpha": _cap([("fast", 0), ("standard", 0), ("deep", 2)]),
    "beta": _cap([("standard", 0)]),
    "gamma": _cap([("standard", 0), ("deep", 0)], min_tier=2),
    "delta": _cap([("standard", 0), ("deep", 2)]),
}


@pytest.fixture(autouse=True)
def manifest(monkeypatch):
    monkeypatch.setattr(
        "src.tools.web.manifest.get_capability",
        lambda provider, verb: MANIFEST.get(provider) if verb == CAPABILITY_SEARCH else None,
    )


async def _permits(tier: int) -> bool:
    return tier <= PLAN


class TestGateSearch:
    @pytest.mark.parametrize(
        ("provider", "depth", "effective_depth", "expected"),
        [
            (None, None, "standard", ("alpha", "standard", None, None)),
            ("beta", None, "standard", ("beta", "standard", None, None)),
            # The user's depth is one beta lacks, so beta's default runs, unflagged.
            ("beta", None, "fast", ("beta", "standard", None, None)),
            ("gamma", None, "standard", ("alpha", "standard", "not_on_plan", None)),
            # Unknown is not a plan question: the main provider, flagged as unknown.
            ("nonesuch", None, "standard", ("alpha", "standard", "unknown", None)),
            (None, "fast", "standard", ("alpha", "fast", None, None)),
            (None, "deep", "standard", ("alpha", "standard", None, "not_on_plan")),
            (None, "deep", "fast", ("alpha", "fast", None, "not_on_plan")),
            (None, "warp9", "standard", ("alpha", "standard", None, "not_offered")),
            ("beta", "deep", "standard", ("beta", "standard", None, "not_offered")),
            ("beta", "deep", "fast", ("beta", "standard", None, "not_offered")),
            ("delta", "deep", "standard", ("delta", "standard", None, "not_on_plan")),
            # The depth is judged against the provider left after the fallback.
            ("gamma", "deep", "standard", ("alpha", "standard", "not_on_plan", "not_on_plan")),
            # The user's own depth is never re-checked, here or on another provider.
            (None, "deep", "deep", ("alpha", "deep", None, None)),
            ("delta", None, "deep", ("delta", "deep", None, None)),
            ("delta", "deep", "deep", ("delta", "deep", None, None)),
            # Set on a provider that lacks it, the user's own depth is not offered there.
            ("beta", "deep", "deep", ("beta", "standard", None, "not_offered")),
        ],
    )
    @pytest.mark.asyncio
    async def test_the_pair_that_runs(self, provider, depth, effective_depth, expected):
        gate = await gate_search(
            provider, depth,
            effective_provider="alpha", effective_depth=effective_depth, permits=_permits,
        )
        assert gate == SearchGate(*expected)

    @pytest.mark.parametrize(
        ("provider", "depth"),
        [(None, None), ("alpha", None), (None, "standard"), ("alpha", "standard")],
    )
    @pytest.mark.asyncio
    async def test_the_users_own_pair_never_asks_the_plan(self, provider, depth):
        """The tier is fetched lazily, once per turn, so a subagent left at the
        user's pair must not be the thing that fetches it."""

        async def refuse(tier: int) -> bool:
            raise AssertionError(f"asked the plan for tier {tier}")

        gate = await gate_search(
            provider, depth,
            effective_provider="alpha", effective_depth="standard", permits=refuse,
        )
        assert gate == SearchGate("alpha", "standard", None, None)

    @pytest.mark.asyncio
    async def test_no_permits_gates_nothing(self):
        gate = await gate_search(
            "gamma", "deep", effective_provider="alpha", effective_depth="standard", permits=None,
        )
        assert gate == SearchGate("gamma", "deep", None, None)


def _roster(**pairs: tuple[str | None, str | None]) -> ResolvedSubagents:
    return ResolvedSubagents(
        definitions={
            name: SubagentDefinition(
                name=name,
                description="placeholder",
                tools=["web_search"],
                search_provider=provider,
                search_depth=depth,
            )
            for name, (provider, depth) in pairs.items()
        },
        enabled=tuple(pairs),
    )


async def _written(resolved, *, search_api="alpha", search_depth="standard"):
    out = await _gate_search(
        "user-1", resolved, search_api=search_api, search_depth=search_depth, permits=_permits,
    )
    return {
        name: (d.search_provider, d.search_depth) for name, d in out.definitions.items()
    }


class TestResolveWritesOnlyADifferentPair:
    """A subagent written (None, None) shares the main agent's search tool, so
    the build makes one extra tool only for a pair that really differs."""

    @pytest.mark.asyncio
    async def test_against_the_users_pair(self):
        written = await _written(
            _roster(
                inherits=(None, None),
                names_main=("alpha", "standard"),
                names_main_provider=("alpha", None),
                provider_off_plan=("gamma", None),
                provider_unknown=("nonesuch", None),
                depth_off_plan=(None, "deep"),
                own_provider=("beta", None),
                own_depth=(None, "fast"),
                depth_falls_back=("delta", "deep"),
            )
        )
        assert written == {
            "inherits": (None, None),
            "names_main": (None, None),
            "names_main_provider": (None, None),
            "provider_off_plan": (None, None),
            "provider_unknown": (None, None),
            "depth_off_plan": (None, None),
            "own_provider": ("beta", "standard"),
            "own_depth": ("alpha", "fast"),
            "depth_falls_back": ("delta", "standard"),
        }

    @pytest.mark.asyncio
    async def test_a_user_depth_the_provider_lacks_compares_as_its_default(self):
        """The main tool settles a depth its provider lacks on the provider's
        default, so naming that default is the main pair, not a new one."""
        written = await _written(
            _roster(names_default=(None, "standard"), names_provider=("beta", None)),
            search_api="beta",
            search_depth="fast",
        )
        assert written == {"names_default": (None, None), "names_provider": (None, None)}

    @pytest.mark.asyncio
    async def test_a_subagent_without_web_keeps_its_saved_values(self):
        resolved = _roster(offline=("gamma", "deep"))
        definitions = dict(resolved.definitions)
        definitions["offline"].tools = ["execute_code"]

        written = await _written(
            ResolvedSubagents(definitions=definitions, enabled=resolved.enabled)
        )
        assert written == {"offline": ("gamma", "deep")}
