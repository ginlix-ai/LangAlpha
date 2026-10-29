"""research-conventions is a shared layer: the research skills mandate reading
it, so a user disable of it alone must not strand their first mandated read."""

import pytest

from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    build_effective_skill_registry,
    resolve_disabled_skills,
)
from ptc_agent.core.sandbox.assets import _collect_local_skill_names

LAYER = "research-conventions"


def _dependents() -> list[str]:
    return sorted(name for name, s in SKILL_REGISTRY.items() if LAYER in s.requires)


def _shipped_dependents() -> list[str]:
    """Shipped skills whose own files point into the layer, from disk.

    The registry entry is what a disable acts on, so a skill that reads the
    layer has to declare it there; scanning the bundles is what catches the
    next research skill that lands without the declaration.
    """
    from ptc_agent.config.plugins import bundled_skill_dirs

    found = set()
    for root in bundled_skill_dirs():
        for skill_dir in root.iterdir():
            if not skill_dir.is_dir() or skill_dir.name in (LAYER, ""):
                continue
            if skill_dir.name not in SKILL_REGISTRY:
                continue
            for path in skill_dir.rglob("*.md"):
                if f".agents/skills/{LAYER}/" in path.read_text(encoding="utf-8"):
                    found.add(skill_dir.name)
                    break
    return sorted(found)


@pytest.mark.parametrize("name", _shipped_dependents())
def test_skill_that_reads_the_layer_declares_it(name):
    assert LAYER in SKILL_REGISTRY[name].requires


def test_layer_survives_its_own_disable():
    """The switch that takes the layer away is the dependent's, not its own."""
    assert resolve_disabled_skills({LAYER}) == frozenset()

    registry = build_effective_skill_registry(disabled_skills={LAYER})
    assert LAYER in registry
    assert "comps-analysis" in registry


def test_layer_goes_when_every_dependent_is_off():
    disabled = {LAYER, *_dependents()}
    assert resolve_disabled_skills(disabled) == frozenset(disabled)
    assert LAYER not in build_effective_skill_registry(disabled_skills=disabled)


def test_disabling_a_dependent_keeps_the_layer():
    registry = build_effective_skill_registry(disabled_skills={"comps-analysis"})
    assert "comps-analysis" not in registry
    assert LAYER in registry


@pytest.mark.asyncio
async def test_sandbox_sync_keeps_the_layer(tmp_path):
    """The sync reads the same rule, so the mandated read finds a file."""
    for name in (LAYER, "comps-analysis"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "SKILL.md").write_text("body", encoding="utf-8")

    names = await _collect_local_skill_names(
        [str(tmp_path)], disabled=frozenset({LAYER})
    )
    assert names == {LAYER, "comps-analysis"}

    names = await _collect_local_skill_names(
        [str(tmp_path)], disabled=frozenset({LAYER, *_dependents()})
    )
    assert names == set()
