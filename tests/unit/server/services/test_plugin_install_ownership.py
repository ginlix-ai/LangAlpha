"""Install refuses a skill name another plugin already owns.

A hand-made skill of the same name is left to the fan-out, which skips that
one skill; only an ownership conflict stops the whole install, and it has to
stop it before the plugin row exists.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.plugins import lifecycle
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.grants import BindGrants
from src.server.services.plugins.skills import SkillPlan, collect_skills

USER = "test-user-123"
OTHER_PLUGIN_ID = "11111111-1111-1111-1111-111111111111"

M = "src.server.services.plugins.lifecycle."


class _PastTheRefusal(Exception):
    """Raised by the plugin-row write, which runs only once install is allowed."""


def _skill_md(name: str) -> bytes:
    return (
        f"---\nname: {name}\ndescription: A test skill that does a thing.\n"
        "---\n\nBody.\n"
    ).encode()


def _package(
    *dirs: str,
    declared: dict[str, str] | None = None,
    skipped: tuple[str, ...] = (),
) -> SimpleNamespace:
    """``declared`` maps a directory to the name its SKILL.md claims."""
    declared = declared or {}
    plans, _ = collect_skills(
        {f"skills/{d}/SKILL.md": _skill_md(declared.get(d, d)) for d in dirs}
    )
    plans += [
        SkillPlan(dir=d, skip_code="missing_skill_md", skip_reason="no SKILL.md")
        for d in skipped
    ]
    return SimpleNamespace(
        name="incoming-plugin",
        version="1.0.0",
        manifest={},
        mcp_document=None,
        mcp_document_invalid=False,
        extension=LangalphaExtension(),
        entry_plans=[],
        skill_plans=plans,
        agent_plans=[],
    )


def _row(name: str, *, owner: str | None = None) -> dict:
    return {
        "name": name,
        "plugin_id": OTHER_PLUGIN_ID if owner else None,
        "plugin_name": owner,
    }


@contextmanager
def _account(skill_rows: list[dict]):
    create = AsyncMock(side_effect=_PastTheRefusal)
    with (
        patch(M + "bundled_names", return_value=frozenset()),
        patch(M + "plugin_fan_out_lock", lambda _: nullcontext()),
        patch(M + "get_plugin", new=AsyncMock(return_value=None)),
        patch(M + "list_plugins", new=AsyncMock(return_value=[])),
        patch(
            "src.server.services.plugins.gate.resolve_bind_grants",
            new=AsyncMock(return_value=BindGrants(granted=frozenset())),
        ),
        patch(M + "list_catalog_servers", new=AsyncMock(return_value=[])),
        patch(M + "list_user_skills", new=AsyncMock(return_value=skill_rows)),
        patch(M + "get_user_secret_names", new=AsyncMock(return_value=[])),
        patch(M + "create_plugin", new=create),
    ):
        yield create


async def _install(package: SimpleNamespace) -> None:
    await lifecycle.install_plugin_package(
        USER, package, source_type="zip", source_ref=None
    )


@pytest.mark.asyncio
async def test_a_skill_another_plugin_owns_refuses_the_whole_install():
    with _account([_row("summarize", owner="first-plugin")]) as create:
        with pytest.raises(
            ValueError,
            match="Skill 'summarize' is owned by plugin 'first-plugin'; "
            "uninstall it first",
        ):
            await _install(_package("digest", "summarize"))
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_hand_made_skill_of_the_same_name_is_left_to_the_fan_out():
    # The fan-out skips that one skill as `exists`; the skip itself is
    # covered in test_plugin_skill_fanout.
    with _account([_row("summarize")]):
        with pytest.raises(_PastTheRefusal):
            await _install(_package("summarize"))


@pytest.mark.asyncio
async def test_a_skill_plan_that_cannot_install_does_not_refuse():
    with _account([_row("summarize", owner="first-plugin")]):
        with pytest.raises(_PastTheRefusal):
            await _install(_package(skipped=("summarize",)))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "declared",
    [
        pytest.param("digest", id="declares-another-name"),
        pytest.param("Bad Name!!", id="invalid-skill"),
    ],
)
async def test_a_skill_that_would_not_install_under_the_owned_name_does_not_refuse(
    declared: str,
):
    # The fan-out reports these as name_mismatch and invalid_skill, so the
    # owned name was never at stake and uninstalling its owner would not help.
    with _account([_row("summarize", owner="first-plugin")]):
        with pytest.raises(_PastTheRefusal):
            await _install(_package("summarize", declared={"summarize": declared}))
