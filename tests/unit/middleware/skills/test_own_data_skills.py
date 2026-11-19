"""A turn that may not use the sender's own data (a shared workspace's) is not
offered the skills whose work is that data, through any door: the registry,
slash commands, or the filesystem scan that finds a shipped skill the
registry does not know."""

import types

import pytest

from ptc_agent.agent.middleware.skills import SkillsMiddleware
from ptc_agent.agent.middleware.skills.registry import build_effective_skill_registry
from src.server.handlers.chat.request_prep import turn_skill_names
from src.server.services.user_skills import shared_computer_withheld

OWN_DATA = {"portfolio-diagnosis", "dashboard-page", "onboarding", "x-api"}


def test_bundles_declare_the_skills_that_need_own_data():
    assert shared_computer_withheld("shared-0000") == OWN_DATA
    assert shared_computer_withheld("b7000000") == frozenset()


def test_registry_and_slash_commands_leave_them_out():
    personal = build_effective_skill_registry()
    shared = build_effective_skill_registry(withheld=OWN_DATA)
    registered = OWN_DATA & set(personal)
    assert registered == {"portfolio-diagnosis", "dashboard-page", "onboarding"}
    assert set(personal) - set(shared) == registered
    # A skill that requires one still stays; only the withheld one goes.
    assert "interactive-dashboard" in shared

    assert turn_skill_names(types.SimpleNamespace()) == set(personal)
    assert turn_skill_names(types.SimpleNamespace(withheld_skills=OWN_DATA)) == set(shared)


@pytest.mark.asyncio
async def test_filesystem_scan_leaves_out_an_unregistered_one():
    found = ["x-api", "web-scraping"]

    class Backend:
        async def als(self, path):
            return types.SimpleNamespace(
                entries=[{"path": f"{path}{n}", "is_dir": True} for n in found]
            )

    known = {
        n: {"name": n, "description": n, "path": f"/s/{n}/SKILL.md"} for n in found
    }

    async def discovered(withheld: frozenset[str]) -> list[str]:
        mw = SkillsMiddleware(
            skill_registry=build_effective_skill_registry(withheld=withheld),
            backend=Backend(),
            sources=["/s/"],
            known_skills=known,
            disabled_skills=withheld,
            inject_manifest=False,
        )
        update = await mw.abefore_agent({"messages": []}, None, config=None)
        return [s["name"] for s in update["discovered_skills"]]

    assert await discovered(frozenset()) == ["x-api"]
    assert await discovered(frozenset(OWN_DATA)) == []


@pytest.mark.asyncio
async def test_a_shared_workspaces_computer_is_not_given_them(monkeypatch):
    """Left on the computer, an agent that lists the skills folder finds them
    and offers them anyway."""
    from src.server.services.user_skills import materialize

    async def bundle(user_id, workspace_id=None):
        return materialize.UserSkillBundle(
            dir=None, skills=(), account_disabled_builtins=frozenset({"pdf"})
        )

    monkeypatch.setattr(materialize, "load_user_skill_bundle", bundle)
    holder = await materialize.sandbox_skill_sync_params("shared-0000")
    person = await materialize.sandbox_skill_sync_params("b7000000")
    assert holder["disabled_skills"] == OWN_DATA | {"pdf"}
    assert person["disabled_skills"] == frozenset({"pdf"})


def test_a_shared_workspaces_computer_goes_without_a_per_user_account_bundles_skills(
    monkeypatch,
):
    """The skills shipped beside a built-in the holder goes without teach only
    that server, so the computer is not given them either."""
    from ptc_agent.config.core import MCPConfig, MCPServerConfig
    from src.server.app import setup

    firecrawl = MCPServerConfig(
        name="firecrawl",
        transport="http",
        url="https://mcp.firecrawl.dev/v2/mcp",
        auth="firecrawl_account",
    )
    monkeypatch.setattr(
        setup, "agent_config", types.SimpleNamespace(mcp=MCPConfig(servers=[firecrawl]))
    )

    assert shared_computer_withheld("shared-0000") == OWN_DATA | {"data-catalog"}
    assert shared_computer_withheld("b7000000") == frozenset()
