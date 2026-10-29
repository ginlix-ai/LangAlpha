"""Saving and removing subagents: the base-aware version both writers check, and what a save leaves in the rows."""

from __future__ import annotations

import copy

import jinja2
import pytest

from ptc_agent.agent.prompts import get_loader
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import BaseSubagent
from src.server.services.subagents.rules import EDITABLE_KEYS
from src.server.services.subagents.writes import VersionConflict, delete_subagent, save_subagent
from tests.unit.server.services.subagents._fakes import USER, research_base, views

pytestmark = pytest.mark.asyncio


def _custom_changed_elsewhere(world):
    world.rows["alpha"]["fields"]["model"] = "model-flat"


def _base_changed_since_the_read(world):
    world.bases["research"] = research_base(role_prompt="A new body from a deploy.")


def _base_changed_under_the_lock(world):
    world.on_lock = lambda: _base_changed_since_the_read(world)


@pytest.mark.parametrize("op", ["save", "delete"])
@pytest.mark.parametrize(
    ("name", "move"),
    [
        ("alpha", _custom_changed_elsewhere),
        ("research", _base_changed_since_the_read),
        # Past the pre-lock check: only the read under the lock sees it.
        ("research", _base_changed_under_the_lock),
    ],
)
async def test_a_version_read_before_a_change_is_a_conflict(world, op, name, move):
    """A base that moves between a read and a write would otherwise pin values
    the user never touched."""
    world.custom("alpha", model="model-a")
    world.override("research", model="model-a")
    version = (await views(world))[name]["version"]
    move(world)
    rows = copy.deepcopy(world.rows)

    with pytest.raises(VersionConflict):
        if op == "save":
            await save_subagent(USER, name, {"effort": "high"}, version=version)
        else:
            await delete_subagent(USER, name, version=version)

    assert world.rows == rows
    # Consumed: the writer got as far as the lock before it refused.
    assert world.on_lock is None


async def test_a_subagent_removed_right_after_its_save_is_a_conflict(world):
    """The save committed, but the view it answers with is gone: the pane
    reloads on a conflict rather than failing the request."""
    world.custom("alpha", model="model-a")
    version = (await views(world))["alpha"]["version"]
    world.on_release = lambda: world.rows.pop("alpha")

    with pytest.raises(VersionConflict):
        await save_subagent(USER, "alpha", {"effort": "high"}, version=version)

    assert world.on_release is None


async def test_resetting_a_base_hidden_since_the_read_is_a_conflict(world):
    """Switching its plugin off hides a base but keeps the user's override for
    when it is back, so a reset aimed at it must not answer "already gone"."""
    world.offered = (*world.offered, "filings-reader")
    world.override("filings-reader", model="model-a")
    version = (await views(world))["filings-reader"]["version"]
    world.offered = tuple(n for n in world.offered if n != "filings-reader")
    rows = copy.deepcopy(world.rows)

    with pytest.raises(VersionConflict):
        await delete_subagent(USER, "filings-reader", version=version)

    assert world.rows == rows


@pytest.mark.parametrize(
    ("name", "stored", "fields", "row"),
    [
        ("research", None, {"model": "model-a", "tools": ["web", "code"], "connectors": True},
         {"model": "model-a", "tools": ["web", "code"]}),
        # Nothing left that differs from the base: the row goes.
        ("research", {"model": "model-a"}, {"model": None}, None),
        ("research", None, {"tools": ["web"], "brokerages": "on"}, None),
        ("research", {"enabled": False}, {"enabled": True}, None),
        ("research", None, {"memory": True}, {"memory": True}),
        ("research", {"memory": True}, {"memory": "off"}, None),
        # An explicit inherit is the thread's model whatever the base pins, so it stays;
        # null clears back to the pin.
        ("data-prep", None, {"model": "inherit"}, {"model": None}),
        ("data-prep", {"model": None}, {"model": None}, None),
    ],
)
async def test_an_override_stores_only_what_differs_from_its_base(world, name, stored, fields, row):
    if stored is not None:
        world.override(name, **stored)
    version = (await views(world))[name]["version"]

    view = await save_subagent(USER, name, fields, version=version)

    assert (world.rows[name]["fields"] if name in world.rows else None) == row
    assert view["edited"] is (row is not None)
    assert set(view["user_fields"]) == set(row or ())


@pytest.mark.parametrize("source", ["research", "trader"])
async def test_start_from_copies_the_effective_values(world, source):
    world.override("research", model="model-a", brokerages="off", preferred_skills=["dcf"])
    world.custom("trader", model="model-a", effort="high", brokerages="off", tools=["market_data", "code"],
                 instructions="Trade carefully.", roles=["analyst"])
    original = (await views(world))[source]

    copied = await save_subagent(USER, "my-copy", {}, version=None, start_from=source)

    assert copied["kind"] == "custom"
    assert {k: copied[k] for k in EDITABLE_KEYS} == {k: original[k] for k in EDITABLE_KEYS}
    keys = ("description", "instructions", "available_to")
    assert {k: copied[k] for k in keys} == {k: original[k] for k in keys}


async def test_a_base_template_that_reads_a_turn_variable_shows_and_copies_its_body(world, monkeypatch):
    """A deployment template may read what a turn passes it, or branch on it.
    Rendered without those, the pane and the file showed no body, and a copy
    ran without its role text; rendered with empty ones, a copy lost the
    branch every turn takes."""
    loader = get_loader()
    template = "deploy/desk.md.j2"
    monkeypatch.setattr(loader.env, "loader", jinja2.ChoiceLoader([
        jinja2.DictLoader({template: (
            "Keep notes under notes/{{ thread_id[:8] }}"
            "{% if thread_id and current_time %} and cite every filing{% endif %}.\n"
        )}),
        loader.env.loader,
    ]))
    world.bases["desk"] = BaseSubagent("deployment:desk", SubagentDefinition(
        name="desk", description="Covers the desk.", custom_prompt_template=template,
        tools=["web_search"], source="user",
    ))
    world.offered += ("desk",)
    body = "Keep notes under notes/ and cite every filing."

    assert (await views(world))["desk"]["instructions"] == body

    await save_subagent(USER, "my-desk", {}, version=None, start_from="desk")

    assert world.rows["my-desk"]["instructions"] == body


async def test_a_copy_leaves_out_a_base_pin_its_plan_lacks(world):
    """A deployment base runs its own pin whatever the plan; a copy is the
    user's own and held to the plan, so it starts on the thread's model
    rather than saving a pin it could only ever fall back from."""
    assert (await views(world))["data-prep"]["problems"] == []

    copied = await save_subagent(USER, "my-prep", {"description": "Mine."}, version=None, start_from="data-prep")

    assert "model" not in world.rows["my-prep"]["fields"]
    assert (copied["model"], copied["effort"]) == (None, "low")
    assert copied["problems"] == []


@pytest.mark.parametrize(
    ("other_on", "description", "matches"),
    [
        (True, "handles  ALPHA work.", ["alpha"]),
        (False, "Handles alpha work.", []),
        (True, "Finds and cites sources on the web.", ["research"]),
    ],
)
async def test_a_description_another_subagent_has_saves_with_a_problem(world, other_on, description, matches):
    world.custom("alpha", enabled=other_on)

    view = await save_subagent(USER, "beta", {"description": description}, version=None)

    assert "beta" in world.rows
    assert [p["detail"] for p in view["problems"] if p["code"] == "description_matches"] == matches
    # A base's description is read-only, so only the custom one is asked to differ.
    assert not [p for p in (await views(world))["research"]["problems"] if p["code"] == "description_matches"]


async def test_a_stale_value_the_save_leaves_alone_shows_as_a_problem(world):
    world.custom(
        "alpha", model="model-off-plan", preferred_skills=["gone"], preferred_connectors=["ghost"],
        search_provider="retired",
    )
    version = (await views(world))["alpha"]["version"]

    view = await save_subagent(USER, "alpha", {"enabled": False}, version=version)

    assert world.rows["alpha"]["fields"]["enabled"] is False
    assert {(p["code"], p["detail"]) for p in view["problems"]} >= {
        ("model_unavailable", "model-off-plan"),
        ("skill_missing", "gone"),
        ("connector_missing", "ghost"),
        ("search_unavailable", "retired"),
    }


@pytest.mark.parametrize(("start_from", "memory"), [(None, True), ("research", False)])
async def test_a_new_subagent_reads_memory_unless_it_copies_one_that_does_not(world, start_from, memory):
    view = await save_subagent(USER, "neo", {"description": "Desk work."}, version=None, start_from=start_from)

    assert view["memory"] is memory


async def test_a_reset_clears_an_override_whose_base_was_replaced(world):
    """A deployment definition that takes a built-in's name leaves the user's
    row applying to nothing; resetting the name removes it, and the warning
    with it."""
    world.override("research", model="model-a")
    world.bases["research"] = BaseSubagent("deployment:research", SubagentDefinition(
        name="research", description="The deployment's research.", role_prompt="Search.",
        tools=["web_search", "filesystem"], source="user",
    ))
    view = (await views(world))["research"]
    assert (view["edited"], view["problems"]) == (False, [{"code": "base_removed", "detail": "builtin:research"}])

    assert await delete_subagent(USER, "research", version=view["version"]) is True

    assert "research" not in world.rows
    assert (await views(world))["research"]["problems"] == []


@pytest.mark.parametrize(("effort", "flagged"), [("high", False), ("medium", True)])
async def test_a_deployment_pin_at_a_level_its_model_lacks_shows_as_a_problem(world, effort, flagged):
    """The writers hold a level to the model's ladder, but nothing writes the
    deployment's own file through them, and the client steps the level down."""
    world.bases["desk"] = BaseSubagent("deployment:desk", SubagentDefinition(
        name="desk", description="Covers the desk.", role_prompt="Cover it.",
        tools=["filesystem"], model="model-pinned", reasoning_effort=effort, source="user",
    ))
    world.offered += ("desk",)

    problems = (await views(world))["desk"]["problems"]

    assert problems == ([{"code": "effort_not_offered", "detail": "model-pinned"}] if flagged else [])
