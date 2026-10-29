"""The subagent files: a rendered file reads back as itself, and what a save, rm or mv of one changes."""

from __future__ import annotations

import copy
import re
from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from ptc_agent.agent.backends.subagents import SubagentsBackend
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import BaseSubagent
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.services.subagents.rules import changed_fields
from src.server.services.subagents.file import SubagentFile, render_subagent
from tests.unit.server.services.subagents._fakes import THREAD, USER, research_base, views

pytestmark = pytest.mark.asyncio

ROOT = "/home/workspace/.agents/user/agents/"
CALL = CallContext(thread_id=THREAD)


@pytest.fixture
def backend(world) -> SubagentsBackend:
    return SubagentsBackend(user_id=USER, call=CALL, sandbox_backend=MagicMock(), root_prefix=ROOT)


def _path(name: str) -> str:
    return f"{ROOT}{name}.md"


def _set(content: str, key: str, value: str) -> str:
    """``content`` with its frontmatter's ``key`` line set to ``value``."""
    out, n = re.subn(rf"^{key}: .*$", f"{key}: {value}", content, count=1, flags=re.MULTILINE)
    assert n == 1, key
    return out


async def _plan(name: str, content: str):
    """What a save of ``content`` over ``name``'s file would change."""
    file = SubagentFile(name)
    planned = await file.parse(USER, CALL, content, None)
    return file.plan(CALL, planned, await file.fetch(USER)).changes


# Each is read differently by YAML when left plain.
DESCRIPTIONS = [
    "Credit work: covenants # and spreads",
    "a: b",
    "#hash first",
    "yes",
    "on",
    "null",
    "1.10",
    '"Quoted" opener',
    "[draft] notes",
    "{brace} first",
    "&anchor *alias",
    "@reserved `tick`",
    "- leading dash",
    "multi\nline",
    "back\\slash",
    "line separator",
    "Ünïcødé ✓ 中文",
    "2026-02-30",  # date-shaped, and a date YAML can't build
]


@pytest.mark.parametrize("description", DESCRIPTIONS)
async def test_a_custom_file_parses_back_to_the_values_it_shows(world, description):
    world.custom(
        "alpha", description=description, model="model-a", effort="high", tools=["market_data", "shell"],
        search_provider="tavily", search_depth="deep", connectors=False, brokerages="off", enabled=False,
        preferred_connectors=["fundamentals", "on"], preferred_skills=["dcf", "yes", "1.0", "a b", "x,y"],
        roles=["chief_of_staff"], instructions="Line one.\n\n---\nA rule in the body: kept.",
    )
    view = (await views(world))["alpha"]
    content = render_subagent(view)

    written = SubagentFile("alpha").written(content)

    assert written.values["description"] == description
    assert changed_fields({k: view[k] for k in written.values}, written.values) == {}
    assert (written.name, written.source, written.body) == ("alpha", "custom", view["instructions"])
    change = await _plan("alpha", content)
    assert (change.action, change.notes) == ("none", [])


@pytest.mark.parametrize("name", ["research", "data-prep"])
async def test_a_base_file_parses_back_to_the_values_it_shows(world, name):
    world.override("research", model="model-a", search_provider="exa", search_depth="fast")
    view = (await views(world))[name]
    content = render_subagent(view)

    written = SubagentFile(name).written(content)

    assert changed_fields({k: view[k] for k in written.values}, written.values) == {}
    change = await _plan(name, content)
    assert (change.action, change.notes) == ("none", [])


@pytest.mark.parametrize(
    ("content", "error_type", "fields"),
    [
        ("---\ncolour: blue\nflavour: mild\n---\nBody.\n", "schema_error", ["colour", "flavour"]),
        # Plain YAML keeps the last of a key given twice; the file refuses it.
        ("---\neffort: high\neffort: low\n---\nBody.\n", "parse_error", []),
        ("No frontmatter at all.\n", "parse_error", []),
        ("---\ndescription: never closed\n", "parse_error", []),
        ("---\n- a list\n---\n", "parse_error", []),
        ("---\ndescription: holds: a colon\n---\n", "parse_error", []),
        ("---\ndescription: 2026-13-45\n---\n", "parse_error", []),
        ("---\ntools: " + "[" * 3000 + "]" * 3000 + "\n---\n", "parse_error", []),
    ],
)
async def test_a_file_the_frontmatter_rules_refuse(world, content, error_type, fields):
    with pytest.raises(UserDataValidationError) as exc:
        await SubagentFile("alpha").parse(USER, CALL, content, None)

    assert exc.value.error_type == error_type
    assert [field for field, _ in exc.value.problems] == fields


@pytest.mark.parametrize(
    ("name", "key", "value", "changed", "notes"),
    [
        # Kept by the server: ignored, each with a note.
        ("alpha", "name", "other", set(), 1),
        ("alpha", "source", "builtin", set(), 1),
        ("research", "source", "custom", set(), 1),
        # Read-only on a base.
        ("research", "description", "Mine now.", set(), 1),
        ("research", "available_to", "[analyst]", set(), 1),
        ("alpha", "description", "Mine now.", {"description"}, 0),
        ("research", "model", "model-flat", {"model"}, 0),
        ("research", "brokerages", "off", {"brokerages"}, 0),
        # Memory is editable on a base, as on one of the user's own.
        ("research", "memory", "on", {"memory"}, 0),
        ("alpha", "memory", "off", {"memory"}, 0),
        ("research", "memory", "off", set(), 0),
    ],
)
async def test_what_a_save_sends_on(world, name, key, value, changed, notes):
    world.custom("alpha")
    content = render_subagent((await views(world))[name])

    change = await _plan(name, _set(content, key, value))

    assert (set(change.fields), len(change.notes)) == (changed, notes)


@pytest.mark.parametrize(
    ("name", "edit", "refused", "changed"),
    [
        ("research", lambda body: body.replace("cite every", "cite no"), True, set()),
        # Reflowing the text is not an edit.
        ("research", lambda body: body.replace("widely, then", "widely,\n  then"), False, set()),
        ("alpha", lambda body: body.replace("Do the work.", "Do other work."), False, {"instructions"}),
    ],
)
async def test_only_a_custom_subagent_takes_a_new_body(world, name, edit, refused, changed):
    world.custom("alpha")
    view = (await views(world))[name]
    head, body = render_subagent(view).rsplit("---\n", 1)
    content = f"{head}---\n{edit(body)}"

    if refused:
        with pytest.raises(UserDataValidationError) as exc:
            await _plan(name, content)
        assert exc.value.error_type == "constraint_error"
    else:
        assert set((await _plan(name, content)).fields) == changed


@pytest.mark.parametrize(
    ("name", "value", "stored"),
    [
        # A base keeps only the difference, and writing its own value back is a reset.
        ("research", "on", {"memory": True}),
        ("research", "off", None),
        ("alpha", "off", {"memory": False}),
        ("alpha", "true", {"memory": True}),
    ],
)
async def test_memory_saves_from_the_file_and_reads_back_as_on_or_off(world, backend, name, value, stored):
    world.custom("alpha", memory=True)
    world.override("research", memory=True)
    content, version = await backend.aread_versioned(_path(name))
    assert "\nmemory: on\n" in content

    await backend.awrite_versioned(_path(name), _set(content, "memory", value), version)

    row = world.rows.get(name)
    assert ({"memory": row["fields"]["memory"]} if row and "memory" in row["fields"] else None) == stored
    shown = "on" if value in ("on", "true") else "off"
    assert f"\nmemory: {shown}\n" in (await backend.aread_versioned(_path(name)))[0]


@pytest.mark.parametrize(
    ("named", "model", "tools"),
    [
        # A copy carries every key the file leaves out from the subagent it names.
        ("research", "model-a", ["web"]),
        # Naming no subagent, the name is ignored and the defaults apply.
        ("nobody", None, ["web", "market_data", "code", "shell"]),
    ],
)
async def test_a_new_file_naming_another_subagent_is_a_copy_of_it(world, backend, named, model, tools):
    world.override("research", model="model-a")
    content = f"---\nname: {named}\ndescription: My own take.\n---\nOwn body.\n"

    await backend.awrite_versioned(_path("mine"), content, None)

    mine = (await views(world))["mine"]
    assert (mine["kind"], mine["model"], mine["tools"]) == ("custom", model, tools)
    assert (mine["description"], mine["instructions"]) == ("My own take.", "Own body.")


@pytest.mark.parametrize("under_the_lock", [False, True])
async def test_a_base_that_moves_after_the_read_is_a_version_conflict(world, backend, under_the_lock):
    """The agent's save is held to the same base-aware version as the pane's,
    including past the route's own check, where only the save's locked read sees it."""
    world.override("research", model="model-a")
    content, version = await backend.aread_versioned(_path("research"))

    def move():
        world.bases["research"] = research_base(role_prompt="A new body from a deploy.")

    if under_the_lock:
        world.on_lock = move
    else:
        move()
    rows = copy.deepcopy(world.rows)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("research"), _set(content, "effort", "high"), version)

    assert exc.value.error_type == "version_conflict"
    assert world.rows == rows
    assert world.on_lock is None


async def test_a_save_hands_the_mount_its_own_file_not_a_later_writers(world, backend):
    """The file and version a save settles on are read before it lets the
    user's lock go, so a pane save right after it is a change the mount has
    not seen rather than one it holds as its own."""
    world.custom("alpha", model="model-a")
    content, version = await backend.aread_versioned(_path("alpha"))
    world.on_release = lambda: world.rows["alpha"]["fields"].update(model="model-flat")

    stored = await backend.awrite_versioned(_path("alpha"), _set(content, "effort", "high"), version)

    assert world.on_release is None
    assert "\nmodel: model-a\neffort: high\n" in stored.content
    assert stored.version != (await backend.aread_versioned(_path("alpha")))[1]


@pytest.mark.parametrize(
    ("content", "field"),
    [
        ('---\ndescription: "Has a \\0 in it."\n---\nBody.\n', "description"),
        ("---\ndescription: Fine.\n---\nHas a \x00 in it.\n", "instructions"),
    ],
)
async def test_a_nul_is_refused_on_its_field_and_saves_nothing(world, backend, content, field):
    rows = copy.deepcopy(world.rows)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("mine"), content, None)

    assert (exc.value.error_type, [f for f, _ in exc.value.problems]) == ("schema_error", [field])
    assert world.rows == rows


@pytest.mark.parametrize(
    "content",
    [
        pytest.param('---\ndescription: "Has a \\ud800 in it."\n---\nBody.\n', id="in-a-value"),
        pytest.param('---\ndescription: Fine.\n"bad\\ud800key": 1\n---\nBody.\n', id="in-a-key"),
    ],
)
async def test_a_lone_surrogate_in_the_frontmatter_is_refused_at_parse(world, backend, content):
    rows = copy.deepcopy(world.rows)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("mine"), content, None)

    assert exc.value.error_type == "parse_error"
    assert "the frontmatter holds an unpaired surrogate (\\ud800 to \\udfff), which saved text can't hold." in str(
        exc.value
    )
    assert world.rows == rows


async def test_a_raw_nul_in_the_frontmatter_is_refused_before_yaml(world, backend):
    rows = copy.deepcopy(world.rows)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("mine"), "---\ndescription: Has a \x00 in it.\n---\nBody.\n", None)

    assert exc.value.error_type == "parse_error"
    assert "the frontmatter holds a NUL character (\\x00), which saved text can't hold." in str(exc.value)
    assert world.rows == rows


async def test_a_nul_and_a_surrogate_escape_in_the_frontmatter_get_the_surrogate_refusal(world, backend):
    content = '---\ndescription: "A \\0 here."\n"bad\\ud800key": 1\n---\nBody.\n'

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("mine"), content, None)

    assert exc.value.error_type == "parse_error"
    assert "an unpaired surrogate" in str(exc.value)


async def test_a_stale_value_left_as_it_was_does_not_refuse_the_save(world, backend):
    world.custom("alpha", preferred_skills=["gone"], model="model-off-plan")
    content, version = await backend.aread_versioned(_path("alpha"))

    await backend.awrite_versioned(_path("alpha"), _set(content, "description", "Now says more."), version)

    assert world.rows["alpha"]["fields"]["description"] == "Now says more."


async def test_an_orphaned_override_has_no_file_and_takes_none(world, backend):
    world.override("ghost", base_key="builtin:ghost", enabled=False)

    assert "ghost.md" not in await backend.names(USER)
    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(_path("ghost"), "---\ndescription: Mine.\n---\n", None)
    assert exc.value.error_type == "constraint_error"
    assert world.rows["ghost"]["kind"] == "override"



async def test_a_deployment_subagent_named_main_has_no_file(world):
    # main.md is the main agent's, so a subagent of that name can't have it.
    world.bases["main"] = BaseSubagent("deployment:main", SubagentDefinition(
        name="main", description="An operator's subagent.", role_prompt="Work.",
        tools=["web_search"], source="user",
    ))
    world.offered = (*world.offered, "main")

    assert (await views(world))["main"]["file"] is None

@pytest.mark.parametrize(
    ("op", "name", "to", "refused", "rows_after"),
    [
        ("rm", "alpha", None, None, {"trader", "research"}),
        # rm on a base is its reset.
        ("rm", "research", None, None, {"alpha", "trader"}),
        # Nothing of the user's to reset.
        ("rm", "data-prep", None, "constraint_error", None),
        # A base keeps its name, which is how its overrides find it.
        ("mv", "research", "research-2", "constraint_error", None),
        ("mv", "alpha", "gamma", None, {"gamma", "trader", "research"}),
        ("mv", "alpha", "trader", "exists", None),
        # A name a defined base holds, offered or not.
        ("mv", "alpha", "dormant", "schema_error", None),
    ],
)
async def test_rm_and_mv(world, backend, op, name, to, refused, rows_after):
    world.custom("alpha", model="model-a")
    world.custom("trader")
    world.override("research", model="model-a")
    rows = copy.deepcopy(world.rows)
    run = backend.adelete_versioned(_path(name)) if op == "rm" else backend.arename_versioned(_path(name), _path(to))

    if refused is not None:
        with pytest.raises(UserDataValidationError) as exc:
            await run
        assert exc.value.error_type == refused
        assert world.rows == rows
        return
    await run
    assert set(world.rows) == rows_after
    if op == "mv":
        assert world.rows[to]["fields"] == rows[name]["fields"]


async def test_a_mv_onto_an_orphaned_override_is_refused_as_that_name_is_held(world, backend):
    world.custom("alpha")
    world.override("ghost", base_key="builtin:ghost", enabled=False)
    rows = copy.deepcopy(world.rows)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.arename_versioned(_path("alpha"), _path("ghost"))

    assert exc.value.error_type == "constraint_error"
    assert (
        "ghost holds the settings of a built-in subagent that was removed; the user can delete them "
        "in the Subagents pane. Pick another name."
    ) in str(exc.value)
    assert world.rows == rows


async def test_a_saved_description_is_named_not_echoed(world, backend):
    world.custom("alpha", description="Handles alpha work.")
    content, version = await backend.aread_versioned(_path("alpha"))

    report = await backend.awrite_versioned(_path("alpha"), _set(content, "description", "Ends in a period."), version)

    assert report.report == "Saved: alpha: description updated. Applies from your next turn."
