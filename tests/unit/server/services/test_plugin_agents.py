"""A package's agents/ files read into plugin subagents: what maps onto ours,
what is held back, and what costs one file without failing the install."""

from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.roles import ALL_ROLES
from ptc_agent.agent.subagents.merge import plugin_definition
from src.server.services.plugins import ValidatedPackage, validate_package
from src.server.services.plugins.agents import (
    AGENT_FILE_MAX_BYTES,
    collect_agents,
    link_connectors,
    read_agent,
)
from src.server.services.plugins.export import export_plugin_zip
from src.server.services.subagents.rules import (
    DESCRIPTION_MAX,
    INSTRUCTIONS_MAX_BYTES,
)
from tests.unit.server.mcp_builders import zip_package
from tests.unit.server.plugin_builders import MCP_SCHEMA, PLUGIN_ID, PLUGIN_SCHEMA, USER

GOOD = b"---\ndescription: Screens tickers against a rule.\n---\nScreen them.\n"


def _agent(front: str, body: str = "Do the thing.\n") -> bytes:
    return f"---\n{front}\n---\n{body}".encode()


def _package(members: dict[str, bytes | str]) -> ValidatedPackage:
    """A canonical plugin named demo whose manifest declares nothing but itself."""
    manifest = {"$schema": PLUGIN_SCHEMA, "name": "demo", "version": "1.0.0"}
    return validate_package(
        zip_package({"plugin.json": json.dumps(manifest), **members})
    )


def _plans(package: ValidatedPackage) -> dict:
    return {p.name: p for p in package.agent_plans}


def _codes(plan) -> set[str]:
    return {d.code for d in plan.diagnostics}


class TestClaudeCodeImport:
    """A Claude Code agent file installs as is, its keys read as ours."""

    def test_a_claude_code_plugin_agent_maps_onto_our_values(self):
        package = validate_package(zip_package({
            ".claude-plugin/plugin.json": json.dumps(
                {"name": "reviewers", "version": "1.0.0"}
            ),
            "agents/reviewer.md": (
                "---\n"
                "name: code-reviewer\n"
                "description: Reviews a change. Use after edits.\n"
                "tools: Read, Grep, Glob, Bash, WebFetch, mcp__tracker__get_issue\n"
                "model: sonnet\n"
                "color: blue\n"
                "skills: [lint-rules]\n"
                "---\n"
                "You review code.\n"
            ),
        }))

        [plan] = package.agent_plans
        # The file name is the subagent's name, whatever the frontmatter says.
        assert (plan.name, plan.skip_code) == ("reviewer", None)
        # File tools need no word; Bash is code and shell; an MCP tool is code
        # plus its server as a preferred connector. A family alias names no
        # model, so none is stored and the thread's model runs it.
        assert plan.fields == {
            "description": "Reviews a change. Use after edits.",
            "tools": ["web", "code", "shell"],
            "preferred_connectors": ["tracker"],
            "preferred_skills": ["lint-rules"],
        }
        assert plan.instructions == "You review code."
        definition = plugin_definition(plan.row)
        assert (definition.model, definition.brokerages, definition.connectors) == (None, "on", True)
        assert definition.roles == ALL_ROLES
        assert _codes(plan) == {"name_ignored", "model_alias", "keys_ignored"}

    def test_a_tool_of_the_plugins_own_server_prefers_that_server(self):
        package = validate_package(zip_package({
            ".claude-plugin/plugin.json": json.dumps({"name": "reviewers"}),
            ".mcp.json": json.dumps({"mcpServers": {
                "tracker": {"type": "http", "url": "https://tracker.example.com/mcp"},
            }}),
            "agents/reviewer.md": _agent(
                "description: Reviews a change.\n"
                "tools: Read, mcp__plugin_reviewers_tracker__get_issue"
            ),
        }))
        [plan] = package.agent_plans
        assert plan.fields["tools"] == ["code"]
        # Linked only to a row the plugin holds, which install knows and
        # validation does not; until then it names the plugin's server.
        assert plan.fields["preferred_connectors"] == ["plugin_reviewers_tracker"]
        [linked] = link_connectors([plan], {"tracker": "tracker"}, "reviewers")
        assert linked.fields["preferred_connectors"] == ["tracker"]
        assert plan.fields["preferred_connectors"] == ["plugin_reviewers_tracker"]

    @pytest.mark.parametrize(
        ("tools", "words", "codes"),
        [
            # None stores no tools, which a custom subagent reads as the Analyst's.
            pytest.param(None, None, set(), id="omitted-is-analyst"),
            pytest.param(
                "Read, Edit, MultiEdit, Grep, LS, NotebookEdit", [], set(), id="file-tools-only",
            ),
            pytest.param(
                ["market_data", "Bash"], ["market_data", "code", "shell"], set(),
                id="our-words-and-theirs",
            ),
            pytest.param(
                ["Bash", "BashOutput", "KillShell"], ["code", "shell"], set(),
                id="bash-companions-ride-along",
            ),
            pytest.param(
                "WebSearch, TodoWrite", ["web"], {"tools_ignored"}, id="unmatched-dropped",
            ),
            pytest.param(5, None, {"tools_ignored"}, id="not-a-list"),
        ],
    )
    def test_tools_map_onto_tool_words(self, tools, words, codes):
        front = "description: Does a thing."
        if tools is not None:
            front += f"\ntools: {json.dumps(tools)}"
        plan = read_agent("worker", _agent(front))
        assert plan.skip_code is None
        assert plan.fields.get("tools") == words
        assert _codes(plan) == codes

    @pytest.mark.parametrize(
        ("model", "expected", "codes"),
        [
            pytest.param("inherit", None, set(), id="inherit"),
            pytest.param("opus", None, {"model_alias"}, id="family-alias"),
            pytest.param("vendor-model-1", "vendor-model-1", set(), id="model-id-kept"),
            pytest.param(7, None, {"model_ignored"}, id="not-a-name"),
        ],
    )
    def test_model(self, model, expected, codes):
        plan = read_agent("worker", _agent(f"description: Does a thing.\nmodel: {model}"))
        assert plan.fields.get("model") == expected
        assert _codes(plan) == codes


class TestHeldBack:
    """What only the user grants never arrives with a package."""

    def test_enabled_false_is_ignored_so_it_arrives_on(self):
        plan = read_agent("quiet", _agent("description: Does a thing.\nenabled: false"))
        assert plan.skip_code is None
        # No switch rides the row: on or off is the user's override alone.
        assert "enabled" not in plan.fields
        assert _codes(plan) == {"enabled_ignored"}

    @pytest.mark.parametrize("written", ["user", "off"])
    def test_a_memory_key_is_not_read_so_it_arrives_reading_memory(self, written):
        """Claude Code spends ``memory`` on a scope (user, project, local), so
        the key says nothing here, whatever it holds."""
        from src.server.services.subagents.plugin_bases import _base

        plan = read_agent("desk", _agent(f"description: Does a thing.\nmemory: {written}"))

        assert "memory" not in plan.fields
        assert _codes(plan) == {"keys_ignored"}
        assert _base("demo", plan.row).definition.memory is True


class TestOneFileOnly:
    """A bad file drops that subagent; the package and its other files install."""

    def test_limits_skip_that_agent_with_its_code(self):
        package = _package({
            "agents/fits-desc.md": _agent(f"description: {'a' * DESCRIPTION_MAX}"),
            "agents/long-desc.md": _agent(f"description: {'a' * (DESCRIPTION_MAX + 1)}"),
            "agents/fits-body.md": _agent(
                "description: Does a thing.", "x" * INSTRUCTIONS_MAX_BYTES
            ),
            # Fewer characters than the limit, more bytes: the limit is bytes.
            "agents/long-body.md": _agent(
                "description: Does a thing.", "é" * (INSTRUCTIONS_MAX_BYTES // 2 + 1)
            ),
            "agents/no-desc.md": _agent("model: inherit"),
        })

        plans = _plans(package)
        assert {name: p.skip_code for name, p in plans.items()} == {
            "fits-desc": None,
            "long-desc": "description_too_long",
            "fits-body": None,
            "long-body": "instructions_too_long",
            "no-desc": "missing_description",
        }
        for name in ("long-desc", "long-body", "no-desc"):
            [diag] = plans[name].diagnostics
            assert (diag.scope, diag.target, diag.code) == (
                "subagent", name, plans[name].skip_code,
            )
        assert not [d for d in package.diagnostics if d.level == "error"]

    @pytest.mark.parametrize(
        ("path", "raw", "code"),
        [
            pytest.param("agents/Reviewer.md", GOOD, "invalid_name", id="uppercase"),
            pytest.param("agents/-lead.md", GOOD, "invalid_name", id="leading-hyphen"),
            pytest.param("agents/snake_case.md", GOOD, "invalid_name", id="underscore"),
            pytest.param(f"agents/{'a' * 49}.md", GOOD, "invalid_name", id="49-chars"),
            pytest.param("agents/main.md", GOOD, "reserved_name", id="reserved"),
            pytest.param(
                "agents/bare.md", b"description: Does a thing.\n", "parse_error",
                id="no-frontmatter",
            ),
            pytest.param(
                "agents/twice.md",
                b"---\ndescription: One.\ndescription: Two.\n---\nBody.\n",
                "parse_error", id="key-given-twice",
            ),
            pytest.param(
                "agents/listed.md", b"---\n- description\n---\nBody.\n", "parse_error",
                id="not-key-value",
            ),
            pytest.param(
                "agents/latin.md", b"---\ndescription: caf\xe9\n---\nBody.\n", "unreadable",
                id="not-utf8",
            ),
            # Postgres stores no NUL: written, the row would fail the install
            # after the plugin's other components landed, and every update.
            pytest.param(
                "agents/nul-body.md", b"---\ndescription: Fine.\n---\nA\x00B\n", "unreadable",
                id="nul-in-body",
            ),
            pytest.param(
                "agents/nul-escape.md", b'---\ndescription: "A\\0B"\n---\nBody.\n', "unreadable",
                id="nul-escape-in-value",
            ),
            pytest.param(
                "agents/nul-nested.md",
                b'---\ndescription: Fine.\nextra:\n  deep: ["ok", {"x": "A\\0B"}]\n---\nBody.\n',
                "unreadable", id="nul-escape-nested",
            ),
            pytest.param(
                "agents/nul-key.md", b'---\ndescription: Fine.\n"A\\0B": 1\n---\nBody.\n',
                "unreadable", id="nul-escape-in-key",
            ),
            pytest.param(
                "agents/lone.md", b'---\ndescription: "A\\ud800B"\n---\nBody.\n',
                "unreadable", id="surrogate-escape-in-value",
            ),
            pytest.param(
                "agents/lone-key.md", b'---\ndescription: Fine.\n"A\\ud800B": 1\n---\nBody.\n',
                "unreadable", id="surrogate-escape-in-key",
            ),
            pytest.param(
                "agents/dated.md", b"---\ndescription: 2026-13-45\n---\nBody.\n", "parse_error",
                id="a-date-yaml-cant-build",
            ),
            pytest.param(
                "agents/deep.md", b"---\nextra: " + b"[" * 3000 + b"]" * 3000 + b"\n---\nBody.\n",
                "parse_error", id="nested-past-recursion",
            ),
            # The instructions have a cap of their own; this one bounds the
            # frontmatter, which would otherwise be read and stored whole.
            pytest.param(
                "agents/huge.md",
                b"---\ndescription: Fine.\nextra: " + b"x" * AGENT_FILE_MAX_BYTES + b"\n---\nBody.\n",
                "file_too_large", id="over-the-file-cap",
            ),
        ],
    )
    def test_a_bad_file_skips_only_itself(self, path, raw, code):
        plans, diagnostics = collect_agents({path: raw, "agents/good.md": GOOD})
        by_name = {p.name: p for p in plans}
        assert by_name.pop("good").skip_code is None
        [bad] = by_name.values()
        assert bad.skip_code == code
        assert diagnostics == []

    def test_only_files_directly_under_agents_are_subagents(self):
        plans, diagnostics = collect_agents({
            "agents/good.md": GOOD,
            "agents/nested/deeper.md": GOOD,
            "agents/notes.txt": b"not a subagent",
        })
        assert [p.name for p in plans] == ["good"]
        assert sorted((d.target, d.code) for d in diagnostics) == [
            ("agents/nested/deeper.md", "loose_file"),
            ("agents/notes.txt", "loose_file"),
        ]


def test_a_preferred_connector_follows_its_server_to_the_installed_name():
    # The sandbox reserves the key ``class``, so its server installs renamed;
    # a subagent that names it by its mcp.json key has to reach that row.
    package = _package({
        "mcp.json": json.dumps({
            "$schema": MCP_SCHEMA,
            "mcpServers": {"class": {"type": "stdio", "command": "npx"}},
        }),
        "agents/lookup.md": _agent(
            "description: Looks things up.\n"
            "tools: [mcp__class__find]\n"
            "preferred_connectors: [elsewhere]"
        ),
    })
    [plan] = link_connectors(package.agent_plans, {"class": "class_server"}, "demo")
    assert plan.fields["preferred_connectors"] == ["elsewhere", "class_server"]


class TestPackage:
    """agents/ is a root of its own: the manifest names nothing new for it."""

    SKILL = b"---\nname: digest\ndescription: Digests a filing.\n---\nBody.\n"

    def test_agents_beside_a_skill_leave_the_skill_as_it_was(self):
        with_agents = _package({
            "skills/digest/SKILL.md": self.SKILL,
            "agents/screener.md": GOOD,
        })
        without = _package({"skills/digest/SKILL.md": self.SKILL})

        assert [(p.dir, p.skip_code, p.zip_bytes) for p in with_agents.skill_plans] == [
            (p.dir, p.skip_code, p.zip_bytes) for p in without.skill_plans
        ]
        assert [(p.name, p.skip_code) for p in with_agents.agent_plans] == [
            ("screener", None)
        ]
        assert with_agents.dropped_files == []
        assert {d.code for d in with_agents.diagnostics} == {
            d.code for d in without.diagnostics
        }

    def test_a_package_of_only_subagents_installs_something(self):
        package = _package({"agents/screener.md": GOOD})
        assert [p.name for p in package.agent_plans] == ["screener"]
        assert "no_components" not in {d.code for d in package.diagnostics}

    def test_an_agents_only_change_is_a_new_version_of_the_tree(self):
        # The hash is what tells the listing an installed plugin is behind.
        before = _package({"agents/screener.md": GOOD})
        after = _package({"agents/screener.md": GOOD + b"Also check volume.\n"})
        assert before.content_hash != after.content_hash

    @pytest.mark.parametrize(
        ("members", "agents", "skills"),
        [
            pytest.param(
                {"skills/digest/SKILL.md": SKILL, "agents/screener.md": GOOD},
                ["screener"], ["digest"], id="beside-skills",
            ),
            # A README beside it, as a repository has: a lone top folder is
            # read as an archive's wrapper and stripped.
            pytest.param(
                {"README.md": b"# kit\n", "agents/screener.md": GOOD}, ["screener"], [],
                id="agents-only",
            ),
            # A SKILL.md at the top makes the whole tree one skill, its agents/ too.
            pytest.param(
                {"SKILL.md": SKILL, "agents/screener.md": GOOD}, [], ["digest"], id="root-skill",
            ),
        ],
    )
    def test_a_tree_with_no_manifest_brings_its_top_agents_folder(self, members, agents, skills):
        # Claude Code reads agents/ with no manifest, so a plugin published
        # that way must not lose its subagents here.
        package = validate_package(zip_package(members), name_hint="Screen Kit")
        assert [p.name for p in package.agent_plans] == agents
        assert [p.dir for p in package.skill_plans] == skills
        assert package.diagnostics[0].code == "synthesized_manifest"

    @pytest.mark.parametrize(
        ("members", "warned"),
        [
            pytest.param({"SKILL.md": SKILL, "agents/screener.md": GOOD}, True, id="root-skill-with-agents"),
            pytest.param({"skills/digest/SKILL.md": SKILL, "agents/screener.md": GOOD}, False, id="no-root-skill"),
            pytest.param({"SKILL.md": SKILL, "agents/notes/screener.md": GOOD}, False, id="agents-only-nested"),
            pytest.param({"SKILL.md": SKILL, "agents/README.txt": b"x"}, False, id="no-agent-markdown"),
        ],
    )
    def test_a_root_skill_that_swallows_the_agents_folder_says_so(self, members, warned):
        package = validate_package(zip_package(members), name_hint="Screen Kit")
        found = [d for d in package.diagnostics if d.code == "agents_in_skill"]
        assert [(d.level, d.scope, d.message) for d in found] == (
            [(
                "warning", "plugin",
                "a SKILL.md at the top makes this package one skill, so its agents/ folder stays "
                "inside that skill and none of it installs as a subagent",
            )]
            if warned else []
        )

    def test_a_zip_of_only_the_agents_folder_installs_as_a_manifestless_plugin(self):
        package = validate_package(zip_package({"agents/x.md": GOOD}), name_hint="Screen Kit")
        assert package.synthesized
        assert [p.name for p in package.agent_plans] == ["x"]

    def test_a_stray_file_in_a_zipped_agents_folder_costs_only_itself(self):
        package = validate_package(
            zip_package({"agents/x.md": GOOD, "agents/README.txt": b"notes"}),
            name_hint="Screen Kit",
        )
        assert [p.name for p in package.agent_plans] == ["x"]
        assert [(d.target, d.code) for d in package.diagnostics if d.code == "loose_file"] == [
            ("agents/README.txt", "loose_file"),
        ]


@pytest.mark.asyncio
async def test_export_ships_each_file_as_installed_and_reads_back_the_same():
    shipped = {
        # A key we ignore, and line endings we normalize when reading: the
        # export is the file, not a rendering of the values read from it.
        "agents/reviewer.md": (
            b"---\r\ndescription: Reviews a change.\r\ncolor: blue\r\n"
            b"tools: Read, Bash\r\n---\r\nYou review code.\r\n"
        ),
        "agents/screener.md": GOOD,
    }
    package = _package(shipped)
    rows = [{"name": p.name, "file_text": p.text} for p in package.agent_plans]
    listed = AsyncMock(return_value=rows)
    with patch(
        "src.server.services.plugins.export.list_plugin_skill_names",
        new=AsyncMock(return_value=[]),
    ), patch("src.server.services.plugins.export.list_plugin_subagents", new=listed):
        exported = await export_plugin_zip(USER, {
            "user_plugin_id": PLUGIN_ID,
            "manifest": package.manifest,
            "mcp_document": None,
        })

    listed.assert_awaited_once_with(USER, PLUGIN_ID)
    with zipfile.ZipFile(io.BytesIO(exported)) as zf:
        assert {n: zf.read(n) for n in zf.namelist() if n.startswith("agents/")} == shipped
    again = validate_package(exported)
    assert {p.name: (p.text, p.row) for p in again.agent_plans} == {
        p.name: (p.text, p.row) for p in package.agent_plans
    }


def test_a_lone_surrogate_skips_the_agent_with_its_own_reason():
    plan = read_agent("lone", b'---\ndescription: "A\\ud800B"\n---\nBody.\n')

    assert plan.skip_code == "unreadable"
    assert plan.skip_reason == (
        "the file holds an unpaired surrogate (\\ud800 to \\udfff), which can't be saved"
    )


def test_nested_aliases_read_in_one_pass():
    # Nine levels of nine aliases: 9**9 strings walked as a tree.
    levels = ['l0: &l0 ["x","x","x","x","x","x","x","x","x"]'] + [
        f"l{i}: &l{i} [" + ",".join([f"*l{i - 1}"] * 9) + "]" for i in range(1, 9)
    ]
    plan = read_agent("bomb", _agent("description: Does a thing.\n" + "\n".join(levels)))

    assert plan.skip_code is None
