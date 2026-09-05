"""A plugin says how its servers read; the installed rows say the same thing.

``mcp.json`` is closed at every level, so a package has nowhere in the portable
document to put a server's description, its usage instruction, or how much of
its tool surface the agent should see. Those go in ``plugin.json`` under
``extensions["ai.langalpha"].servers``, the one extension point the format
defines, and land on the plan before the row is created. Without this the
server installs wearing our defaults and introduces itself to the agent as
nothing at all.

Nothing here is fatal, for the same reason binds are not: the block is our own
invention and a defect in it is an authoring slip in a field the spec does not
define, while the cost of refusing would be the whole package.
"""

from __future__ import annotations

from src.server.models.mcp_server import DESCRIPTION_MAX, INSTRUCTION_MAX
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.extension import (
    LangalphaExtension,
    apply_server_metadata,
    parse_extension,
)
from src.server.services.plugins.mcp import McpEntryPlan


def _plan(key: str, *, skip: str | None = None) -> McpEntryPlan:
    return McpEntryPlan(
        key=key,
        name=key,
        renamed=False,
        transport="" if skip else "http",
        config={} if skip else {
            "name": key, "transport": "http", "url": "https://example.test",
            "headers": {},
        },
        skip_code=skip,
    )


def _apply(servers: dict, plans: list[McpEntryPlan], **kwargs):
    diagnostics: list[Diagnostic] = []
    apply_server_metadata(
        LangalphaExtension(servers=servers), plans,
        diagnostics=diagnostics, **kwargs,
    )
    return diagnostics


class TestApply:
    def test_declared_fields_land_on_the_plan(self):
        plan = _plan("remote")
        _apply(
            {"remote": {
                "description": "A described server",
                "instruction": "Reach for it when asked",
                "tool_exposure_mode": "detailed",
            }},
            [plan],
        )
        assert plan.config["description"] == "A described server"
        assert plan.config["instruction"] == "Reach for it when asked"
        assert plan.config["tool_exposure_mode"] == "detailed"

    def test_an_undeclared_server_is_left_to_the_defaults(self):
        plan = _plan("remote")
        _apply({}, [plan])
        assert "description" not in plan.config
        assert "tool_exposure_mode" not in plan.config

    def test_an_omitted_exposure_mode_does_not_pin_one(self):
        # None means "we did not say", which must not become the literal
        # "summary" and override whatever the row would otherwise choose.
        plan = _plan("remote")
        _apply({"remote": {"description": "d"}}, [plan])
        assert "tool_exposure_mode" not in plan.config

    def test_keyed_by_the_manifest_key_not_the_installed_name(self):
        plan = McpEntryPlan(
            key="my-server", name="my_server", renamed=True, transport="http",
            config={"name": "my_server", "transport": "http",
                    "url": "https://example.test", "headers": {}},
        )
        _apply({"my-server": {"description": "found by key"}}, [plan])
        assert plan.config["description"] == "found by key"


class TestDefectsAreNotFatal:
    def test_over_long_text_is_clipped_and_reported(self):
        plan = _plan("remote")
        diagnostics = _apply(
            {"remote": {
                "description": "d" * (DESCRIPTION_MAX + 1),
                "instruction": "i" * (INSTRUCTION_MAX + 1),
            }},
            [plan],
        )
        assert len(plan.config["description"]) == DESCRIPTION_MAX
        assert len(plan.config["instruction"]) == INSTRUCTION_MAX
        assert [d.code for d in diagnostics] == [
            "server_meta_clipped", "server_meta_clipped"
        ]

    def test_describing_a_server_that_is_not_there_is_a_warning(self):
        diagnostics = _apply({"ghost": {"description": "d"}}, [_plan("remote")])
        assert [(d.level, d.code, d.target) for d in diagnostics] == [
            ("warning", "server_meta_unknown", "ghost")
        ]

    def test_a_dropped_document_does_not_report_every_entry(self):
        # There are no plans to name and the reason is not the meta block's;
        # one diagnostic already explains the drop.
        assert _apply(
            {"ghost": {"description": "d"}}, [], document_dropped=True
        ) == []

    def test_a_skipped_entry_is_left_alone(self):
        plan = _plan("remote", skip="plugin_tree_unsupported")
        _apply({"remote": {"description": "d"}}, [plan])
        assert plan.config == {}


class TestNamespaceParsing:
    def test_bundle_only_keys_do_not_cost_the_rest_of_the_namespace(self):
        # One model describes the whole namespace, so a package carrying the
        # keys our own bundles use keeps its secrets instead of failing
        # extra=forbid and losing every declaration with them.
        diagnostics: list[Diagnostic] = []
        extension = parse_extension(
            {
                "icon": "vendor.example.com",
                "skills": ["some-skill"],
                "secrets": [{"name": "PROBE_TOKEN", "label": "Probe token"}],
                "servers": {"remote": {"tool_exposure_mode": "detailed"}},
            },
            diagnostics,
        )
        assert diagnostics == []
        assert [s.name for s in extension.secrets] == ["PROBE_TOKEN"]
        assert extension.servers["remote"].tool_exposure_mode == "detailed"

    def test_an_unknown_key_still_costs_the_namespace(self):
        diagnostics: list[Diagnostic] = []
        extension = parse_extension({"nonsense": 1}, diagnostics)
        assert [d.code for d in diagnostics] == ["extension_invalid"]
        assert extension.servers == {}
