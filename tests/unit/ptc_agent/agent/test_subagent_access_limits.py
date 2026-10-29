"""A subagent's own search tool, its allowed connector set, and the tool guide that set leaves it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.agent.context_stack import SubagentContext
from ptc_agent.agent.subagents.compiler import SubagentCompiler
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.config import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.mcp_schema import MCPToolInfo
from src.server.services.brokerages import BROKERAGES
from tests.unit.ptc_agent.agent._subagent_baseline import baseline_text

# A connection is a brokerage by its address, so the fixture needs a real
# vendor host; the name stays neutral to show the name plays no part.
BROKER_URL = BROKERAGES[0].url
SERVERS = {"srv_market": None, "srv_notes": "https://example.com/mcp", "srv_broker": BROKER_URL}


def _config() -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(name="main-placeholder"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        search_api="alpha",
        search_depth="standard",
    )


class _Registry:
    """The two reads ``read_registry_tools`` makes of a turn's MCP registry."""

    def __init__(self, servers: dict[str, str | None]) -> None:
        self._servers = servers
        self.config = SimpleNamespace(
            mcp=SimpleNamespace(
                servers=[
                    MCPServerConfig(name=n, transport="http" if url else "stdio", url=url)
                    for n, url in servers.items()
                ]
            )
        )

    def get_all_tools(self):
        return {
            n: [MCPToolInfo(name="lookup", description="Look up.", input_schema={}, server_name=n)]
            for n in self._servers
        }


def _tool(name: str, **extra):
    return SimpleNamespace(name=name, **extra)


MAIN = {
    "web_search": _tool("web_search"),
    "web_fetch": _tool("web_fetch"),
    "execute_code": _tool("execute_code"),
    "bash": _tool("bash"),
}


class _Build:
    """A compiler wired the way ``agent.py`` wires it, recording each tool it makes."""

    def __init__(self, registry=None) -> None:
        self.searches: list[tuple[str, str]] = []
        self.limits: list[frozenset[str]] = []
        self.compiler = SubagentCompiler(
            config=_config(),
            context=SubagentContext(),
            mcp_registry=registry,
            tool_sets={
                "web_search": [MAIN["web_search"], MAIN["web_fetch"]],
                "execute_code": [MAIN["execute_code"]],
                "bash": [MAIN["bash"]],
            },
            search_tool=self._search,
            sandbox_tools=self._sandbox,
        )

    def _search(self, *, provider, depth):
        self.searches.append((provider, depth))
        return _tool("web_search", pair=(provider, depth))

    def _sandbox(self, allowed):
        self.limits.append(allowed)
        return [_tool("execute_code", allowed=allowed), _tool("bash", allowed=allowed)]

    def tools(self, defn: SubagentDefinition) -> dict[str, object]:
        return {t.name: t for t in self.compiler.compile(defn)["tools"]}


def _defn(name: str, *, prompt: str | None = "stub prompt", **fields) -> SubagentDefinition:
    fields.setdefault("tools", ["web_search", "execute_code", "bash"])
    return SubagentDefinition(
        name=name,
        description="placeholder",
        custom_prompt=prompt,
        role_prompt="Do the task.",
        **fields,
    )


class TestSearchTool:
    def test_one_tool_per_distinct_pair_and_the_main_one_otherwise(self):
        build = _Build()
        tools = {
            name: build.tools(_defn(name, search_provider=p, search_depth=d))
            for name, (p, d) in {
                "inherits": (None, None),
                "names_main": ("alpha", "standard"),
                "own": ("beta", "standard"),
                "own_again": ("beta", None),
                "own_depth": (None, "fast"),
            }.items()
        }
        build.tools(_defn("offline", search_provider="beta", search_depth="deep", tools=["bash"]))

        assert build.searches == [("beta", "standard"), ("alpha", "fast")]
        assert tools["inherits"]["web_search"] is MAIN["web_search"]
        assert tools["names_main"]["web_search"] is MAIN["web_search"]
        assert tools["own"]["web_search"].pair == ("beta", "standard")
        assert tools["own_again"]["web_search"] is tools["own"]["web_search"]
        assert tools["own_depth"]["web_search"].pair == ("alpha", "fast")
        # Only the search tool is swapped; the rest of its tool set is shared.
        assert all(t["web_fetch"] is MAIN["web_fetch"] for t in tools.values())


class TestAllowedServers:
    @pytest.mark.parametrize(
        ("connectors", "brokerages", "allowed"),
        [
            (True, "on", None),
            (False, "on", {"srv_broker"}),
            (True, "off", {"srv_market", "srv_notes"}),
            (False, "off", set()),
        ],
    )
    def test_the_switches_map_to_a_server_set(self, connectors, brokerages, allowed):
        build = _Build(_Registry(SERVERS))
        tools = build.tools(_defn("limited", connectors=connectors, brokerages=brokerages))

        if allowed is None:
            assert build.limits == []
            assert tools["execute_code"] is MAIN["execute_code"]
            assert tools["bash"] is MAIN["bash"]
        else:
            assert build.limits == [frozenset(allowed)]
            assert tools["execute_code"].allowed == frozenset(allowed)
            assert tools["bash"].allowed == frozenset(allowed)

    @pytest.mark.parametrize(
        ("registry", "connectors", "brokerages", "allowed"),
        [
            # No brokerage connected: brokerages off still restricts, to the rest.
            (_Registry({"srv_market": None}), True, "off", {"srv_market"}),
            (_Registry({"srv_market": None}), False, "on", set()),
            (None, False, "on", set()),
            (None, True, "off", set()),
        ],
        ids=["no-broker-off", "no-broker-connectors-off", "no-registry-a", "no-registry-b"],
    )
    def test_a_switch_off_restricts_even_with_no_server_of_its_kind(
        self, registry, connectors, brokerages, allowed
    ):
        """A limit is a promise about what the code may reach, so it holds
        when the workspace happens to have nothing for it to exclude."""
        build = _Build(registry)
        build.tools(_defn("limited", connectors=connectors, brokerages=brokerages))
        assert build.limits == [frozenset(allowed)]

    def test_one_code_and_shell_pair_per_distinct_set(self):
        build = _Build(_Registry(SERVERS))
        first = build.tools(_defn("a", connectors=False, brokerages="off"))
        second = build.tools(_defn("b", connectors=False, brokerages="off"))
        third = build.tools(_defn("c", connectors=True, brokerages="off"))

        assert build.limits == [frozenset(), frozenset({"srv_market", "srv_notes"})]
        assert second["execute_code"] is first["execute_code"]
        assert second["bash"] is first["bash"]
        assert third["execute_code"] is not first["execute_code"]

    @pytest.mark.parametrize(("connectors", "brokerages"), [(True, "on"), (False, "on"), (True, "off")])
    def test_a_shell_without_code_reaches_no_connector(self, connectors, brokerages):
        """The settings put connectors and brokerages under Code, so a shell
        that could start Python gets none of them either."""
        build = _Build(_Registry(SERVERS))
        tools = build.tools(_defn("shell", connectors=connectors, brokerages=brokerages, tools=["bash"]))
        assert build.limits == [frozenset()]
        assert tools["bash"].allowed == frozenset()

    def test_no_pair_is_built_for_a_subagent_without_code_or_shell(self):
        build = _Build(_Registry(SERVERS))
        build.tools(_defn("reader", connectors=False, tools=["web_search"]))
        assert build.limits == []


def _compiled(registry, **fields) -> dict:
    return _Build(registry).compiler.compile(_defn("guided", prompt=None, **fields))


def _prompt(registry, **fields) -> str:
    return _compiled(registry, **fields)["system_prompt"]


def _guide(registry, **fields) -> tuple[str, str]:
    """The prompt, and the roster its baseline states in <mcp-servers>."""
    compiled = _compiled(registry, **fields)
    return compiled["system_prompt"], baseline_text(compiled)


class TestToolGuide:
    def test_it_lists_only_the_allowed_servers(self):
        prompt, roster = _guide(_Registry(SERVERS), brokerages="off")
        assert "<tool_guide>" in prompt
        assert "srv_market" in roster and "srv_notes" in roster
        assert "srv_broker" not in roster

    def test_an_unrestricted_subagent_lists_every_server(self):
        _, roster = _guide(_Registry(SERVERS))
        assert all(name in roster for name in SERVERS)

    def test_the_prompt_names_no_server(self):
        prompt, _ = _guide(_Registry(SERVERS))
        assert not any(name in prompt for name in SERVERS)

    @pytest.mark.parametrize(
        ("registry", "connectors", "brokerages"),
        [
            (_Registry(SERVERS), False, "off"),
            (_Registry({"srv_market": None}), False, "on"),
            (None, True, "off"),
        ],
        ids=["all-off", "only-kind-off", "no-registry"],
    )
    def test_no_guide_when_no_connector_is_left(self, registry, connectors, brokerages):
        prompt, baseline = _guide(registry, connectors=connectors, brokerages=brokerages)
        assert "<tool_guide>" not in prompt
        assert "<mcp-servers>" not in baseline
        assert not any(name in prompt + baseline for name in SERVERS)

    @pytest.mark.parametrize("tools", [["bash"], ["web_search", "filesystem"]], ids=["shell", "no-code"])
    def test_no_guide_without_code(self, tools):
        """Code is how a subagent reaches a connector, so without it there is
        none to list, whatever its switches say."""
        prompt, baseline = _guide(_Registry(SERVERS), tools=tools)
        assert "<tool_guide>" not in prompt
        assert "<mcp-servers>" not in baseline

    def test_preferred_servers_lead_and_never_limit(self):
        _, roster = _guide(_Registry(SERVERS), preferred_connectors=["srv_notes"])
        headers = [roster.index(f"\n{name}:") for name in ("srv_notes", "srv_market", "srv_broker")]
        assert headers == sorted(headers)
