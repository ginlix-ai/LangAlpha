"""One session per machine, one frozen tool configuration per project on it.

The session's MCP fields are rewritten by whichever sibling resolved last, so a
turn reads the view frozen the moment its own composite was installed.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.server.services.computer_manager import ComputerManager
from src.server.services.computer_manager._types import WorkspaceToolView


def _make_manager():
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider="daytona")
    config.filesystem = SimpleNamespace(working_directory="/home/workspace")
    return ComputerManager.get_instance(config=config)


def _session(owner, *, registry=None, direct=None, egress=None, version=1):
    session = MagicMock()
    session.computer_id = "comp-1"
    session.mcp_config_workspace_id = owner
    session.mcp_registry = registry or MagicMock(name=f"registry-{owner}")
    session._builtin_mcp_registry = MagicMock(name="builtin")
    session.mcp_tool_summary = f"summary-{owner}"
    session.direct_mcp_tools = direct if direct is not None else {owner: ["t"]}
    session.egress_binding = egress
    session.mcp_config_version = version
    return session


class TestToolView:
    def setup_method(self):
        ComputerManager.reset_instance()

    def teardown_method(self):
        ComputerManager.reset_instance()

    def test_a_view_survives_a_siblings_install(self):
        manager = _make_manager()
        session = _session("ws-a")
        view_a = manager._freeze_tool_view("comp-1", "ws-a", session)
        registry_a = session.mcp_registry

        # ws-b's composite lands on the same session.
        session.mcp_config_workspace_id = "ws-b"
        session.mcp_registry = MagicMock(name="registry-b")
        session.direct_mcp_tools = {"ws-b": ["u"]}
        session.egress_binding = object()
        manager._freeze_tool_view("comp-1", "ws-b", session)

        assert manager.tool_view(session, "ws-a") is view_a
        assert view_a.mcp_registry is registry_a
        assert dict(view_a.direct_mcp_tools) == {"ws-a": ["t"]}
        assert view_a.egress_binding is None
        assert view_a.mcp_tool_summary == "summary-ws-a"

    def test_the_direct_tools_copy_is_detached_and_read_only(self):
        manager = _make_manager()
        session = _session("ws-a", direct={"srv": ["t1"]})
        view = manager._freeze_tool_view("comp-1", "ws-a", session)
        session.direct_mcp_tools["srv"] = ["changed"]
        assert dict(view.direct_mcp_tools) == {"srv": ["t1"]}
        with pytest.raises(TypeError):
            view.direct_mcp_tools["x"] = 1  # type: ignore[index]

    def test_a_session_carrying_a_sibling_freezes_nothing(self):
        manager = _make_manager()
        session = _session("ws-b")
        assert manager._freeze_tool_view("comp-1", "ws-a", session) is None
        assert manager._machine("comp-1").tool_views == {}

    def test_a_project_without_a_view_gets_the_builtins_only(self):
        manager = _make_manager()
        session = _session("ws-b")
        view = manager.tool_view(session, "ws-a")
        assert isinstance(view, WorkspaceToolView)
        assert view.mcp_registry is session._builtin_mcp_registry
        assert dict(view.direct_mcp_tools) == {}
        assert view.egress_binding is None
        assert view.mcp_tool_summary is None

    def test_tool_view_freezes_on_demand_when_the_session_matches(self):
        manager = _make_manager()
        session = _session("ws-a")
        view = manager.tool_view(session, "ws-a")
        assert view.mcp_registry is session.mcp_registry
        assert manager._machine("comp-1").tool_views["ws-a"] is view

    def test_a_view_from_a_retired_session_is_not_reused(self):
        manager = _make_manager()
        old = _session("ws-a")
        manager._freeze_tool_view("comp-1", "ws-a", old)
        new = _session("ws-a")
        view = manager.tool_view(new, "ws-a")
        assert view.session is new
        assert view.mcp_registry is new.mcp_registry

    def test_forgetting_the_session_drops_every_view(self):
        manager = _make_manager()
        manager._freeze_tool_view("comp-1", "ws-a", _session("ws-a"))
        manager._machine("comp-1").forget_session()
        assert manager._machine("comp-1").tool_views == {}

    def test_the_workspace_view_cache_evicts_the_least_recently_used_entry(self):
        manager = _make_manager()
        session = _session("ws-0")
        manager._freeze_tool_view("comp-1", "ws-0", session)
        for index in range(1, 64):
            workspace_id = f"ws-{index}"
            session.mcp_config_workspace_id = workspace_id
            manager._freeze_tool_view("comp-1", workspace_id, session)

        manager.tool_view(session, "ws-0")
        session.mcp_config_workspace_id = "ws-64"
        manager._freeze_tool_view("comp-1", "ws-64", session)

        views = manager._machine("comp-1").tool_views
        assert len(views) == 64
        assert "ws-0" in views
        assert "ws-1" not in views
