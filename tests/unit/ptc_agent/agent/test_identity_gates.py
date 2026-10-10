"""Identity gating: no user identity, no user-scoped store surface.

Memory, memo and the workflow store all namespace on the user id. A build
without one must disable them outright rather than fall back to a shared
namespace that would cross-pollinate unauthenticated sessions.
"""

from types import SimpleNamespace

import pytest

from ptc_agent.agent.filesystem_routes import resolve_identity_gates


@pytest.fixture
def workflows(monkeypatch):
    """Pin the workflow feature flag so the identity axes read independently."""

    def _set(enabled: bool) -> None:
        import src.config.settings as settings

        monkeypatch.setattr(
            settings,
            "get_workflow_orchestration_config",
            lambda: SimpleNamespace(enabled=enabled),
        )

    return _set


def test_no_user_id_closes_every_user_scoped_surface(workflows):
    workflows(True)

    gates = resolve_identity_gates(
        store=object(),
        user_id=None,
        workspace_id="ws-1",
        disable_subagents=False,
    )

    assert not gates.memory
    assert not gates.user_memory
    assert not gates.workspace_memory
    assert not gates.memo
    assert not gates.user_data
    assert not gates.workflow_fs


def test_no_store_closes_the_store_routes_but_not_the_user_data_backend(workflows):
    """The user-profile and automations backends read the application DB, not
    the LangGraph store, so they are gated on identity alone."""
    workflows(True)

    gates = resolve_identity_gates(
        store=None,
        user_id="user-1",
        workspace_id="ws-1",
        disable_subagents=False,
    )

    assert not gates.memory
    assert not gates.memo
    assert not gates.workflow_fs
    assert gates.user_data


def test_workspace_memory_needs_a_workspace_as_well_as_a_user(workflows):
    workflows(False)

    gates = resolve_identity_gates(
        store=object(),
        user_id="user-1",
        workspace_id=None,
        disable_subagents=False,
    )

    assert gates.user_memory
    assert not gates.workspace_memory
    # Either tier alone is enough to inject a memory block.
    assert gates.memory


def test_the_recursion_gate_drops_the_tool_but_not_the_workflow_filesystem(workflows):
    """A notification turn may not dispatch subagents, so RunWorkflow drops with
    them — but the workflow directory stays mounted and readable."""
    workflows(True)

    gates = resolve_identity_gates(
        store=object(),
        user_id="user-1",
        workspace_id="ws-1",
        disable_subagents=True,
    )

    assert not gates.workflow_tool
    assert gates.workflow
    assert gates.workflow_fs


def test_the_workflow_flag_closes_both_the_tool_and_the_filesystem(workflows):
    workflows(False)

    gates = resolve_identity_gates(
        store=object(),
        user_id="user-1",
        workspace_id="ws-1",
        disable_subagents=False,
    )

    assert not gates.workflow
    assert not gates.workflow_fs
    assert not gates.workflow_tool


@pytest.mark.parametrize(
    ("asked", "user_id", "expected"),
    [
        (True, "user-1", True),
        (True, None, False),
        (True, "", False),
        (False, "user-1", False),
    ],
)
def test_channels_need_the_ask_and_a_user(workflows, asked, user_id, expected):
    """The chat-app settings folder is asked for by a build whose deployment
    has the channel gateway, and holds one user's settings."""
    workflows(False)

    gates = resolve_identity_gates(
        store=None,
        user_id=user_id,
        workspace_id=None,
        disable_subagents=False,
        channels=asked,
    )

    assert gates.channels is expected


def test_channels_are_off_unless_asked_for(workflows):
    """The file mount builds its routes without asking, which keeps the
    folder off it."""
    workflows(False)

    gates = resolve_identity_gates(
        store=object(), user_id="user-1", workspace_id="ws-1", disable_subagents=False
    )

    assert gates.channels is False
