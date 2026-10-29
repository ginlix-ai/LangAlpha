"""GetPreviewUrl files its runs under the sandbox provider and channel only when
it can reach the sandbox."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.agent.tools.preview_url import create_preview_url_tool


@pytest.mark.parametrize(
    ("workspace_id", "metadata"),
    [("ws-1", {"tool_provider": "docker", "tool_channel": "docker"}), ("", None)],
)
def test_a_preview_names_the_sandbox_provider_only_with_a_workspace(workspace_id, metadata):
    tool = create_preview_url_tool(
        SimpleNamespace(provider="docker", channel="docker"), workspace_id=workspace_id
    )

    assert tool.metadata == metadata
