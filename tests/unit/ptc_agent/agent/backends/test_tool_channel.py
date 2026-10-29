"""The channel a tool call is filed under: how it reached what served it."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.backends.composite import CompositeFilesystemBackend
from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.config.core import SSHConfig
from ptc_agent.core.paths import WorkspaceLayout, on_file_mount
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

ROOT = "/home/workspace"


SPLIT = WorkspaceLayout(ROOT, "Macro")
UNSPLIT = WorkspaceLayout(ROOT, "")


@pytest.mark.parametrize(
    ("path", "workspace", "served"),
    [
        (f"{ROOT}/.agents/threads.jsonl", SPLIT, True),
        (f"{ROOT}/.agents/user/memory/a.md", SPLIT, True),
        (f"{ROOT}/.agents/workflows/w.md", SPLIT, True),
        (f"{ROOT}/.agents/memory/a.md", SPLIT, False),
        (f"{ROOT}/.agents/transcripts/t.jsonl", SPLIT, False),
        (f"{ROOT}/.agents/memory/a.md", UNSPLIT, True),
        (f"{ROOT}/.agents/transcripts/t.jsonl", UNSPLIT, True),
        (f"{ROOT}/Macro/.agents/user/profile/user.json", SPLIT, True),
        (f"{ROOT}/Macro/.agents/transcripts/t.jsonl", SPLIT, True),
        (f"{ROOT}/Macro/.agents/memory", SPLIT, True),
        (f"{ROOT}/Macro/.agents/workflows/w.md", SPLIT, True),
        (f"{ROOT}/Home/.agents/memory/a.md", SPLIT, True),
        (f"{ROOT}/Macro/notes/a.md", SPLIT, False),
        (f"{ROOT}/Macro/.agents/skills/s/SKILL.md", SPLIT, False),
        (f"{ROOT}/Macro/.agents/threads.jsonl", SPLIT, False),
        (f"{ROOT}/Macro/deep/.agents/memory/a.md", SPLIT, False),
        (f"{ROOT}/..agents/user/a.md", SPLIT, False),
        ("/home/.agents/user/memory/a.md", SPLIT, False),
        ("/tmp/a.md", SPLIT, False),
    ],
)
def test_the_mount_serves_its_links_at_the_root_and_in_each_folder(
    path, workspace, served
):
    assert on_file_mount(path, workspace) is served


@pytest.mark.parametrize(("tunnel", "channel"), [("tun-1", "ssh_tunnel"), ("", None)])
def test_an_ssh_machine_reached_through_the_gateway_has_its_own_channel(
    tunnel, channel
):
    provider = SSHProvider(SSHConfig(host="example.com", tunnel=tunnel))

    assert provider.channel == channel


@pytest.mark.parametrize(
    ("own", "channel"), [("ssh_tunnel", "ssh_tunnel"), (None, "e2b")]
)
def test_a_sandbox_names_the_kind_unless_its_provider_says_more(own, channel):
    sandbox = PTCSandbox.__new__(PTCSandbox)
    sandbox.provider = SimpleNamespace(channel=own)
    sandbox.config = SimpleNamespace(sandbox=SimpleNamespace(provider="e2b"))

    assert sandbox.channel == channel


def _backend(*, mounted: bool) -> SandboxBackend:
    sandbox = MagicMock()
    sandbox.channel = "daytona"
    sandbox.livefs = object() if mounted else None
    sandbox.workspace.return_value = SPLIT
    sandbox.normalize_path.side_effect = lambda p: (
        p if p.startswith("/") else f"{ROOT}/Macro/{p}"
    )
    return SandboxBackend(sandbox)


@pytest.mark.parametrize(
    ("path", "mounted", "channel"),
    [
        ("../.agents/threads.jsonl", True, "fuse"),
        (".agents/transcripts/t.jsonl", True, "fuse"),
        ("notes/a.md", True, "daytona"),
        (".agents/transcripts/t.jsonl", False, "daytona"),
    ],
)
def test_a_file_call_names_the_mount_only_while_it_serves(path, mounted, channel):
    assert _backend(mounted=mounted).channel_for(path) == channel


def test_a_file_call_a_route_answers_never_reaches_the_machine():
    route = MagicMock(root_prefix=f"{ROOT}/.agents/user/memory/")
    composite = CompositeFilesystemBackend(
        sandbox=_backend(mounted=True), routes=[route]
    )

    assert composite.channel_for(f"{ROOT}/.agents/user/memory/a.md") == "server"
    assert composite.channel_for(f"{ROOT}/.agents/threads.jsonl") == "fuse"
