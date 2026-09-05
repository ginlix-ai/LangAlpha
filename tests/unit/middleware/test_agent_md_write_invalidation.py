"""Every spelling of agent.md's path must invalidate the session cache.

The agent is shown these files at the workspace root and writes them as
"/agent.md" as readily as "agent.md". Only the bare form used to match, so an
edit through the absolute path left the cache holding the pre-edit text: the
rest of the turn read the old front matter, the reconcile filed no change, and
an agent rename never reached the workspace row.
"""

import pytest

from ptc_agent.agent.middleware.file_operations.sse_middleware import FileOperationMiddleware
from ptc_agent.core.paths import workspace_relative_path


def _normalize(path: str, work_dir: str = "/home/workspace") -> str:
    """Fold a path exactly as the middleware does, via its own helper.

    Reached through the middleware's configured work_dir rather than a literal
    so a change to how that is stored shows up here too.
    """
    mw = FileOperationMiddleware(on_agent_md_write=lambda: None, work_dir=work_dir)
    return workspace_relative_path(path, mw._work_dir)


@pytest.mark.parametrize(
    "path",
    [
        "agent.md",
        "/agent.md",
        "./agent.md",
        "/home/workspace/agent.md",
    ],
)
def test_every_spelling_of_the_workspace_root_file_matches(path):
    assert _normalize(path) == "agent.md"


@pytest.mark.parametrize(
    "path",
    [
        "notes/agent.md",
        "/etc/agent.md",
        "agent.md.bak",
    ],
)
def test_a_different_file_does_not_match(path):
    assert _normalize(path) != "agent.md"


def test_a_dotted_directory_keeps_its_leading_dot():
    # removeprefix("./") rather than lstrip(".") — lstrip would eat the dot
    # that makes this a hidden directory.
    assert _normalize(".agents/user/memory/memory.md") == ".agents/user/memory/memory.md"


def test_a_directory_mirroring_the_work_dir_keeps_its_segments():
    # removeprefix, not replace: replace strips every occurrence, so a
    # directory named after the work dir had its name spliced out of the
    # middle and ".../mirror/home/workspace/c.png" became "mirrorc.png".
    assert (
        _normalize("/home/workspace/mirror/home/workspace/c.png")
        == "mirror/home/workspace/c.png"
    )
