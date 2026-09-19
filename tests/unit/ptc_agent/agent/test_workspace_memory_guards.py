"""Every guard that names the workspace memory tier agrees with the layout.

Workspace memory lives in the turn's own folder, at `.agents/memory/`, and five
surfaces gate that path: the bash and execute_code refusals, the provenance
classifier, the workflow script reader, and the context-file cap. A guard that
does not recognise it fails open -- bash would silently write memory onto the
sandbox disk where nothing reads it -- so each one is pinned here against
`WorkspaceLayout.MEMORY_DIR`, the constant they all derive the path from.

There is one spelling. The computer-level `.agents/workspace/memory/` the tier
used to sit at is gone: no guard honours it, the layout migration deletes the
mirror rather than carrying it forward, and the prompt surface bans the string.
"""

from __future__ import annotations

from ptc_agent.agent.middleware.background_subagent.workflow.tool import (
    _UNREADABLE_SCRIPT_PREFIXES,
)
from ptc_agent.agent.middleware.provenance.middleware import (
    _classify_file_source_type,
)
from ptc_agent.agent.tools.bash import _command_touches_memory
from ptc_agent.agent.tools.code_execution import _code_touches_memory
from ptc_agent.agent.tools.context_file_policy import (
    MAX_MEMORY_BLOCK_SIZE,
    capped_file,
)
from ptc_agent.core.paths import MEMORY_INDEX_FILENAME, WorkspaceLayout

WORKSPACE_MEMORY = WorkspaceLayout.MEMORY_DIR
ROOT = "/home/workspace"
FOLDER = f"{ROOT}/acme-a1b2"


def test_the_workspace_tier_is_where_the_layout_says():
    assert WORKSPACE_MEMORY == ".agents/memory"


class TestTheWorkspaceTierIsGuarded:
    def test_bash_refuses_the_path(self):
        assert _command_touches_memory(f"cat {WORKSPACE_MEMORY}/memory.md")

    def test_execute_code_refuses_the_path(self):
        assert _code_touches_memory(
            f"open('{WORKSPACE_MEMORY}/memory.md').read()"
        )

    def test_provenance_classifies_a_read_as_memory(self):
        classified = _classify_file_source_type(f"{WORKSPACE_MEMORY}/memory.md")
        assert classified == "memory_read"

    def test_a_workflow_script_may_not_be_read_from_the_path(self):
        assert any(
            f"{WORKSPACE_MEMORY}/x.py".startswith(p)
            for p in _UNREADABLE_SCRIPT_PREFIXES
        )

    def test_the_memory_index_is_capped(self):
        capped = capped_file(
            f"{FOLDER}/{WORKSPACE_MEMORY}/{MEMORY_INDEX_FILENAME}",
            workspace_dir=FOLDER,
            computer_root=ROOT,
        )
        assert capped is not None
        assert capped.cap == MAX_MEMORY_BLOCK_SIZE


class TestTheGuardsStayNarrow:
    def test_an_ordinary_agents_path_is_not_refused(self):
        assert not _command_touches_memory("ls .agents/skills/pdf")

    def test_a_work_file_is_not_capped(self):
        assert (
            capped_file(
                f"{FOLDER}/work/analysis/notes.md",
                workspace_dir=FOLDER,
                computer_root=ROOT,
            )
            is None
        )

    def test_a_work_read_is_not_classified_as_memory(self):
        assert _classify_file_source_type("work/out.csv") == "file_read"
