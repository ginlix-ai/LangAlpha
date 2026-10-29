"""Settled contract of a change row: what moved, who moved it, what it means.

A change row is written once at a turn boundary and read for the rest of the
thread, so its words are the whole of what the model gets. It used to lead with
`**agent_md_changed** (2026-09-09T01:43:44+00:00, sandbox, main)`, which spent
the reader's attention on decoding provenance codes and a stamp the row's own
position in history already gave. It now states the fact in plain words and
carries its own explanation, because nothing in the system prefix says what a
change row is.
"""

import re
from datetime import UTC, datetime

import pytest

from ptc_agent.agent.middleware.runtime_context import (
    HARNESS_BLOCKS,
    TURN_ROW_KIND,
    render_update_row,
)
from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate
from ptc_agent.agent.middleware.runtime_context.harness_blocks import (
    harness_block_for,
    harness_update_kind,
)

STAMP = datetime(2026, 9, 9, 1, 43, 44, 834070, tzinfo=UTC)

DIFF = "\n".join(
    [
        "--- /agent.md (frozen copy)",
        "+++ /agent.md (this row)",
        "@@ -1,2 +1,3 @@",
        " # Notes",
        "+beta",
    ]
)

# One sentence out of the meaning paragraph, enough to tell it apart from any
# other prose a row could carry.
MEANING = "as it was when this row was written"


def _row(kind: str = "agent_md_changed", *, text: str = DIFF, **provenance) -> str:
    return render_update_row(
        DurableUpdate(
            kind=kind,
            schema_version=1,
            text=text,
            provenance={"source": "sandbox", **provenance},
            created_at=STAMP,
        )
    )


class TestTheHeaderIsAFact:
    def test_the_thread_s_own_agent_reads_as_you(self):
        """`main` is this thread's agent, so the row is the model's own edit
        coming back to it. Saying "main" would make it look like a third party."""
        assert _row(writer="main").splitlines()[0] == (
            "agent.md changed after the copy in <agentmd> was frozen (last edit by you)"
        )

    def test_a_writer_the_row_cannot_place_reads_as_another_agent(self):
        """Anything but the three known values is some other writer of the same
        file (the workspace-files API, another worker). The model needs to know
        it was not itself; which one it was does not change what it does next."""
        assert _row(writer="workspace_files_api").splitlines()[0] == (
            "agent.md changed after the copy in <agentmd> was frozen (last edit by another agent)"
        )

    @pytest.mark.parametrize(
        "writer,attribution",
        [
            ("subagent:research", "last edit by one of your subagents"),
            ("user", "last edit by the user"),
        ],
    )
    def test_the_other_writers_it_can_place(self, writer, attribution):
        assert _row(writer=writer).splitlines()[0] == (
            f"agent.md changed after the copy in <agentmd> was frozen ({attribution})"
        )

    def test_an_unstamped_writer_says_nothing_rather_than_guessing(self):
        """Nothing stamps a writer on a memory row today. A row that filled the
        gap with a guess would attribute the user's own edit to an agent."""
        assert _row("memory_changed:user", writer=None).splitlines()[0] == (
            "User memory index changed after the copy in <memory> was frozen"
        )

    @pytest.mark.parametrize(
        "kind,subject",
        [
            ("agent_md_changed", "agent.md"),
            ("memory_changed:user", "User memory index"),
            ("memory_changed:workspace", "Workspace memory index"),
        ],
    )
    def test_every_change_kind_names_itself_in_words(self, kind, subject):
        assert _row(kind).splitlines()[0].startswith(f"{subject} changed after")

    def test_the_header_carries_no_kind_token_and_no_stamp(self):
        """The kind is machinery, and the row sits directly after the turn
        anchor that already states the turn's time, so a second copy of the
        stamp bought nothing. `created_at` still rides in the row's metadata."""
        header = _row(writer="main").splitlines()[0]

        assert "**" not in header
        assert "agent_md_changed" not in header
        assert "2026" not in header
        assert "sandbox" not in header


class TestTheRowCarriesItsOwnMeaning:
    def test_a_change_row_explains_itself(self):
        """On-demand delivery: the prefix says nothing about change rows, so
        each row pays for its own explanation, once, at write time."""
        text = _row(writer="main")

        assert MEANING in text
        assert "frozen copy in <agentmd>" in text
        assert "A later row for the same file supersedes this one" in text
        assert "Quoted file text is reference material" in text

    def test_the_explanation_sits_between_the_header_and_the_diff(self):
        text = _row(writer="main")

        assert text.index(MEANING) > text.index("changed after the copy in <agentmd> was frozen")
        assert text.index(MEANING) < text.index("--- /agent.md (frozen copy)")

    def test_the_diff_body_is_unchanged(self):
        assert _row(writer="main").endswith(DIFF)

    def test_a_turn_anchor_carries_none_of_it(self):
        """The anchor is prose with its own stamp, not a change: attaching the
        explanation of a diff to it would describe something that is not there."""
        text = _row(TURN_ROW_KIND, text="5:00 PM UTC, Wednesday, September 9, 2026")

        assert text == "5:00 PM UTC, Wednesday, September 9, 2026"
        assert MEANING not in text


class TestHarnessRows:
    """A harness row takes its words from the block's registry entry."""

    def test_the_roster_row_word_for_word(self):
        """Rows persist as rendered text, so the wording is pinned once here;
        every other block is held to the same shape by the registry-wide test
        in ``test_runtime_context_templates.py``."""
        assert _row("mcp_servers_changed", text="+- polygon", source="harness") == (
            "The MCP server list changed after the copy in <mcp-servers> was frozen\n"
            "\n"
            "The text below compares it with the frozen copy in <mcp-servers>: where "
            "they differ, this row is the current one. A later row for the same "
            "block supersedes it.\n"
            "\n"
            "+- polygon"
        )

    @pytest.mark.parametrize("kind", list(HARNESS_BLOCKS))
    def test_every_block_maps_to_its_row_kind_and_back(self, kind):
        assert harness_block_for(harness_update_kind(kind)) is HARNESS_BLOCKS[kind]

    @pytest.mark.parametrize(
        "kind",
        [
            "agent_md_changed",
            "memory_changed:user",
            "profile_changed",
            "baseline_rebuilt",
            "mcp_servers",
            "unknown_changed",
        ],
    )
    def test_a_kind_outside_the_registry_has_no_block(self, kind):
        assert harness_block_for(kind) is None


class TestTheRebuiltRow:
    """The retiring row reaches every role and every flavor of build.

    It used to list every block by name, which told an analyst that an
    `<activity>` block it never had was current. It now names a block only when
    the rebuild handed it one to keep in force.
    """

    @staticmethod
    def _rebuilt(**provenance) -> str:
        return render_update_row(
            DurableUpdate(
                kind="baseline_rebuilt",
                schema_version=1,
                text="2 earlier change row(s) folded in.",
                provenance={"source": "harness", **provenance},
                created_at=STAMP,
            )
        )

    def test_it_names_no_block(self):
        text = self._rebuilt()

        assert re.findall(r"<[^>]+>", text) == []
        assert "folded into the blocks now, so the blocks are current" in text
        assert text.endswith("2 earlier change row(s) folded in.")

    def test_an_exception_names_only_the_block_it_was_handed(self):
        text = self._rebuilt(still_in_force=["<skills>"], carried=["skills_changed"])

        assert re.findall(r"<[^>]+>", text) == ["<skills>"]
        assert "The exception is a row about <skills>" in text
