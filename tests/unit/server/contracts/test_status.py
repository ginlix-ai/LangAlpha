"""Locks the canonical run-status vocabulary (F14).

TERMINAL_STATUSES is the one internal terminal set: both run ledgers import
it, and migration 020's CHECK constraint must enumerate exactly the same
outcomes (plus the live 'in_progress'). The CHECK-binding test parses the
migration SQL so drift between the Python constant and the schema fails the
default unit suite — no live database required.
"""

import re
from pathlib import Path

from langgraph.types import Interrupt

from src.server.contracts.status import (
    INTERRUPT_REASONS,
    LIVE_PUBLIC_STATUSES,
    PUBLIC_STATUSES,
    RAW_LIVE_STATUSES,
    RAW_TERMINAL_SNAPSHOT_STATUSES,
    TERMINAL_PUBLIC_STATUSES,
    TERMINAL_STATUSES,
    classify_interrupt_reason,
    is_live,
    is_terminal,
    to_public,
)

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[4] / "migrations" / "versions"
)


class TestTerminalSet:
    def test_terminal_statuses_members(self):
        assert set(TERMINAL_STATUSES) == {
            "completed",
            "interrupted",
            "error",
            "cancelled",
        }

    def test_ledgers_share_the_constant(self):
        from src.server.database.runs import subagent_runs as sr_db
        from src.server.database.runs import lifecycle as tl_db

        assert tl_db.TERMINAL_STATUSES is TERMINAL_STATUSES
        assert sr_db.TERMINAL_STATUSES is TERMINAL_STATUSES


class TestPredicates:
    def test_is_terminal(self):
        for status in TERMINAL_STATUSES:
            assert is_terminal(status)
        assert not is_terminal("in_progress")
        assert not is_terminal("active")
        assert not is_terminal(None)
        assert not is_terminal("bogus")

    def test_is_live(self):
        assert is_live("in_progress")
        assert is_live("active")
        for status in TERMINAL_STATUSES:
            assert not is_live(status)
        assert not is_live(None)

    def test_enum_like_values_unwrap(self):
        class Raw:
            value = "interrupted"

        assert is_terminal(Raw())
        assert not is_live(Raw())

    def test_every_terminal_maps_into_public_vocabulary(self):
        # 'error' -> 'failed'; the rest pass through unchanged.
        assert {to_public(s) for s in TERMINAL_STATUSES} == {
            "completed",
            "interrupted",
            "failed",
            "cancelled",
        }


class TestPartitions:
    """The classification tuples must stay a partition of their vocabulary.

    The feed classifies twice — in SQL (RAW_* IN-lists, threads_read /
    threads_write) and in Python (`to_public` + TERMINAL_PUBLIC_STATUSES,
    thread_lifecycle). These pins make the two agree BY CONTRACT: a raw
    status added to one family and not the other, or a raw→public mapping
    that crosses families, fails here instead of drifting silently.
    """

    def test_raw_tuples_partition_the_run_row_vocabulary(self):
        raw_live = set(RAW_LIVE_STATUSES)
        raw_terminal = set(RAW_TERMINAL_SNAPSHOT_STATUSES)
        assert raw_live.isdisjoint(raw_terminal)
        assert raw_live | raw_terminal == {"in_progress", *TERMINAL_STATUSES}

    def test_public_vocabulary_partitions_into_families(self):
        live = set(LIVE_PUBLIC_STATUSES)
        terminal = set(TERMINAL_PUBLIC_STATUSES)
        assert live.isdisjoint(terminal)
        assert live | terminal | {"idle", "interrupted"} == PUBLIC_STATUSES

    def test_raw_families_map_into_matching_public_families(self):
        # SQL's `live` branch must agree with what the Python projection
        # would call live (or awaiting input); ditto terminal → unseen.
        for status in RAW_LIVE_STATUSES:
            assert to_public(status) in {*LIVE_PUBLIC_STATUSES, "interrupted"}
        for status in RAW_TERMINAL_SNAPSHOT_STATUSES:
            assert to_public(status) in TERMINAL_PUBLIC_STATUSES


def _pause(*requests):
    """One interrupt payload, in the shape the graph actually raises.

    A real ``Interrupt`` and not its dict form: that is what every caller hands
    the classifier, and it is the shape the checkpointer's msgpack serde
    reconstructs on the recovery path.
    """
    return [Interrupt(value={"action_requests": list(requests)})]


class TestInterruptReason:
    """The reason column is the run ledger's classification of a pause.

    It is read back by the resume query, which selects on one exact spelling,
    and relayed on the lifecycle feed, so a wrong value strands stopped tasks
    and tells the client the wrong thing about a live thread. What is pinned
    here is that the classifier never invents specificity it does not have."""

    def test_credit_pause_keeps_its_exact_spelling(self):
        # Load-bearing, not cosmetic: the subagent resume query selects on this
        # literal, so a rename here silently strands stopped tasks.
        assert classify_interrupt_reason(
            _pause({"type": "credit_pause", "message": "out of credits"})
        ) == "credit_pause"

    def test_a_question_reads_as_a_question(self):
        assert classify_interrupt_reason(
            _pause({"type": "ask_user_question", "question": "which ticker?"})
        ) == "user_question"

    def test_only_an_actual_plan_claims_plan_review(self):
        assert classify_interrupt_reason(
            _pause({"name": "SubmitPlan", "args": {}, "description": "a plan"})
        ) == "plan_review_required"

    def test_a_proposal_is_an_approval_not_a_plan_review(self):
        # The regression this class exists for: every proposal used to land on
        # 'plan_review_required' because it was the catch-all, so the lifecycle
        # feed announced a plan review for a workspace confirmation.
        for kind in ("create_workspace", "delete_workspace", "delete_thread"):
            assert classify_interrupt_reason(
                _pause({"type": kind, "workspace_id": "w-1"})
            ) == "approval_required"

    def test_an_unknown_action_generalizes_instead_of_guessing(self):
        assert classify_interrupt_reason(
            _pause({"type": "some_future_action", "detail": "x"})
        ) == "approval_required"

    def test_an_unreadable_payload_is_unclassified_rather_than_labelled(self):
        assert classify_interrupt_reason(_pause({"detail": "no discriminator"})) is None
        assert classify_interrupt_reason([Interrupt(value="not a dict")]) is None
        assert classify_interrupt_reason([]) is None

    def test_credit_pause_outranks_a_proposal_buffered_ahead_of_it(self):
        # A pause can carry several payloads; the one with behaviour attached
        # has to win regardless of the order they were buffered in.
        assert classify_interrupt_reason(
            _pause({"type": "create_workspace", "workspace_name": "scratch"})
            + _pause({"type": "credit_pause", "message": "out of credits"})
        ) == "credit_pause"

    def test_a_batched_approval_set_is_read_whole(self):
        # The approval middleware puts a turn's whole approval set in ONE
        # payload, so precedence has to hold within a payload and not just
        # across them — reading only the leading request made the answer depend
        # on which tool the model happened to call first.
        assert classify_interrupt_reason(
            _pause({"name": "run_backtest", "args": {}},
                   {"name": "SubmitPlan", "args": {}})
        ) == "plan_review_required"


class TestMigrationCheckBinding:
    def test_subagent_runs_check_matches_constant(self):
        sql = (MIGRATIONS_DIR / "020_subagent_run_ledger.py").read_text()
        match = re.search(r"CHECK \(status IN\s*\(([^)]*)\)", sql)
        assert match, "status CHECK not found in migration 020"
        check_values = set(re.findall(r"'(\w+)'", match.group(1)))
        assert check_values == {"in_progress", *TERMINAL_STATUSES}


class TestInterruptReasonVocabulary:
    """No schema CHECK backs this column, so the binding is enforced here.

    ``conversation_responses.interrupt_reason`` predates the vocabulary and
    still holds spellings from retired writers, so a CHECK could not be
    validated against the existing rows and would fail any later UPDATE of one.
    The constant is the authority instead: every producer returns a member, and
    every SQL literal is bound from it rather than typed out.
    """

    def test_the_classifier_only_ever_returns_a_member(self):
        for request in (
            {"type": "credit_pause"},
            {"type": "ask_user_question"},
            {"type": "some_future_action"},
            {"name": "SubmitPlan", "args": {}},
            {"name": "any_approved_tool", "args": {}},
        ):
            assert classify_interrupt_reason(_pause(request)) in INTERRUPT_REASONS

    def test_the_resume_query_binds_the_constant_not_a_literal(self):
        from src.server.database.runs import credit_ledger

        source = Path(credit_ledger.__file__).read_text()
        assert "interrupt_reason = 'credit_pause'" not in source
        assert "INTERRUPT_REASON_CREDIT_PAUSE" in source
