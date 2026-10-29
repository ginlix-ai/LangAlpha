"""The delivery signature must move exactly when the sandbox inputs move.

It is the warm-path convergence trigger: too-stable means a skill change never
reaches a warm sandbox; too-jumpy means every turn pays a redundant sync.
"""

from src.server.services.user_skills import skills_delivery_signature


def test_stable_for_identical_inputs():
    a = skills_delivery_signature("/views/abc123", frozenset({"x", "y"}))
    b = skills_delivery_signature("/views/abc123", frozenset({"y", "x"}))
    assert a == b


def test_moves_when_the_view_dir_moves():
    # The dir path embeds the content-addressed view hash, so any
    # upload/delete/toggle of a user-tier row lands here.
    a = skills_delivery_signature("/views/abc123", frozenset())
    b = skills_delivery_signature("/views/def456", frozenset())
    assert a != b


def test_moves_when_the_disabled_set_moves():
    a = skills_delivery_signature("/views/abc123", frozenset())
    b = skills_delivery_signature("/views/abc123", frozenset({"chart-annotation"}))
    assert a != b


def test_no_skills_and_no_disables_is_a_real_state():
    # Deleting the last skill flips dir to None; that must read as a change.
    some = skills_delivery_signature("/views/abc123", frozenset())
    none = skills_delivery_signature(None, frozenset())
    assert some != none
    # And the empty state is deterministic across processes.
    assert none == skills_delivery_signature(None, frozenset())
