"""Pairing a branch's turns with the thread's persisted turns.

One rule serves replay and the edit/regenerate listing: a stamped turn keeps
its stamp, any other turn names the next persisted turn after the one before.
Replay used to pair an unstamped turn by its position instead, so the rule is
held to agree with that wherever replay accepted the pairing.
"""

from __future__ import annotations

from itertools import combinations, product

from src.server.services.history.reader import TurnAnchor, pair_turns


def _anchors(stamps):
    return [
        TurnAnchor(
            turn_ordinal=k,
            input_checkpoint_id=f"cp-{k}",
            tail_checkpoint_id=None,
            turn_index=stamp,
            is_resume=stamp is None,
        )
        for k, stamp in enumerate(stamps)
    ]


def test_a_resume_after_a_dead_turn_names_the_turn_after_the_one_it_answers():
    # Turn 1 died before its first checkpoint; turn 3 resumes turn 2.
    assert pair_turns(_anchors([0, 2, None]), [0, 1, 2, 3]) == [0, 2, 3]
    # By position the resume named turn 2 again, and replay gave up.
    assert _positional([0, 2, None], [0, 1, 2, 3]) is None


def test_unstamped_turns_take_the_persisted_turns_in_order():
    assert pair_turns(_anchors([None, None, None]), [0, 1]) == [0, 1, None]


def test_a_stamp_is_kept_even_where_no_persisted_turn_has_it():
    assert pair_turns(_anchors([5, None]), [0, 1]) == [5, None]


def _positional(stamps, persisted):
    """Replay's former rule: a stamp, else the persisted turn at the turn's
    position; None where it raised."""
    known = set(persisted)
    paired = []
    for k, stamp in enumerate(stamps):
        if stamp is not None:
            if stamp not in known:
                return None
            paired.append(stamp)
        elif k < len(persisted):
            paired.append(persisted[k])
        else:
            return None
    return paired if paired == sorted(set(paired)) else None


def test_every_pairing_replay_accepted_by_position_is_unchanged():
    values = range(5)
    checked = 0
    for size in range(len(values) + 1):
        for persisted in combinations(values, size):
            for length in range(1, 5):
                for stamps in product([None, *values, 5], repeat=length):
                    accepted = _positional(stamps, list(persisted))
                    if accepted is None:
                        continue
                    checked += 1
                    assert pair_turns(_anchors(stamps), list(persisted)) == accepted
    assert checked == 584  # every accepted case of up to four turns over five
