"""Why a replay read cannot project, by what the caller does about it."""

from __future__ import annotations


class CheckpointReplayUnavailable(Exception):
    """Checkpoint history cannot faithfully cover this thread's replay."""


class ClaimsPassNeeded(CheckpointReplayUnavailable):
    """The turns can be projected only after a pass over the whole thread
    places its run launches, which this caller does not wait for."""


class ClaimsUnavailable(CheckpointReplayUnavailable):
    """The whole-thread claims pass failed, or ran and still left a launch
    unplaced: a turn that launched a run cannot be projected, though one
    that launched none still can, alone."""


class ClaimsNeeded(Exception):
    """A launch without a usable ledger stamp, in a turn whose claims were
    never assigned: only a pass over the whole thread can place it."""
