"""Why a replay read cannot project, by what the caller does about it."""

from __future__ import annotations

import psycopg


class CheckpointReplayUnavailable(Exception):
    """Checkpoint history cannot faithfully cover this thread's replay."""


class ClaimsPassNeeded(CheckpointReplayUnavailable):
    """The turns can be projected only after a pass over the whole thread
    places its run launches, which this caller does not wait for."""


class ClaimsUnavailable(CheckpointReplayUnavailable):
    """The whole-thread claims pass failed, or ran and still left a launch
    unplaced: no turn of the read can be told apart by retrying it alone."""


class ClaimsNeeded(Exception):
    """A launch without a usable ledger stamp, in a turn whose claims were
    never assigned: only a pass over the whole thread can place it."""


# A database that refuses connections or queries fails every turn alike, so
# the read gives up rather than wait on it once per turn.
INFRASTRUCTURE = (psycopg.OperationalError, psycopg.InterfaceError)
