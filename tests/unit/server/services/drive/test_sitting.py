"""One version per sitting (src/server/database/drive/writes.py).

A save replaces the head version in place only when the same actor wrote
it under the same non-null sitting; anything else is a new version.
"""

import dataclasses

import pytest

from src.server.database.drive.policy import same_sitting
from src.server.services.drive import Actor

USER = "user-1"


def _actor(**overrides):
    fields = {"kind": "agent", "user_id": USER, "via": "tool", "agent": "main", "sitting": "run-7"}
    fields.update(overrides)
    return Actor(**fields)


HEAD = _actor()


def _head(**overrides):
    return dataclasses.replace(HEAD, **overrides)


def test_the_same_actor_in_the_same_sitting_replaces_the_head():
    assert same_sitting(HEAD, _actor())


def test_the_agent_saving_through_the_mount_is_still_the_same_sitting():
    assert same_sitting(HEAD, _actor(via="mount"))


@pytest.mark.parametrize(
    ("head", "actor"),
    [
        (HEAD, _actor(sitting=None)),
        (_head(sitting=None), _actor(sitting=None)),
        (HEAD, _actor(sitting="run-8")),
        (HEAD, _actor(agent="research")),
        (HEAD, _actor(user_id="user-2")),
        (HEAD, _actor(kind="person", agent=None)),
        # A restored version is recorded with no sitting, so a deliberate
        # new one is never folded into.
        (_head(sitting=None), _actor()),
        # A folder has no head.
        (None, _actor()),
    ],
)
def test_anything_else_is_a_new_version(head, actor):
    assert not same_sitting(head, actor)
