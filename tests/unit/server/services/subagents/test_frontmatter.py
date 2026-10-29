"""The character check the subagent files and the pane share."""

from __future__ import annotations

import pytest

from src.server.services.subagents.frontmatter import unsavable_char


@pytest.mark.parametrize(
    ("value", "found"),
    [
        pytest.param("clean", None, id="clean"),
        pytest.param("A\x00B", "\x00", id="nul-only"),
        pytest.param("A\ud800B", "\ud800", id="surrogate-only"),
        pytest.param("\x00 then \ud800", "\ud800", id="nul-before-surrogate"),
        pytest.param("\ud800 then \x00", "\ud800", id="surrogate-before-nul"),
        pytest.param({"a\x00": "x", "k": "\ud800"}, "\ud800", id="nul-in-a-key-surrogate-in-a-value"),
        pytest.param({"k": ["fine", "\x00"], "\ud800": 1}, "\ud800", id="surrogate-in-a-key"),
        pytest.param({"k": ["\x00"]}, "\x00", id="nul-nested"),
    ],
)
def test_a_lone_surrogate_wins_over_a_nul_wherever_each_sits(value, found):
    assert unsavable_char(value) == found


def test_a_value_aliased_in_many_places_is_walked_once():
    # What nested YAML aliases load to: 2**60 strings as a tree, 60 lists as objects.
    node: list = ["x"]
    for _ in range(60):
        node = [node, node]

    assert unsavable_char({"k": node}) is None
