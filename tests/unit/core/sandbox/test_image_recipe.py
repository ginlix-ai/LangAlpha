"""The image recipe's name and its fallback pick, which Daytona and E2B share."""

from __future__ import annotations

from datetime import UTC, datetime

from ptc_agent.core.sandbox.providers._image_recipe import (
    Copy,
    ImageName,
    Recipe,
    Run,
    config_hash,
    image_identity,
    image_recipe,
)


def _at(day: int) -> datetime:
    return datetime(2026, 11, day, tzinfo=UTC)


def test_a_name_is_its_family_and_its_tag():
    assert str(ImageName("ptc-base-standard-1a2b3c4d", "1-0123abcd")) == (
        "ptc-base-standard-1a2b3c4d-t1-0123abcd"
    )


def test_siblings_are_the_family_under_other_tags_newest_first():
    wanted = ImageName("base-std-11111111", "1-aaaaaaaa")
    listed = [
        ("base-std-11111111-t1-bbbbbbb1", _at(1)),
        (str(wanted), _at(9)),  # the exact name is not its own sibling
        ("base-std-11111111-t1-bbbbbbb3", _at(3)),
        ("base-std-11111111-t1-bbbbbbb2", _at(2)),
        ("base-std-22222222-t1-bbbbbbb4", _at(8)),  # another base
        ("base-std-11111111-t1-bbbbbbb5-x", _at(7)),  # not a tag
        ("base-std-11111111-tBBBBBBB6", _at(7)),  # not a tag
        ("base-std-11111111-tbbbbbbb", _at(7)),  # too short
        ("base-std-11111111", _at(7)),  # no tag at all
        ("base-std-11111111-t1-bbbbbbb3", _at(3)),  # listed twice, kept once
    ]

    assert wanted.siblings(listed) == [
        "base-std-11111111-t1-bbbbbbb3",
        "base-std-11111111-t1-bbbbbbb2",
        "base-std-11111111-t1-bbbbbbb1",
    ]


def test_a_sibling_of_a_later_table_revision_is_never_a_fallback():
    """Its computers would hold a table this server refuses to replace."""
    wanted = ImageName("base-std-11111111", "2-aaaaaaaa")
    listed = [
        ("base-std-11111111-t3-bbbbbbb1", _at(5)),
        ("base-std-11111111-t2-bbbbbbb2", _at(2)),
        ("base-std-11111111-t1-bbbbbbb3", _at(3)),
        ("base-std-11111111-tbbbbbbb4", _at(4)),  # a tag from before revisions
    ]
    assert wanted.siblings(listed) == ["base-std-11111111-t1-bbbbbbb3", "base-std-11111111-t2-bbbbbbb2"]


def test_a_family_is_not_a_prefix_match():
    """``base-1`` must not pick up ``base-12``'s images."""
    wanted = ImageName("base-1", "1-aaaaaaaa")
    assert wanted.siblings([("base-12-t1-bbbbbbbb", _at(1))]) == []


def test_the_toolset_comes_last_and_stays_out_of_the_base_hash():
    """A new table moves only the tag, so the base hash ignores the toolset's
    steps, which run after the working directory is set."""
    recipe = image_recipe(working_dir="/home/workspace", mcp_packages=None, cpu=2)
    copy, run = recipe.toolset
    assert isinstance(copy, Copy) and isinstance(run, Run)
    assert recipe.steps == (*recipe.base, copy, run)

    other_toolset = Recipe(base=recipe.base, toolset=(Run(("true",)),))
    assert config_hash(image_identity(other_toolset, None)) == config_hash(
        image_identity(recipe, None)
    )
