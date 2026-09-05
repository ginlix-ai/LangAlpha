"""Anchors and aliases must die at the door, before any safe_load.

``yaml.safe_load`` still expands aliases, so a frontmatter block far under the
SKILL.md byte cap can inflate into gigabytes (billion laughs) inside the
validation thread. The guard rejects on the event stream, which never expands.
"""

import io
import zipfile

import pytest

from src.server.services.user_skills.validate import (
    SkillValidationError,
    validate_skill_archive,
)


def _zip_with_frontmatter(frontmatter: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("demo/SKILL.md", f"---\n{frontmatter}---\n\n# demo\n")
    return buf.getvalue()


def test_alias_expansion_bomb_is_rejected_without_expanding():
    # Each tier multiplies by 9; a few more lines would be gigabytes once
    # composed. The reject must come from the event scan, i.e. instantly.
    bomb = (
        'name: demo\ndescription: a test skill\n'
        'a: &a ["x","x","x","x","x","x","x","x","x"]\n'
        "b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a]\n"
        "c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b]\n"
        "d: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c]\n"
        "e: &e [*d,*d,*d,*d,*d,*d,*d,*d,*d]\n"
        "f: &f [*e,*e,*e,*e,*e,*e,*e,*e,*e]\n"
        "g: &g [*f,*f,*f,*f,*f,*f,*f,*f,*f]\n"
        "h: &h [*g,*g,*g,*g,*g,*g,*g,*g,*g]\n"
        "i: &i [*h,*h,*h,*h,*h,*h,*h,*h,*h]\n"
        "j: &j [*i,*i,*i,*i,*i,*i,*i,*i,*i]\n"
    )
    with pytest.raises(SkillValidationError, match="anchors and aliases"):
        validate_skill_archive(_zip_with_frontmatter(bomb))


def test_a_single_anchor_is_already_rejected():
    # No expansion possible yet, but frontmatter has no legitimate use for
    # anchors, so the guard rejects the declaration itself.
    fm = "name: demo\ndescription: a test skill\nx: &a 1\ny: *a\n"
    with pytest.raises(SkillValidationError, match="anchors and aliases"):
        validate_skill_archive(_zip_with_frontmatter(fm))


def test_plain_frontmatter_still_validates():
    fm = "name: demo\ndescription: a test skill\nmetadata:\n  author: someone\n"
    validated = validate_skill_archive(_zip_with_frontmatter(fm))
    assert validated.name == "demo"


def test_broken_yaml_still_gets_the_named_syntax_error():
    # The guard swallows YAMLError so the downstream parse names the reason.
    fm = "name: demo\ndescription: [unclosed\n"
    with pytest.raises(SkillValidationError, match="frontmatter"):
        validate_skill_archive(_zip_with_frontmatter(fm))
