"""langalpha-doc's pages and the pointers into them.

The agent reaches a page two ways: the skill's own index (reference pages
under "## Reference files", facts pages under "## Facts"), and a prompt
section that names a page directly. A page missing from its index is found
only by a prompt that happens to name it, and a pointer to a page that does
not ship sends the agent to read a file that is not there, so both directions
are pinned here.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SKILL_DIR = REPO_ROOT / "plugins" / "langalpha_service" / "skills" / "langalpha-doc"
TEMPLATES_DIR = REPO_ROOT / "src" / "ptc_agent" / "agent" / "prompts" / "templates"
SANDBOX_PREFIX = ".agents/skills/langalpha-doc/"
_POINTER_RE = re.compile(re.escape(SANDBOX_PREFIX) + r"([\w./-]+\.md)")


# Each folder of pages and the SKILL.md section that indexes it.
_INDEXES = {"references": "## Reference files", "facts": "## Facts"}


def _index(heading: str) -> str:
    """The section of SKILL.md under ``heading``."""
    body = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    section = body.split(f"\n{heading}\n", 1)[1]
    return section.split("\n## ", 1)[0]


def _shipped_pages() -> list[tuple[str, str]]:
    return sorted(
        (p.relative_to(SKILL_DIR).as_posix(), heading)
        for folder, heading in _INDEXES.items()
        for p in (SKILL_DIR / folder).glob("*.md")
    )


@pytest.mark.parametrize("page,heading", _shipped_pages())
def test_every_page_is_in_its_index(page, heading):
    assert f"`{SANDBOX_PREFIX}{page}`" in _index(heading)


def _pointers() -> list[tuple[str, str]]:
    """Every langalpha-doc page named by the skill itself or a prompt template."""
    sources = [*SKILL_DIR.rglob("*.md"), *TEMPLATES_DIR.rglob("*.j2")]
    return sorted(
        {
            (source.relative_to(REPO_ROOT).as_posix(), match)
            for source in sources
            for match in _POINTER_RE.findall(source.read_text(encoding="utf-8"))
        }
    )


@pytest.mark.parametrize("source,page", _pointers())
def test_every_pointer_names_a_page_that_ships(source, page):
    assert (SKILL_DIR / page).is_file(), f"{source} points at missing {page}"
