"""langalpha-doc's reference pages and the pointers into them.

The agent reaches a reference page two ways: the skill's own index under
"## Reference files", and a prompt section that names a page directly. A page
missing from the index is found only by a prompt that happens to name it, and
a pointer to a page that does not ship sends the agent to read a file that is
not there, so both directions are pinned here.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SKILL_DIR = REPO_ROOT / "plugins" / "langalpha_service" / "skills" / "langalpha-doc"
TEMPLATES_DIR = REPO_ROOT / "src" / "ptc_agent" / "agent" / "prompts" / "templates"
SANDBOX_PREFIX = ".agents/skills/langalpha-doc/"
_POINTER_RE = re.compile(re.escape(SANDBOX_PREFIX) + r"([\w./-]+\.md)")


def _index() -> str:
    """The "## Reference files" section of SKILL.md."""
    body = (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")
    section = body.split("## Reference files", 1)[1]
    return section.split("\n## ", 1)[0]


def _shipped_pages() -> list[str]:
    return sorted(
        p.relative_to(SKILL_DIR).as_posix()
        for p in (SKILL_DIR / "references").glob("*.md")
    )


@pytest.mark.parametrize("page", _shipped_pages())
def test_every_reference_page_is_in_the_index(page):
    assert f"`{SANDBOX_PREFIX}{page}`" in _index()


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
