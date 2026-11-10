"""File search leaves out in SQL exactly what the file panel leaves out of a
stopped workspace's listing, so a page is cut from what is shown."""

import re

import pytest

from src.server.services.served_paths import (
    STORED_FILE_EXCLUDED_LIKES,
    STORED_FILE_ROOT_EXCLUDED_LIKE,
    panel_lists_stored_file,
)


def _like(pattern: str, value: str) -> bool:
    """Postgres ``LIKE`` with its default ``\\`` escape, case-sensitive."""
    out, chars = [], iter(pattern)
    for c in chars:
        if c == "\\":
            out.append(re.escape(next(chars)))
        elif c == "%":
            out.append(".*")
        elif c == "_":
            out.append(".")
        else:
            out.append(re.escape(c))
    return re.fullmatch("".join(out), value, re.DOTALL) is not None


def _sql_keeps(path: str) -> bool:
    """What ``search_files``'s WHERE clause keeps of a path."""
    if any(_like(p, "/" + path) for p in STORED_FILE_EXCLUDED_LIKES):
        return False
    return not (_like(STORED_FILE_ROOT_EXCLUDED_LIKE, path) and "/" not in path)


@pytest.mark.parametrize(
    "path",
    [
        # Listed.
        "reports/dcf_model.xlsx",
        "dcf.md",
        "work/.agents_notes.md",
        "Xinternal/a.md",  # an unescaped ``_`` would match the X
        "notes/my__init__py.txt",
        "pkg/a.pyx",
        "deep/.wsfiles-kept/a.csv",  # transient only at the root
        "agents/skills/x.md",
        # System directories, the profile and automation files included.
        ".agents/skills/dcf-model/SKILL.md",
        ".agents/user/profile/holdings.json",
        ".agents/user/automations/daily.json",
        ".system/x.py",
        ".self-improve/log.md",
        "mcp_servers/fmp.py",
        # Hidden.
        "_internal",
        "_internal/cache.json",
        # Always hidden, at any depth.
        "app/node_modules/react/index.js",
        "node_modules/react/index.js",
        "lib/__pycache__/x.cpython-313.pyc",
        "pkg/__init__.py",
        "pkg/mod.pyc",
        ".bashrc",
        "home/.profile",
        "x/.file_sync_marker",
        "proj/.git/HEAD",
        ".venv/bin/python",
        ".wsfiles-upload-1234",
    ],
)
def test_sql_exclusions_agree_with_the_panel(path):
    assert _sql_keeps(path) == panel_lists_stored_file(path)


def test_the_panel_keeps_user_files_and_drops_skills():
    assert panel_lists_stored_file("reports/dcf_model.xlsx")
    assert not panel_lists_stored_file(".agents/skills/dcf-model/SKILL.md")


def test_asked_for_system_and_hidden_files_the_panel_lists_them():
    assert panel_lists_stored_file(".agents/skills/x.md", include_system=True)
    assert panel_lists_stored_file("_internal/cache.json", include_hidden=True)
    assert not panel_lists_stored_file(
        "app/node_modules/x.js", include_system=True, include_hidden=True
    )
