"""The archive containment boundary: every member path an upload can carry.

Member paths are later interpolated into the sandbox command line that stages
a skill, so the guard here is the primary one and quoting downstream is the
backstop, not the other way round. Each case is a path shape that has to be
refused before any byte is read.
"""

import io
import stat
import zipfile

import pytest

from src.server.services.user_skills.validate import (
    SkillValidationError,
    validate_skill_archive,
)

_SKILL_MD = "---\nname: demo\ndescription: a test skill\n---\n\n# demo\n"


def _zip(*members: tuple[str, str], mode: int | None = None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("demo/SKILL.md", _SKILL_MD)
        for path, body in members:
            info = zipfile.ZipInfo(path)
            if mode is not None:
                info.external_attr = mode << 16
            zf.writestr(info, body)
    return buf.getvalue()


def _rejects(raw: bytes) -> str:
    with pytest.raises(SkillValidationError) as excinfo:
        validate_skill_archive(raw)
    return str(excinfo.value)


class TestEscapesTheExtractionRoot:
    @pytest.mark.parametrize(
        "path",
        [
            "../outside.txt",
            "demo/../../outside.txt",
            "/etc/passwd",
            "C:/Windows/system.ini",
            "demo\\nested\\file.txt",
        ],
    )
    def test_path_traversal_and_absolute_paths(self, path):
        assert "unsafe path" in _rejects(_zip((path, "x")))


class TestShellMetacharacters:
    @pytest.mark.parametrize(
        "path",
        [
            "demo/a'/$(id)/b.txt",  # closes a single-quoted argument
            'demo/say "hi".txt',
            "demo/`id`.txt",
            "demo/$HOME.txt",
            "demo/a;rm -rf x.txt",
            "demo/a|b.txt",
            "demo/a&b.txt",
            "demo/a>b.txt",
            "demo/a<b.txt",
            "demo/glob*.txt",
            "demo/glob?.txt",
            "demo/two\nlines.txt",
        ],
    )
    def test_metacharacters_are_refused(self, path):
        assert "unsafe path" in _rejects(_zip((path, "x")))

    def test_leading_dash_is_refused(self):
        """A component starting with ``-`` reads as a flag wherever the path
        lands in an argument list."""
        assert "unsafe path" in _rejects(_zip(("demo/-rf.txt", "x")))
        assert "unsafe path" in _rejects(_zip(("-evil/notes.txt", "x")))

    def test_control_and_delete_characters_are_refused(self):
        assert "unsafe path" in _rejects(_zip(("demo/bell\x07.txt", "x")))
        assert "unsafe path" in _rejects(_zip(("demo/del\x7f.txt", "x")))


class TestNonRegularEntries:
    def test_symlink_members_are_refused(self):
        raw = _zip(("demo/link.txt", "../../etc/passwd"), mode=stat.S_IFLNK | 0o777)
        assert "not a regular file" in _rejects(raw)

    def test_fifo_members_are_refused(self):
        raw = _zip(("demo/pipe", ""), mode=stat.S_IFIFO | 0o644)
        assert "not a regular file" in _rejects(raw)


class TestFileDirectoryPrefixConflicts:
    """A zip can name a file and a directory identically; a filesystem cannot.

    Extraction of such an archive runs inside ``resolve_llm_config``, so an
    accepted one would fail every turn the uploading user takes until the row
    is deleted. The refusal has to happen at upload.
    """

    def test_a_member_that_is_also_a_parent_directory(self):
        msg = _rejects(_zip(("demo/x", "file\n"), ("demo/x/y", "child\n")))
        assert "both a file and a directory" in msg
        assert "demo/x" in msg

    def test_the_order_inside_the_zip_does_not_matter(self):
        msg = _rejects(_zip(("demo/x/y", "child\n"), ("demo/x", "file\n")))
        assert "both a file and a directory" in msg

    def test_a_grandparent_conflict_is_caught_too(self):
        msg = _rejects(_zip(("demo/a", "file\n"), ("demo/a/b/c", "deep\n")))
        assert "demo/a" in msg

    def test_the_skill_root_itself_cannot_be_a_file(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("demo/SKILL.md", _SKILL_MD)
            zf.writestr("demo", "the top dir as a file\n")
        assert "both a file and a directory" in _rejects(buf.getvalue())


class TestAcceptedShapes:
    def test_ordinary_nested_paths_survive(self):
        validated = validate_skill_archive(
            _zip(
                ("demo/scripts/run.py", "print('hi')\n"),
                ("demo/reference/notes.md", "# notes\n"),
                ("demo/data/2026-01 report (final).csv", "a,b\n"),
            )
        )
        assert validated.name == "demo"
        assert validated.file_count == 4

    def test_a_zip_with_only_a_directory_entry_is_empty_not_unsafe(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("demo/", "")
        assert "no files" in _rejects(buf.getvalue())
