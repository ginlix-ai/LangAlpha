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

from src.server.services.user_skills.limits import MAX_SKILL_FILES
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


def _bad_crc(raw: bytearray) -> None:
    raw[raw.find(b"# demo")] ^= 0x20


def _encrypted(raw: bytearray) -> None:
    # The encryption bit of the general-purpose flags, in both headers.
    raw[6] |= 0x01
    raw[raw.find(b"PK\x01\x02") + 8] |= 0x01


@pytest.mark.parametrize("damage", [_bad_crc, _encrypted])
def test_a_member_that_will_not_decompress_is_a_broken_zip(damage):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("demo/SKILL.md", _SKILL_MD)
    raw = bytearray(buf.getvalue())
    damage(raw)
    with pytest.raises(SkillValidationError) as excinfo:
        validate_skill_archive(bytes(raw))
    assert excinfo.value.code == "not_zip"


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

    def test_a_finder_zip_is_one_skill_not_two_folders(self):
        validated = validate_skill_archive(
            _zip(
                ("demo/.DS_Store", "x"),
                ("__MACOSX/demo/._SKILL.md", "x"),
                ("__MACOSX/demo/._.DS_Store", "x"),
            )
        )
        assert validated.name == "demo"
        assert validated.file_count == 1

    def test_a_folder_named_otherwise_is_named_by_its_skill_md(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("demo v2/SKILL.md", _SKILL_MD)
            zf.writestr("demo v2/scripts/run.py", "print('hi')\n")
        validated = validate_skill_archive(buf.getvalue())
        assert validated.name == "demo"
        with zipfile.ZipFile(io.BytesIO(validated.canonical_zip)) as zf:
            assert zf.namelist() == ["demo/SKILL.md", "demo/scripts/run.py"]

    def test_an_upload_sets_aside_what_a_skill_never_holds(self):
        # macOS tar writes ``._*`` beside every file, and a zipped working
        # folder carries its .git and dependencies; a skill at the file cap
        # still fits once they are set aside, and none of it is stored.
        real = [(f"demo/ref/{i}.md", "x") for i in range(MAX_SKILL_FILES - 1)]
        forks = [(f"demo/ref/._{i}.md", "x") for i in range(MAX_SKILL_FILES - 1)]
        validated = validate_skill_archive(
            _zip(
                *real,
                *forks,
                ("demo/._SKILL.md", "x"),
                ("demo/.env", "SECRET=test-not-real"),
                ("demo/.git/HEAD", "ref"),
                ("demo/scripts/node_modules/dep/index.js", "x"),
            ),
            upload=True,
        )
        assert validated.file_count == MAX_SKILL_FILES
        with zipfile.ZipFile(io.BytesIO(validated.canonical_zip)) as zf:
            assert not [n for n in zf.namelist() if "/." in n or "node_modules" in n]

    def test_an_upload_does_not_judge_what_it_sets_aside(self):
        # A zipped working folder: dependency symlinks and a git ref whose
        # name would be refused on a file that is kept. None of it is read.
        link = stat.S_IFLNK | 0o777
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("demo/SKILL.md", _SKILL_MD)
            for path, mode in (
                ("demo/node_modules/.bin/tsc", link),
                ("demo/.venv/bin/python", link),
                ("demo/.git/refs/x$y", stat.S_IFREG | 0o644),
                ("__MACOSX/demo/._it's.md", stat.S_IFREG | 0o644),
            ):
                info = zipfile.ZipInfo(path)
                info.external_attr = mode << 16
                zf.writestr(info, "x")
        validated = validate_skill_archive(buf.getvalue(), upload=True)
        assert validated.file_count == 1

    def test_a_synced_skill_keeps_its_dotfiles_and_dependencies(self):
        # Only the upload door sets these aside: a skill the agent made comes
        # up from the sandbox through this same validator and keeps them.
        validated = validate_skill_archive(
            _zip(
                ("demo/._SKILL.md", "x"),
                ("demo/.gitkeep", ""),
                ("demo/.env.example", "KEY="),
                ("demo/scripts/node_modules/dep/index.js", "x"),
            )
        )
        with zipfile.ZipFile(io.BytesIO(validated.canonical_zip)) as zf:
            assert sorted(zf.namelist()) == [
                "demo/.env.example",
                "demo/.gitkeep",
                "demo/SKILL.md",
                "demo/scripts/node_modules/dep/index.js",
            ]

    def test_a_zip_with_only_a_directory_entry_is_empty_not_unsafe(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("demo/", "")
        assert "no files" in _rejects(buf.getvalue())
