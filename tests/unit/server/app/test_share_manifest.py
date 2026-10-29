"""The file list behind a shared file link: what a visitor may fetch.

The reference scanners and ``resolve_ref`` are pure and tested directly.
``build_manifest`` runs against a fake reader standing in for the mirror or
the sandbox, so the walk (which files are parsed, how deep, what is listed)
is locked without a database or a sandbox.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import pytest

from src.server.app import share_manifest as sm
from src.server.app.share_manifest import (
    REASON_ENTRY,
    REASON_MARKDOWN,
    REASON_PAGE,
    REASON_SCRIPT,
    REASON_STYLE,
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
    build_manifest,
    css_refs,
    html_refs,
    manifest_drift,
    markdown_refs,
    resolve_ref,
    script_refs,
)

WORK_DIR = "/home/workspace/proj"
OWNER = "owner-fake-1"
WORKSPACE = {"workspace_id": "ws-fake-0001", "user_id": OWNER, "status": "stopped"}


class FakeReader:
    """The reader contract ``build_manifest`` needs: sizes for a batch, bytes for one."""

    def __init__(self, files: dict[str, bytes | str]) -> None:
        self.files = {
            p: (c.encode("utf-8") if isinstance(c, str) else c) for p, c in files.items()
        }
        self.read_paths: list[str] = []

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        return {p: (p, len(self.files[p])) for p in paths if p in self.files}

    async def read(self, path: str) -> bytes | None:
        self.read_paths.append(path)
        return self.files.get(path)


@pytest.fixture
def reader(monkeypatch):
    """Install a fake reader; the test fills ``.files`` before building."""
    fake = FakeReader({})

    @asynccontextmanager
    async def _reader_for(workspace, user_id, work_dir):
        yield fake, work_dir, ()

    monkeypatch.setattr(sm, "_reader_for", _reader_for)
    return fake


async def _build(entry: str) -> list[ManifestEntry]:
    return await build_manifest(WORKSPACE, entry, user_id=OWNER, work_dir=WORK_DIR)


def _paths(entries: list[ManifestEntry]) -> list[str]:
    return [e.path for e in entries]


def _reasons(entries: list[ManifestEntry]) -> dict[str, str]:
    return {e.path: e.reason for e in entries}


# --- reference extraction -------------------------------------------------


def test_html_refs_split_page_style_and_script():
    page, style, script = html_refs(
        """
        <html><head>
          <link rel="stylesheet" href="style.css">
          <style>body { background: url("bg.png") }</style>
          <script src="app.js"></script>
          <script>fetch("data/chart.json")</script>
          <meta http-equiv="refresh" content="0; url=next.html">
        </head><body>
          <img src="a.png" srcset="a@2x.png 2x, a@3x.png 3x">
          <div style="background-image: url(inline.png)"></div>
          <video poster="poster.jpg"></video>
          <object data="doc.pdf"></object>
          <use xlink:href="icons.svg#x"/>
        </body></html>
        """
    )
    assert page == [
        "style.css",
        "app.js",
        "next.html",
        "a.png",
        "a@2x.png",
        "a@3x.png",
        "poster.jpg",
        "doc.pdf",
        "icons.svg#x",
    ]
    assert style == ["bg.png", "inline.png"]
    assert script == ["data/chart.json"]


def test_css_refs_take_url_and_import_in_every_spelling():
    assert css_refs(
        """
        @import "base.css";
        @import url(theme.css);
        a { background: url( 'x.png' ) } b { src: url(fonts/a.woff2) }
        c { background: url("charts/revenue (1).png") }
        """
    ) == ["theme.css", "x.png", "fonts/a.woff2", "charts/revenue (1).png", "base.css"]


def test_markdown_refs_take_links_images_and_reference_definitions():
    assert markdown_refs(
        "![chart](img/c.png)\n[doc](other.md)\n[ref]: <img/d.png>\n![t](<e.png>)"
    ) == ["img/c.png", "other.md", "e.png", "img/d.png"]


def test_markdown_refs_read_bracketed_names_and_titled_definitions():
    assert markdown_refs("![c](<img/c 1.png>)") == ["img/c 1.png"]
    assert markdown_refs('![a](x.png "T")') == ["x.png"]
    assert markdown_refs('![chart][ref]\n\n[ref]: chart.png "Title"') == ["chart.png"]


def test_markdown_refs_stay_linear_on_unmatched_brackets():
    # Quadratic before: every "[" rescanned to the end of the file.
    started = time.monotonic()
    assert markdown_refs("[" * 400_000) == []
    assert time.monotonic() - started < 2


def test_script_refs_are_quoted_relative_paths_with_an_extension():
    refs = script_refs(
        """
        const a = "data/chart.json"; const b = 'x.csv'; const c = `y.png`;
        const url = "https://cdn.example.com/lib.js";
        const tpl = `${base}/z.json`; const glob = "*.csv"; const word = "hello";
        img.src = "chart.png?v=2"; const d = 'data/a.json#top'; const q = "?v=2";
        """
    )
    assert refs == ["data/chart.json", "x.csv", "y.png", "chart.png", "data/a.json"]


# --- resolve_ref ------------------------------------------------------------


@pytest.mark.parametrize(
    "ref, expected",
    [
        ("style.css", "reports/style.css"),
        ("./img/a.png", "reports/img/a.png"),
        ("../shared/logo.png", "shared/logo.png"),
        ("../../escape.png", None),
        ("a.png?v=2#top", "reports/a.png"),
        ("https://cdn.example.com/x.js", None),
        ("//cdn.example.com/x.js", None),
        ("data:image/png;base64,AAAA", None),
        ("#anchor", None),
        ("mailto:someone@example.com", None),
        ("", None),
        (f"{WORK_DIR}/data/table.csv", "data/table.csv"),
        (f"file://{WORK_DIR}/data/notes.md", "data/notes.md"),
        # One leading slash is the client's spelling of the workspace root, so
        # it folds inside the workspace rather than onto the machine.
        ("/data/virtual.csv", "data/virtual.csv"),
        ("/etc/passwd", "etc/passwd"),
        ("%E6%8A%A5%E5%91%8A.png", "reports/报告.png"),
    ],
)
def test_resolve_ref(ref, expected):
    assert resolve_ref(ref, "reports/index.html", WORK_DIR) == expected


# --- manifest_drift ---------------------------------------------------------


def test_manifest_drift_is_none_until_a_list_was_confirmed():
    current = [ManifestEntry("a.html", 1, REASON_ENTRY)]
    assert manifest_drift(current, None) is None
    assert manifest_drift(current, ["a.html"]) is None


def test_manifest_drift_names_what_moved_in_sorted_order():
    current = [
        ManifestEntry("a.html", 1, REASON_ENTRY),
        ManifestEntry("z.png", 1, REASON_PAGE),
        ManifestEntry("b.png", 1, REASON_PAGE),
    ]
    assert manifest_drift(current, ["a.html", "old.css"]) == {
        "added": ["b.png", "z.png"],
        "removed": ["old.css"],
    }


# --- build_manifest: HTML -------------------------------------------------


@pytest.mark.asyncio
async def test_html_entry_lists_its_assets_with_reasons(reader):
    reader.files = FakeReader(
        {
            "reports/index.html": """
                <link rel="stylesheet" href="style.css">
                <script src="app.js"></script>
                <img src="../shared/logo.png">
                <a href="../../etc/passwd">x</a>
                <script src="https://cdn.example.com/lib.js"></script>
                <img src="//cdn.example.com/y.png">
                <img src="data:image/png;base64,AAAA">
                <a href="#top">top</a>
                <a href="mailto:a@example.com">mail</a>
            """,
            "reports/style.css": "@import 'base.css'; a { background: url(fonts/a.woff2) }",
            "reports/base.css": "b { background: url(../img/bg.png) }",
            "reports/fonts/a.woff2": b"\x00font",
            "img/bg.png": b"\x89PNG",
            "reports/app.js": 'fetch("data/chart.json")',
            "reports/data/chart.json": "{}",
            "shared/logo.png": b"\x89PNG",
            "etc/passwd": "never",
        }
    ).files
    entries = await _build("reports/index.html")
    assert entries[0] == ManifestEntry("reports/index.html", entries[0].size, REASON_ENTRY)
    assert set(_paths(entries)) == {
        "reports/index.html",
        "reports/style.css",
        "reports/app.js",
        "shared/logo.png",
        "reports/base.css",
        "reports/fonts/a.woff2",
        "img/bg.png",
        "reports/data/chart.json",
    }
    reasons = _reasons(entries)
    assert reasons["reports/style.css"] == REASON_PAGE
    assert reasons["reports/app.js"] == REASON_PAGE
    assert reasons["shared/logo.png"] == REASON_PAGE
    assert reasons["reports/base.css"] == REASON_STYLE
    assert reasons["reports/fonts/a.woff2"] == REASON_STYLE
    assert reasons["img/bg.png"] == REASON_STYLE
    assert reasons["reports/data/chart.json"] == REASON_SCRIPT
    assert all(e.size == len(reader.files[e.path]) for e in entries)


@pytest.mark.asyncio
async def test_a_scripts_strings_resolve_against_its_page_and_itself(reader):
    reader.files = FakeReader(
        {
            "reports/index.html": '<script type="module" src="js/app.js"></script>',
            "reports/js/app.js": 'import "./chart.js"; img.src = "images/c.png"',
            "reports/js/chart.js": "",
            "reports/images/c.png": b"\x89PNG",
        }
    ).files
    entries = await _build("reports/index.html")
    assert set(_paths(entries)) == {
        "reports/index.html",
        "reports/js/app.js",
        "reports/js/chart.js",
        "reports/images/c.png",
    }


@pytest.mark.asyncio
async def test_sandbox_absolute_and_file_url_references_fold_to_the_workspace(reader):
    reader.files = FakeReader(
        {
            "index.html": (
                f'<a href="{WORK_DIR}/data/table.csv">t</a>'
                f'<a href="file://{WORK_DIR}/data/notes.md">n</a>'
                '<a href="/data/virtual.csv">v</a>'
            ),
            "data/table.csv": "a,b",
            "data/notes.md": "![](img/z.png)",
            "data/virtual.csv": "c,d",
            "img/z.png": b"\x89PNG",
        }
    ).files
    entries = await _build("index.html")
    assert set(_paths(entries)) == {
        "index.html",
        "data/table.csv",
        "data/notes.md",
        "data/virtual.csv",
    }


@pytest.mark.asyncio
async def test_a_page_linked_from_the_entry_is_parsed_but_the_next_one_is_not(reader):
    reader.files = FakeReader(
        {
            "index.html": '<a href="a.html">a</a>',
            "a.html": '<a href="b.html">b</a><link href="a.css">',
            "a.css": "x { background: url(deep.png) }",
            "deep.png": b"\x89PNG",
            "b.html": '<a href="c.html">c</a>',
            "c.html": "unreachable",
        }
    ).files
    entries = await _build("index.html")
    assert set(_paths(entries)) == {"index.html", "a.html", "a.css", "deep.png", "b.html"}
    assert "b.html" not in reader.read_paths


# --- build_manifest: Markdown ----------------------------------------------


@pytest.mark.asyncio
async def test_markdown_entry_lists_only_images_resolved_from_the_workspace_root(reader):
    reader.files = FakeReader(
        {
            "notes/a.md": (
                "![chart](img/c.png)\n"
                "[doc](other.md)\n"
                "[data](data.csv)\n"
                "[ref]: img/d.png\n"
                '<img src="img/e.svg">\n'
                "![remote](https://example.com/r.png)\n"
            ),
            "img/c.png": b"\x89PNG",
            "notes/img/c.png": b"\x89PNG",
            "img/d.png": b"\x89PNG",
            "img/e.svg": '<svg><image href="secret.png"/></svg>',
            "secret.png": b"\x89PNG",
            "other.md": "![](img/d.png)",
            "notes/other.md": "x",
            "data.csv": "a,b",
            "notes/data.csv": "a,b",
        }
    ).files
    entries = await _build("notes/a.md")
    assert set(_paths(entries)) == {"notes/a.md", "img/c.png", "img/d.png", "img/e.svg"}
    assert _reasons(entries)["img/e.svg"] == REASON_MARKDOWN
    assert "img/e.svg" not in reader.read_paths


@pytest.mark.asyncio
async def test_markdown_reached_from_a_page_is_listed_but_not_parsed(reader):
    reader.files = FakeReader(
        {
            "index.html": '<a href="notes.md">notes</a>',
            "notes.md": "![](img/z.png)",
            "img/z.png": b"\x89PNG",
        }
    ).files
    entries = await _build("index.html")
    assert _paths(entries) == ["index.html", "notes.md"]
    assert "notes.md" not in reader.read_paths


# --- build_manifest: other kinds, missing and hidden ----------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["data/table.csv", "report.pdf"])
async def test_a_plain_file_lists_only_itself(reader, entry):
    reader.files = {entry: b"a,b\n" * 3, "other.png": b"\x89PNG"}
    entries = await _build(entry)
    assert entries == [ManifestEntry(entry, len(reader.files[entry]), REASON_ENTRY)]
    assert reader.read_paths == []


@pytest.mark.asyncio
async def test_missing_and_hidden_references_are_left_out(reader):
    reader.files = FakeReader(
        {
            "index.html": (
                '<img src="gone.png">'
                '<a href=".agents/x.txt">x</a>'
                '<a href="_internal/y.txt">y</a>'
                '<a href="agent.md">notes</a>'
                '<img src="here.png">'
            ),
            ".agents/x.txt": "hidden",
            "_internal/y.txt": "hidden",
            "agent.md": "notes",
            "here.png": b"\x89PNG",
        }
    ).files
    entries = await _build("index.html")
    assert _paths(entries) == ["index.html", "here.png"]


@pytest.mark.asyncio
async def test_entry_that_is_absent_or_hidden_is_missing(reader):
    reader.files = {".agents/x.html": "<p>x</p>"}
    with pytest.raises(ManifestEntryMissing):
        await _build("nope.html")
    with pytest.raises(ManifestEntryMissing):
        await _build(".agents/x.html")


# --- build_manifest: caps ---------------------------------------------------


@pytest.mark.asyncio
async def test_too_many_files_is_refused_not_truncated(reader, monkeypatch):
    monkeypatch.setattr(sm, "MAX_FILES", 3)
    reader.files = FakeReader(
        {
            "index.html": "".join(f'<img src="{i}.png">' for i in range(4)),
            **{f"{i}.png": b"\x89PNG" for i in range(4)},
        }
    ).files
    with pytest.raises(ManifestTooLarge) as exc:
        await _build("index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_files", 3)


@pytest.mark.asyncio
async def test_too_many_bytes_is_refused_not_truncated(reader, monkeypatch):
    monkeypatch.setattr(sm, "MAX_TOTAL_BYTES", 16)
    reader.files = {"index.html": '<img src="big.png">', "big.png": b"\x00" * 32}
    with pytest.raises(ManifestTooLarge) as exc:
        await _build("index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_bytes", 16)


# --- build_manifest: names --------------------------------------------------


@pytest.mark.asyncio
async def test_cjk_references_resolve_to_the_raw_unicode_path(reader):
    reader.files = FakeReader(
        {
            "报告/index.html": (
                '<img src="%E5%9B%BE%E8%A1%A8.png"><img src="图表2.png">'
            ),
            "报告/图表.png": b"\x89PNG",
            "报告/图表2.png": b"\x89PNG",
        }
    ).files
    entries = await _build("报告/index.html")
    assert _paths(entries) == ["报告/index.html", "报告/图表.png", "报告/图表2.png"]


# --- build_manifest: the live folder ----------------------------------------


@pytest.mark.asyncio
async def test_a_live_walk_holds_the_folder_until_the_list_is_built(monkeypatch):
    """Files are read by path level after level, so a rename settling mid-walk
    would drop every file after the move; the hold spans the whole walk."""
    events: list[str] = []

    class LoggingReader(FakeReader):
        async def stat(self, paths):
            events.append("stat")
            return await super().stat(paths)

    live = LoggingReader(
        {"index.html": '<link href="style.css">', "style.css": "a{}"}
    )

    @asynccontextmanager
    async def acquire(workspace_id, user_id):
        events.append("held")
        try:
            yield object(), {**WORKSPACE, "status": "running"}
        finally:
            events.append("released")

    monkeypatch.setattr(sm, "acquire_sandbox_to_change", acquire)
    monkeypatch.setattr(sm, "owner_work_dir", lambda workspace: WORK_DIR)
    monkeypatch.setattr(sm, "_SandboxReader", lambda *args: live)

    entries = await build_manifest(
        {**WORKSPACE, "status": "running"},
        "index.html",
        user_id=OWNER,
        work_dir=WORK_DIR,
    )

    assert _paths(entries) == ["index.html", "style.css"]
    assert events == ["held", "stat", "stat", "released"]
