"""The file list behind a published version: what a visitor may fetch.

``walk_manifest`` runs against a fake reader standing in for the mirror or the
sandbox, so the walk (which files are parsed, how deep, what is listed) is
locked without a database or a sandbox.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from src.server.services.artifacts import manifest as mf
from src.server.services.artifacts import reader as reader_module
from src.server.services.artifacts.manifest import (
    REASON_ENTRY,
    REASON_LOADED,
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
    resolve_ref,
    walk_manifest,
)
from src.server.services.artifacts.reader import (
    Reader,
    ReadMode,
    snapshot_reader,
)
from src.server.services.artifacts.references import (
    REASON_MARKDOWN,
    REASON_LINK,
    REASON_PAGE,
    REASON_SCRIPT,
    REASON_STYLE,
)

WORK_DIR = "/home/workspace/proj"
OWNER = "owner-fake-1"
WORKSPACE = {"workspace_id": "ws-fake-0001", "user_id": OWNER, "status": "stopped"}


class FakeReader:
    """The reader contract ``walk_manifest`` needs: sizes for a batch, bytes for one."""

    def __init__(
        self, files: dict[str, bytes | str], links: dict[str, str] | None = None
    ) -> None:
        self.files = {
            p: (c.encode("utf-8") if isinstance(c, str) else c) for p, c in files.items()
        }
        #: A symlink's path and the file it resolves to.
        self.links = links or {}
        self.read_paths: list[str] = []

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        found = {}
        for p in paths:
            target = self.links.get(p, p)
            if target in self.files:
                found[p] = (target, len(self.files[target]))
        return found

    async def read(self, path: str) -> bytes | None:
        self.read_paths.append(path)
        return self.files.get(self.links.get(path, path))


@pytest.fixture
def reader():
    """A fake reader; the test fills ``.files`` before walking."""
    return FakeReader({})


async def _build(
    files: FakeReader, entry: str, loaded: tuple[str, ...] | list[str] = ()
) -> list[ManifestEntry]:
    return (await walk_manifest(Reader(files, WORK_DIR, ()), entry, loaded=loaded)).entries


async def _leaving_out(
    files: dict[str, bytes | str],
    *left_out: str,
    loaded: tuple[str, ...] = (),
    remembered: tuple[str, ...] = (),
) -> tuple[list[str], dict[str, str]]:
    found = await walk_manifest(
        Reader(FakeReader(files), WORK_DIR, ()),
        "index.html",
        loaded=loaded,
        remembered=remembered,
        left_out=left_out,
    )
    return _paths(found.entries), {e.path: e.reason for e in found.left_out}


def _paths(entries: list[ManifestEntry]) -> list[str]:
    return [e.path for e in entries]


def _reasons(entries: list[ManifestEntry]) -> dict[str, str]:
    return {e.path: e.reason for e in entries}



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
        # A backslash in the path is a slash to the URL parser; written %5C it
        # is part of the name, and in a query it is the query's.
        ("pages\\details.html", "reports/pages/details.html"),
        ("..\\shared\\logo.png", "shared/logo.png"),
        ("\\\\cdn.example.com\\x.js", None),
        ("a%5Cb.png", "reports/a\\b.png"),
        ("x.png?a\\b", "reports/x.png"),
    ],
)
def test_resolve_ref(ref, expected):
    assert resolve_ref(ref, "reports/index.html", WORK_DIR) == expected


# --- walk_manifest: HTML -------------------------------------------------


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
    entries = await _build(reader, "reports/index.html")
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
    entries = await _build(reader, "reports/index.html")
    assert set(_paths(entries)) == {
        "reports/index.html",
        "reports/js/app.js",
        "reports/js/chart.js",
        "reports/images/c.png",
    }


@pytest.mark.asyncio
async def test_a_page_reads_a_rooted_or_file_url_from_no_workspace_file(reader):
    # The browser reads these from the root of the origin serving the page,
    # in the live view and a version alike, so listing them carries a file the
    # version answers 404 for.
    reader.files = FakeReader(
        {
            "index.html": (
                f'<img src="{WORK_DIR}/data/a.png"><script src="/assets/app.js"></script>'
                f'<a href="file://{WORK_DIR}/data/notes.md">n</a><a href="/data/virtual.csv">v</a>'
                '<style>p { background: url(/img/bg.png) }</style>'
            ),
            "data/a.png": b"\x89PNG",
            "assets/app.js": "x",
            "data/notes.md": "n",
            "data/virtual.csv": "c,d",
            "img/bg.png": b"\x89PNG",
        }
    ).files
    assert _paths(await _build(reader, "index.html")) == ["index.html"]


@pytest.mark.asyncio
async def test_what_a_page_loads_from_the_origin_root_is_named_for_the_review(reader):
    # A built app's default (/assets/...): no view serves it, so the review
    # warns. Only what markup and stylesheets load: a link home and a script's
    # string may be no load at all.
    reader.files = FakeReader(
        {
            "index.html": (
                '<script src="/assets/app.js"></script><link rel="stylesheet" href="s.css">'
                '<a href="/">home</a><script src="main.js"></script>'
            ),
            "s.css": "p { background: url(/img/bg.png) } q { background: url( /img/bg.png ) }",
            "main.js": 'fetch("/api/data.json")',
        }
    ).files
    found = await walk_manifest(Reader(reader, WORK_DIR, ()), "index.html")
    assert found.root_refs == ["/assets/app.js", "/img/bg.png"]
    assert set(_paths(found.entries)) == {"index.html", "s.css", "main.js"}

    # A backslash roots it too, as the URL parser reads one.
    reader.files["index.html"] = b'<script src="\\assets\\app.js"></script>'
    found = await walk_manifest(Reader(reader, WORK_DIR, ()), "index.html")
    assert found.root_refs == ["\\assets\\app.js"]
    assert _paths(found.entries) == ["index.html"]


@pytest.mark.asyncio
async def test_root_loads_named_for_the_review_stop_at_a_few(reader):
    imgs = "".join(f'<img src="/i/{n}.png">' for n in range(40))
    reader.files = FakeReader({"index.html": imgs}).files
    found = await walk_manifest(Reader(reader, WORK_DIR, ()), "index.html")
    assert found.root_refs == [f"/i/{n}.png" for n in range(20)]


@pytest.mark.asyncio
async def test_a_document_reads_sandbox_and_file_url_paths_from_the_workspace(reader):
    # The Markdown viewer reads a path from the workspace root.
    reader.files = FakeReader(
        {
            "r/notes.md": (
                f"![]({WORK_DIR}/data/table.png) ![](file://{WORK_DIR}/data/f.png) ![](/data/virtual.png)"
            ),
            "data/table.png": b"\x89PNG",
            "data/f.png": b"\x89PNG",
            "data/virtual.png": b"\x89PNG",
        }
    ).files
    assert set(_paths(await _build(reader, "r/notes.md"))) == {
        "r/notes.md",
        "data/table.png",
        "data/f.png",
        "data/virtual.png",
    }


@pytest.mark.asyncio
async def test_a_page_linked_from_the_entry_is_followed_but_the_next_ones_links_are_not(reader):
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
    entries = await _build(reader, "index.html")
    assert set(_paths(entries)) == {"index.html", "a.html", "a.css", "deep.png", "b.html"}
    assert "c.html" not in reader.read_paths


@pytest.mark.asyncio
async def test_a_file_the_page_only_links_to_says_so(reader):
    # A linked page goes along for the visitor who follows the link, which is
    # not the same claim as the page needing it to draw.
    reader.files = FakeReader(
        {
            "index.html": (
                '<a href="other.html">o</a><img src="chart.png">'
                '<a href="both.png"><img src="both.png"></a>'
            ),
            "other.html": "other",
            "chart.png": b"\x89PNG",
            "both.png": b"\x89PNG",
        }
    ).files
    reasons = _reasons(await _build(reader, "index.html"))
    assert reasons["other.html"] == REASON_LINK
    assert reasons["chart.png"] == REASON_PAGE
    assert reasons["both.png"] == REASON_PAGE


@pytest.mark.asyncio
async def test_a_page_left_out_stays_on_offer_and_takes_only_its_own_files_with_it():
    paths, left_out = await _leaving_out(
        {
            "index.html": '<a href="other.html">o</a><img src="shared.png">',
            "other.html": '<img src="only.png"><img src="shared.png"><a href="next.html">n</a>',
            "only.png": b"\x89PNG",
            "shared.png": b"\x89PNG",
            "next.html": "next",
        },
        "other.html",
    )
    assert paths == ["index.html", "shared.png"]
    assert left_out == {"other.html": REASON_LINK}


@pytest.mark.asyncio
async def test_a_file_the_page_loads_goes_along_whatever_the_owner_said():
    files = {
        "index.html": '<a href="a.html">a</a><a href="b.html">b</a><a href="c.csv">c</a>',
        "a.html": '<img src="b.html"><script>const x = "c.csv";</script>',
        "b.html": "b",
        "c.csv": "c",
    }
    # b.html loaded from the next page down, after its link left it out.
    paths, left_out = await _leaving_out(files, "b.html")
    assert "b.html" in paths and left_out == {}

    # The live page reported loading it.
    paths, left_out = await _leaving_out(files, "c.csv", loaded=("c.csv",))
    assert "c.csv" in paths and left_out == {}


@pytest.mark.asyncio
async def test_what_the_owner_may_leave_out_is_what_the_page_does_not_need_now():
    found = await walk_manifest(
        Reader(
            FakeReader(
                {
                    "index.html": '<a href="a.html">a</a><img src="b.png">'
                    '<script>const x = "c.json";</script>',
                    "a.html": "a",
                    "b.png": b"\x89PNG",
                    "c.json": "{}",
                    "now.csv": "n",
                    "old.csv": "o",
                }
            ),
            WORK_DIR,
            (),
        ),
        "index.html",
        loaded=("now.csv",),
        remembered=("now.csv", "old.csv"),
    )
    assert {e.path: e.optional for e in found.entries} == {
        "index.html": False,
        "a.html": True,
        "b.png": False,
        "c.json": True,
        "now.csv": False,
        "old.csv": True,
    }


@pytest.mark.asyncio
async def test_a_link_or_a_guess_the_page_loads_after_all_is_no_longer_the_owners_to_leave_out():
    found = await walk_manifest(
        Reader(
            FakeReader(
                {
                    "index.html": '<a href="a.html">a</a><a href="b.html">b</a>'
                    '<script>const x = "c.json";</script>',
                    "a.html": '<img src="b.html">',
                    "b.html": "b",
                    "c.json": "{}",
                }
            ),
            WORK_DIR,
            (),
        ),
        "index.html",
        loaded=("c.json",),
    )
    assert {e.path: (e.reason, e.optional) for e in found.entries} == {
        "index.html": ("entry", False),
        "a.html": ("link", True),
        "b.html": ("link", False),
        "c.json": ("script", False),
    }


@pytest.mark.asyncio
async def test_a_file_named_by_its_own_path_after_its_link_is_read_from_its_own_folder():
    files = {
        "index.html": '<link rel="stylesheet" href="alias/style.css"><a href="b.html">b</a>',
        "b.html": '<link rel="stylesheet" href="vendor/style.css">',
        "vendor/style.css": "a { background: url(img.png) }",
        "vendor/img.png": "png",
    }
    found = await walk_manifest(
        Reader(FakeReader(files, links={"alias/style.css": "vendor/style.css"}), WORK_DIR, ()),
        "index.html",
    )
    assert [e.path for e in found.entries] == [
        "index.html",
        "alias/style.css",
        "vendor/style.css",
        "b.html",
        "vendor/img.png",
    ]


@pytest.mark.asyncio
async def test_a_link_left_out_by_its_own_spelling_is_reported_by_that_one():
    files = {"index.html": '<a href="docs.html">d</a>', "store/docs.html": "d"}
    found = await walk_manifest(
        Reader(FakeReader(files, links={"docs.html": "store/docs.html"}), WORK_DIR, ()),
        "index.html",
        left_out=("store/docs.html",),
    )
    assert [e.path for e in found.entries] == ["index.html"]
    assert [e.path for e in found.left_out] == ["store/docs.html"]


@pytest.mark.asyncio
async def test_a_file_left_out_by_one_spelling_is_one_row_and_none_once_it_goes_along():
    links = {"docs.html": "store/docs.html"}
    # Linked by its link and by its own path: one choice, under the spelling left out.
    files = {"index.html": '<a href="docs.html">d</a><a href="store/docs.html">s</a>', "store/docs.html": "d"}
    found = await walk_manifest(
        Reader(FakeReader(files, links=links), WORK_DIR, ()), "index.html", left_out=("store/docs.html",)
    )
    assert [e.path for e in found.left_out] == ["store/docs.html"]
    # Linked by its own path and loaded through its link: it goes, so no row says it stays.
    files = {"index.html": '<a href="store/docs.html">s</a><iframe src="docs.html"></iframe>', "store/docs.html": "d"}
    found = await walk_manifest(
        Reader(FakeReader(files, links=links), WORK_DIR, ()), "index.html", left_out=("store/docs.html",)
    )
    assert "store/docs.html" in [e.path for e in found.entries]
    assert found.left_out == []


@pytest.mark.asyncio
async def test_a_load_no_report_names_now_can_be_left_out_and_comes_back_when_one_does():
    files = {"index.html": "<p>x</p>", "old.csv": "o"}
    paths, left_out = await _leaving_out(files, "old.csv", remembered=("old.csv",))
    assert paths == ["index.html"]
    assert left_out == {"old.csv": REASON_LOADED}

    paths, left_out = await _leaving_out(
        files, "old.csv", loaded=("old.csv",), remembered=("old.csv",)
    )
    assert paths == ["index.html", "old.csv"] and left_out == {}


@pytest.mark.asyncio
async def test_a_string_that_only_looks_like_a_path_does_not_undo_leaving_a_file_out():
    paths, left_out = await _leaving_out(
        {
            "index.html": '<a href="a.csv">a</a><script>const x = "a.csv";</script>',
            "a.csv": "a",
        },
        "a.csv",
    )
    assert paths == ["index.html"]
    assert left_out == {"a.csv": REASON_LINK}


@pytest.mark.asyncio
async def test_the_entry_cannot_be_left_out_and_a_name_nothing_uses_is_not_offered():
    paths, left_out = await _leaving_out(
        {"index.html": "<p>x</p>"}, "index.html", "gone.html"
    )
    assert paths == ["index.html"]
    assert left_out == {}


@pytest.mark.asyncio
async def test_what_is_left_out_counts_toward_no_cap(monkeypatch):
    monkeypatch.setattr(mf, "MAX_BYTES", 32)
    paths, left_out = await _leaving_out(
        {"index.html": '<a href="big.html">b</a>', "big.html": "x" * 64}, "big.html"
    )
    assert paths == ["index.html"]
    assert list(left_out) == ["big.html"]


@pytest.mark.asyncio
async def test_a_page_past_the_link_limit_keeps_all_it_loads_and_none_of_its_links(reader):
    # A visitor who clicks through to b.html must see it whole, while a site's
    # navigation must not pull in every page it reaches.
    reader.files = FakeReader(
        {
            "index.html": '<a href="a.html">a</a>',
            "a.html": '<a href="b.html">b</a>',
            "b.html": """
                <img src="b.png"><link rel="stylesheet" href="b.css">
                <script src="b.js"></script><iframe src="frame.html"></iframe>
                <a href="both.png"><img src="both.png"></a>
                <a href="c.html">c</a><area href="d.html">
                <link rel="next" href="e.html">
                <meta http-equiv="refresh" content="0; url=f.html">
            """,
            "b.png": b"\x89PNG",
            "both.png": b"\x89PNG",
            "b.css": "x { background: url(b-bg.png) }",
            "b-bg.png": b"\x89PNG",
            "b.js": 'img.src = "b-js.png"',
            "b-js.png": b"\x89PNG",
            "frame.html": '<img src="frame.png"><a href="g.html">g</a>',
            "frame.png": b"\x89PNG",
            **{f"{page}.html": "linked" for page in "cdefg"},
        }
    ).files
    entries = await _build(reader, "index.html")
    assert set(_paths(entries)) == {
        "index.html",
        "a.html",
        "b.html",
        "b.png",
        "both.png",
        "b.css",
        "b-bg.png",
        "b.js",
        "b-js.png",
        "frame.html",
        "frame.png",
        # A refresh is no choice: it takes the visitor there at any depth.
        "f.html",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "markup, target, reason",
    [
        ("""<button onclick="fetch('a/t.json')">x</button>""", "a/t.json", REASON_SCRIPT),
        ("""<a onclick="location.href='a/t.html'">x</a>""", "a/t.html", REASON_SCRIPT),
        ("""<a href="javascript:fetch('a/t.json')">x</a>""", "a/t.json", REASON_SCRIPT),
        ("""<div data-src="a/t.svg"></div>""", "a/t.svg", REASON_SCRIPT),
        ("""<iframe srcdoc="<img src='a/t.png'>"></iframe>""", "a/t.png", REASON_PAGE),
        (
            """<iframe srcdoc="<script>fetch('a/t')</script>"></iframe>""",
            "a/t",
            REASON_SCRIPT,
        ),
        (
            """<iframe srcdoc="<p style='background:url(a/t.png)'></p>"></iframe>""",
            "a/t.png",
            REASON_STYLE,
        ),
        (
            """<script>s.textContent='h2{background:url(a/t.svg)}'</script>""",
            "a/t.svg",
            REASON_SCRIPT,
        ),
        ("""<script>fetch('a/t')</script>""", "a/t", REASON_SCRIPT),
        ("""<script>fetch('a/t t.json')</script>""", "a/t t.json", REASON_SCRIPT),
        ("""<meta http-equiv="refresh" content="0; url=a/t.html">""", "a/t.html", REASON_PAGE),
    ],
)
async def test_each_form_a_page_uses_is_listed_relative_to_it(reader, markup, target, reason):
    reader.files = FakeReader(
        {"r/index.html": markup, f"r/{target}": b"x", target: b"wrong folder"}
    ).files
    entries = await _build(reader, "r/index.html")
    assert _paths(entries) == ["r/index.html", f"r/{target}"]
    assert _reasons(entries)[f"r/{target}"] == reason


@pytest.mark.asyncio
async def test_a_script_two_pages_run_is_read_from_each_page(reader):
    # Its fetch resolves from the page running it, so the page in another
    # folder asks for that folder's file.
    reader.files = FakeReader(
        {
            "index.html": '<script src="shared.js"></script><iframe src="pages/details.html"></iframe>',
            "pages/details.html": '<script src="../shared.js"></script>',
            "shared.js": "fetch('data.json')",
            "data.json": b"{}",
            "pages/data.json": b"{}",
        }
    ).files
    entries = await _build(reader, "index.html")
    assert set(_paths(entries)) == {
        "index.html",
        "shared.js",
        "pages/details.html",
        "data.json",
        "pages/data.json",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["data.json", "data.geojson"])
async def test_paths_inside_a_json_file_resolve_from_it_and_from_its_page(reader, name):
    reader.files = FakeReader(
        {
            "r/index.html": f"""<script>fetch("d/{name}")</script>""",
            f"r/d/{name}": (
                '{"own": "own/x.png", "page": "page/y.png", "n": "12.5",'
                r' "escaped": "esc\/z.png", "gone": "nothing/w.png"}'
            ),
            "r/d/own/x.png": b"\x89PNG",
            "r/page/y.png": b"\x89PNG",
            "r/d/esc/z.png": b"\x89PNG",
        }
    ).files
    entries = await _build(reader, "r/index.html")
    assert set(_paths(entries)) == {
        "r/index.html",
        f"r/d/{name}",
        "r/d/own/x.png",
        "r/page/y.png",
        "r/d/esc/z.png",
    }
    reasons = _reasons(entries)
    assert reasons["r/d/own/x.png"] == reasons["r/page/y.png"] == REASON_SCRIPT


# --- walk_manifest: what the live page reported ---------------------------


@pytest.mark.asyncio
async def test_a_loaded_path_is_listed_and_parsed_in_turn(reader):
    reader.files = FakeReader(
        {
            "r/index.html": "<p>the page builds its paths in code</p>",
            "r/a.html": '<img src="a.png">',
            "r/a.png": b"\x89PNG",
            "r/s.css": "x { background: url(s.png) }",
            "r/s.png": b"\x89PNG",
            "r/j.js": 'img.src = "j.png"',
            "r/j.png": b"\x89PNG",
            "r/d.json": '{"image": "d.png"}',
            "r/d.png": b"\x89PNG",
            "r/plain.csv": "a,b",
        }
    ).files
    loaded = ["r/a.html", "r/s.css", "r/j.js", "r/d.json", "r/plain.csv"]
    entries = await _build(reader, "r/index.html", loaded)
    reasons = _reasons(entries)
    assert _paths(entries)[0] == "r/index.html"
    assert {p: reasons[p] for p in loaded} == dict.fromkeys(loaded, REASON_LOADED)
    assert reasons["r/a.png"] == REASON_PAGE
    assert reasons["r/s.png"] == REASON_STYLE
    assert reasons["r/j.png"] == reasons["r/d.png"] == REASON_SCRIPT


@pytest.mark.asyncio
async def test_a_path_found_statically_keeps_its_reason_however_deep(reader):
    # deep.json sits two levels down, where a seed that joined the first level
    # would reach it before the static walk did.
    reader.files = FakeReader(
        {
            "index.html": '<script src="app.js"></script>',
            "app.js": """fetch("deep.json")""",
            "deep.json": "{}",
        }
    ).files
    entries = await _build(reader, "index.html", ["deep.json", "app.js", "index.html"])
    assert _reasons(entries) == {
        "index.html": REASON_ENTRY,
        "app.js": REASON_PAGE,
        "deep.json": REASON_SCRIPT,
    }


@pytest.mark.asyncio
async def test_a_loaded_path_folds_and_cannot_leave_the_workspace(reader):
    reader.files = FakeReader(
        {
            "r/index.html": "<p>x</p>",
            "data/a.csv": "a",
            "data/b.csv": "b",
            "data/c.csv": "c",
            "escape.csv": "must only be reached by its own name",
            ".agents/x.txt": "hidden",
        }
    ).files
    found = await walk_manifest(
        Reader(reader, WORK_DIR, ()),
        "r/index.html",
        loaded=[
            f"{WORK_DIR}/data/a.csv",
            "/data/b.csv",
            f"file://{WORK_DIR}/data/c.csv",
            "../escape.csv",
            "data/../../escape.csv",
            ".agents/x.txt",
            "gone.csv",
            "https://cdn.example.com/x.js",
            "",
        ],
    )
    assert _paths(found.entries) == ["r/index.html", "data/c.csv"]
    # The page reports a workspace file without a leading "/": one with it is
    # an address the page built from its host's root, which no view serves.
    assert found.root_refs == [f"{WORK_DIR}/data/a.csv", "/data/b.csv"]


@pytest.mark.asyncio
async def test_a_loaded_path_is_a_decoded_name_not_a_url(reader):
    # The bridge decodes what it reports, so these characters are in the name.
    reader.files = FakeReader(
        {"index.html": "<p>x</p>", "a#1.csv": "a", "a b%20c.csv": "b", "a": "not this"}
    ).files
    entries = await _build(reader, "index.html", ["a#1.csv", "a b%20c.csv"])
    assert _paths(entries) == ["index.html", "a#1.csv", "a b%20c.csv"]


@pytest.mark.asyncio
async def test_loaded_paths_do_not_stand_in_for_a_missing_entry(reader):
    reader.files = {"a.csv": "a"}
    with pytest.raises(ManifestEntryMissing):
        await _build(reader, "nope.html", ["a.csv"])


# --- walk_manifest: Markdown ----------------------------------------------


@pytest.mark.asyncio
async def test_markdown_entry_lists_only_images_resolved_from_the_workspace_root(reader):
    reader.files = FakeReader(
        {
            "notes/a.md": (
                "![chart](img/c.png)\n"
                "[doc](other.md)\n"
                "[data](data.csv)\n"
                "![d][ref]\n"
                "[ref]: img/d.png\n"
                "[shot](img/f.png)\n"
                '<img src="img/e.svg">\n'
                "![remote](https://example.com/r.png)\n"
            ),
            "img/c.png": b"\x89PNG",
            "notes/img/c.png": b"\x89PNG",
            "img/d.png": b"\x89PNG",
            "img/f.png": b"\x89PNG",
            "img/e.svg": '<svg><image href="secret.png"/></svg>',
            "secret.png": b"\x89PNG",
            "other.md": "![](img/d.png)",
            "notes/other.md": "x",
            "data.csv": "a,b",
            "notes/data.csv": "a,b",
        }
    ).files
    entries = await _build(reader, "notes/a.md")
    # The viewer draws a link to an image file as the image: img/f.png is shown.
    assert set(_paths(entries)) == {
        "notes/a.md",
        "img/c.png",
        "img/d.png",
        "img/f.png",
        "img/e.svg",
    }
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
    entries = await _build(reader, "index.html")
    assert _paths(entries) == ["index.html", "notes.md"]
    assert "notes.md" not in reader.read_paths


# --- walk_manifest: other kinds, missing and hidden ----------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["data/table.csv", "report.pdf"])
async def test_a_plain_file_lists_only_itself(reader, entry):
    reader.files = {entry: b"a,b\n" * 3, "other.png": b"\x89PNG"}
    entries = await _build(reader, entry)
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
    entries = await _build(reader, "index.html")
    assert _paths(entries) == ["index.html", "here.png"]


@pytest.mark.asyncio
async def test_entry_that_is_absent_or_hidden_is_missing(reader):
    reader.files = {".agents/x.html": "<p>x</p>"}
    with pytest.raises(ManifestEntryMissing):
        await _build(reader, "nope.html")
    with pytest.raises(ManifestEntryMissing):
        await _build(reader, ".agents/x.html")


class LinkedReader(FakeReader):
    """A live tree's stat: a link reports the file it resolves to."""

    def __init__(self, files: dict[str, bytes | str], links: dict[str, str]) -> None:
        super().__init__(files)
        self.links = links

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        found = await super().stat(paths)
        return {p: (self.links.get(p, p), size) for p, (_, size) in found.items()}


@pytest.mark.asyncio
async def test_a_link_to_a_hidden_file_is_as_missing_as_no_file():
    # Listed under its own name, the read would refuse it on what it resolves
    # to, and every status and publish would fail on a file it cannot read.
    linked = LinkedReader(
        {
            "index.html": '<img src="chart.png"><script src="data.js"></script>',
            "chart.png": b"\x89PNG",
            "data.js": "x",
            "report.html": "<p>r</p>",
        },
        {"data.js": ".agents/data.js", "report.html": ".agents/report.html"},
    )

    assert _paths(await _build(linked, "index.html")) == ["index.html", "chart.png"]
    with pytest.raises(ManifestEntryMissing):
        await _build(linked, "report.html")


# --- walk_manifest: caps ---------------------------------------------------


@pytest.mark.asyncio
async def test_too_many_files_is_refused_not_truncated(reader, monkeypatch):
    monkeypatch.setattr(mf, "MAX_FILES", 3)
    reader.files = FakeReader(
        {
            "index.html": "".join(f'<img src="{i}.png">' for i in range(4)),
            **{f"{i}.png": b"\x89PNG" for i in range(4)},
        }
    ).files
    with pytest.raises(ManifestTooLarge) as exc:
        await _build(reader, "index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_files", 3)


@pytest.mark.asyncio
async def test_too_many_bytes_is_refused_not_truncated(reader, monkeypatch):
    monkeypatch.setattr(mf, "MAX_BYTES", 16)
    reader.files = {"index.html": '<img src="big.png">', "big.png": b"\x00" * 32}
    with pytest.raises(ManifestTooLarge) as exc:
        await _build(reader, "index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_bytes", 16)


@pytest.mark.asyncio
async def test_copies_past_the_listed_cap_name_that_cap(monkeypatch):
    """One file under several links stays under the file cap, but each path is
    stored, so the copies cross the listed cap, and that is the limit named."""
    monkeypatch.setattr(mf, "MAX_BYTES", 200)
    monkeypatch.setattr(mf, "MAX_LISTED_BYTES", 250)
    page = "".join(f'<script src="{n}.js"></script>' for n in "abcd")
    files = {"index.html": page, "store/lib.js": b"x" * 60}
    links = {f"{n}.js": "store/lib.js" for n in "abcd"}
    with pytest.raises(ManifestTooLarge) as exc:
        await _build(FakeReader(files, links=links), "index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_bytes", 250)


@pytest.mark.asyncio
async def test_references_past_the_candidate_cap_are_refused(reader, monkeypatch):
    monkeypatch.setattr(mf, "_MAX_CANDIDATES", 2)
    reader.files = FakeReader(
        {"index.html": "".join(f'<img src="{i}.png">' for i in range(3))}
    ).files
    with pytest.raises(ManifestTooLarge) as exc:
        await _build(reader, "index.html")
    assert exc.value.code == "too_many_files"


@pytest.mark.asyncio
async def test_a_loaded_path_counts_toward_the_candidate_cap(reader, monkeypatch):
    monkeypatch.setattr(mf, "_MAX_CANDIDATES", 2)
    reader.files = FakeReader({"index.html": "<p>x</p>"}).files
    with pytest.raises(ManifestTooLarge):
        await _build(reader, "index.html", ["a.csv", "b.csv"])


@pytest.mark.asyncio
async def test_guesses_past_their_budget_are_dropped_not_refused(reader, monkeypatch):
    # A data file of dotted strings is not a page naming a thousand files, and
    # refusing the publish over it would be worse than missing one.
    monkeypatch.setattr(mf, "_MAX_GUESSES", 2)
    reader.files = FakeReader(
        {
            "index.html": """
                <div data-a="d.png"></div>
                <script>x = ["a.png", "b.png"]</script>
                <img src="named.png">
            """,
            **{f"{name}.png": b"\x89PNG" for name in ("named", "a", "b", "d")},
        }
    ).files
    assert _paths(await _build(reader, "index.html")) == [
        "index.html",
        "named.png",
        "d.png",
        "a.png",
    ]


@pytest.mark.asyncio
async def test_a_name_decides_a_path_a_string_also_hints_at(reader):
    reader.files = FakeReader(
        {
            "index.html": '<script>x = "a.png"</script><img src="a.png">',
            "a.png": b"\x89PNG",
        }
    ).files
    assert _reasons(await _build(reader, "index.html"))["a.png"] == REASON_PAGE


@pytest.mark.asyncio
async def test_a_missing_path_is_probed_once_however_often_it_is_named(reader):
    probes: list[str] = []

    class CountingReader(FakeReader):
        async def stat(self, paths):
            probes.extend(paths)
            return await super().stat(paths)

    counting = CountingReader(
        {
            "index.html": '<img src="gone.png"><script src="a.js"></script>',
            "a.js": 'x = "gone.png"',
        }
    )
    await _build(counting, "index.html")
    assert probes.count("gone.png") == 1


@pytest.mark.asyncio
async def test_a_page_over_the_byte_cap_is_refused_and_never_read(reader):
    # No publish could carry it, so parsing it would be work for nothing.
    reader.files = {"index.html": b"x" * (25 * 1024 * 1024 + 1)}
    with pytest.raises(ManifestTooLarge) as exc:
        await _build(reader, "index.html")
    assert (exc.value.code, exc.value.limit) == ("too_many_bytes", 25 * 1024 * 1024)
    assert reader.read_paths == []


# --- walk_manifest: names --------------------------------------------------


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
    entries = await _build(reader, "报告/index.html")
    assert _paths(entries) == ["报告/index.html", "报告/图表.png", "报告/图表2.png"]


# --- walk_manifest: the live folder ----------------------------------------


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

    monkeypatch.setattr(reader_module, "acquire_sandbox_to_change", acquire)
    monkeypatch.setattr(reader_module, "owner_work_dir", lambda workspace: WORK_DIR)
    monkeypatch.setattr(reader_module, "previous_dir_names_of", lambda workspace: ())
    monkeypatch.setattr(reader_module, "_SandboxReader", lambda *args: live)

    running = {**WORKSPACE, "status": "running"}
    async with snapshot_reader(running, OWNER, ReadMode.OWNER) as held:
        entries = (await walk_manifest(held, "index.html")).entries

    assert _paths(entries) == ["index.html", "style.css"]
    assert events == ["held", "stat", "stat", "released"]
