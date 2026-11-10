"""Snapshot rules that keep a shared link from costing a read per request: a
link shared before versions is tried once under the caps it was shared with,
a keep-updated read never lands over a newer version, and a path whose file
may not be shared is as missing as one that is not there. What the owner left
out stays out of every cut, and a stored copy without a page's libraries never
stands in for the computer."""

from __future__ import annotations

import asyncio
import hashlib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.artifacts import Artifact, PublishResult, ReadBasis
from src.server.database.artifacts.versions import LeftOutChanged
from src.server.services.artifacts import manifest as manifest_mod
from src.server.services.artifacts import reader as reader_mod
from src.server.services.artifacts import dashboards, snapshot
from src.server.services.artifacts.manifest import MAX_FILES, ManifestEntryMissing
from src.server.services.artifacts.reader import NotInStoredCopy, Reader, ReadMode
from src.server.services.served_paths import may_be_artifact

_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


def _captured(*paths: str) -> SimpleNamespace:
    return SimpleNamespace(
        files=[SimpleNamespace(path=p) for p in paths],
        entries=[SimpleNamespace(path=p, reason="page") for p in paths],
        left_out=[],
        blobs={},
        title=None,
        absent=frozenset(),
    )


@pytest.fixture
def lands(monkeypatch):
    """The four ways a version lands, each answering that nothing changed."""
    monkeypatch.setattr(snapshot, "store_unregistered_blobs", AsyncMock())
    found = {}
    for kind in ("publish", "keep", "turn", "legacy"):
        found[kind] = AsyncMock(
            return_value=SimpleNamespace(artifact="landed", changed=False)
        )
        monkeypatch.setattr(snapshot.artifacts_db, f"{kind}_version", found[kind])
    return SimpleNamespace(**found)


def _artifact(**over) -> Artifact:
    fields = dict(
        artifact_id="00000000-0000-0000-0000-00000000000a",
        code="c0de",
        workspace_id="00000000-0000-0000-0000-00000000000f",
        path="reports/q3.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=True,
        latest_seq=0,
        shared_at=_NOW,
        files=("reports/q3.html", "reports/big.bin"),
        revision=1,
        created_at=_NOW,
    )
    fields.update(over)
    return Artifact(**fields)


class _Files:
    """The reader contract, over the files a test names. ``links`` maps a path
    to the file it resolves to."""

    def __init__(self, files: dict[str, bytes], links: dict[str, str]) -> None:
        self.files = files
        self.links = links

    async def stat(self, paths):
        return {
            p: (self.links.get(p, p), len(self.files[self.links.get(p, p)]))
            for p in paths
            if self.links.get(p, p) in self.files
        }

    async def read(self, path):
        return self.files.get(self.links.get(path, path))


@pytest.fixture
def workspace_holds(monkeypatch):
    """A capture that really walks and reads the files the test names."""
    monkeypatch.setattr(
        snapshot,
        "db_get_workspace",
        AsyncMock(return_value={"user_id": "u", "status": "stopped"}),
    )
    monkeypatch.setattr(
        snapshot, "get_vault_secrets_for_redaction", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        snapshot,
        "get_redactor",
        lambda: SimpleNamespace(redact_bytes=lambda data, vault_secrets: data),
    )
    monkeypatch.setattr(snapshot, "is_storage_enabled", lambda: False)

    def hold(files: dict[str, bytes], links: dict[str, str] | None = None) -> None:
        @asynccontextmanager
        async def reader(workspace, user_id, mode):
            yield Reader(_Files(files, links or {}), "/home/workspace/proj", ())

        monkeypatch.setattr(snapshot, "snapshot_reader", reader)

    return hold


@pytest.mark.asyncio
async def test_a_review_adds_what_the_page_says_to_what_it_was_published_with(
    workspace_holds,
):
    workspace_holds(
        {
            "reports/q3.html": b"<p>x</p>",
            "data/a.csv": b"a",
            "data/b.csv": b"b",
            "data/c.csv": b"c",
        }
    )

    found = await snapshot.status(
        _artifact(loaded_files=("data/a.csv", "data/b.csv")),
        user_id="u",
        loaded=("data/b.csv", "data/c.csv"),
    )

    assert [e.path for e in found.snapshot.entries] == [
        "reports/q3.html",
        "data/a.csv",
        "data/b.csv",
        "data/c.csv",
    ]


@pytest.mark.asyncio
async def test_a_review_names_what_the_page_loads_from_the_origin_root(workspace_holds):
    workspace_holds({"reports/q3.html": b'<script src="/assets/app.js"></script>'})

    found = await snapshot.status(_artifact(), user_id="u", loaded=())

    assert [f.path for f in found.snapshot.files] == ["reports/q3.html"]
    assert found.snapshot.root_refs == ["/assets/app.js"]


@pytest.mark.asyncio
async def test_a_file_the_owner_left_out_is_offered_back_and_not_in_the_version(
    workspace_holds,
):
    workspace_holds(
        {
            "reports/q3.html": b'<a href="other.html">o</a>',
            "reports/other.html": b"other",
        }
    )

    found = await snapshot.status(
        _artifact(left_out_files=("reports/other.html",)), user_id="u", loaded=()
    )

    assert [f.path for f in found.snapshot.files] == ["reports/q3.html"]
    assert [e.path for e in found.snapshot.left_out] == ["reports/other.html"]


@pytest.mark.asyncio
async def test_a_review_past_a_cap_offers_what_may_be_left_out_to_fit(
    workspace_holds, monkeypatch
):
    # A link to a download past the byte cap: the review still lists it, with
    # its box, and stays refused to publish until the owner leaves it out.
    monkeypatch.setattr(manifest_mod, "MAX_BYTES", 100)
    workspace_holds(
        {
            "reports/q3.html": b'<img src="a.png"><a href="raw.zip">raw</a>',
            "reports/a.png": b"\x89PNG",
            "reports/raw.zip": b"z" * 500,
        }
    )

    over = await snapshot.status(_artifact(), user_id="u", loaded=())

    assert over.snapshot is None
    assert (over.over_cap.code, over.over_cap.limit) == ("too_many_bytes", 100)
    assert [(e.path, e.optional) for e in over.over_cap.entries] == [
        ("reports/q3.html", False),
        ("reports/a.png", False),
        ("reports/raw.zip", True),
    ]
    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.publish(
            _artifact(),
            user_id="u",
            reviewed=[e.path for e in over.over_cap.entries],
            label=None,
            keep_updated=None,
            loaded=(),
        )
    assert refused.value.code == "too_many_bytes"

    fits = await snapshot.status(
        _artifact(left_out_files=("reports/raw.zip",)), user_id="u", loaded=()
    )
    assert fits.over_cap is None
    assert [f.path for f in fits.snapshot.files] == ["reports/q3.html", "reports/a.png"]
    assert [e.path for e in fits.snapshot.left_out] == ["reports/raw.zip"]


@pytest.mark.asyncio
async def test_only_what_the_page_alone_could_name_is_remembered(
    workspace_holds, lands
):
    # A file the source names drops out when the reference does, so it is not
    # kept with the record, however the page also came to load it.
    workspace_holds(
        {
            "reports/q3.html": b'<img src="named.png">',
            "reports/named.png": b"\x89PNG",
            "data/a.csv": b"a",
        }
    )
    await snapshot.publish(
        _artifact(),
        user_id="u",
        reviewed=["reports/q3.html", "reports/named.png", "data/a.csv"],
        label=None,
        keep_updated=None,
        loaded=("reports/named.png", "data/a.csv"),
    )

    assert lands.publish.await_args.kwargs["loaded_files"] == ["data/a.csv"]


@pytest.mark.asyncio
async def test_a_list_the_owner_did_not_review_is_refused_with_the_list_as_it_is(
    workspace_holds, lands
):
    workspace_holds({"reports/q3.html": b'<img src="new.png">', "reports/new.png": b"n"})

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.publish(
            _artifact(),
            user_id="u",
            reviewed=["reports/q3.html"],
            label=None,
            keep_updated=None,
            loaded=(),
        )

    assert refused.value.code == "files_changed"
    assert [e.path for e in refused.value.files] == [
        "reports/q3.html",
        "reports/new.png",
    ]
    lands.publish.assert_not_awaited()


@pytest.fixture
def legacy(monkeypatch):
    monkeypatch.setattr(
        snapshot,
        "db_get_workspace",
        AsyncMock(return_value={"user_id": "u", "status": "stopped"}),
    )
    capture = AsyncMock(side_effect=snapshot.SnapshotRefused("too_many_bytes"))
    refuse = AsyncMock()
    published = AsyncMock()
    monkeypatch.setattr(snapshot, "_capture", capture)
    monkeypatch.setattr(snapshot.artifacts_db, "refuse_legacy", refuse)
    monkeypatch.setattr(snapshot, "publish_artifact_change", published)
    return capture, refuse, published


def test_a_legacy_link_keeps_the_caps_it_was_shared_under():
    assert (snapshot._LEGACY_CAPS.files, snapshot._LEGACY_CAPS.bytes) == (
        MAX_FILES,
        256 * 1024 * 1024,
    )
    assert snapshot._VERSION_CAPS.bytes == snapshot.MAX_VERSION_BYTES
    assert snapshot._VERSION_CAPS.bytes < snapshot._LEGACY_CAPS.bytes


@pytest.mark.asyncio
async def test_a_legacy_link_past_the_caps_is_recorded_and_told_once(legacy):
    capture, refuse, published = legacy
    refuse.side_effect = [_artifact(legacy_refused="too_many_bytes"), None]

    for _ in range(2):
        with pytest.raises(snapshot.LegacyRefused):
            await snapshot.legacy_snapshot(_artifact())

    assert capture.await_args.kwargs["source"].caps is snapshot._LEGACY_CAPS
    assert refuse.await_count == 2
    published.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [True, False])
async def test_other_views_hear_of_a_version_only_when_it_changed_the_record(
    monkeypatch, changed
):
    record = _artifact(latest_seq=1)
    published = AsyncMock()
    monkeypatch.setattr(snapshot, "publish_artifact_change", published)

    await snapshot._announce(
        "u",
        SimpleNamespace(files=[], total=0),
        PublishResult(record, 1, False, False, changed),
    )

    assert published.await_count == (1 if changed else 0)


@pytest.mark.asyncio
async def test_an_owners_publish_names_what_its_read_began_from(monkeypatch, lands):
    record = _artifact(latest_seq=7, revision=3, left_out_files=("x.html",))
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(record.path))
    )

    await snapshot.publish(
        record,
        user_id="u",
        reviewed=[record.path],
        label=None,
        keep_updated=None,
        loaded=(),
    )

    assert lands.publish.await_args.kwargs["read"] == ReadBasis(
        3, 7, frozenset({"x.html"})
    )


@pytest.mark.asyncio
async def test_a_recorded_refusal_reads_nothing(legacy):
    capture, refuse, _ = legacy

    with pytest.raises(snapshot.LegacyRefused):
        await snapshot.legacy_snapshot(_artifact(legacy_refused="too_many_files"))

    capture.assert_not_awaited()
    refuse.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_missing_legacy_file_is_not_remembered(legacy):
    capture, refuse, _ = legacy
    capture.side_effect = snapshot.SnapshotRefused("file_missing")

    with pytest.raises(snapshot.LegacyRefused):
        await snapshot.legacy_snapshot(_artifact())

    refuse.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_keep_updated_cut_is_based_on_the_version_before_the_read(
    monkeypatch, lands
):
    artifact = _artifact(latest_seq=7)
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(*artifact.files))
    )

    await snapshot._keep_one(artifact, "u")

    assert lands.turn.await_args.kwargs["read"].latest_seq == 7


@pytest.mark.asyncio
async def test_keep_updated_cuts_no_version_without_a_listed_file(monkeypatch, lands):
    """The list never widens on its own, so a version cut while a file is being
    rebuilt would leave the link without it for good."""
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured("reports/q3.html"))
    )

    await snapshot._keep_one(_artifact(), "u")

    lands.turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_past_one_read_is_refused_rather_than_unavailable():
    """It will not fit next time either, so a legacy link records the refusal
    instead of answering 503 and reading everything again on each view."""
    reader = SimpleNamespace(files=SimpleNamespace(read=AsyncMock(return_value=None)))
    big = SimpleNamespace(path="big.bin", size=snapshot.EXEC_READ_MAX_BYTES + 1)
    small = SimpleNamespace(path="gone.bin", size=1)

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot._read_all(reader, [big], {})
    assert refused.value.code == "too_many_bytes"
    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot._read_all(reader, [small], {})


def test_a_title_is_read_in_one_pass():
    """On the event loop, on anyone's bytes: a pattern tried at each of a run
    of unclosed tags took seconds."""
    assert snapshot._html_title(b'<title lang="en">Q3 &amp; Q4</TITLE >') == "Q3 & Q4"
    assert snapshot._html_title(b"<title>open") is None
    started = time.monotonic()
    assert snapshot._html_title(b"<title>" * 40_000) is None
    assert time.monotonic() - started < 5


class _LinkedReader:
    def __init__(self, found: dict[str, tuple[str, int]]) -> None:
        self.found = found

    async def stat(self, paths):
        return {p: self.found[p] for p in paths if p in self.found}


@pytest.mark.asyncio
async def test_a_listed_path_resolving_to_a_hidden_file_is_left_out():
    linked = _LinkedReader(
        {
            "reports/q3.html": ("reports/q3.html", 10),
            "reports/data.json": (".agents/data.json", 5),
            "reports/style.css": ("reports/style.css", 3),
        }
    )
    reader = Reader(linked, "/home/workspace/proj", ())
    artifact = _artifact(files=("reports/q3.html", "reports/data.json"))

    entries = (
            await snapshot._Listed(
                ("reports/data.json", "reports/style.css")
            ).manifest(reader, artifact)
        ).entries

    assert [e.path for e in entries] == ["reports/q3.html", "reports/style.css"]

    linked.found["reports/q3.html"] = (".agents/q3.html", 10)
    with pytest.raises(ManifestEntryMissing):
        await snapshot._Listed(()).manifest(reader, artifact)


def test_a_path_past_what_the_schema_holds_may_not_be_an_artifact():
    """It would get a record, then fail the check on its first comment or
    version with a 500."""
    assert may_be_artifact("reports/" + "a" * 1016)
    assert not may_be_artifact("reports/" + "a" * 1017)


# --- what the owner left out --------------------------------------------------


@pytest.mark.asyncio
async def test_a_turns_end_cut_leaves_out_what_the_owner_took_off(monkeypatch, lands):
    """It reads the frozen list rather than walking, so the list must lose the
    page itself, and the page's absence is not a listed file gone."""
    artifact = _artifact(
        latest_seq=3,
        files=("reports/q3.html", "reports/other.html"),
        left_out_files=("reports/other.html",),
    )
    capture = AsyncMock(return_value=_captured("reports/q3.html"))
    monkeypatch.setattr(snapshot, "_capture", capture)

    await snapshot._keep_one(artifact, "u")

    assert capture.await_args.kwargs["source"].paths == ("reports/q3.html",)
    lands.turn.assert_awaited_once()


def _linked_page(page: bytes) -> Reader:
    """``reports/link.png`` is a symlink to ``data/real.png``."""
    files = {"reports/q3.html": page, "data/real.png": b"png"}
    return Reader(
        _Files(files, {"reports/link.png": "data/real.png"}), "/home/workspace/proj", ()
    )


_BOTH_PATHS = ("reports/q3.html", "reports/link.png", "data/real.png")


@pytest.mark.parametrize("left", ["reports/link.png", "data/real.png"])
@pytest.mark.asyncio
async def test_a_file_left_out_by_one_path_goes_by_every_path_a_link_reached(left):
    """A walk leaves the file out under either spelling, so the frozen list
    may not keep the other one: the version would still hold the file."""
    reader = _linked_page(b'<a href="link.png">chart</a>')
    artifact = _artifact(files=_BOTH_PATHS, left_out_files=(left,))

    found = await snapshot._Listed(snapshot._frozen_list(artifact)).manifest(
        reader, artifact
    )

    assert [e.path for e in found.entries] == ["reports/q3.html"]
    assert [e.path for e in found.left_out] == [p for p in _BOTH_PATHS[1:] if p != left]
    assert found.byte_total == len(b'<a href="link.png">chart</a>')


@pytest.mark.asyncio
async def test_a_path_the_page_loads_stays_when_another_path_to_its_file_is_left_out():
    page = b'<a href="link.png">chart</a><img src="../data/real.png">'
    artifact = _artifact(files=_BOTH_PATHS, left_out_files=("reports/link.png",))

    found = await snapshot._Listed(snapshot._frozen_list(artifact)).manifest(
        _linked_page(page), artifact
    )

    assert [e.path for e in found.entries] == ["reports/q3.html", "data/real.png"]
    assert found.left_out == []


@pytest.mark.asyncio
async def test_a_turns_end_cut_drops_the_other_path_and_still_cuts(
    workspace_holds, lands
):
    workspace_holds(
        {"reports/q3.html": b'<a href="link.png">chart</a>', "data/real.png": b"png"},
        {"reports/link.png": "data/real.png"},
    )
    await snapshot._keep_one(
        _artifact(files=_BOTH_PATHS, left_out_files=("reports/link.png",)), "u"
    )

    kwargs = lands.turn.await_args.kwargs
    assert [f.path for f in kwargs["files"]] == ["reports/q3.html"]
    # Left out with it, so visitors still follow the cut.
    assert kwargs["left_out"] == ["data/real.png"]


@pytest.mark.asyncio
async def test_a_turns_end_cut_steps_aside_for_a_file_left_out_during_its_read(
    monkeypatch, lands
):
    artifact = _artifact(left_out_files=("reports/other.html",))
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(*artifact.files))
    )
    lands.turn.side_effect = LeftOutChanged

    await snapshot._keep_one(artifact, "u")

    assert lands.turn.await_args.kwargs["read"].left_out == {"reports/other.html"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raced",
    [
        LeftOutChanged,
        snapshot.artifacts_db.VersionMoved,
        snapshot.artifacts_db.SharingChanged,
    ],
)
async def test_a_legacy_cut_raced_by_the_record_waits_for_the_next_view(
    legacy, lands, raced
):
    capture, refuse, _ = legacy
    capture.side_effect = None
    capture.return_value = _captured("reports/q3.html", "reports/big.bin")
    lands.legacy.side_effect = raced

    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot.legacy_snapshot(_artifact(left_out_files=("reports/x.html",)))

    assert lands.legacy.await_args.kwargs["read"].left_out == {"reports/x.html"}
    refuse.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_reached_through_a_link_counts_once_against_the_byte_cap(
    workspace_holds,
):
    """pnpm links every package into ``node_modules``, so a page names its
    library by a path that resolves to another: both are listed, as one file's
    bytes."""
    page = b'<script src="lib.js"></script><script src="alt.js"></script>'
    workspace_holds(
        {"reports/q3.html": page, "store/lib.js": b"x" * 60},
        links={"reports/lib.js": "store/lib.js", "reports/alt.js": "store/lib.js"},
    )
    unique = len(page) + 60

    found = await snapshot._capture(
        _artifact(),
        user_id="u",
        mode=snapshot.ReadMode.OWNER,
        source=snapshot._Walked(
            caps=snapshot._Caps(files=10, bytes=unique, listed=2 * unique)
        ),
    )

    assert [f.path for f in found.files] == [
        "reports/q3.html",
        "reports/lib.js",
        "store/lib.js",
        "reports/alt.js",
    ]


class _EditedAfterOneRead(dict):
    """A workspace whose page the agent rewrites the moment it is first read."""

    def __init__(self, files: dict[str, bytes], path: str, after: bytes) -> None:
        super().__init__(files)
        self.path, self.after, self.reads = path, after, 0

    def get(self, key, default=None):
        value = super().get(key, default)
        if key == self.path:
            self.reads += 1
            self[key] = self.after
        return value


@pytest.mark.asyncio
async def test_a_version_holds_the_bytes_its_list_was_built_from(workspace_holds):
    before = b'<img src="chart.png">'
    files = _EditedAfterOneRead(
        {"reports/q3.html": before, "reports/chart.png": b"png"},
        "reports/q3.html",
        b'<img src="chart.png"><img src="added.png">',
    )
    workspace_holds(files)

    found = await snapshot._capture(
        _artifact(), user_id="u", mode=snapshot.ReadMode.OWNER, source=snapshot._Walked()
    )

    page = next(f for f in found.files if f.path == "reports/q3.html")
    assert page.sha256 == hashlib.sha256(before).hexdigest()
    assert [f.path for f in found.files] == ["reports/q3.html", "reports/chart.png"]
    assert files.reads == 1


@pytest.mark.asyncio
async def test_one_file_named_through_many_links_stops_at_what_is_read(workspace_holds):
    """Each path is read and stored as a file of its own, so one file under
    many links costs as many copies, whatever its own size."""
    names = ("a", "b", "c", "d")
    page = "".join(f'<script src="{n}.js"></script>' for n in names).encode()
    workspace_holds(
        {"reports/q3.html": page, "store/lib.js": b"x" * 60},
        links={f"reports/{n}.js": "store/lib.js" for n in names},
    )
    unique = len(page) + 60

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot._capture(
            _artifact(),
            user_id="u",
            mode=snapshot.ReadMode.OWNER,
            source=snapshot._Walked(
                caps=snapshot._Caps(files=10, bytes=unique, listed=2 * unique)
            ),
        )
    assert refused.value.code == "too_many_bytes"
    # The copies crossed the cap, so that is the limit the owner is shown.
    assert refused.value.limit == 2 * unique


@pytest.mark.asyncio
async def test_a_legacy_links_first_cut_leaves_out_what_the_owner_took_off(legacy):
    capture, _, _ = legacy
    capture.side_effect = snapshot.SnapshotUnavailable("stop after the list")

    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot.legacy_snapshot(
            _artifact(left_out_files=("reports/big.bin",))
        )

    assert capture.await_args.kwargs["source"].paths == ("reports/q3.html",)


async def _page_code(loaded: tuple[str, ...] = (), earlier: tuple[str, ...] = ()) -> str:
    found = await snapshot._capture(
        _artifact(),
        user_id="u",
        mode=snapshot.ReadMode.OWNER,
        source=snapshot._Walked(loaded=loaded, earlier=earlier),
    )
    return found.page_sha256


_PAGE = {
    "reports/q3.html": b'<script src="app.js"></script><a href="more.html">m</a>',
    "reports/app.js": b"import('./mod.js'); fetch('data/' + t + '.csv')",
    "reports/mod.js": b"fetch('a.csv')",
    "reports/more.html": b"more",
    "reports/data/a.csv": b"a",
}


@pytest.mark.asyncio
async def test_the_page_code_moves_with_the_page_and_what_it_runs_and_nothing_else(
    workspace_holds,
):
    workspace_holds(dict(_PAGE))
    first = await _page_code()
    # A module another script imports counts, loaded or not yet.
    edits = (
        ("reports/q3.html", b"<p>new</p>"),
        ("reports/app.js", b"import('./mod.js'); fetch('x.csv')"),
        ("reports/mod.js", b"fetch('data/' + id + '.json')"),
    )
    for path, edit in edits:
        workspace_holds({**_PAGE, path: edit})
        assert await _page_code() != first, path
    for path, edit in (("reports/more.html", b"edited"), ("reports/data/a.csv", b"b")):
        workspace_holds({**_PAGE, path: edit})
        assert await _page_code() == first, path
    # A report adds what the page loads, not what it is, and a module it loaded
    # holds its place once the document that loaded it is gone.
    workspace_holds({**_PAGE, "reports/data/c.png": b"\x89PNG"})
    assert await _page_code(loaded=("reports/data/c.png",)) == first
    workspace_holds({**_PAGE, "reports/data/c.png": b"\x89PNG2"})
    assert await _page_code(loaded=("reports/data/c.png",)) == first
    # Text data it loaded is read like a script: its next path may come from it.
    workspace_holds(dict(_PAGE))
    with_data = await _page_code(loaded=("reports/data/a.csv",))
    assert with_data != first
    workspace_holds({**_PAGE, "reports/data/a.csv": b"MSFT"})
    assert await _page_code(loaded=("reports/data/a.csv",)) != with_data
    workspace_holds(dict(_PAGE))
    assert await _page_code(loaded=("reports/mod.js",)) == first
    assert await _page_code(earlier=("reports/mod.js",)) == first


@pytest.mark.asyncio
async def test_data_of_any_type_the_page_loaded_moves_the_page_code(workspace_holds):
    """A script may build its next path from a file of any type, or none."""
    for name in ("reports/config.ini", "reports/data/settings"):
        workspace_holds({**_PAGE, name: b"next=a"})
        before = await _page_code(loaded=(name,))
        workspace_holds({**_PAGE, name: b"next=b"})
        assert await _page_code(loaded=(name,)) != before, name
    # A font is drawn, never read for a path.
    workspace_holds({**_PAGE, "reports/f.woff2": b"wOF2a"})
    before = await _page_code(loaded=("reports/f.woff2",))
    workspace_holds({**_PAGE, "reports/f.woff2": b"wOF2b"})
    assert await _page_code(loaded=("reports/f.woff2",)) == before


@pytest.mark.asyncio
async def test_a_publish_is_refused_once_the_page_code_moved_from_what_was_reviewed(
    workspace_holds, lands
):
    workspace_holds(dict(_PAGE))
    reviewed = await _page_code()
    publish = dict(
        user_id="u",
        reviewed=["reports/q3.html", "reports/app.js", "reports/more.html"],
        label=None,
        keep_updated=None,
        loaded=(),
    )
    workspace_holds({**_PAGE, "reports/app.js": b"fetch('elsewhere.csv')"})

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.publish(_artifact(), **publish, reviewed_page=reviewed)
    assert refused.value.code == "files_changed"
    lands.publish.assert_not_awaited()
    # A tab on the previous build sends none, and publishes as it did.
    await snapshot.publish(_artifact(), **publish)
    lands.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_publish_keeps_only_the_left_out_paths_the_page_still_names(
    workspace_holds, lands
):
    """A path no page names any more would hold a place under the cap with no
    box in the review to clear it."""
    workspace_holds(
        {
            "reports/q3.html": b'<a href="other.html">o</a>',
            "reports/other.html": b"other",
        }
    )
    record = _artifact(left_out_files=("reports/gone.html", "reports/other.html"))

    await snapshot.publish(
        record,
        user_id="u",
        reviewed=["reports/q3.html"],
        label=None,
        keep_updated=None,
        loaded=(),
    )

    kwargs = lands.publish.await_args.kwargs
    assert kwargs["read"].left_out == {"reports/gone.html", "reports/other.html"}
    assert kwargs["left_out_files"] == ["reports/other.html"]


@pytest.mark.asyncio
async def test_a_file_left_out_during_a_publish_is_the_changed_list(
    workspace_holds, monkeypatch
):
    workspace_holds({"reports/q3.html": b"<p>x</p>"})
    monkeypatch.setattr(snapshot, "store_unregistered_blobs", AsyncMock())
    monkeypatch.setattr(
        snapshot.artifacts_db,
        "publish_version",
        AsyncMock(side_effect=LeftOutChanged),
    )

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.publish(
            _artifact(),
            user_id="u",
            reviewed=["reports/q3.html"],
            label=None,
            keep_updated=None,
            loaded=(),
        )

    assert refused.value.code == "files_changed"
    assert [e.path for e in refused.value.files] == ["reports/q3.html"]


# --- the stored copy ----------------------------------------------------------


@pytest.fixture
def stored_copy(monkeypatch):
    """The stored copy of a stopped computer, holding the files a test names."""

    def hold(files: dict[str, bytes]) -> reader_mod._MirrorReader:
        monkeypatch.setattr(
            reader_mod.FilePersistenceService,
            "get_file_tree",
            AsyncMock(
                return_value=[{"path": p, "size": len(b)} for p, b in files.items()]
            ),
        )

        async def fallback(workspace, workspace_id, path, mime):
            return (files[path], "") if path in files else None

        monkeypatch.setattr(reader_mod, "db_fallback_bytes", fallback)
        return reader_mod._MirrorReader({"workspace_id": "w"})

    return hold


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ["node_modules/chart/index.js", "app/vendor/lib.js", ".next/static/a.js"]
)
async def test_the_stored_copy_will_not_say_whether_a_library_file_exists(
    stored_copy, path
):
    mirror = stored_copy({"reports/q3.html": b"x"})

    with pytest.raises(NotInStoredCopy):
        await mirror.stat(["reports/q3.html", path])
    with pytest.raises(NotInStoredCopy):
        await mirror.read(path)


@pytest.mark.asyncio
async def test_a_file_named_like_a_library_folder_is_still_the_stored_copys(
    stored_copy,
):
    mirror = stored_copy({"reports/vendor": b"v"})

    assert await mirror.stat(["reports/vendor"]) == {"reports/vendor": ("reports/vendor", 1)}
    assert await mirror.read("reports/vendor") == b"v"


@pytest.fixture
def stopped_computer(workspace_holds, stored_copy, monkeypatch):
    def hold(files: dict[str, bytes]) -> None:
        mirror = stored_copy(files)

        @asynccontextmanager
        async def reader(workspace, user_id, mode):
            yield Reader(mirror, "/home/workspace/proj", ())

        monkeypatch.setattr(snapshot, "snapshot_reader", reader)

    return hold


@pytest.mark.asyncio
async def test_an_owners_review_of_a_page_with_libraries_needs_its_computer(
    stopped_computer,
):
    stopped_computer({"reports/q3.html": b'<script src="vendor/lib.js"></script>'})

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.status(_artifact(), user_id="u", loaded=())

    assert refused.value.code == "needs_computer"


@pytest.mark.asyncio
async def test_a_library_file_the_owner_left_out_does_not_need_the_computer(
    stopped_computer,
):
    stopped_computer(
        {"reports/q3.html": b'<script>fetch("vendor/lib.js"); fetch("more.json")</script>', "reports/more.json": b"{}"}
    )
    record = _artifact(left_out_files=("reports/vendor/lib.js", "reports/more.json"))

    found = await snapshot.status(record, user_id="u", loaded=())

    # Kept as they left it; only its size is unknown, and a file the stored
    # copy holds still says its own.
    assert {e.path: e.size for e in found.snapshot.left_out} == {
        "reports/vendor/lib.js": None,
        "reports/more.json": 2,
    }


@pytest.mark.asyncio
async def test_a_visitors_first_cut_without_the_libraries_is_not_made(
    stopped_computer,
):
    stopped_computer({"reports/q3.html": b"x"})

    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot._capture(
            _artifact(),
            user_id="u",
            mode=ReadMode.WARM_OR_STORED,
            source=snapshot._Listed(("reports/q3.html", "reports/vendor/lib.js")),
        )


def _kept(**over) -> Artifact:
    """A record kept for its owner alone, its last version holding the page
    and the script it runs. Not shared, so it has no visitors' list."""
    fields = dict(
        shared_at=None,
        kept_at=_NOW,
        keep_updated=False,
        latest_seq=2,
        files=None,
    )
    fields.update(over)
    return _artifact(**fields)


_RUNS_DATA = b'<html><script src="data.js"></script></html>'


@pytest.fixture
def held(monkeypatch):
    """The latest version's paths, which a kept record reads off the version."""
    hashes = AsyncMock(return_value={"reports/data.js": "h", "reports/q3.html": "h"})
    monkeypatch.setattr(snapshot.artifacts_db, "get_version_hashes", hashes)
    return hashes


@pytest.mark.asyncio
async def test_a_turns_end_cuts_no_dashboard_version_while_a_file_it_held_is_gone(
    workspace_holds, held, lands
):
    """The walk drops a file that is not there without a word; a version cut
    then (a rebuild, a failed turn) is the Dashboard without its data."""
    workspace_holds({"reports/q3.html": _RUNS_DATA})

    await snapshot._keep_one(_kept(), "u")

    lands.turn.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_the_page_stopped_naming_leaves_the_dashboards_next_version(
    workspace_holds, held, lands
):
    workspace_holds({"reports/q3.html": b"<html>no script</html>"})

    await snapshot._keep_one(_kept(), "u")

    lands.turn.assert_awaited_once()
    assert [f.path for f in lands.turn.await_args.kwargs["files"]] == ["reports/q3.html"]


@pytest.mark.asyncio
async def test_a_turns_end_cut_steps_aside_for_a_share_changing_during_its_read(
    workspace_holds, held, lands
):
    """A share changes what the cut may give visitors; the next turn's end
    looks again."""
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"1"})
    lands.turn.side_effect = snapshot.artifacts_db.SharingChanged

    await snapshot._keep_one(_kept(revision=5), "u")

    lands.turn.assert_awaited_once()
    assert lands.turn.await_args.kwargs["read"] == ReadBasis(5, 2, frozenset())


@pytest.mark.asyncio
async def test_a_turns_end_cut_looks_once_more_when_a_version_lands_during_its_read(
    workspace_holds, held, lands, monkeypatch
):
    """Two turns ending together: the one that read first can land first, and
    its version is then older than the bytes the other holds."""
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"1"})
    lands.turn.side_effect = snapshot.artifacts_db.VersionMoved
    moved = _kept(revision=5, latest_seq=3)
    monkeypatch.setattr(snapshot.artifacts_db, "get_artifact", AsyncMock(return_value=moved))

    await snapshot._keep_one(_kept(revision=5), "u")

    assert [c.kwargs["read"] for c in lands.turn.await_args_list] == [
        ReadBasis(5, 2, frozenset()),
        ReadBasis(5, 3, frozenset()),
    ]


@pytest.mark.asyncio
async def test_keeping_cuts_the_first_version_in_the_write_that_keeps_it(
    workspace_holds, lands
):
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"1"})
    kept = _kept(kept_at=None, latest_seq=0, files=None, revision=3)

    assert await snapshot.keep(kept, user_id="u") == "landed"
    assert lands.keep.await_args.kwargs["read"] == ReadBasis(3, 0, frozenset())


@pytest.mark.asyncio
async def test_keeping_takes_what_the_open_page_loaded_and_remembers_it(
    workspace_holds, lands
):
    """A file the page fetches by a path built in code is named only by its
    report: without it the kept copy answers 404 for it while the computer is
    off, and every turn's cut after leaves it out too."""
    workspace_holds(
        {"reports/q3.html": b"<p>x</p>", "data/a.csv": b"a", "data/b.csv": b"b"}
    )

    await snapshot.keep(
        _kept(kept_at=None, loaded_files=("data/a.csv",)),
        user_id="u",
        loaded=("data/b.csv",),
    )

    kwargs = lands.keep.await_args.kwargs
    assert [f.path for f in kwargs["files"]] == [
        "reports/q3.html",
        "data/a.csv",
        "data/b.csv",
    ]
    assert kwargs["loaded_files"] == ["data/a.csv", "data/b.csv"]


@pytest.mark.asyncio
async def test_a_keep_raced_by_a_left_out_file_is_the_changed_list(
    workspace_holds, lands
):
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"1"})
    lands.keep.side_effect = LeftOutChanged

    with pytest.raises(snapshot.SnapshotRefused) as refused:
        await snapshot.keep(_kept(kept_at=None), user_id="u")
    assert refused.value.code == "files_changed"


@pytest.mark.asyncio
async def test_keeping_a_shared_record_walks_and_cuts_like_an_unshared_one(
    workspace_holds, lands
):
    """Its visitors stay on their version (``keep_version`` never moves it);
    the Dashboard gets the page as it is now."""
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"1"})
    shared = _artifact(latest_seq=3, shared_seq=3, files=("reports/q3.html",))

    assert await snapshot.keep(shared, user_id="u") == "landed"
    kwargs = lands.keep.await_args.kwargs
    assert kwargs["read"].latest_seq == 3
    assert [f.path for f in kwargs["files"]] == [
        "reports/q3.html",
        "reports/data.js",
    ]


@pytest.mark.asyncio
async def test_a_kept_and_shared_record_is_walked_at_a_turns_end(
    workspace_holds, held, lands
):
    """The Dashboard follows the newest copy, a file the page starts reading
    with it; whether visitors move is ``visitors_follow_turn``'s to decide."""
    workspace_holds({"reports/q3.html": _RUNS_DATA, "reports/data.js": b"2"})
    record = _kept(
        shared_at=_NOW,
        keep_updated=True,
        latest_seq=4,
        shared_seq=3,
        files=("reports/q3.html",),
    )

    await snapshot._keep_one(record, "u")

    assert [f.path for f in lands.turn.await_args.kwargs["files"]] == [
        "reports/q3.html",
        "reports/data.js",
    ]
    # The record's list is the shared version's, not the latest's.
    held.assert_awaited_once_with(record.artifact_id, 4)


@pytest.mark.asyncio
async def test_a_kept_walk_carries_what_the_owner_left_out_to_the_visitors_rule(
    workspace_holds, held, lands
):
    """The walk drops a file left out after the publish; the cut names it, so
    visitors still follow (``visitors_follow_turn``)."""
    workspace_holds(
        {
            "reports/q3.html": b'<a href="other.html">o</a>',
            "reports/other.html": b"other",
        }
    )
    record = _kept(
        shared_at=_NOW,
        keep_updated=True,
        latest_seq=4,
        shared_seq=4,
        files=("reports/q3.html", "reports/other.html"),
        left_out_files=("reports/other.html",),
    )

    await snapshot._keep_one(record, "u")

    kwargs = lands.turn.await_args.kwargs
    assert [f.path for f in kwargs["files"]] == ["reports/q3.html"]
    assert kwargs["left_out"] == ["reports/other.html"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shared_at, against", [(_NOW, 3), (None, 4)], ids=["shared", "private"]
)
async def test_status_drift_is_against_what_a_publish_would_replace(
    workspace_holds, monkeypatch, shared_at, against
):
    """Shared, the version visitors see, not a newer one cut for the Dashboard;
    otherwise the latest, which a publish of the same files resumes at."""
    workspace_holds({"reports/q3.html": b"<html></html>"})
    hashes = AsyncMock(return_value={"reports/q3.html": "old"})
    monkeypatch.setattr(snapshot.artifacts_db, "get_version_hashes", hashes)
    record = _kept(
        shared_at=shared_at,
        latest_seq=4,
        shared_seq=3 if shared_at else 0,
        files=("reports/q3.html",) if shared_at else None,
    )

    found = await snapshot.status(record, user_id="u", loaded=())

    hashes.assert_awaited_once_with(record.artifact_id, against)
    assert (found.latest_seq, found.changed) == (4, True)


# --- a kept page off the Dashboard list, and a change outside a turn -------------

_LISTED = "00000000-0000-0000-0000-0000000000b1"
_OFF = "00000000-0000-0000-0000-0000000000b2"
_LONG_AGO = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def pass_over(monkeypatch):
    """A running workspace holding the records a test names, each cut by a
    stub, and the release answering with what the test says it let go."""
    found = SimpleNamespace(
        kept=AsyncMock(return_value=[]),
        listed=AsyncMock(return_value=frozenset({_LISTED})),
        release=AsyncMock(return_value=[]),
        cut=AsyncMock(),
        announced=AsyncMock(),
    )
    monkeypatch.setattr(snapshot.artifacts_db, "list_kept_updated", found.kept)
    monkeypatch.setattr(snapshot.artifacts_db, "release_unlisted", found.release)
    monkeypatch.setattr(snapshot.dashboards, "listed_pages", found.listed)
    monkeypatch.setattr(snapshot, "_keep_one", found.cut)
    monkeypatch.setattr(snapshot, "publish_artifact_change", found.announced)
    monkeypatch.setattr(
        snapshot,
        "db_get_workspace",
        AsyncMock(return_value={"user_id": "u", "status": "running"}),
    )
    return found


def _cut_ids(found) -> list[str]:
    return [c.args[0].artifact_id for c in found.cut.await_args_list]


@pytest.mark.asyncio
async def test_a_kept_page_off_the_list_is_let_go_after_the_grace_not_cut(pass_over):
    listed, off = _kept(artifact_id=_LISTED, kept_at=_LONG_AGO), _kept(
        artifact_id=_OFF, kept_at=_LONG_AGO
    )
    pass_over.kept.return_value = [listed, off]
    pass_over.release.return_value = [_kept(artifact_id=_OFF, kept_at=None)]

    await snapshot.keep_updated_after_turn(workspace_id="w", user_id="u")

    kwargs = pass_over.release.await_args.kwargs
    assert kwargs["listed"] == frozenset({_LISTED})
    assert kwargs["grace"] == snapshot.KEPT_RELEASE_GRACE
    assert _cut_ids(pass_over) == [_LISTED]
    pass_over.announced.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_page_kept_inside_the_grace_is_left_alone(pass_over):
    """A pin keeps the page a moment before it lists it."""
    pass_over.kept.return_value = [
        _kept(artifact_id=_OFF, kept_at=datetime.now(timezone.utc))
    ]

    await snapshot.keep_updated_after_turn(workspace_id="w", user_id="u")

    pass_over.listed.assert_not_awaited()
    pass_over.release.assert_not_awaited()
    assert _cut_ids(pass_over) == [_OFF]


@pytest.mark.asyncio
async def test_a_list_that_cannot_be_read_lets_go_of_nothing(pass_over):
    pass_over.kept.return_value = [_kept(artifact_id=_OFF, kept_at=_LONG_AGO)]
    pass_over.listed.return_value = None

    await snapshot.keep_updated_after_turn(workspace_id="w", user_id="u")

    pass_over.release.assert_not_awaited()
    assert _cut_ids(pass_over) == [_OFF]


@pytest.mark.parametrize(
    "value, ids",
    [
        (
            {
                "custom_dashboards": {
                    "current": "a",
                    "list": [
                        {"kind": "page", "code": "a", "artifactId": _LISTED},
                        # An app names no record, whatever it carries.
                        {"kind": "app", "code": "b", "artifactId": _OFF},
                        "junk",
                    ],
                }
            },
            frozenset({_LISTED}),
        ),
        ({}, frozenset()),
        ({"custom_dashboards": {"current": None}}, None),
        ({"custom_dashboards": ["not", "the", "shape"]}, None),
    ],
    ids=["pages-only", "no-list", "no-items", "not-the-shape"],
)
def test_the_list_names_its_pages_or_is_unknown(value, ids):
    assert dashboards.listed_page_ids(value) == ids


class _Claims:
    """Redis's SET NX, as every worker sees it."""

    def __init__(self) -> None:
        self.held: set[str] = set()

    async def set(self, key, value, *, nx, px):
        if key in self.held:
            return None
        self.held.add(key)
        return True


@pytest.mark.asyncio
async def test_a_burst_of_changes_in_a_workspace_with_a_kept_page_runs_one_pass(
    monkeypatch,
):
    from src.utils.cache import redis_cache

    claims = _Claims()
    monkeypatch.setattr(
        redis_cache,
        "get_cache_client",
        lambda: SimpleNamespace(enabled=True, client=claims),
    )
    monkeypatch.setattr(
        snapshot.artifacts_db, "list_kept_updated", AsyncMock(return_value=[_kept()])
    )
    monkeypatch.setattr(snapshot, "_CHANGE_SETTLE_S", 0)
    passes = AsyncMock()
    monkeypatch.setattr(snapshot, "keep_updated_after_turn", passes)

    await asyncio.gather(*(snapshot._after_change("w", "u") for _ in range(3)))

    passes.assert_awaited_once_with(workspace_id="w", user_id="u")


@pytest.mark.asyncio
async def test_a_change_in_a_workspace_with_nothing_kept_claims_no_pass(monkeypatch):
    from src.utils.cache import redis_cache

    claims = _Claims()
    monkeypatch.setattr(
        redis_cache,
        "get_cache_client",
        lambda: SimpleNamespace(enabled=True, client=claims),
    )
    monkeypatch.setattr(
        snapshot.artifacts_db, "list_kept_updated", AsyncMock(return_value=[])
    )
    passes = AsyncMock()
    monkeypatch.setattr(snapshot, "keep_updated_after_turn", passes)

    await snapshot._after_change("w", "u")

    assert not claims.held
    passes.assert_not_awaited()
