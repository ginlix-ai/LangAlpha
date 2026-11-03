"""Snapshot rules that keep a shared link from costing a read per request: a
link shared before versions is tried once under the caps it was shared with,
a keep-updated read never lands over a newer version, and a path whose file
may not be shared is as missing as one that is not there. What the owner left
out stays out of every cut, and a stored copy without a page's libraries never
stands in for the computer."""

from __future__ import annotations

import hashlib
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.artifacts import Artifact
from src.server.database.artifacts.versions import LeftOutChanged
from src.server.services.artifacts import manifest as manifest_mod
from src.server.services.artifacts import reader as reader_mod
from src.server.services.artifacts import snapshot
from src.server.services.artifacts.manifest import MAX_FILES, ManifestEntryMissing
from src.server.services.artifacts.reader import NotInStoredCopy, Reader, ReadMode
from src.server.services.served_paths import may_be_artifact

_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


def _captured(*paths: str) -> SimpleNamespace:
    return SimpleNamespace(
        files=[SimpleNamespace(path=p) for p in paths],
        entries=[SimpleNamespace(path=p, reason="page") for p in paths],
        left_out=[],
    )


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
    workspace_holds, monkeypatch
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
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot.publish(
        _artifact(),
        user_id="u",
        reviewed=["reports/q3.html", "reports/named.png", "data/a.csv"],
        label=None,
        keep_updated=None,
        loaded=("reports/named.png", "data/a.csv"),
    )

    assert cut.await_args.kwargs["loaded_files"] == ["data/a.csv"]


@pytest.mark.asyncio
async def test_a_list_the_owner_did_not_review_is_refused_with_the_list_as_it_is(
    workspace_holds, monkeypatch
):
    workspace_holds({"reports/q3.html": b'<img src="new.png">', "reports/new.png": b"n"})
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

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
    cut.assert_not_awaited()


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
async def test_other_views_hear_of_a_publish_only_when_it_changed_the_record(
    monkeypatch, changed
):
    record = _artifact(latest_seq=1)
    monkeypatch.setattr(snapshot, "store_unregistered_blobs", AsyncMock())
    monkeypatch.setattr(
        snapshot.artifacts_db,
        "publish_version",
        AsyncMock(
            return_value=snapshot.artifacts_db.PublishResult(
                record, 1, False, False, changed
            )
        ),
    )
    published = AsyncMock()
    monkeypatch.setattr(snapshot, "publish_artifact_change", published)

    await snapshot._cut(
        record,
        SimpleNamespace(files=[], title=None, blobs=[], total=0),
        user_id="u",
        origin="owner",
        keep_updated=False,
    )

    assert published.await_count == (1 if changed else 0)


@pytest.mark.asyncio
async def test_an_owners_publish_names_the_version_its_read_began_from(monkeypatch):
    record = _artifact(latest_seq=7)
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(record.path))
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot.publish(
        record,
        user_id="u",
        reviewed=[record.path],
        label=None,
        keep_updated=None,
        loaded=(),
    )

    assert cut.await_args.kwargs["based_on"] == 7


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
    monkeypatch,
):
    artifact = _artifact(latest_seq=7)
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(*artifact.files))
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(artifact, "u")

    assert cut.await_args.kwargs["origin"] == "keep_updated"
    assert cut.await_args.kwargs["based_on"] == 7


@pytest.mark.asyncio
async def test_keep_updated_cuts_no_version_without_a_listed_file(monkeypatch):
    """The list never widens on its own, so a version cut while a file is being
    rebuilt would leave the link without it for good."""
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured("reports/q3.html"))
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(_artifact(), "u")

    cut.assert_not_awaited()


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
async def test_a_turns_end_cut_leaves_out_what_the_owner_took_off(monkeypatch):
    """It reads the frozen list rather than walking, so the list must lose the
    page itself, and the page's absence is not a listed file gone."""
    artifact = _artifact(
        latest_seq=3,
        files=("reports/q3.html", "reports/other.html"),
        left_out_files=("reports/other.html",),
    )
    capture = AsyncMock(return_value=_captured("reports/q3.html"))
    monkeypatch.setattr(snapshot, "_capture", capture)
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(artifact, "u")

    assert capture.await_args.kwargs["source"].paths == ("reports/q3.html",)
    cut.assert_awaited_once()


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
    workspace_holds, monkeypatch
):
    workspace_holds(
        {"reports/q3.html": b'<a href="link.png">chart</a>', "data/real.png": b"png"},
        {"reports/link.png": "data/real.png"},
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(
        _artifact(files=_BOTH_PATHS, left_out_files=("reports/link.png",)), "u"
    )

    cut.assert_awaited_once()
    assert [f.path for f in cut.await_args.args[1].files] == ["reports/q3.html"]


@pytest.mark.asyncio
async def test_a_turns_end_cut_steps_aside_for_a_file_left_out_during_its_read(
    monkeypatch,
):
    artifact = _artifact(left_out_files=("reports/other.html",))
    monkeypatch.setattr(
        snapshot, "_capture", AsyncMock(return_value=_captured(*artifact.files))
    )
    cut = AsyncMock(side_effect=LeftOutChanged)
    monkeypatch.setattr(snapshot, "_cut", cut)

    await snapshot._keep_one(artifact, "u")

    assert cut.await_args.kwargs["left_out_read"] == ("reports/other.html",)


@pytest.mark.asyncio
async def test_a_legacy_cut_raced_by_a_left_out_file_waits_for_the_next_view(
    legacy, monkeypatch
):
    capture, refuse, _ = legacy
    capture.side_effect = None
    capture.return_value = _captured("reports/q3.html", "reports/big.bin")
    cut = AsyncMock(side_effect=LeftOutChanged)
    monkeypatch.setattr(snapshot, "_cut", cut)

    with pytest.raises(snapshot.SnapshotUnavailable):
        await snapshot.legacy_snapshot(_artifact(left_out_files=("reports/x.html",)))

    assert cut.await_args.kwargs["left_out_read"] == ("reports/x.html",)
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
    workspace_holds, monkeypatch
):
    workspace_holds(dict(_PAGE))
    reviewed = await _page_code()
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)
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
    cut.assert_not_awaited()
    # A tab on the previous build sends none, and publishes as it did.
    await snapshot.publish(_artifact(), **publish)
    cut.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_publish_keeps_only_the_left_out_paths_the_page_still_names(
    workspace_holds, monkeypatch
):
    """A path no page names any more would hold a place under the cap with no
    box in the review to clear it."""
    workspace_holds(
        {
            "reports/q3.html": b'<a href="other.html">o</a>',
            "reports/other.html": b"other",
        }
    )
    cut = AsyncMock()
    monkeypatch.setattr(snapshot, "_cut", cut)
    record = _artifact(left_out_files=("reports/gone.html", "reports/other.html"))

    await snapshot.publish(
        record,
        user_id="u",
        reviewed=["reports/q3.html"],
        label=None,
        keep_updated=None,
        loaded=(),
    )

    kwargs = cut.await_args.kwargs
    assert kwargs["left_out_read"] == ("reports/gone.html", "reports/other.html")
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
