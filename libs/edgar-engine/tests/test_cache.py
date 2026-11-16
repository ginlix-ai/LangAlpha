import gzip
import json

import pytest

from edgar_engine.cache import LocalDirStore, SectionsCache, SubmissionCache
from edgar_engine.sections.model import FilingSections, Section, Span, Target
from edgar_engine.sections.version import SECTIONS_VERSION


def make_sections(accession="0001234567-24-000001"):
    target = Target(kind="in_document", description="see below", spans=[Span(5, 9)], text="body")
    section = Section(item="1A", status="stub", text="Item 1A", start=0, end=7, target=target)
    return FilingSections(
        accession=accession,
        form="10-K",
        filing_date="2024-02-01",
        items={"1A": section},
        text="Item 1A body",
        engine_version=SECTIONS_VERSION,
    )


def test_store_round_trip_and_delete(tmp_path):
    store = LocalDirStore(tmp_path)
    assert store.get("a/b.bin") is None
    store.put("a/b.bin", b"one")
    store.put("a/b.bin", b"two")
    assert store.get("a/b.bin") == b"two"
    assert [p.name for p in (tmp_path / "a").iterdir()] == ["b.bin"]
    store.delete("a/b.bin")
    store.delete("a/b.bin")
    assert store.get("a/b.bin") is None


@pytest.mark.parametrize("key", ["../x", "a/../x", "/abs", "a\\b", "", "a//b", "./a"])
def test_store_rejects_unsafe_keys(tmp_path, key):
    store = LocalDirStore(tmp_path)
    for call in (lambda: store.get(key), lambda: store.put(key, b""), lambda: store.delete(key)):
        with pytest.raises(ValueError):
            call()


def test_store_root_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("EDGAR_ENGINE_CACHE", str(tmp_path / "env"))
    store = LocalDirStore()
    store.put("k", b"v")
    assert (tmp_path / "env" / "k").read_bytes() == b"v"


def test_sections_cache_round_trip_and_key(tmp_path):
    store = LocalDirStore(tmp_path)
    cache = SectionsCache(store)
    sections = make_sections()
    assert cache.get(sections.accession) is None
    cache.put(sections)
    assert cache.get(sections.accession) == sections
    raw = store.get(f"sections/v{SECTIONS_VERSION}/{sections.accession}.json.gz")
    assert json.loads(gzip.decompress(raw)) == sections.to_dict()


def test_amendment_fields_round_trip(tmp_path):
    cache = SectionsCache(LocalDirStore(tmp_path))
    sections = make_sections(accession="0001234567-24-000002")
    sections.form, sections.filing_status, sections.amends, sections.scope = "10-K/A", "amendment", "0001234567-24-000001", "partial"
    sections.items["1A"].source = "0001234567-24-000002"
    cache.put(sections)
    assert cache.get(sections.accession) == sections
    assert FilingSections.from_dict(sections.to_dict()) == sections


def test_entry_cached_before_amendment_fields_still_loads(tmp_path):
    store = LocalDirStore(tmp_path)
    v1 = make_sections().to_dict()
    for key in ("filing_status", "amends", "view", "amendments", "scope"):
        v1.pop(key)
    v1["items"]["1A"].pop("source")
    v1["form"] = "10-K/A"
    store.put(f"sections/v{SECTIONS_VERSION}/0001234567-24-000001.json.gz", gzip.compress(json.dumps(v1).encode()))
    got = SectionsCache(store).get("0001234567-24-000001")
    assert (got.filing_status, got.amends, got.view, got.amendments, got.items["1A"].source) == (
        "amendment", None, "as_filed", [], None)


def test_sections_cache_requires_accession(tmp_path):
    with pytest.raises(ValueError):
        SectionsCache(LocalDirStore(tmp_path)).put(make_sections(accession=None))


def test_sections_cache_refuses_an_as_amended_view(tmp_path):
    from dataclasses import replace
    with pytest.raises(ValueError):
        SectionsCache(LocalDirStore(tmp_path)).put(replace(make_sections(), view="as_amended"))


def test_sections_cache_get_or_compute_computes_once(tmp_path):
    cache = SectionsCache(LocalDirStore(tmp_path))
    calls = []

    def compute():
        calls.append(1)
        return make_sections()

    first = cache.get_or_compute("0001234567-24-000001", compute)
    second = cache.get_or_compute("0001234567-24-000001", compute)
    assert first == second
    assert len(calls) == 1


@pytest.mark.parametrize("junk", [b"not gzip", gzip.compress(b"not json"), gzip.compress(b'{"items": 3}')])
def test_sections_cache_corrupt_entry_is_a_miss_and_is_overwritten(tmp_path, junk):
    store = LocalDirStore(tmp_path)
    cache = SectionsCache(store)
    store.put(f"sections/v{SECTIONS_VERSION}/0001234567-24-000001.json.gz", junk)
    assert cache.get("0001234567-24-000001") is None
    recomputed = cache.get_or_compute("0001234567-24-000001", make_sections)
    assert cache.get("0001234567-24-000001") == recomputed


def test_submission_cache_round_trip_and_fetch_once(tmp_path):
    store = LocalDirStore(tmp_path)
    cache = SubmissionCache(store)
    assert cache.get("0001234567-24-000001") is None
    calls = []

    def fetch():
        calls.append(1)
        return b"<SEC-DOCUMENT>\xe9</SEC-DOCUMENT>"

    assert cache.get_or_fetch("0001234567-24-000001", fetch) == b"<SEC-DOCUMENT>\xe9</SEC-DOCUMENT>"
    assert cache.get_or_fetch("0001234567-24-000001", fetch) == b"<SEC-DOCUMENT>\xe9</SEC-DOCUMENT>"
    assert len(calls) == 1
    assert store.get("submissions/0001234567-24-000001.txt.gz") is not None


def test_submission_cache_corrupt_entry_is_refetched(tmp_path):
    store = LocalDirStore(tmp_path)
    store.put("submissions/0001234567-24-000001.txt.gz", b"junk")
    assert SubmissionCache(store).get_or_fetch("0001234567-24-000001", lambda: "fresh") == b"fresh"
    assert SubmissionCache(store).get("0001234567-24-000001") == b"fresh"
