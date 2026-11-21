import gzip
import io
import json

import pytest
from edgar_engine.cache import (PARSED_KINDS, DocumentsCache, LocalDirStore, S3Store, SectionsCache, StatementsCache,
                                SubmissionCache, kind_of)
from edgar_engine.sections import sections_for_filing
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


def _bad_deflate() -> bytes:
    data = bytearray(gzip.compress(bytes(range(256)) * 50, mtime=0))
    data[40:60] = bytes(20)
    return bytes(data)


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


class FakeS3:
    """boto3's call shape over a dict; a missing key raises the error the test hands in."""

    def __init__(self, missing=None):
        self.objects, self.calls = {}, []

        class NoSuchKey(Exception):
            pass

        self.exceptions = type("Exceptions", (), {"NoSuchKey": NoSuchKey})
        self.missing = missing or NoSuchKey()

    def get_object(self, Bucket, Key):
        self.calls.append(("get", Bucket, Key))
        if (Bucket, Key) not in self.objects:
            raise self.missing
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.calls.append(("put", Bucket, Key))
        self.objects[(Bucket, Key)] = Body

    def delete_object(self, Bucket, Key):
        self.calls.append(("delete", Bucket, Key))
        self.objects.pop((Bucket, Key), None)


class ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


def test_s3_store_round_trip_under_its_prefix_and_delete():
    client = FakeS3()
    store = S3Store(client, "bucket", prefix="edgar/")
    assert store.get("a/b.bin") is None
    store.put("a/b.bin", b"one")
    store.put("a/b.bin", b"two")
    assert store.get("a/b.bin") == b"two"
    assert list(client.objects) == [("bucket", "edgar/a/b.bin")]
    store.delete("a/b.bin")
    store.delete("a/b.bin")
    assert store.get("a/b.bin") is None
    S3Store(client, "bucket").put("k", b"v")
    assert ("bucket", "k") in client.objects


@pytest.mark.parametrize("code", ["NoSuchKey", "404", "NotFound"])
def test_s3_store_reads_a_missing_object_from_the_error_code_when_the_client_has_no_exception_class(code):
    client = FakeS3(missing=ClientError(code))
    del client.exceptions
    assert S3Store(client, "bucket").get("k") is None


def test_s3_store_propagates_other_errors():
    for error in (ClientError("AccessDenied"), ClientError("500"), RuntimeError("boom")):
        with pytest.raises(type(error)):
            S3Store(FakeS3(missing=error), "bucket").get("k")


@pytest.mark.parametrize("key", ["../x", "a/../x", "/abs", "a\\b", "", "a//b", "./a"])
def test_s3_store_rejects_unsafe_keys_without_calling_the_client(key):
    client = FakeS3()
    store = S3Store(client, "bucket", prefix="edgar/")
    for call in (lambda: store.get(key), lambda: store.put(key, b""), lambda: store.delete(key)):
        with pytest.raises(ValueError):
            call()
    assert client.calls == []


def test_a_sections_cache_runs_over_an_s3_store():
    cache = SectionsCache(S3Store(FakeS3(), "bucket", prefix="edgar/"))
    sections = make_sections()
    cache.put(sections)
    assert cache.get(sections.accession) == sections


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


def test_an_empty_custom_store_is_kept():
    class DictStore(dict):
        def get(self, key):
            return dict.get(self, key)

        def put(self, key, data):
            self[key] = data

        def delete(self, key):
            self.pop(key, None)
    store = DictStore()
    assert SectionsCache(store).store is store and SubmissionCache(store).store is store


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


@pytest.mark.parametrize("junk", [b"not gzip", gzip.compress(b"not json"), gzip.compress(b'{"items": 3}'), _bad_deflate()])
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


@pytest.mark.parametrize("junk", [b"junk", gzip.compress(b"", mtime=0), _bad_deflate()], ids=["junk", "empty", "bad deflate"])
def test_submission_cache_corrupt_entry_is_refetched(tmp_path, junk):
    store = LocalDirStore(tmp_path)
    store.put("submissions/0001234567-24-000001.txt.gz", junk)
    assert SubmissionCache(store).get_or_fetch("0001234567-24-000001", lambda: "fresh") == b"fresh"
    assert SubmissionCache(store).get("0001234567-24-000001") == b"fresh"


class _Filing:
    form, filing_date, accession_no = "10-K", "2024-02-01", "0001234567-24-000001"

    def __init__(self, raw):
        self.raw, self.fetched = raw, 0

    def full_text_submission(self):
        self.fetched += 1
        return self.raw


SUBMISSION = ("<SEC-HEADER>ACCESSION NUMBER: 0001234567-24-000001\n</SEC-HEADER>\n<DOCUMENT>\n<TYPE>10-K\n<TEXT>\n"
              "ITEM 1. BUSINESS\nWe make things.\n</TEXT>\n</DOCUMENT>\n")


def test_sections_for_filing_reads_the_submission_through_its_cache(tmp_path):
    submissions = SubmissionCache(LocalDirStore(tmp_path))
    filing = _Filing(SUBMISSION)
    first = sections_for_filing(filing, submissions=submissions)
    second = sections_for_filing(filing, submissions=submissions)
    assert filing.fetched == 1 and first == second and "1" in first.items
    assert submissions.get(filing.accession_no) == SUBMISSION.encode()


def test_a_truncated_submission_is_not_kept(tmp_path):
    submissions = SubmissionCache(LocalDirStore(tmp_path))
    with pytest.raises(ValueError):
        sections_for_filing(_Filing(SUBMISSION.replace("</DOCUMENT>", "")), submissions=submissions)
    assert submissions.get(_Filing.accession_no) is None


def test_a_key_names_its_cache_and_the_parsed_caches_are_the_three_of_a_parsed_filing():
    keys = [cache._key("a") for cache in (SectionsCache, DocumentsCache, StatementsCache, SubmissionCache)]
    assert [kind_of(k) for k in keys] == ["sections", "documents", "statements", "submissions"]
    assert PARSED_KINDS == ("sections", "documents", "statements")
