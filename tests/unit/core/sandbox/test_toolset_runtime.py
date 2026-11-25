"""The toolset's runtime is the contract every computer converges by: what it
installs, what it keeps, and what it reports back to the server."""

from __future__ import annotations

import contextlib
import copy
import errno
import hashlib
import io
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ptc_agent.core.sandbox._shared import _internal_package_uploads
from ptc_agent.core.sandbox.browser_runtime import install, protocol
from ptc_agent.core.sandbox.toolset_runtime import cli, converge, units

_ROOT = Path(__file__).resolve().parents[4]
_PACKAGE = Path(units.__file__).parent
PLATFORM = converge.platform_name()


@pytest.fixture
def box(tmp_path):
    return Box(tmp_path)


class Box:
    """A machine in a folder: a toolset root, releases served from file://
    URLs, a fake Playwright tree, and expose paths of its own."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.root = base / "toolset"
        self.releases = base / "releases"
        self.bin = base / "usr" / "local" / "bin"
        self.table = base / "units.json"

    def release(self, version: str, body: bytes | None = None) -> dict:
        body = body if body is not None else f"engine {version}".encode()
        path = self.releases / version / "agent-browser"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return {"url": path.as_uri(), "sha256": hashlib.sha256(body).hexdigest()}

    def engine(self, version: str, release: dict | None = None) -> dict:
        return {
            "name": "agent-browser",
            "version": version,
            "kind": "binary",
            "platforms": {PLATFORM: release or self.release(version)},
            "file": "agent-browser",
            "mode": "0755",
            "expose": [str(self.base / "usr" / "local" / "lib" / "agent-browser" / "agent-browser")],
        }

    def chrome(self, revisions: tuple[str, ...] = ("1200",)) -> dict:
        for revision in revisions:
            chrome = self.base / "ms-playwright" / f"chromium-{revision}" / "chrome-linux64" / "chrome"
            chrome.parent.mkdir(parents=True, exist_ok=True)
            chrome.write_text(revision)
        return {
            "name": "chrome-for-testing",
            "version": "1",
            "kind": "link",
            "target_glob": str(self.base / "ms-playwright" / "chromium-*" / "chrome-linux*" / "chrome"),
            "file": "chrome",
            "expose": [str(self.bin / "chrome-for-testing")],
        }

    def policy(self, content: object = None) -> dict:
        unit = {
            "name": "chrome-policy",
            "version": "",
            "kind": "file",
            "content": content if content is not None else {"BrowserSignin": 0},
            "file": "langalpha.json",
            "mode": "0644",
            "expose": [str(self.base / "etc" / "policies" / "managed" / "langalpha.json")],
        }
        unit["version"] = units.content_version(unit)
        return unit

    def write(self, *unit_list: dict, revision: int = 1) -> units.Table:
        self.table.write_text(json.dumps({"schema": 1, "revision": revision, "units": list(unit_list)}))
        return units.load(str(self.table))

    def converge(self, *extra: str) -> tuple[int, dict]:
        return self._run("converge", *extra)

    def status(self) -> dict:
        return self._run("status")[1]

    def _run(self, command: str, *extra: str) -> tuple[int, dict]:
        argv = [command, "--root", str(self.root), "--units", str(self.table), *extra]
        out, code = _capture(lambda: cli.main(argv))
        return code, json.loads(out.strip().splitlines()[-1])

    def record(self) -> dict:
        return json.loads((self.root / "state.json").read_text())

    def state(self) -> dict:
        """The record with each unit's pin read as its version, without the
        names each was given, which ``record`` keeps."""
        record = self.record()
        record.pop("exposed", None)
        return {**record, "units": {name: pin.split("@")[0] for name, pin in record["units"].items()}}

    def versions(self, name: str) -> set[str]:
        """The unit's folders, each by the version its pin names."""
        return {p.name.split("@")[0] for p in (self.root / name).iterdir()}

    def current(self, name: str) -> str:
        """The version ``current`` names."""
        return os.readlink(self.root / name / "current").split("@")[0]

    def folder(self, name: str, version: str) -> Path:
        (found,) = (self.root / name).glob(f"{version}@*")
        return found


def _capture(call):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = call()
    return out.getvalue(), code


def test_a_fresh_machine_installs_every_unit_and_records_the_set(box):
    table = box.write(box.engine("1.0.0"), box.chrome(), box.policy())

    code, answer = box.converge()

    assert code == 0
    target = units.version(table)
    assert answer == {
        "version": target,
        "target": target,
        "units": {"agent-browser": "installed", "chrome-for-testing": "installed", "chrome-policy": "installed"},
    }
    engine = Path(table.units[0]["expose"][0])
    assert engine.read_bytes() == b"engine 1.0.0"
    assert os.readlink(engine) == str(box.root / "agent-browser" / "current" / "agent-browser")
    assert os.access(engine, os.X_OK)
    assert Path(table.units[1]["expose"][0]).read_text() == "1200"
    assert json.loads(Path(table.units[2]["expose"][0]).read_bytes()) == {"BrowserSignin": 0}
    assert os.stat(table.units[2]["expose"][0]).st_mode & 0o777 == 0o644
    assert box.state() == {
        "revision": 1,
        "version": target,
        "units": {"agent-browser": "1.0.0", "chrome-for-testing": "1", "chrome-policy": table.units[2]["version"]},
    }
    assert box.record()["exposed"] == {unit["name"]: unit["expose"] for unit in table.units}
    assert box.status() == {
        "revision": 1,
        "version": target,
        "target": target,
        "converging": False,
        "units": {unit["name"]: units.pin(unit) for unit in table.units},
    }


def test_a_unit_the_table_drops_is_taken_off_the_machine(box):
    engine, policy = box.engine("1.0.0"), box.policy()
    box.write(engine, policy)
    box.converge()
    table = box.write(engine, revision=2)

    code, answer = box.converge()

    assert code == 0
    assert answer == {"version": units.version(table), "target": units.version(table), "units": {"agent-browser": "current"}}
    assert not os.path.lexists(policy["expose"][0])
    assert not (box.root / "chrome-policy").exists()
    assert box.record()["exposed"] == {"agent-browser": engine["expose"]}


def test_a_name_the_table_drops_is_unlinked_and_the_others_kept(box):
    engine = box.engine("1.0.0")
    extra = str(box.bin / "ab")
    box.write({**engine, "expose": [*engine["expose"], extra]})
    box.converge()
    assert os.path.islink(extra)

    box.write(engine, revision=2)
    code, answer = box.converge()

    assert code == 0
    assert answer["version"] == answer["target"]
    assert not os.path.lexists(extra)
    assert Path(engine["expose"][0]).read_bytes() == b"engine 1.0.0"
    assert box.record()["exposed"] == {"agent-browser": engine["expose"]}


def test_a_dropped_unit_leaves_what_an_image_put_at_its_names(box):
    # Adopted, the path is the image's own file, which the toolset never wrote.
    policy = box.policy()
    path = Path(policy["expose"][0])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(units.content_bytes(policy))
    path.chmod(0o644)
    box.write(box.engine("1.0.0"), policy)
    _, answer = box.converge()
    assert answer["units"]["chrome-policy"] == "adopted"

    box.write(box.engine("1.0.0"), revision=2)
    code, answer = box.converge()

    assert code == 0
    assert answer["version"] == answer["target"]
    assert not path.is_symlink()
    assert path.read_bytes() == units.content_bytes(policy)


def test_a_dropped_unit_whose_name_cannot_be_removed_keeps_the_set_behind(box, monkeypatch):
    engine, policy = box.engine("1.0.0"), box.policy()
    first = box.write(engine, policy)
    box.converge()
    box.write(engine, revision=2)
    unlink = os.unlink

    def refuse(path, *args, **kwargs):
        if str(path) == policy["expose"][0]:
            raise PermissionError(path)
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(converge.os, "unlink", refuse)
    code, answer = box.converge()

    assert code == 0
    assert answer["version"] == units.version(first)
    assert answer["units"] == {"agent-browser": "current", "chrome-policy": "failed:retire"}
    assert box.record()["exposed"]["chrome-policy"] == policy["expose"]

    monkeypatch.setattr(converge.os, "unlink", unlink)
    _, answer = box.converge()
    assert answer["version"] == answer["target"]
    assert not os.path.lexists(policy["expose"][0])


def test_a_name_a_kept_unit_drops_that_cannot_be_removed_keeps_the_set_behind(box, monkeypatch):
    engine = box.engine("1.0.0")
    extra = str(box.bin / "ab")
    first = box.write({**engine, "expose": [*engine["expose"], extra]})
    box.converge()
    box.write(engine, revision=2)
    unlink = os.unlink

    def refuse(path, *args, **kwargs):
        if str(path) == extra:
            raise PermissionError(path)
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(converge.os, "unlink", refuse)
    code, answer = box.converge()

    assert answer["version"] == units.version(first)
    assert answer["units"] == {"agent-browser": "failed:retire"}
    assert box.record()["exposed"]["agent-browser"] == [extra, *engine["expose"]]


def test_a_name_spelled_anew_through_a_link_to_its_folder_is_kept(box):
    engine = box.engine("1.0.0")
    real = Path(engine["expose"][0])
    alias = box.base / "alias"
    alias.symlink_to(real.parent, target_is_directory=True)
    real.parent.mkdir(parents=True, exist_ok=True)
    box.write(engine)
    box.converge()

    respelled = {**engine, "expose": [str(alias / real.name)]}
    box.write(respelled, revision=2)
    code, answer = box.converge()

    assert answer["version"] == answer["target"]
    assert real.read_bytes() == b"engine 1.0.0"
    assert box.record()["exposed"]["agent-browser"] == respelled["expose"]


def test_a_name_whose_link_cannot_be_read_stays_in_the_record(box, monkeypatch):
    engine, policy = box.engine("1.0.0"), box.policy()
    box.write(engine, policy)
    box.converge()
    box.write(engine, revision=2)
    readlink = os.readlink

    def unreadable(path, *args, **kwargs):
        if str(path) == policy["expose"][0]:
            raise OSError(errno.EIO, "I/O error", path)
        return readlink(path, *args, **kwargs)

    monkeypatch.setattr(converge.os, "readlink", unreadable)
    _, answer = box.converge()

    assert answer["units"]["chrome-policy"] == "failed:retire"
    assert box.record()["exposed"]["chrome-policy"] == policy["expose"]


def test_new_names_are_recorded_before_any_is_made(box, monkeypatch):
    engine = box.engine("1.0.0")
    extra = str(box.bin / "ab")
    box.write(engine)
    box.converge()
    box.write({**engine, "expose": [*engine["expose"], extra]}, revision=2)
    real = converge._converge_unit

    def cut_short(*args, **kwargs):
        real(*args, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(converge, "_converge_unit", cut_short)
    with pytest.raises(KeyboardInterrupt):
        box.converge()

    assert os.path.islink(extra)
    assert box.record()["exposed"]["agent-browser"] == [*engine["expose"], extra]


@pytest.mark.parametrize("through", ["root", "a link to it"])
def test_a_name_inside_the_toolset_root_fails_its_unit(box, through):
    policy = box.policy()
    box.write(box.engine("1.0.0"))
    box.converge()
    before = json.loads((box.root / "state.json").read_bytes())
    folder = box.root
    if through == "a link to it":
        folder = box.base / "alias"
        folder.symlink_to(box.root)
    record = str(folder / "state.json")
    box.write(box.engine("1.0.0"), {**policy, "expose": [record]}, revision=2)

    _, answer = box.converge()

    assert answer["units"]["chrome-policy"] == "failed:expose"
    assert not os.path.islink(box.root / "state.json")
    after = json.loads((box.root / "state.json").read_bytes())
    # Nothing of the unit is recorded: only the revision moved.
    assert {**after, "revision": before["revision"]} == before


def test_a_download_past_its_pinned_size_stops_there(box, monkeypatch):
    body = b"engine 2.0.0 and then some more"
    release = {**box.release("2.0.0", body), "size": 8}
    written = []
    monkeypatch.setattr(converge, "_CHUNK", 4)
    real_open = open

    class Counting:
        def __init__(self, f):
            self.f = f

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self.f.__exit__(*exc)

        def write(self, data):
            written.append(len(data))
            return self.f.write(data)

        def __getattr__(self, name):
            return getattr(self.f, name)

    def counting_open(path, mode="r", *args, **kwargs):
        f = real_open(path, mode, *args, **kwargs)
        return Counting(f) if mode == "wb" and str(path).endswith("agent-browser") else f

    monkeypatch.setattr(converge, "open", counting_open, raising=False)
    box.write(box.engine("2.0.0", release))

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:size"}
    assert sum(written) <= 8


def test_a_dropped_unit_a_process_still_runs_from_is_removed_once_it_stops(box, monkeypatch):
    engine, policy = box.engine("1.0.0"), box.policy()
    box.write(engine, policy)
    box.converge()
    running = {box.folder("chrome-policy", policy["version"]).name}
    monkeypatch.setattr(converge, "_running_from", lambda unit_dir: running if unit_dir.endswith("chrome-policy") else set())
    box.write(engine, revision=2)

    box.converge()
    assert (box.root / "chrome-policy").exists()
    assert not os.path.lexists(policy["expose"][0])

    running.clear()
    _, answer = box.converge()

    assert answer["version"] == answer["target"]
    assert not (box.root / "chrome-policy").exists()


def test_a_relative_root_links_every_name_to_an_absolute_path(box, monkeypatch):
    table = box.write(box.engine("1.0.0"))
    box.root.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(box.root.parent)

    answer = converge.converge(box.root.name, table)

    exposed = table.units[0]["expose"][0]
    assert answer.failures == {}
    assert os.path.isabs(os.readlink(exposed))
    assert Path(exposed).read_bytes() == b"engine 1.0.0"


def test_a_current_machine_downloads_nothing(box):
    box.write(box.engine("1.0.0"), box.policy())
    box.converge()
    # The release is gone, so a second download would fail.
    (box.releases / "1.0.0" / "agent-browser").unlink()

    code, answer = box.converge()

    assert code == 0
    assert answer["units"] == {"agent-browser": "current", "chrome-policy": "current"}
    assert answer["version"] == answer["target"]


def test_a_bump_keeps_the_version_before_it_and_drops_older_ones(box):
    for version in ("1.0.0", "2.0.0", "3.0.0"):
        table = box.write(box.engine(version))
        code, answer = box.converge()
        assert answer["units"] == {"agent-browser": "installed"}

    assert box.versions("agent-browser") == {"current", "2.0.0", "3.0.0"}
    assert box.current("agent-browser") == "3.0.0"
    assert Path(table.units[0]["expose"][0]).read_bytes() == b"engine 3.0.0"
    assert box.state()["units"] == {"agent-browser": "3.0.0"}


def test_a_release_not_the_pinned_one_fails_and_keeps_the_old_version(box):
    box.write(box.engine("1.0.0"))
    _, first = box.converge()
    release = box.release("2.0.0")
    release["sha256"] = "0" * 64
    table = box.write(box.engine("2.0.0", release))

    code, answer = box.converge()

    assert code == 0
    assert answer == {"version": first["version"], "target": units.version(table), "units": {"agent-browser": "failed:digest"}}
    assert box.versions("agent-browser") == {"current", "1.0.0"}
    assert Path(table.units[0]["expose"][0]).read_bytes() == b"engine 1.0.0"
    assert box.state() == {"revision": 1, "version": first["version"], "units": {"agent-browser": "1.0.0"}}
    assert box.converge("--strict")[0] == 1


def test_a_release_that_cannot_be_fetched_fails_that_unit_only(box):
    release = {"url": (box.releases / "missing").as_uri(), "sha256": "0" * 64}
    box.write(box.engine("1.0.0", release), box.policy())

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:download", "chrome-policy": "installed"}


def test_a_chrome_target_that_is_a_folder_is_no_target(box):
    unit = box.chrome(revisions=())
    (box.base / "ms-playwright" / "chromium-1200" / "chrome-linux64" / "chrome").mkdir(parents=True)
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": "failed:no-target"}
    assert not os.path.lexists(unit["expose"][0])


def test_a_record_that_cannot_be_written_answers_as_a_failure(box, monkeypatch):
    """The answer still reaches the server, as a set not yet complete."""
    box.write(box.policy())

    def full(root, state):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(converge, "write_state", full)
    code, answer = box.converge()

    assert (code, answer["version"], answer["units"]) == (0, None, {"chrome-policy": "failed:state"})


def test_a_replace_that_fails_puts_back_the_names_before_it(box, monkeypatch):
    first, second = box.bin / "one", box.bin / "two"
    box.bin.mkdir(parents=True)
    first.write_text("an image's file")
    second.symlink_to("/elsewhere")
    replace = os.replace

    def second_fails(src, dst):
        if str(dst) == str(second):
            raise OSError(5, "Input/output error")
        replace(src, dst)

    monkeypatch.setattr(converge.os, "replace", second_fails)
    with pytest.raises(OSError):
        converge.swap_links([("/new/one", str(first)), ("/new/two", str(second))])
    monkeypatch.undo()

    assert not first.is_symlink() and first.read_text() == "an image's file"
    assert os.readlink(second) == "/elsewhere"
    assert sorted(p.name for p in box.bin.iterdir()) == ["one", "two"]


def test_a_name_that_cannot_be_put_back_is_left_beside_its_working_link(box, monkeypatch):
    first, second = box.bin / "one", box.bin / "two"
    box.bin.mkdir(parents=True)
    first.write_text("an image's file")
    second.symlink_to("/elsewhere")
    replace = os.replace

    def both_fail(src, dst):
        # The second name cannot be replaced, nor the first put back.
        if str(dst) == str(second) or str(src).endswith(converge.KEPT_SUFFIX):
            raise OSError(5, "Input/output error")
        replace(src, dst)

    monkeypatch.setattr(converge.os, "replace", both_fail)
    with pytest.raises(OSError):
        converge.swap_links([("/new/one", str(first)), ("/new/two", str(second))])
    monkeypatch.undo()

    kept = Path(f"{first}{converge.KEPT_SUFFIX}")
    assert kept.read_text() == "an image's file"

    converge.swap_links([("/new/one", str(first)), ("/new/two", str(second))])

    assert os.readlink(first) == "/new/one" and os.readlink(second) == "/new/two"
    assert sorted(p.name for p in box.bin.iterdir()) == ["one", "two"]


def test_a_second_name_left_over_is_never_put_back_over_a_working_link(box, monkeypatch):
    name = box.bin / "one"
    box.bin.mkdir(parents=True)
    name.symlink_to("/new/one")
    # One a finished swap could not remove: stale.
    Path(f"{name}{converge.KEPT_SUFFIX}").symlink_to("/old/one")
    symlink = os.symlink

    def no_room(src, dst, *args, **kwargs):
        if str(dst).endswith(converge.STAGED_SUFFIX):
            raise OSError(28, "No space left on device")
        return symlink(src, dst, *args, **kwargs)

    monkeypatch.setattr(converge.os, "symlink", no_room)
    with pytest.raises(OSError):
        converge.swap_links([("/newer/one", str(name))])

    assert os.readlink(name) == "/new/one"


@pytest.mark.parametrize(("revisions", "reason"), [((), "no-target"), (("1200", "1300"), "several-targets")])
def test_a_link_that_does_not_match_exactly_one_path_fails(box, revisions, reason):
    (box.base / "ms-playwright").mkdir()
    table = box.write(box.chrome(revisions))

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": f"failed:{reason}"}
    assert not os.path.lexists(table.units[0]["expose"][0])
    assert box.versions("chrome-for-testing") == set()


def test_a_regular_file_an_old_image_left_is_replaced_by_the_link(box):
    table = box.write(box.engine("1.0.0"))
    engine = Path(table.units[0]["expose"][0])
    engine.parent.mkdir(parents=True)
    engine.write_bytes(b"baked into the image")

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert engine.is_symlink()
    assert engine.read_bytes() == b"engine 1.0.0"


def test_an_image_s_engine_at_the_pinned_digest_is_adopted_where_it_is(box):
    release = box.release("1.0.0")
    unit = box.engine("1.0.0", release)
    engine = Path(unit["expose"][0])
    engine.parent.mkdir(parents=True)
    engine.write_bytes(b"engine 1.0.0")
    engine.chmod(0o755)
    before = engine.stat()
    # The release is gone, so only adopting what is there succeeds.
    (box.releases / "1.0.0" / "agent-browser").unlink()
    table = box.write(unit)

    code, answer = box.converge()

    assert code == 0
    assert answer == {"version": units.version(table), "target": units.version(table), "units": {"agent-browser": "adopted"}}
    assert not engine.is_symlink()
    assert engine.stat().st_ino == before.st_ino
    assert engine.stat().st_mtime_ns == before.st_mtime_ns
    assert not (box.root / "agent-browser").exists()
    assert box.state() == {
        "revision": 1,
        "version": units.version(table),
        "units": {"agent-browser": "1.0.0"},
    }


def test_an_adopted_unit_stays_current_while_it_matches_and_a_later_version_replaces_it(box):
    release = box.release("1.0.0")
    unit = box.engine("1.0.0", release)
    engine = Path(unit["expose"][0])
    engine.parent.mkdir(parents=True)
    engine.write_bytes(b"engine 1.0.0")
    engine.chmod(0o755)
    box.write(unit)
    assert box.converge()[1]["units"] == {"agent-browser": "adopted"}

    assert box.converge()[1]["units"] == {"agent-browser": "current"}
    assert not engine.is_symlink()

    box.write(box.engine("2.0.0"))
    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert engine.is_symlink()
    assert engine.read_bytes() == b"engine 2.0.0"
    assert box.state()["units"] == {"agent-browser": "2.0.0"}


def test_an_image_s_engine_that_cannot_run_is_installed_rather_than_adopted(box):
    unit = box.engine("1.0.0")
    engine = Path(unit["expose"][0])
    engine.parent.mkdir(parents=True)
    engine.write_bytes(b"engine 1.0.0")
    engine.chmod(0o644)
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert engine.is_symlink() and os.access(engine, os.X_OK)


def test_an_image_s_file_at_another_mode_is_installed_rather_than_adopted(box):
    unit = box.policy()
    policy = Path(unit["expose"][0])
    policy.parent.mkdir(parents=True)
    policy.write_bytes(units.content_bytes(unit))
    policy.chmod(0o600)
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"chrome-policy": "installed"}
    assert policy.is_symlink() and os.stat(policy).st_mode & 0o777 == 0o644


def test_a_link_unit_whose_file_is_renamed_installs_rather_than_adopts_its_own_link(box):
    """The toolset's link through ``current`` to the old file still reaches the
    one Chrome, but it is the toolset's own, not an image's to adopt."""
    unit = box.chrome()
    box.write(unit)
    box.converge()
    renamed = {**unit, "file": "chrome-v2"}
    box.write(renamed)

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": "installed"}
    assert os.readlink(unit["expose"][0]) == str(box.root / "chrome-for-testing" / "current" / "chrome-v2")
    assert Path(unit["expose"][0]).read_text() == "1200"


def test_a_name_whose_place_is_a_file_fails_before_any_image_name_is_replaced(box):
    unit = box.engine("1.0.0")
    image_engine = Path(unit["expose"][0])
    image_engine.parent.mkdir(parents=True)
    image_engine.write_bytes(b"an older engine")
    image_engine.chmod(0o755)
    (box.base / "in-the-way").write_text("a file")
    unit["expose"].append(str(box.base / "in-the-way" / "agent-browser"))
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:io"}
    assert not image_engine.is_symlink() and image_engine.read_bytes() == b"an older engine"


def test_a_link_that_cannot_be_made_fails_before_any_image_name_is_replaced(box):
    """Every new link is made beside its name before any name is replaced."""
    unit = box.engine("1.0.0")
    image_engine = Path(unit["expose"][0])
    image_engine.parent.mkdir(parents=True)
    image_engine.write_bytes(b"an older engine")
    image_engine.chmod(0o755)
    second = box.bin / "agent-browser"
    unit["expose"].append(str(second))
    second.parent.mkdir(parents=True)
    Path(f"{second}.toolset.tmp").mkdir()
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:io"}
    assert not image_engine.is_symlink() and image_engine.read_bytes() == b"an older engine"
    assert not Path(f"{image_engine}.toolset.tmp").exists()


def test_an_adopted_file_that_stops_matching_is_installed(box):
    unit = box.policy()
    policy = Path(unit["expose"][0])
    policy.parent.mkdir(parents=True)
    policy.write_bytes(units.content_bytes(unit))
    # At the pinned mode, whatever the umask the suite runs under.
    policy.chmod(0o644)
    box.write(unit)
    assert box.converge()[1]["units"] == {"chrome-policy": "adopted"}
    policy.write_bytes(b"{}")

    _, answer = box.converge()

    assert answer["units"] == {"chrome-policy": "installed"}
    assert policy.is_symlink()
    assert policy.read_bytes() == units.content_bytes(unit)


def test_a_link_already_at_the_one_chrome_is_adopted(box):
    unit = box.chrome()
    chrome = box.base / "ms-playwright" / "chromium-1200" / "chrome-linux64" / "chrome"
    link = Path(unit["expose"][0])
    link.parent.mkdir(parents=True)
    link.symlink_to(chrome)
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": "adopted"}
    assert os.readlink(link) == str(chrome)
    assert box.state()["units"] == {"chrome-for-testing": "1"}
    assert box.converge()[1]["units"] == {"chrome-for-testing": "current"}


def test_a_link_at_another_chrome_is_not_adopted(box):
    unit = box.chrome()
    other = box.base / "elsewhere" / "chrome"
    other.parent.mkdir(parents=True)
    other.write_text("other")
    link = Path(unit["expose"][0])
    link.parent.mkdir(parents=True)
    link.symlink_to(other)
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": "installed"}
    assert link.read_text() == "1200"


def test_a_link_unit_s_new_version_is_installed_over_its_own_link_to_the_same_chrome(box):
    """The toolset's own link reaches the one Chrome through ``current``, so it
    is never taken for an image's to adopt."""
    unit = box.chrome()
    box.write(unit)
    assert box.converge()[1]["units"] == {"chrome-for-testing": "installed"}
    box.write({**unit, "version": "2"})

    _, answer = box.converge()

    assert answer["units"] == {"chrome-for-testing": "installed"}
    assert (box.current("chrome-for-testing"), box.state()["units"]) == ("2", {"chrome-for-testing": "2"})
    assert Path(unit["expose"][0]).read_text() == "1200"


def test_the_shipped_policy_is_byte_for_byte_what_older_images_carry():
    """Images before the toolset echoed ``json.dumps(policy, sort_keys=True)``,
    so their policy file is adopted rather than rewritten."""
    policy = next(u for u in units.load().units if u["name"] == "chrome-policy")

    assert units.content_bytes(policy) == (json.dumps(protocol.chrome_policy(), sort_keys=True) + "\n").encode()
    assert policy["version"] == units.content_version(policy)


def test_a_version_folder_cut_short_by_a_crash_is_installed_again(box):
    table = box.write(box.engine("1.0.0"))
    box.converge()
    # A crash after the folder was renamed in and before the record was kept.
    (box.folder("agent-browser", "1.0.0") / "agent-browser").write_bytes(b"eng")
    (box.root / "state.json").unlink()

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert Path(table.units[0]["expose"][0]).read_bytes() == b"engine 1.0.0"
    assert box.versions("agent-browser") == {"current", "1.0.0"}


def test_a_folder_cut_short_stays_in_place_when_its_replacement_cannot_be_renamed_in(box, monkeypatch):
    table = box.write(box.engine("1.0.0"))
    box.converge()
    folder = box.folder("agent-browser", "1.0.0")
    (folder / "agent-browser").write_bytes(b"eng")
    (box.root / "state.json").unlink()
    rename = os.rename

    def refuse(src, dst, *args, **kwargs):
        if str(dst) == str(folder) and os.path.basename(str(src)).startswith(converge._TEMP_PREFIX):
            raise OSError(errno.EIO, "I/O error", dst)
        return rename(src, dst, *args, **kwargs)

    monkeypatch.setattr(converge.os, "rename", refuse)
    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:io"}
    assert Path(table.units[0]["expose"][0]).read_bytes() == b"eng"


def test_what_is_renamed_into_place_is_on_disk_first(box, monkeypatch):
    synced = set()
    published = []
    sync = converge._sync
    rename, replace = os.rename, os.replace

    def recording(f):
        sync(f)
        synced.add(os.fstat(f.fileno()).st_ino)

    def unsynced(src) -> list[str]:
        """What a rename of *src* publishes that was not synced first: each file
        of a version's staging folder, or the record's temporary file."""
        base = os.path.basename(str(src))
        if base.startswith(converge._TEMP_PREFIX):
            files = [os.path.join(str(src), f) for f in os.listdir(src)]
        elif base.startswith(f".{converge.STATE}-"):
            files = [str(src)]
        else:
            return []
        published.extend(files)
        return [f for f in files if not os.path.islink(f) and os.stat(f).st_ino not in synced]

    def checked(move):
        def run(src, dst, *args, **kwargs):
            assert unsynced(src) == [], (src, dst)
            return move(src, dst, *args, **kwargs)

        return run

    monkeypatch.setattr(converge, "_sync", recording)
    monkeypatch.setattr(converge.os, "rename", checked(rename))
    monkeypatch.setattr(converge.os, "replace", checked(replace))
    box.write(box.engine("1.0.0"), box.policy())

    box.converge()

    assert published

    for path in (
        box.folder("agent-browser", "1.0.0") / "agent-browser",
        box.folder("chrome-policy", box.policy()["version"]) / "langalpha.json",
        box.root / "state.json",
    ):
        assert path.stat().st_ino in synced, path


def test_a_folder_where_a_name_goes_fails_and_keeps_the_old_version(box):
    table = box.write(box.engine("1.0.0"))
    box.converge()
    unit = box.engine("2.0.0")
    unit["expose"].append(str(box.base / "a-folder"))
    (box.base / "a-folder").mkdir()
    box.write(unit)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:expose"}
    assert box.current("agent-browser") == "1.0.0"
    assert Path(table.units[0]["expose"][0]).read_bytes() == b"engine 1.0.0"


def test_a_moved_name_is_put_back_though_the_version_is_current(box):
    table = box.write(box.engine("1.0.0"))
    box.converge()
    engine = Path(table.units[0]["expose"][0])
    engine.unlink()
    engine.write_bytes(b"put back by something else")

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert engine.read_bytes() == b"engine 1.0.0"


def test_the_set_version_is_written_only_once_every_unit_is_current(box):
    (box.base / "ms-playwright").mkdir()
    box.write(box.engine("1.0.0"), box.chrome(revisions=()))

    _, answer = box.converge()

    assert answer["version"] is None
    assert box.state() == {"revision": 1, "version": None, "units": {"agent-browser": "1.0.0"}}

    table = box.write(box.engine("1.0.0"), box.chrome())
    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "current", "chrome-for-testing": "installed"}
    assert answer["version"] == units.version(table) == box.state()["version"]


def test_a_file_renamed_in_a_new_version_moves_its_names_with_current(box):
    """A name through ``current`` to the new file finds nothing until ``current``
    moves; one that cannot be made leaves the old version's names working."""
    unit = box.engine("1.0.0")
    unit["expose"].append(str(box.bin / "agent-browser"))
    box.write(unit)
    box.converge()
    renamed = {**box.engine("2.0.0"), "file": "agent-browser-2", "expose": unit["expose"]}
    blocked = {**renamed, "expose": [*unit["expose"], str(box.bin / "taken")]}
    (box.bin / "taken").mkdir()
    box.write(blocked)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:expose"}
    assert [Path(path).read_bytes() for path in unit["expose"]] == [b"engine 1.0.0"] * 2

    box.write(renamed)
    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert [Path(path).read_bytes() for path in unit["expose"]] == [b"engine 2.0.0"] * 2


def test_every_name_works_at_every_step_of_a_rename_and_after_one_cut_short(box, monkeypatch):
    unit = box.engine("1.0.0")
    unit["expose"].append(str(box.bin / "agent-browser"))
    box.write(unit)
    box.converge()
    renamed = {**box.engine("2.0.0"), "file": "agent-browser-2", "expose": unit["expose"]}
    box.write(renamed)
    swap, swap_one = converge.swap_links, converge.swap_link
    swaps = []

    def after(paths):
        swaps.append(paths)
        assert all(os.path.isfile(name) for name in unit["expose"]), swaps
        if any(path.endswith("current") for path in paths):
            # The disk fills before the names move through ``current``.
            raise OSError(28, "No space left on device")

    def checked(links):
        swap(links)
        after([path for _, path in links])

    def checked_one(value, path):
        swap_one(value, path)
        after([path])

    monkeypatch.setattr(converge, "swap_links", checked)
    monkeypatch.setattr(converge, "swap_link", checked_one)
    _, answer = box.converge()
    monkeypatch.undo()

    assert answer["units"] == {"agent-browser": "failed:io"}
    assert [Path(path).read_bytes() for path in unit["expose"]] == [b"engine 2.0.0"] * 2

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    through = str(box.root / "agent-browser" / "current" / "agent-browser-2")
    assert [os.readlink(path) for path in unit["expose"]] == [through] * 2


def test_a_record_that_is_there_and_cannot_be_read_fails_the_run(box, monkeypatch):
    """Read as none, it would let an older table past the revision this machine took."""
    box.write(box.engine("2.0.0"), revision=2)
    box.converge()
    box.write(box.engine("1.0.0"), revision=1)
    opened = open

    def unreadable(path, *args, **kwargs):
        if str(path).endswith(converge.STATE) and "r" in (args[0] if args else kwargs.get("mode", "r")):
            raise PermissionError(13, "Permission denied")
        return opened(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", unreadable)
    code, answer = box.converge()
    monkeypatch.undo()

    assert (code, answer["units"]) == (0, {"agent-browser": "failed:state"})
    assert box.current("agent-browser") == "2.0.0"
    assert box.state()["revision"] == 2


def test_a_release_pinned_anew_under_the_same_version_is_installed(box):
    """A table may move a binary's digest without its version; the machine
    takes the new release rather than keep the old bytes under the new set. It
    installs beside the old one, which a browser that started on it keeps, so
    the engine it records is no longer the one the name resolves to, and the
    browser is seen to be behind (``engine.stale``)."""
    box.write(box.engine("1.0.0"))
    box.converge()
    started_on = os.path.realpath(box.engine("1.0.0")["expose"][0])
    repinned = box.engine("1.0.0", box.release("1.0.0", b"engine 1.0.0 rebuilt"))
    table = box.write(repinned, revision=2)

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "installed"}
    assert answer["version"] == units.version(table)
    assert Path(repinned["expose"][0]).read_bytes() == b"engine 1.0.0 rebuilt"
    assert os.path.realpath(repinned["expose"][0]) != started_on
    assert Path(started_on).read_bytes() == b"engine 1.0.0"
    assert box.record()["units"] == {"agent-browser": units.pin(repinned)}
    assert box.converge()[1]["units"] == {"agent-browser": "current"}


def test_a_run_cut_short_at_its_first_swap_has_already_taken_the_revision(box, monkeypatch):
    """Otherwise the older table would pass the guard and swap the unit back."""
    box.write(box.engine("1.0.0"), revision=1)
    box.converge()
    box.write(box.engine("2.0.0"), revision=2)
    real = converge._Slot.swap_in

    def swap_then_stop(slot):
        real(slot)
        raise KeyboardInterrupt

    monkeypatch.setattr(converge._Slot, "swap_in", swap_then_stop)
    with pytest.raises(KeyboardInterrupt):
        box.converge()
    monkeypatch.undo()
    box.write(box.engine("1.0.0"), revision=1)

    _, answer = box.converge()

    assert answer["refused"] == "older"
    assert box.current("agent-browser") == "2.0.0"


def test_an_older_table_is_refused_and_a_newer_one_taken(box):
    box.write(box.engine("2.0.0"), revision=2)
    _, taken = box.converge()
    older = box.write(box.engine("1.0.0"), revision=1)

    code, answer = box.converge()

    assert code == 0
    assert answer == {"version": taken["version"], "target": units.version(older), "units": {}, "refused": "older"}
    assert box.current("agent-browser") == "2.0.0"
    assert box.versions("agent-browser") == {"current", "2.0.0"}
    assert box.state()["revision"] == 2
    assert box.converge("--strict")[0] == 1

    newer = box.write(box.engine("3.0.0"), revision=3)
    code, answer = box.converge()

    assert code == 0
    assert "refused" not in answer
    assert answer["units"] == {"agent-browser": "installed"}
    assert answer["version"] == units.version(newer)
    assert box.state()["revision"] == 3
    assert box.status()["revision"] == 3


def test_a_record_from_before_revisions_takes_any_table(box):
    box.root.mkdir()
    (box.root / "state.json").write_text(json.dumps({"version": None, "units": {}}))
    box.write(box.policy())

    _, answer = box.converge()

    assert answer["units"] == {"chrome-policy": "installed"}
    assert box.state()["revision"] == 1


def test_a_record_that_lists_adopted_units_is_read_as_before(box):
    """Runtimes before this one kept an ``adopted`` list; the expose paths
    answer it now, so the list is ignored rather than voiding the record."""
    unit = box.policy()
    policy = Path(unit["expose"][0])
    policy.parent.mkdir(parents=True)
    policy.write_bytes(units.content_bytes(unit))
    # At the pinned mode, whatever the umask the suite runs under.
    policy.chmod(0o644)
    table = box.write(unit)
    box.root.mkdir()
    record = {"revision": 1, "version": units.version(table), "units": {"chrome-policy": units.pin(unit)}}
    (box.root / "state.json").write_text(json.dumps({**record, "adopted": ["chrome-policy"]}))

    _, answer = box.converge()

    assert answer == {"version": units.version(table), "target": units.version(table), "units": {"chrome-policy": "current"}}
    assert not policy.is_symlink()
    assert box.status()["units"] == record["units"]


def test_a_root_that_cannot_be_made_fails_every_unit(box):
    box.root.write_text("a file where the root goes")
    table = box.write(box.engine("1.0.0"), box.policy())

    code, answer = box.converge("--strict")

    assert code == 1
    assert answer == {
        "version": None,
        "target": units.version(table),
        "units": {"agent-browser": "failed:root", "chrome-policy": "failed:root"},
    }


def test_status_reports_a_converge_holding_the_lock(box):
    box.write(box.policy())
    box.root.mkdir()
    with converge._locked(str(box.root)):
        assert box.status()["converging"] is True
    assert box.status()["converging"] is False
    assert not (box.root / converge.CONVERGING).exists()


def test_a_converge_that_died_is_not_under_way(box):
    box.write(box.policy())
    box.root.mkdir()
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()
    (box.root / converge.CONVERGING).write_text(str(gone.pid))
    assert box.status()["converging"] is False


def test_a_marker_whose_pid_another_process_took_is_not_under_way(box, monkeypatch):
    """A converge killed outright leaves its marker, and its pid may name a
    process started since: the start time it recorded tells them apart."""
    box.write(box.policy())
    box.root.mkdir()
    me = os.getpid()
    started = {me: "100"}
    monkeypatch.setattr(converge, "_started", started.get)
    (box.root / converge.CONVERGING).write_text(f"{me} 100")
    assert box.status()["converging"] is True
    started[me] = "200"
    assert box.status()["converging"] is False


def test_a_converge_that_cannot_take_the_lock_answers_busy(box, monkeypatch):
    """One stuck holder costs later runs an answer, not a process each waiting."""
    table = box.write(box.policy())
    box.root.mkdir()
    monkeypatch.setattr(converge, "_LOCK_WAIT_S", 0.2)
    monkeypatch.setattr(converge, "_LOCK_POLL_S", 0.05)
    holder = os.open(box.root / converge.LOCK, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        held = subprocess.Popen(
            [sys.executable, "-c", "import fcntl, os, sys, time; fd = os.open(sys.argv[1], os.O_RDWR);"
             " fcntl.flock(fd, fcntl.LOCK_EX); print(flush=True); time.sleep(30)", str(box.root / converge.LOCK)],
            stdout=subprocess.PIPE,
        )
        held.stdout.readline()
        answer = converge.converge(str(box.root), table)
    finally:
        held.kill()
        held.wait()
        os.close(holder)
    assert answer.failures == {"chrome-policy": "busy"}


def test_the_lock_is_root_s_alone_and_what_others_read_is_readable(box):
    """Anyone who could open the lock could hold it and stall every converge,
    so it stays its owner's, whatever an earlier run left; the browser's
    ``status`` reads the state and the converging marker as the computer's
    user, whatever the umask."""
    box.write(box.policy())
    box.root.mkdir()
    (box.root / converge.LOCK).touch()
    os.chmod(box.root / converge.LOCK, 0o644)
    was = os.umask(0o077)
    try:
        with converge._locked(str(box.root)):
            assert os.stat(box.root / converge.CONVERGING).st_mode & 0o777 == 0o644
        box.converge()
    finally:
        os.umask(was)
    assert os.stat(box.root / converge.LOCK).st_mode & 0o777 == 0o600
    assert os.stat(box.root / converge.STATE).st_mode & 0o777 == 0o644


def test_folders_a_converge_makes_are_open_to_every_user_whatever_the_umask(box):
    """The computer's user reaches the engine and reads the record through them."""
    unit = box.engine("1.0.0")
    unit["expose"] = [str(box.base / "new" / "lib" / "agent-browser")]
    box.write(unit)
    was = os.umask(0o077)
    try:
        box.converge()
    finally:
        os.umask(was)
    for folder in (box.root, box.root / "agent-browser", box.base / "new", box.base / "new" / "lib"):
        assert os.stat(folder).st_mode & 0o777 == 0o755, folder


def test_the_browser_reads_the_set_version_and_a_converge_under_way(box, monkeypatch):
    table = box.write(box.policy())
    monkeypatch.setattr(units, "DEFAULT_ROOT", str(box.root))
    assert protocol.toolset_seen() == protocol.Toolset(None, converging=False)
    box.converge()
    with converge._locked(str(box.root)):
        assert protocol.toolset_seen() == protocol.Toolset(units.version(table), converging=True)
    assert protocol.toolset_seen() == protocol.Toolset(units.version(table), converging=False)


def test_the_package_runs_as_a_folder_under_isolated_python(box):
    box.write(box.policy())

    run = subprocess.run(
        [sys.executable, "-I", str(_PACKAGE), "converge", "--root", str(box.root), "--units", str(box.table)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout.splitlines()[-1])["units"] == {"chrome-policy": "installed"}


def test_an_unusable_table_installs_nothing(box):
    box.table.write_text(json.dumps({"schema": 1, "revision": 1, "units": [{"name": "x", "kind": "pip"}]}))

    assert cli.main(["converge", "--root", str(box.root), "--units", str(box.table)]) == 2
    assert not box.root.exists()


def _valid(box: Box) -> list[dict]:
    return [box.engine("1.0.0"), box.chrome(), box.policy()]


def _break(change):
    def apply(table: list[dict]) -> list[dict]:
        table = copy.deepcopy(table)
        change(table)
        return table

    return apply


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (_break(lambda t: t[0].update(kind="pip")), "unknown kind"),
        (_break(lambda t: t[0].pop("file")), "missing file"),
        (_break(lambda t: t[1].pop("target_glob")), "missing target_glob"),
        (_break(lambda t: t[0].update(extra=1)), "unknown extra"),
        (_break(lambda t: t[0]["platforms"][PLATFORM].pop("sha256")), "missing sha256"),
        (_break(lambda t: t[0]["platforms"][PLATFORM].update(sha256="abc")), "sha256 is 64"),
        (_break(lambda t: t[0]["platforms"][PLATFORM].update(url="http://example.com/x")), "url is https"),
        (_break(lambda t: t[0].update(platforms={})), "platforms"),
        (_break(lambda t: t[0].update(mode="755")), "mode"),
        (_break(lambda t: t[0].update(version="../x")), "version"),
        (_break(lambda t: t[0].update(version="current")), "current"),
        (_break(lambda t: t[0].update(name="Agent")), "unit name"),
        (_break(lambda t: t[0].update(file="a/b")), "file"),
        (_break(lambda t: t[0].update(expose=["relative/path"])), "expose"),
        (_break(lambda t: t[2].update(content={"BrowserSignin": 1})), "content's"),
        (_break(lambda t: t.append(copy.deepcopy(t[0]))), "named twice"),
        (_break(lambda t: t[2].update(expose=t[0]["expose"])), "exposed twice"),
        # One name under another spelling would be exposed twice unseen.
        (_break(lambda t: t[2].update(expose=["/usr/local/./bin/x"])), "expose"),
        (_break(lambda t: t[2].update(expose=["//usr/local/bin/x"])), "expose"),
        (_break(lambda t: t[2].update(expose=["/usr/local/bin/x/"])), "expose"),
        (_break(lambda t: t[0].update(kind=["binary"])), "unknown kind"),
        (_break(lambda t: t[0]["platforms"][PLATFORM].update(url="https://[example.com/x")), "url is https"),
        (_break(lambda t: t.clear()), "non-empty"),
        (_break(lambda t: t[2].update(expose=[t[0]["expose"][0] + ".toolset.tmp"])), "swap's own"),
        (_break(lambda t: t[2].update(expose=[t[0]["expose"][0] + ".toolset.was"])), "swap's own"),
        (_break(lambda t: t[2].update(content="\ud800")), "UTF-8"),
    ],
)
def test_a_table_that_cannot_be_used_is_refused(box, change, message):
    with pytest.raises(units.UnitsError, match=message):
        units.validate({"schema": 1, "revision": 1, "units": change(_valid(box))})


@pytest.mark.parametrize("schema", [2, True, 1.0])
def test_a_table_of_another_schema_is_refused(box, schema):
    with pytest.raises(units.UnitsError, match="schema"):
        units.validate({"schema": schema, "revision": 1, "units": _valid(box)})


@pytest.mark.parametrize("revision", [None, 0, -1, "1", 1.0, True])
def test_a_table_without_a_positive_integer_revision_is_refused(box, revision):
    table = {"schema": 1, "units": _valid(box)}
    if revision is not None:
        table["revision"] = revision
    with pytest.raises(units.UnitsError, match="revision"):
        units.validate(table)


def test_the_set_version_follows_the_revision_the_order_and_every_unit_but_not_key_order(box):
    table = units.Table(1, tuple(_valid(box)))
    reordered = units.Table(1, tuple(dict(reversed(list(unit.items()))) for unit in table.units))
    bumped = copy.deepcopy(table.units)
    bumped[1]["version"] = "2"

    assert table.version == units.version(table) == units.version(reordered)
    assert units.version(table) != units.version(units.Table(1, bumped))
    assert units.version(table) != units.version(units.Table(2, table.units))
    assert units.version(table) != units.version(units.Table(1, table.units[::-1]))
    assert len(units.version(table)) == 16
    assert units.version() == units.version(units.load())


#: Every table that has shipped, by revision. A change to the table takes a
#: new revision and a new line here; an existing line never changes, since a
#: machine that took a revision refuses any table numbered below it.
_SHIPPED = {1: "7980d11cd0ddf85c"}


def test_every_change_to_the_shipped_table_raises_its_revision():
    table = units.load()

    assert table.revision == max(_SHIPPED)
    assert units.version(table) == _SHIPPED[table.revision], "raise the table's revision and add it to _SHIPPED"


def test_the_guard_installs_before_what_it_guards():
    assert [unit["name"] for unit in units.load().units] ==["chrome-policy", "chrome-for-testing", "agent-browser"]


def test_the_shipped_table_is_the_engine_and_policy_the_browser_uses():
    shipped = {unit["name"]: unit for unit in units.load().units}

    assert set(shipped) == {"agent-browser", "chrome-for-testing", "chrome-policy"}
    engine = shipped["agent-browser"]
    for name, release in engine["platforms"].items():
        assert protocol.engine_release(name) == protocol.EngineRelease(
            engine["version"], release["url"], release["sha256"]
        )
        assert release["url"] == (
            f"https://github.com/vercel-labs/agent-browser/releases/download/v{engine['version']}/agent-browser-{name}"
        )
    assert set(engine["platforms"]) == {"linux-x64", "linux-arm64", "darwin-x64", "darwin-arm64"}
    assert engine["expose"] == [protocol.ENGINE_BIN]
    assert shipped["chrome-for-testing"]["expose"] == [protocol.CHROME_BIN]
    # Playwright's Chromium is Chrome for Testing, which reads policy only here.
    assert shipped["chrome-policy"]["expose"] == ["/etc/opt/chrome_for_testing/policies/managed/langalpha.json"]
    assert units.DEFAULT_ROOT == "/opt/langalpha/toolset"
    assert protocol.ENGINE_UNIT in shipped


def test_the_runtime_names_platforms_as_the_browser_install_does():
    assert converge.platform_name() == install.platform_name()


def test_the_runtime_ships_beside_the_browsers():
    shipped = {str(rel) for _, rel, _ in _internal_package_uploads(_ROOT / "src")}

    expected = {f"toolset_runtime/{p.name}" for p in _PACKAGE.iterdir() if p.suffix in (".py", ".json")}
    assert expected <= shipped
    assert "toolset_runtime/units.json" in shipped
    assert "browser_runtime/protocol.py" in shipped


def test_the_browser_looks_for_the_policy_where_the_table_puts_it():
    from ptc_agent.core.sandbox.browser_runtime import protocol

    (policy,) = (u for u in units.load().units if u["name"] == "chrome-policy")
    assert policy["expose"] == [protocol.CHROME_POLICY_FILE]


def test_a_release_with_no_pinned_size_stops_at_the_cap(box, monkeypatch):
    release = box.release("2.0.0", b"engine 2.0.0 and then some more")
    monkeypatch.setattr(converge, "_CHUNK", 4)
    monkeypatch.setattr(converge, "_MAX_UNSIZED", 8)
    box.write(box.engine("2.0.0", release))

    _, answer = box.converge()

    assert answer["units"] == {"agent-browser": "failed:size"}


def test_a_dropped_unit_s_names_go_under_another_spelling_of_the_root(box):
    engine, policy = box.engine("1.0.0"), box.policy()
    box.write(engine, policy)
    box.converge()
    alias = box.base / "root-alias"
    alias.symlink_to(box.root, target_is_directory=True)
    table = box.write(engine, revision=2)

    answer = converge.converge(str(alias), table)

    assert answer.failures == {}
    assert not os.path.lexists(policy["expose"][0])


def test_the_table_carries_the_policy_the_browser_runtime_derives():
    """Regenerated in memory, as ``scripts/gen_toolset_pins.py`` writes it."""
    spec = importlib.util.spec_from_file_location("gen_toolset_pins", _ROOT / "scripts" / "gen_toolset_pins.py")
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    shipped = gen.TABLE.read_text()
    assert gen.pinned(shipped) == shipped
