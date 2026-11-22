"""The person's everyday browsers on their own machine: which profiles are listed,
and how a profile's cookie file becomes the cookies DevTools sets."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ptc_agent.core.sandbox.browser_runtime import care, choices, everyday, launch, protocol
from ptc_agent.core.sandbox.browser_runtime.launch import Launch

def _never_started(wait_s: float):
    raise AssertionError("nothing to bring, so no browser to start")


def _started(wait_s: float):
    return SimpleNamespace(low_memory=False, running=True)


_LINUX_KEY = hashlib.pbkdf2_hmac("sha1", b"peanuts", b"saltysalt", 1, 16)
_DAY_S = 86_400


def _chromium_time(unix_s: float) -> int:
    return int((unix_s + 11644473600) * 1_000_000)


def _encrypt(value: str, host: str) -> bytes:
    """As Chromium on Linux writes a value at cookie file version 24: its host's digest, then the value."""
    padder = padding.PKCS7(128).padder()
    plain = padder.update(hashlib.sha256(host.encode()).digest() + value.encode()) + padder.finalize()
    encryptor = Cipher(algorithms.AES(_LINUX_KEY), modes.CBC(b" " * 16)).encryptor()
    return b"v10" + encryptor.update(plain) + encryptor.finalize()


def _cookie_file(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    db.execute("INSERT INTO meta VALUES ('version', '24')")
    db.execute(
        "CREATE TABLE cookies (host_key TEXT, name TEXT, value TEXT, encrypted_value BLOB, path TEXT,"
        " expires_utc INTEGER, is_secure INTEGER, is_httponly INTEGER, samesite INTEGER,"
        " is_persistent INTEGER, top_frame_site_key TEXT)"
    )
    for row in rows:
        db.execute(
            "INSERT INTO cookies VALUES (:host_key, :name, :value, :encrypted_value, :path, :expires_utc,"
            " :is_secure, :is_httponly, :samesite, :is_persistent, :top_frame_site_key)",
            {
                "value": "",
                "encrypted_value": b"",
                "path": "/",
                "expires_utc": 0,
                "is_secure": 0,
                "is_httponly": 0,
                "samesite": -1,
                "is_persistent": 0,
                "top_frame_site_key": "",
                **row,
            },
        )
    db.commit()
    db.close()


@pytest.fixture
def linux_home(tmp_path, monkeypatch) -> Path:
    """A Linux desktop's config folder, its keyring holding nothing."""
    monkeypatch.setattr(everyday.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setattr(everyday, "_keyring", lambda application: None)
    return tmp_path


def test_each_profile_with_cookies_is_listed_in_the_browser_s_own_order(linux_home):
    root = linux_home / "chromium"
    root.mkdir()
    (root / "Local State").write_text(
        json.dumps(
            {
                "profile": {
                    "profiles_order": ["Profile 2", "Default"],
                    "info_cache": {
                        "Default": {"name": "Me"},
                        "Profile 1": {"name": "No cookies yet"},
                        "Profile 2": {"name": "Work"},
                        "../elsewhere": {"name": "Not a profile folder"},
                        "Profile 3": {"name": "__ARC_SYSTEM_PROFILE"},
                    },
                }
            }
        )
    )
    _cookie_file(root / "Default" / "Network" / "Cookies", [])
    _cookie_file(root / "Profile 2" / "Cookies", [])
    _cookie_file(root / "Profile 3" / "Cookies", [])
    (root / "Profile 1").mkdir()
    assert [s.to_wire() for s in everyday.sources()] == [
        {"browser": "chromium", "name": "Chromium", "profile": "Profile 2", "profile_name": "Work"},
        {"browser": "chromium", "name": "Chromium", "profile": "Default", "profile_name": "Me"},
    ]
    assert everyday.source("chromium", "../elsewhere") is None


def test_a_machine_with_no_desktop_browser_lists_none(linux_home):
    assert everyday.sources() == []


def test_cookies_come_as_devtools_sets_them_with_their_flags(linux_home):
    now = time.time()
    _cookie_file(
        linux_home / "chromium" / "Default" / "Network" / "Cookies",
        [
            # For this host alone, so set by its address rather than widened to a domain.
            {
                "host_key": "app.example.com",
                "name": "session",
                "encrypted_value": _encrypt("s3cret", "app.example.com"),
                "expires_utc": _chromium_time(now + _DAY_S),
                "is_persistent": 1,
                "is_secure": 1,
                "is_httponly": 1,
                "samesite": 1,
            },
            # For the domain and its subdomains, gone when the browser closes.
            {"host_key": ".example.com", "name": "pref", "value": "dark", "samesite": 2},
            {"host_key": ".example.com", "name": "old", "value": "x", "is_persistent": 1, "expires_utc": _chromium_time(now - 60)},
            {"host_key": "widget.example", "name": "embedded", "value": "x", "top_frame_site_key": "https://example.com"},
            # Under the desktop keyring's key, which this machine does not have.
            {"host_key": ".example.com", "name": "locked", "encrypted_value": b"v11" + b"\0" * 32},
        ],
    )
    cookies, unread = everyday.cookies(everyday.source("chromium", "Default"))
    assert unread == 1
    session, pref = cookies
    assert session["url"] == "https://app.example.com/" and "domain" not in session
    assert {k: session[k] for k in ("name", "value", "secure", "httpOnly", "sameSite")} == {
        "name": "session",
        "value": "s3cret",
        "secure": True,
        "httpOnly": True,
        "sameSite": "Lax",
    }
    assert abs(session["expires"] - (now + _DAY_S)) < 1
    assert pref == {
        "name": "pref",
        "value": "dark",
        "path": "/",
        "secure": False,
        "httpOnly": False,
        "domain": ".example.com",
        "sameSite": "Strict",
    }


def test_a_browser_whose_key_cannot_be_read_says_so_rather_than_bringing_nothing(linux_home):
    _cookie_file(
        linux_home / "google-chrome" / "Default" / "Cookies",
        [{"host_key": ".example.com", "name": "locked", "encrypted_value": b"v11" + b"\0" * 32}],
    )
    with pytest.raises(everyday.KeyUnavailable):
        everyday.cookies(everyday.source("chrome", "Default"))


def test_cookies_that_have_expired_never_ask_for_the_key(linux_home):
    """Left behind before their value is unlocked, so they are neither read nor unread."""
    _cookie_file(
        linux_home / "google-chrome" / "Default" / "Cookies",
        [
            {
                "host_key": ".example.com",
                "name": "old",
                "encrypted_value": b"v11" + b"\0" * 32,
                "is_persistent": 1,
                "expires_utc": _chromium_time(time.time() - 60),
            }
        ],
    )
    assert everyday.cookies(everyday.source("chrome", "Default")) == ([], 0)


def test_a_profile_order_holding_what_is_no_folder_name_still_lists_the_profiles(linux_home):
    root = linux_home / "chromium"
    root.mkdir()
    (root / "Local State").write_text(
        json.dumps({"profile": {"profiles_order": [{"x": 1}, ["y"], "Default"], "info_cache": {"Default": {"name": "Me"}}}})
    )
    _cookie_file(root / "Default" / "Cookies", [])
    assert [s.profile for s in everyday.sources()] == ["Default"]


def test_a_python_without_cryptography_answers_that_the_bring_failed(linux_home, tmp_path, monkeypatch):
    """A Mac's own python3 may lack the package, and the server reads an answer, not a traceback."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path / "runtime")
    choices.keep(protocol.Choices(signins=True))
    _cookie_file(
        linux_home / "chromium" / "Default" / "Cookies",
        [{"host_key": ".example.com", "name": "sid", "encrypted_value": _encrypt("x", ".example.com")}],
    )
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.ciphers", None)
    assert everyday.bring_signins("chromium", "Default", _never_started) == {"done": False, "reason": "failed"}


@pytest.mark.parametrize(
    "meanwhile",
    [
        # A sign-out the person asked for, which the cookies read before it must not undo.
        protocol.Choices(signins=True, signins_revision="0" * 19 + "2", revision="0" * 19 + "2"),
        # The folder taken over by another computer, whose first save can carry
        # the same sign-ins revision: its profile is not the one asked for.
        protocol.Choices(signins=True, signins_revision="0" * 19 + "1", revision="0" * 19 + "1", computer="c-2"),
    ],
)
def test_a_bring_that_outlives_a_sign_out_brings_nothing(linux_home, tmp_path, monkeypatch, meanwhile):
    """Reading can wait on the person's keychain, and the choices can change meanwhile."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path / "runtime")
    choices.keep(protocol.Choices(signins=True, signins_revision="0" * 19 + "1", revision="0" * 19 + "1", computer="c-1"))

    def read_then_sign_out(chosen):
        choices.keep(meanwhile)
        return [{"name": "sid", "value": "x", "domain": ".example.com"}], 0

    monkeypatch.setattr(everyday, "cookies", read_then_sign_out)
    monkeypatch.setattr(everyday, "source", lambda browser, profile: object())
    monkeypatch.setattr(launch, "current", lambda: Launch("", "standard", kept=True))
    monkeypatch.setattr(care.Look, "browser_up", lambda self: True)
    engine = []
    monkeypatch.setattr(Launch, "run", lambda *a, **k: engine.append(a))
    assert everyday.bring_signins("chromium", "Default", _started) == {"done": False, "reason": "not_kept"}
    assert engine == []
