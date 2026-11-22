"""The person's everyday browsers on a machine of their own, and the sign-ins in
them, which they can bring into the agent's kept profile (``signin``).

Only Chromium-family browsers: their cookie file is one shape. Each profile's is
read on the machine and unlocked with the browser's own key: on a Mac the one
it keeps in the login keychain, which the Mac asks the person to let us read,
and on Linux Chromium's fixed key or the one the desktop's keyring holds. A
server with no desktop has none of these folders, so it lists nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from . import care, choices, launch
from .browsers import BROWSERS, Browser
from .devtools import DevTools, DevToolsError
from .protocol import BringRefusal, Status

# How Chromium derives the cookie key from what it keeps: PBKDF2-SHA1 over a fixed
# salt, many rounds on a Mac and one on Linux, where with no keyring it uses a
# fixed password.
_SALT = b"saltysalt"
_MAC_ROUNDS = 1003
_LINUX_ROUNDS = 1
_LINUX_FIXED_PASSWORD = b"peanuts"
_IV = b" " * 16
# From this version of the cookie file, each value starts with a digest of its
# host, which is not the value.
_HOST_DIGEST_FROM = 24
_HOST_DIGEST_SIZE = 32
# Chromium's times count microseconds from 1601.
_EPOCH_OFFSET_S = 11644473600
_SAME_SITE = {0: "None", 1: "Lax", 2: "Strict"}
# A profile the browser keeps for itself, not one the person made (Arc's
# ``__ARC_SYSTEM_PROFILE``).
_INTERNAL_PROFILE_PREFIX = "__"
# The Mac's keychain waits on the person's answer.
_KEYCHAIN_WAIT_S = 90
# Cookies set in one DevTools call: Chrome refuses a call whole if any of its
# cookies is bad, so a refused batch is set again one by one.
_COOKIE_BATCH = 200
_DEVTOOLS_WAIT_S = 30
# The browser's start for a bring, as long as the tab's own start waits.
_START_FOR_BRING_S = 30
# How long a read of the cookie file waits on the browser's lock before copying
# its files instead.
_COOKIE_LOCK_WAIT_S = 5


@dataclass(frozen=True)
class Source:
    """One profile of one everyday browser, as the person picks it."""

    browser: Browser
    profile: str
    profile_name: str
    cookies: Path

    def to_wire(self) -> dict[str, str]:
        return {
            "browser": self.browser.id,
            "name": self.browser.name,
            "profile": self.profile,
            "profile_name": self.profile_name,
        }


class KeyUnavailable(Exception):
    """The browser's key could not be read: the person said no, or nothing on
    this login can ask them (a Mac reached over SSH outside its desktop)."""


def sources() -> list[Source]:
    """Each everyday browser profile here that holds a cookie file."""
    found = []
    for browser in BROWSERS:
        root = _root(browser)
        if root is None or not root.is_dir():
            continue
        for folder, label in _profiles(root):
            cookies = _cookie_file(root / folder)
            if cookies is not None:
                found.append(Source(browser, folder, label, cookies))
    return found


def source(browser: str, profile: str) -> Source | None:
    """The source the person picked, only from those this machine has."""
    return next((s for s in sources() if (s.browser.id, s.profile) == (browser, profile)), None)


def cookies(chosen: Source) -> tuple[list[dict[str, object]], int]:
    """The source's cookies as DevTools sets them, and how many could not be
    read. Those that have expired are neither."""
    version, rows = _rows(chosen.cookies)
    keys = _Keys(chosen.browser)
    now = time.time()
    found, unread = [], 0
    for row in rows:
        # Left behind before its value is unlocked, so one that has expired
        # never asks for the key, nor counts as unread.
        cookie = _cookie(row, row.get("value") or "", now)
        if cookie is None:
            continue
        encrypted = row.get("encrypted_value") or b""
        if encrypted:
            value = _decrypt(bytes(encrypted), keys, strip=version >= _HOST_DIGEST_FROM)
            if value is None:
                unread += 1
                continue
            cookie["value"] = value
        found.append(cookie)
    if unread and not found:
        # Not one could be read: the key, not the cookies, is what is wrong.
        keys.check()
    return found, unread


def bring_signins(browser: str, profile: str, start: Callable[[float], Status]) -> dict[str, object]:
    """Bring the sign-ins of the person's *browser* at *profile* into the kept
    profile: its cookies, set in the agent's browser, which *start* opens for it.

    Read first, since on a Mac reading asks the person, who may take a while,
    and the browser should not wait on them. Set under the admission lock, so a
    save that erases the profile lands wholly before or after, never between.
    The cookies go from the person's browser to the agent's in memory, the
    agent's storing them under its own key.
    """
    chosen = source(browser, profile)
    if chosen is None:
        return _refused("not_found")
    # The save these are brought in under: one made meanwhile erased the
    # profile, a sign-out the person asked for, which they must not undo.
    under = choices.held()
    if not (under and under.signins):
        return _refused("not_kept")
    try:
        found, unread = cookies(chosen)
    except KeyUnavailable:
        return _refused("keychain")
    except (OSError, ValueError, ImportError, sqlite3.Error):
        # ImportError: a python with no ``cryptography`` cannot unlock them.
        return _refused("failed")
    if not found:
        return {"done": True, "count": 0, "skipped": unread}
    opened = start(_START_FOR_BRING_S)
    if opened.low_memory:
        return _refused("low_memory")
    if not opened.running:
        return _refused("failed")
    with care.admitted() as seen:
        launched = launch.current()
        now = choices.held()
        if not (
            now
            and now.computer == under.computer
            and now.signins_revision == under.signins_revision
            and launched
            and launched.kept
            and seen.browser_up()
        ):
            # Open on a profile of its own, from before sign-ins were kept, or
            # saved again meanwhile, or the folder another computer's since: these
            # would go with it, undo a sign-out, or land in the other's profile.
            return _refused("not_kept")
        try:
            address = launched.run(seen.host, "get", "cdp-url", timeout=_DEVTOOLS_WAIT_S)
            if not address:
                return _refused("failed")
            with DevTools(address, timeout=_DEVTOOLS_WAIT_S) as devtools:
                count, refused = _set_cookies(devtools, found)
        except (OSError, subprocess.SubprocessError, DevToolsError):
            return _refused("failed")
    return {"done": True, "count": count, "skipped": unread + refused}


def _set_cookies(devtools: DevTools, found: list[dict[str, object]]) -> tuple[int, int]:
    """How many of *found* Chrome took, and how many it refused."""
    count = refused = 0
    for at in range(0, len(found), _COOKIE_BATCH):
        batch = found[at : at + _COOKIE_BATCH]
        try:
            devtools.call("Storage.setCookies", {"cookies": batch})
            count += len(batch)
            continue
        except DevToolsError:
            pass
        for cookie in batch:
            try:
                devtools.call("Storage.setCookies", {"cookies": [cookie]})
                count += 1
            except DevToolsError:
                refused += 1
    return count, refused


def _refused(reason: BringRefusal) -> dict[str, object]:
    return {"done": False, "reason": reason}


def _root(browser: Browser) -> Path | None:
    if sys.platform == "darwin":
        return Path.home() / "Library/Application Support" / browser.mac_data if browser.mac_data else None
    if browser.linux_data is None:
        return None
    config = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(config) / browser.linux_data


def _profiles(root: Path) -> list[tuple[str, str]]:
    """Each profile folder and the name the person gave it, in the browser's
    own order, from its ``Local State``; ``Default`` alone without one."""
    try:
        state = json.loads((root / "Local State").read_text())
        profiles = state["profile"]
        cache = profiles["info_cache"]
    except (OSError, ValueError, KeyError, TypeError):
        return [("Default", "Default")]
    if not isinstance(cache, dict):
        return [("Default", "Default")]
    order = profiles.get("profiles_order") if isinstance(profiles.get("profiles_order"), list) else []
    # A folder is a name; anything else in the list could not be one, nor be looked up.
    order = [f for f in order if isinstance(f, str)]
    folders = [f for f in order if f in cache] + sorted(f for f in cache if f not in order)
    named = []
    for folder in folders:
        info = cache.get(folder)
        if not isinstance(folder, str) or "/" in folder or folder in (".", "..") or not isinstance(info, dict):
            continue
        label = info.get("name")
        if isinstance(label, str) and label.startswith(_INTERNAL_PROFILE_PREFIX):
            continue
        named.append((folder, label if isinstance(label, str) and label.strip() else folder))
    return named


def _cookie_file(profile: Path) -> Path | None:
    for candidate in (profile / "Network" / "Cookies", profile / "Cookies"):
        if candidate.is_file():
            return candidate
    return None


def _rows(path: Path) -> tuple[int, list[dict[str, object]]]:
    """The cookie file's version and rows, read from a copy: the browser may
    have it open, and its newest writes may still be in the journal beside it."""
    with tempfile.TemporaryDirectory(prefix="langalpha-cookies-") as scratch:
        copy = Path(scratch) / "Cookies"
        try:
            source = sqlite3.connect(
                f"file:{quote(str(path))}?mode=ro", uri=True, timeout=_COOKIE_LOCK_WAIT_S
            )
            try:
                target = sqlite3.connect(copy)
                try:
                    with target:
                        source.backup(target, progress=_give_up_after(_COOKIE_LOCK_WAIT_S))
                except sqlite3.Error:
                    target.close()
                    raise
            finally:
                source.close()
        except sqlite3.Error:
            # One the browser holds for itself: its files, journal included.
            for suffix in ("", "-wal", "-journal"):
                if Path(f"{path}{suffix}").is_file():
                    shutil.copyfile(f"{path}{suffix}", f"{copy}{suffix}")
            target = sqlite3.connect(copy)
        try:
            target.row_factory = sqlite3.Row
            try:
                (version,) = target.execute("SELECT value FROM meta WHERE key = 'version'").fetchone()
                version = int(version)
            except (sqlite3.Error, TypeError, ValueError):
                version = 0
            rows = [dict(row) for row in target.execute("SELECT * FROM cookies")]
        finally:
            target.close()
    return version, rows


def _give_up_after(seconds: float) -> Callable[[int, int, int], None]:
    """A backup progress callback that ends the backup past *seconds*.

    ``backup`` retries a locked source for as long as the lock is held, and a
    running browser can hold its cookie file's for as long as it runs.
    """
    deadline = time.monotonic() + seconds

    def progress(status: int, remaining: int, total: int) -> None:
        if status != sqlite3.SQLITE_DONE and time.monotonic() >= deadline:
            raise sqlite3.OperationalError("cookie file locked")

    return progress


class _Keys:
    """The browser's cookie keys, by the prefix each value starts with, read
    the first time one is needed, since reading one on a Mac asks the person."""

    def __init__(self, browser: Browser) -> None:
        self._browser = browser
        self._keys: dict[bytes, bytes | None] = {}

    def get(self, prefix: bytes) -> bytes | None:
        if prefix not in self._keys:
            self._keys[prefix] = self._read(prefix)
        return self._keys[prefix]

    def check(self) -> None:
        """Raise ``KeyUnavailable`` if a key this browser needed was not read."""
        if any(key is None for key in self._keys.values()):
            raise KeyUnavailable(self._browser.id)

    def _read(self, prefix: bytes) -> bytes | None:
        if sys.platform == "darwin":
            return self._mac() if prefix == b"v10" else None
        if prefix == b"v10":
            return _derive(_LINUX_FIXED_PASSWORD, _LINUX_ROUNDS)
        if prefix == b"v11":
            secret = _keyring(self._browser.keyring)
            return _derive(secret, _LINUX_ROUNDS) if secret is not None else None
        return None

    def _mac(self) -> bytes | None:
        service, account = self._browser.keychain
        try:
            result = subprocess.run(
                # By its own path: the keychain asks the person on behalf of
                # whichever ``security`` runs, and a PATH a command can set
                # could name one that keeps the key.
                ["/usr/bin/security", "find-generic-password", "-w", "-s", service, "-a", account],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=_KEYCHAIN_WAIT_S,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        password = result.stdout.rstrip(b"\n")
        return _derive(password, _MAC_ROUNDS) if result.returncode == 0 and password else None


def _keyring(application: str | None) -> bytes | None:
    """The secret the desktop's keyring keeps for *application*, by either
    schema Chromium has filed it under."""
    if application is None or shutil.which("secret-tool") is None:
        return None
    for schema in ("chrome_libsecret_os_crypt_password_v2", "chrome_libsecret_os_crypt_password_v1"):
        try:
            result = subprocess.run(
                ["secret-tool", "lookup", "xdg:schema", schema, "application", application],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=_KEYCHAIN_WAIT_S,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode == 0 and result.stdout:
            return result.stdout.rstrip(b"\n")
    return None


def _derive(password: bytes, rounds: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha1", password, _SALT, rounds, 16)


def _decrypt(blob: bytes, keys: _Keys, *, strip: bool) -> str | None:
    # Here, not at the top: a python with no ``cryptography``, as a Mac's own
    # may be, still lists the browsers, and a bring answers that it failed.
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    key = keys.get(blob[:3])
    body = blob[3:]
    if key is None or not body or len(body) % 16:
        return None
    decryptor = Cipher(algorithms.AES(key), modes.CBC(_IV)).decryptor()
    padded = decryptor.update(body) + decryptor.finalize()
    pad = padded[-1]
    if not 1 <= pad <= 16 or padded[-pad:] != bytes([pad]) * pad:
        return None
    plain = padded[:-pad]
    if strip:
        plain = plain[_HOST_DIGEST_SIZE:]
    try:
        return plain.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _cookie(row: dict[str, object], value: str, now: float) -> dict[str, object] | None:
    """The row as DevTools sets a cookie, or None for one to leave behind: it
    has expired, or belongs to one site inside another (partitioned), which the
    browser keys by more than this file says."""
    host = str(row.get("host_key") or "")
    name = str(row.get("name") or "")
    if not host or row.get("top_frame_site_key"):
        return None
    path = str(row.get("path") or "/")
    secure = bool(row.get("is_secure"))
    cookie: dict[str, object] = {
        "name": name,
        "value": value,
        "path": path,
        "secure": secure,
        "httpOnly": bool(row.get("is_httponly")),
    }
    if host.startswith("."):
        cookie["domain"] = host
    else:
        # A cookie for this host alone, which naming a domain would widen to
        # its subdomains.
        cookie["url"] = f"{'https' if secure else 'http'}://{host}{path}"
    persistent = row.get("is_persistent", row.get("has_expires", 1))
    if persistent:
        expires = int(row.get("expires_utc") or 0) / 1_000_000 - _EPOCH_OFFSET_S
        if expires <= now:
            return None
        cookie["expires"] = expires
    same_site = _SAME_SITE.get(int(row.get("samesite") if row.get("samesite") is not None else -1))
    if same_site:
        cookie["sameSite"] = same_site
    return cookie
