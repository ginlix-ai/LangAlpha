"""The Chrome-family browsers a person's own machine may have: where each runs
from, which the runtime drives when no image gave it a Chrome (``install``),
and where each keeps its profiles and the key to their cookies, which a person
can bring into the agent's browser (``everyday``)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Browser:
    id: str
    name: str
    #: Its binary inside ``/Applications`` on a Mac, None for one the runtime
    #: does not drive.
    mac_app: str | None
    #: Its binaries on Linux, not the scripts on PATH that start them, so the
    #: browser's first argument is the path the runtime knows it by.
    linux_binaries: tuple[str, ...]
    #: Its folder under ``~/Library/Application Support`` on a Mac.
    mac_data: str | None
    #: Its folder under ``~/.config`` on Linux.
    linux_data: str | None
    #: The keychain item a Mac keeps its cookie key in: service, then account.
    keychain: tuple[str, str]
    #: The application its cookie key is filed under in a Linux desktop's keyring.
    keyring: str | None


#: In the order the runtime prefers one to drive.
BROWSERS = (
    Browser(
        "chrome",
        "Chrome",
        "Google Chrome.app/Contents/MacOS/Google Chrome",
        ("/opt/google/chrome/chrome",),
        "Google/Chrome",
        "google-chrome",
        ("Chrome Safe Storage", "Chrome"),
        "chrome",
    ),
    Browser(
        "chromium",
        "Chromium",
        "Chromium.app/Contents/MacOS/Chromium",
        (
            "/usr/lib/chromium/chromium",
            "/usr/lib/chromium-browser/chromium-browser",
            "/usr/lib64/chromium-browser/chromium-browser",
        ),
        "Chromium",
        "chromium",
        ("Chromium Safe Storage", "Chromium"),
        "chromium",
    ),
    Browser(
        "edge",
        "Edge",
        "Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ("/opt/microsoft/msedge/msedge",),
        "Microsoft Edge",
        "microsoft-edge",
        ("Microsoft Edge Safe Storage", "Microsoft Edge"),
        "microsoft-edge",
    ),
    Browser(
        "brave",
        "Brave",
        "Brave Browser.app/Contents/MacOS/Brave Browser",
        ("/opt/brave.com/brave/brave",),
        "BraveSoftware/Brave-Browser",
        "BraveSoftware/Brave-Browser",
        ("Brave Safe Storage", "Brave"),
        "brave",
    ),
    Browser("arc", "Arc", None, (), "Arc/User Data", None, ("Arc Safe Storage", "Arc"), None),
)
