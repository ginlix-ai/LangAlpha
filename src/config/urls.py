"""The URL settings this process runs on: where each was read from, and what
stops a managed deployment.

Every process logs one line per URL setting at boot, naming its source and the
shape of its value (scheme, host class, path, trailing slash), never the value.
A managed deployment (``HOST_MODE=platform`` with ``APP_ENV=production``)
refuses to start on a URL it depends on that is not set, or on one set to
something that is not a usable URL; anywhere else those are warnings. The
sandbox-side checks live in src/server/services/egress/reachability.py, which
knows the providers.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from urllib.parse import SplitResult, urlsplit

from src.config import env

logger = logging.getLogger(__name__)

#: Settings a managed deployment cannot start without setting itself.
_REQUIRED_WHEN_MANAGED = (
    "SERVER_BASE_URL",
    "PUBLIC_APP_URL",
    "PLATFORM_SERVICE_URL",
    "SERVER_SANDBOX_URL",
    "LANGALPHA_DATA_URL",
    "CHANNEL_GATEWAY_URL",
    "AUTOMATION_WEBHOOK_URL",
)

_CONTAINER_ALIASES = {"host.docker.internal", "host.containers.internal"}
_NUMERIC_HOST = re.compile(r"^(0x[0-9a-f]+|[0-9]+)(\.(0x[0-9a-f]+|[0-9]+)){0,3}$")
#: What a host name, an IPv4 address or an IPv6 one with its zone can hold.
_HOST_CHARS = re.compile(r"[A-Za-z0-9._%:-]+")
_KNOWN_SCHEMES = {"http", "https", "ws", "wss", "ftp", "file"}
#: Settings that name a whole endpoint rather than a base paths are appended to.
_ENDPOINTS = {"AUTOMATION_WEBHOOK_URL"}


def managed() -> bool:
    """Whether this is a managed deployment, which refuses to start on a URL problem."""
    return env.HOST_MODE == "platform" and env.APP_ENV == "production"


def _split(url: str) -> SplitResult | None:
    """*url* split into parts, or None when it does not parse (a bad port included)."""
    try:
        parts = urlsplit(url)
        _ = parts.port
    except ValueError:
        return None
    return parts


def host_class(url: str) -> str:
    """Where *url*'s host is: ``loopback``, ``private``, ``container`` (an alias only
    containers resolve), ``public``, or ``none`` when it names no host.

    A dotted DNS name reads as public, since telling otherwise takes a lookup;
    a bare name or one under .internal, .local or .lan reads as private. A
    shorthand IPv4 address (``127.1``, ``0x7f.1``) reads as the address it
    resolves to.
    """
    parts = _split(url)
    host = (parts.hostname or "").lower().rstrip(".") if parts else ""
    if not host:
        return "none"
    if host == "localhost" or host.endswith(".localhost"):
        return "loopback"
    if host in _CONTAINER_ALIASES:
        return "container"
    if _NUMERIC_HOST.match(host):
        try:
            host = socket.inet_ntoa(socket.inet_aton(host))
        except OSError:
            pass
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return "private" if "." not in host or host.endswith((".internal", ".local", ".lan")) else "public"
    if ip.is_loopback or ip.is_unspecified:
        return "loopback"
    return "public" if ip.is_global else "private"


def _defect(name: str, url: str) -> str | None:
    """Why *url* is not a usable URL for *name*, without repeating any of it, or None."""
    parts = _split(url)
    if parts is None:
        return "it does not parse as a URL"
    schemes = ("ws", "wss") if name.endswith("_WS_URL") else ("http", "https")
    if parts.scheme not in schemes:
        return f"its scheme is not {' or '.join(schemes)}"
    if not parts.hostname:
        return "it names no host"
    if not _HOST_CHARS.fullmatch(parts.hostname):
        return "its host is not a valid host name"
    if parts.fragment or url.endswith("#"):
        return "it carries a fragment, which is never sent"
    if name not in _ENDPOINTS and (parts.query or url.endswith("?")):
        return "it carries a query, and paths are appended to it"
    return None


def _shape(url: str) -> str:
    if not url:
        return "empty"
    parts = _split(url)
    if parts is None:
        return "unparsable"
    scheme = parts.scheme if parts.scheme in _KNOWN_SCHEMES else ("none" if not parts.scheme else "other")
    return (
        f"scheme={scheme} host={host_class(url)} "
        f"path={'yes' if parts.path.strip('/') else 'no'} slash={'yes' if url.endswith('/') else 'no'}"
    )


def _problems() -> list[str]:
    """What is wrong with the URL settings: each required one a managed deployment
    left unset, and each one set to something that is not a usable URL. A value a
    setting fell back to is its parent's, checked under the parent's name."""
    problems = [
        f"{name} is not set"
        for name in _REQUIRED_WHEN_MANAGED
        if managed() and env.URL_SOURCES[name].source != "set"
    ]
    for name, (source, value) in env.URL_SOURCES.items():
        if source == "set" and (defect := _defect(name, value)):
            problems.append(f"{name} is not usable: {defect}")
        if source.startswith("whitespace,"):
            problems.append(f"{name} holds only whitespace, read as unset")
    return problems


def check_at_boot() -> None:
    """Log one line per URL setting (where it was read from and its shape, never
    its value), then refuse or warn on its problems."""
    for name, (source, value) in env.URL_SOURCES.items():
        logger.info("config.url | %s source=%s %s", name, source, _shape(value))
    enforce(_problems())


def enforce(problems: list[str]) -> None:
    """Refuse to start a managed deployment on *problems*; anywhere else, warn of each."""
    if not problems:
        return
    if managed():
        raise RuntimeError(
            "A managed deployment (HOST_MODE=platform, APP_ENV=production) cannot start: "
            + "; ".join(problems) + "."
        )
    for problem in problems:
        logger.warning("config.url | %s", problem)
