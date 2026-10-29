"""Probing a preview port with the credential its URL needs."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Mapping
from urllib.parse import urlsplit

import httpx

_MAX_HOPS = 5


def is_loopback_url(url: str) -> bool:
    """Whether *url* names this host's loopback.

    A preview link's stored upstream is dialed by whichever worker serves the
    request, so one on loopback is refused unless the preview is
    process-local: another worker holds nothing on that port, or a forward to
    another machine. An unspecified address counts, since dialing one reaches
    this host, and so do the shorthand IPv4 forms the resolver accepts
    (``127.1``).
    """
    try:
        host = (urlsplit(url).hostname or "").rstrip(".")
    except ValueError:
        return False
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        try:
            address = ipaddress.IPv4Address(socket.inet_aton(host))
        except OSError:
            return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return address.is_loopback or address.is_unspecified


async def head_preview(
    client: httpx.AsyncClient, url: str, headers: Mapping[str, str]
) -> httpx.Response:
    """HEAD *url*, following redirects only while they stay on its origin.

    The headers open the sandbox's ports, and httpx keeps custom headers
    across a redirect to another host, so an app that sends its visitors to a
    login page elsewhere would hand that host the credential. A redirect that
    leaves the origin is the answer instead: the app is up and replying.
    """
    first = httpx.URL(url)
    response = await client.head(first, headers=dict(headers), follow_redirects=False)
    for _ in range(_MAX_HOPS):
        if not response.is_redirect:
            break
        target = response.url.join(response.headers["location"])
        if (target.scheme, target.host, target.port) != (
            first.scheme,
            first.host,
            first.port,
        ):
            break
        response = await client.head(
            target, headers=dict(headers), follow_redirects=False
        )
    return response
