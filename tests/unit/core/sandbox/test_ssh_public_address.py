"""Which addresses the server dials while private ones are not allowed.

An IPv6 form that carries an IPv4 address is judged by that address, so a
user cannot reach the server's own network, or the cloud's metadata service,
by writing an IPv4 address the IPv6 way.
"""

import pytest

from ptc_agent.core.sandbox.providers._ssh_connect import is_public_address


@pytest.mark.parametrize(
    ("address", "public"),
    [
        ("8.8.8.8", True),
        ("2606:4700::1111", True),
        ("10.0.0.5", False),
        ("127.0.0.1", False),
        ("169.254.169.254", False),
        ("fe80::1", False),
        ("ff02::1", False),
        ("::ffff:169.254.169.254", False),  # IPv4-mapped
        ("64:ff9b::a9fe:a9fe", False),  # NAT64
        ("::a9fe:a9fe", False),  # IPv4-compatible
        ("::ffff:0:a9fe:a9fe", False),  # IPv4-translated (SIIT)
        ("::ffff:0:808:808", True),
        ("::7f00:1", False),
        ("::808:808", True),
    ],
)
def test_an_address_is_judged_by_the_ipv4_it_carries(address, public):
    assert is_public_address(address) is public
