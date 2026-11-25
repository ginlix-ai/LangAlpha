"""Which kinds the server converges onto the tool table.

The table holds the browser's units, so a kind is served exactly where the
browser comes from the image: a machine that sets its own up keeps doing so
until the table has rules for user-space installs.
"""

import pytest

from ptc_agent.core.sandbox.providers import PROVIDER_KINDS, provider_class


@pytest.mark.parametrize("kind", PROVIDER_KINDS)
def test_a_kind_is_served_where_the_image_carries_the_browser(kind):
    cls = provider_class(kind)
    assert cls.serves_toolset is not cls.installs_browser
