"""What a plugin source's redirect chain may say.

``fetch_plugin_source`` promises PluginFatal on every failure class, and the
endpoint above it turns anything else into a package conflict. A Location
header is written by the upstream, so it is exactly the kind of string that
can be unparseable.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.fetch import fetch_plugin_source


def _redirecting_to(location: str):
    """A client whose first answer is a 302 carrying ``location``."""

    @asynccontextmanager
    async def _client(target, max_bytes=None):
        response = MagicMock(status_code=302, headers={"location": location})
        yield MagicMock(get=AsyncMock(return_value=response))

    return _client


class TestAnInvalidRedirectLocation:
    @pytest.mark.asyncio
    async def test_it_arrives_as_the_failure_class_the_caller_handles(
        self, monkeypatch
    ):
        from src.server.services.plugins import fetch

        async def _pin(url):
            return url

        monkeypatch.setattr(fetch, "pin_public_url", _pin)
        monkeypatch.setattr(
            fetch, "pinned_stream_client", _redirecting_to("http://[bad")
        )

        # Not a bare ValueError: the endpoint's generic handler answers 409 to
        # that, telling the user their package conflicts when the source is
        # simply broken.
        with pytest.raises(PluginFatal) as caught:
            await fetch_plugin_source("https://vendor.test/pkg.zip")

        assert "redirect" in str(caught.value).lower()

    @pytest.mark.asyncio
    async def test_an_ordinary_relative_redirect_is_still_followed(
        self, monkeypatch
    ):
        from src.server.services.plugins import fetch

        async def _pin(url):
            return url

        seen: list[str] = []

        @asynccontextmanager
        async def _client(target, max_bytes=None):
            async def _get(url):
                seen.append(url)
                if len(seen) == 1:
                    return MagicMock(
                        status_code=302, headers={"location": "/moved.zip"}
                    )
                return MagicMock(status_code=200, content=b"zip", headers={})

            yield MagicMock(get=_get)

        monkeypatch.setattr(fetch, "pin_public_url", _pin)
        monkeypatch.setattr(fetch, "pinned_stream_client", _client)

        body, _subdir = await fetch_plugin_source("https://vendor.test/pkg.zip")

        assert body == b"zip"
        assert seen[-1] == "https://vendor.test/moved.zip"
