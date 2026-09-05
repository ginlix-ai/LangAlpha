"""A server's business card must never cost it its tools.

``_server_identity`` reads a display-only field out of a handshake result, and
the handshake is the only chance to learn what a server offers. Anything that
raises here aborts negotiation, so a server that stamps nonsense in a field
nothing needs would lose every tool it has over a decorative value. The
docstring promises that every malformed shape reads as absent; these pin it,
because the shapes that break it are the ones no well-behaved server sends.
"""

import pytest

from ptc_agent.core.sandbox import mcp_client_runtime as m


class TestModernEra:
    def test_the_card_comes_back_when_the_server_stamped_one(self):
        card = {"name": "Acme", "websiteUrl": "https://acme.test"}
        result = {"_meta": {m._SERVER_INFO_META_KEY: card}}
        assert m._server_identity(result, modern=True) == card

    @pytest.mark.parametrize("meta", ["text", ["list"], 7, True, 1.5])
    def test_a_non_object_meta_reads_as_absent_rather_than_raising(self, meta):
        # The regression: ``(result.get("_meta") or {}).get(...)`` reaches
        # ``.get`` on whatever truthy value arrived and raises AttributeError,
        # which propagates out of negotiation and drops the server's tools.
        assert m._server_identity({"_meta": meta}, modern=True) is None

    @pytest.mark.parametrize("card", ["text", ["list"], 7, None])
    def test_a_card_that_is_not_an_object_reads_as_absent(self, card):
        result = {"_meta": {m._SERVER_INFO_META_KEY: card}}
        assert m._server_identity(result, modern=True) is None

    def test_a_result_with_no_meta_at_all_is_fine(self):
        assert m._server_identity({}, modern=True) is None


class TestLegacyEra:
    def test_the_card_comes_back_from_server_info(self):
        card = {"name": "Acme"}
        assert m._server_identity({"serverInfo": card}, modern=False) == card

    @pytest.mark.parametrize("card", ["text", ["list"], 7, None])
    def test_a_card_that_is_not_an_object_reads_as_absent(self, card):
        assert m._server_identity({"serverInfo": card}, modern=False) is None

    def test_the_modern_key_is_not_read_in_the_legacy_era(self):
        result = {"_meta": {m._SERVER_INFO_META_KEY: {"name": "Acme"}}}
        assert m._server_identity(result, modern=False) is None
