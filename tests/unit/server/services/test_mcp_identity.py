"""Which mark a server's handshake card names, and how it may name one.

The card arrives from a server the user added, so every rule here exists to
make a bad card cost a monogram rather than anything else. The ranking is the
part with real judgement in it: a server may offer several icons and only one
of them gets drawn, at a size and on a background this code cannot see.
"""

from __future__ import annotations

import base64

import pytest

from ptc_agent.core.mcp_schema import client_identity
from src.server.services.brand_icons import _from_data_uri
from src.server.services.mcp_identity import bounded_identity, icon_source


def _icon(src: str, **kw) -> dict:
    return {"src": src, **kw}


def _data_uri(payload: bytes, mime: str = "image/png") -> str:
    return f"data:{mime};base64,{base64.b64encode(payload).decode()}"


# A 64x64 PNG: past MIN_PIXELS, so the size floor never decides these cases.
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000400000004008060000"
) + b"\x00" * 32


class TestRanking:
    def test_the_only_icon_wins(self):
        assert icon_source({"icons": [_icon("a.png")]}) == "a.png"

    def test_bigger_wins(self):
        assert icon_source({"icons": [
            _icon("small.png", sizes=["16x16"]),
            _icon("big.png", sizes=["512x512"]),
        ]}) == "big.png"

    def test_scalable_outranks_every_fixed_size(self):
        # "any" means SVG, which is right at 28px in a row and at 40px in a
        # detail header; a 512px raster is right at neither.
        assert icon_source({"icons": [
            _icon("raster.png", sizes=["512x512"]),
            _icon("vector.svg", sizes=["any"]),
        ]}) == "vector.svg"

    def test_untinted_beats_a_larger_themed_one(self):
        # The tile follows the reader's theme and this choice is made once, on
        # the host, for everyone. A mark that works on both beds wins even when
        # a themed one would look better on half of the screens.
        assert icon_source({"icons": [
            _icon("dark.svg", sizes=["any"], theme="dark"),
            _icon("plain.png", sizes=["32x32"]),
        ]}) == "plain.png"

    def test_a_themed_icon_is_still_better_than_none(self):
        assert icon_source({"icons": [
            _icon("dark.png", theme="dark"),
        ]}) == "dark.png"

    def test_a_non_square_entry_scores_its_short_edge(self):
        assert icon_source({"icons": [
            _icon("wide.png", sizes=["512x16"]),
            _icon("square.png", sizes=["64x64"]),
        ]}) == "square.png"


class TestRefusals:
    @pytest.mark.parametrize("icons", [
        [],
        [{"src": ""}],
        [{"src": "   "}],
        [{"src": 42}],
        [{"nosrc": "x.png"}],
        ["not-a-dict"],
        [_icon("doc.pdf", mimeType="application/pdf")],
    ])
    def test_nothing_drawable_names_nothing(self, icons):
        assert icon_source({"icons": icons}) is None

    def test_an_undeclared_mime_is_not_held_against_it(self):
        # Most servers omit it, and the bytes are sniffed on arrival anyway.
        assert icon_source({"icons": [_icon("mark")]}) == "mark"

    def test_a_card_that_is_not_a_card(self):
        assert icon_source(None) is None
        assert icon_source("nonsense") is None
        assert icon_source({}) is None


class TestWebsiteFallback:
    def test_the_host_stands_in_when_no_icon_is_declared(self):
        assert icon_source(
            {"websiteUrl": "https://finance.yahoo.com/quote?s=x"}
        ) == "finance.yahoo.com"

    def test_a_declared_icon_outranks_the_website(self):
        # An icon is the mark the author chose; a favicon is whatever their web
        # host happens to serve.
        assert icon_source({
            "icons": [_icon("chosen.png")],
            "websiteUrl": "https://example.com/",
        }) == "chosen.png"

    def test_an_unparseable_website_names_nothing(self):
        assert icon_source({"websiteUrl": "not a url"}) is None
        assert icon_source({"websiteUrl": ""}) is None


class TestDataUris:
    def test_a_real_image_comes_back(self):
        icon = _from_data_uri(_data_uri(_PNG))
        assert icon is not None
        assert icon.content_type == "image/png"
        assert icon.content == _PNG

    def test_the_declared_type_is_not_believed(self):
        # Sniffed, like every fetched icon: a server naming its own content
        # type is not evidence about the bytes behind it.
        assert _from_data_uri(_data_uri(b"<script>alert(1)</script>")) is None

    @pytest.mark.parametrize("uri", [
        "data:image/png;base64,",              # nothing after the comma
        "data:image/png,rawbytes",             # not base64
        "data:image/png;base64,!!!not-b64!!!",
        "data:",
    ])
    def test_a_malformed_uri_is_a_refusal_not_a_crash(self, uri):
        assert _from_data_uri(uri) is None

    def test_an_oversized_payload_is_refused_before_decoding(self):
        # The cap is on the encoded length precisely so a hostile pump never
        # gets allocated.
        assert _from_data_uri("data:image/png;base64," + "A" * (4 << 20) ) is None


class TestClientIdentity:
    """A business card is never worth failing a connection over."""

    def test_a_client_holding_nothing(self):
        assert client_identity(object()) is None

    def test_a_property_that_raises_is_absent_not_fatal(self):
        class Angry:
            @property
            def server_info(self):
                raise RuntimeError("no")

        assert client_identity(Angry()) is None

    def test_a_card_that_will_not_dump_is_absent_not_fatal(self):
        class Undumpable:
            def model_dump(self, **kw):
                raise ValueError("no")

        class Holder:
            server_info = Undumpable()

        assert client_identity(Holder()) is None


class TestHandleIndirection:
    """The handle is the access control, and it is the only one there is.

    The icon route is unauthenticated because an ``<img>`` cannot carry a
    bearer token. What keeps it from being an open image proxy is that a
    source is only reachable after some authenticated list call minted a
    handle for it. A route that resolved a URL handed to it directly would
    fetch whatever any caller named, from any address the egress guard allows.
    """

    @pytest.fixture
    def cache(self, monkeypatch):
        class FakeCache:
            def __init__(self):
                self.store: dict = {}

            async def get(self, key):
                return self.store.get(key)

            async def set(self, key, value, ttl=None):
                self.store[key] = value

        fake = FakeCache()
        # Patched where it is used, not where it is defined: brand_icons binds
        # the name at import, so patching the source module would leave the
        # negative cases passing for the wrong reason.
        monkeypatch.setattr(
            "src.server.services.brand_icons.get_cache_client", lambda: fake
        )
        return fake

    @pytest.mark.asyncio
    async def test_a_minted_source_round_trips(self, cache):
        from src.server.services.brand_icons import publish_icon_source

        handle = await publish_icon_source(_data_uri(_PNG))
        response = await _resolve(handle)
        assert response.status_code == 200
        assert response.body == _PNG

    @pytest.mark.asyncio
    async def test_the_same_source_always_gets_the_same_handle(self, cache):
        # Content-addressed, so a browser can cache the image across list calls
        # and two users declaring one mark share a single entry.
        from src.server.services.brand_icons import publish_icon_source

        assert await publish_icon_source("a.example") == await publish_icon_source(
            "a.example"
        )

    @pytest.mark.asyncio
    async def test_an_unminted_handle_resolves_to_nothing(self, cache):
        assert (await _resolve("0" * 32)).status_code == 404

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempt", [
        "https://attacker.test/pixel.png",
        "http://169.254.169.254/latest/meta-data/",
        "data:image/png;base64,iVBORw0KGgo=",
        "attacker.test",
    ])
    async def test_a_source_handed_in_as_a_handle_is_not_resolved(
        self, cache, attempt
    ):
        # No mint, no fetch. This is the property: the path segment is a key
        # into what was published, never an address to go and read.
        assert (await _resolve(attempt)).status_code == 404
        assert cache.store == {}


async def _resolve(handle: str):
    from src.server.services.brand_icons import icon_response_for_handle

    return await icon_response_for_handle(handle)


class TestIdentityReductionIsTotal:
    """Nothing a server puts in its handshake can raise out of here.

    These fields are the server operator's own text, persisted once and read
    again on every catalog listing. A reducer for a decorative field that
    raises takes down the page holding the row you would delete to fix it, so
    every shape below is answered rather than rejected.
    """

    @pytest.mark.parametrize(
        "server_info",
        [
            {"icons": 42},
            {"icons": "not-a-list"},
            {"icons": {"src": "https://a.test/i.png"}},
            {"icons": [None, "str", 7]},
            {"icons": [{"src": "https://a.test/i.png", "sizes": 123}]},
            # 4,300 digits is CPython's own int() conversion ceiling, so a
            # case below it exercises nothing: the unbounded conversion it is
            # here to catch succeeds. Both operands, because either side of
            # the "x" is converted.
            {"icons": [{"src": "https://a.test/i.png", "sizes": ["9" * 4000 + "x1"]}]},
            {"icons": [{"src": "https://a.test/i.png", "sizes": ["9" * 5000 + "x1"]}]},
            {"icons": [{"src": "https://a.test/i.png", "sizes": ["1x" + "9" * 5000]}]},
            {"websiteUrl": "https://[::1"},
            {"websiteUrl": "http://["},
        ],
    )
    def test_a_malformed_shape_answers_instead_of_raising(self, server_info):
        icon_source(server_info)

    def test_an_unconvertible_dimension_never_outranks_a_real_one(self):
        # Answering 0 rather than raising is only half of it: a size nobody can
        # read has to lose the ranking too, or the guard trades a 500 for the
        # wrong mark.
        assert (
            icon_source(
                {
                    "icons": [
                        {"src": "https://a.test/huge.png", "sizes": ["9" * 5000 + "x1"]},
                        {"src": "https://a.test/real.png", "sizes": ["64x64"]},
                    ]
                }
            )
            == "https://a.test/real.png"
        )

    def test_a_broken_website_does_not_hide_a_good_icon(self):
        # Order matters: the icon wins before the website is ever parsed, so a
        # server with both keeps its mark even when the fallback is garbage.
        assert icon_source(
            {
                "icons": [{"src": "https://a.test/i.png"}],
                "websiteUrl": "https://[::1",
            }
        ) == "https://a.test/i.png"


class TestBoundedIdentity:
    """What of a server's card survives the trip into a row.

    The card is that server's text and the row is read back on every listing,
    so an unreduced copy lets one server decide what every later read costs.
    The reduction has to keep exactly what ``icon_source`` looks at, because
    dropping a field here removes a mark that would otherwise have drawn.
    """

    def test_the_fields_the_mark_is_chosen_from_survive(self):
        card = {
            "name": "Acme",
            "version": "1.2.3",
            "websiteUrl": "https://acme.test",
            "icons": [_icon("https://acme.test/i.png", mimeType="image/png",
                            theme="dark", sizes=["48x48"])],
        }
        out = bounded_identity(card)
        assert out == card
        # The reduction cannot change which mark gets drawn.
        assert icon_source(out) == icon_source(card)

    def test_an_unresolvable_source_is_dropped_rather_than_stored(self):
        # Longer than the icon route will ever resolve, so it could not have
        # produced a mark; storing it would cost every read and buy nothing.
        card = {"icons": [_icon("data:image/png;base64," + "A" * 400_000)],
                "websiteUrl": "https://acme.test"}
        out = bounded_identity(card)
        assert "icons" not in out
        assert out["websiteUrl"] == "https://acme.test"

    def test_undeclared_keys_do_not_ride_along(self):
        out = bounded_identity({"name": "Acme", "decoration": "x" * 100_000})
        assert out == {"name": "Acme"}

    @pytest.mark.parametrize("card", ["text", ["list"], 7, None, {}])
    def test_a_card_that_is_not_one_reads_as_absent(self, card):
        assert bounded_identity(card) is None

    def test_the_result_is_bounded_whatever_the_server_sent(self):
        import json
        card = {
            "name": "n" * 100_000,
            "websiteUrl": "https://acme.test/" + "q" * 100_000,
            "icons": [_icon(f"https://acme.test/{i}.png", sizes=["1x1"] * 500)
                      for i in range(500)],
        }
        assert len(json.dumps(bounded_identity(card))) < 20_000


class TestTheCardCannotOutgrowTheRow:
    """The per-icon cut alone is not a bound.

    Every listing reads this JSONB back, so what matters is the total the card
    can reach, not the size of any one field in it.
    """

    def test_eight_near_maximum_sources_do_not_all_survive(self):
        from src.server.services.brand_icons import MAX_SOURCE_CHARS

        # Each one is individually legal: just under the per-icon cut.
        big = "https://vendor.test/" + ("a" * (MAX_SOURCE_CHARS - 40))
        card = bounded_identity({"icons": [_icon(big) for _ in range(8)]})

        kept = card["icons"]
        assert len(kept) == 1
        assert sum(len(i["src"]) for i in kept) <= MAX_SOURCE_CHARS

    def test_ordinary_variants_all_fit(self):
        # The budget must not cost a server its real themed/scalable set.
        icons = [
            _icon(f"https://vendor.test/{n}.svg", theme=n)
            for n in ("light", "dark", "hc")
        ]
        card = bounded_identity({"icons": icons})

        assert len(card["icons"]) == 3

    def test_a_lone_surrogate_is_not_kept(self):
        # A handshake delivers this as a "\\ud800" escape; Python holds it and
        # no UTF-8 encoder takes it, so storing it breaks the write and the
        # listing that reads it back.
        bad = "https://vendor.test/" + chr(0xD800)
        card = bounded_identity(
            {"icons": [_icon(bad), _icon("https://vendor.test/ok.png")],
             "name": "fine", "title": "bad" + chr(0xDC00)}
        )

        assert [i["src"] for i in card["icons"]] == ["https://vendor.test/ok.png"]
        assert card["name"] == "fine"
        assert "title" not in card
