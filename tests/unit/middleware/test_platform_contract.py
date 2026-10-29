"""The platform contract: one surface vocabulary, and who writes its rules.

The surface a turn arrives on is declared by the caller, pointed at by the
turn's own row in history, and defined by a paragraph in that same row. Three
things are load-bearing enough to pin here.

The pointer has to survive a name this build has never seen, because the caller
owns the vocabulary and ships on its own schedule; a dropped pointer would be a
silent downgrade to the web shape. The symbol has to reach the model as its own
value rather than glued to the surface key, since that is the whole reason the
grammar has a suffix. And the split of who writes the rules has to hold:
``envelope/surface_rules.md.j2`` states a line of its own only for the surfaces
langalpha renders, and every other surface gets whatever its client sent in
``ChatRequest.surface_rules``, or nothing at all.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest
from langchain_anthropic.chat_models import ChatAnthropic
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.runtime_context import (
    KNOWN_SURFACES,
    Surface,
    RowCarrierMiddleware,
    TurnContextMiddleware,
    is_known_surface,
    parse_surface,
    split_surface,
)

NOW = datetime(2026, 9, 8, 15, 2, tzinfo=UTC)


def _flatten(content) -> str:
    """All text in one message body, whether it is a string or a block list.

    The envelope may ride as its own trailing message or as a block appended to
    the last user message, and this test is about what the model reads, not
    about which carrier placed it there.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(_flatten(part) for part in content)
    if isinstance(content, dict):
        return " ".join(str(v) for v in content.values() if isinstance(v, str))
    return ""


async def _rendered_request(**kwargs) -> str:
    """Everything the model would read on one call, the turn's row included."""
    written = await TurnContextMiddleware(now=NOW, **kwargs).abefore_agent({})
    rows = (written or {}).get("messages") or []
    captured: dict = {}

    async def handler(request: ModelRequest):
        captured["messages"] = list(request.messages)
        return MagicMock()

    await RowCarrierMiddleware().awrap_model_call(
        ModelRequest(
            model=MagicMock(spec=ChatAnthropic),
            messages=[HumanMessage(content="What moved today?"), *rows],
            system_prompt="Static system prompt.",
            state={},
            tools=[],
        ),
        handler,
    )
    return " ".join(_flatten(m.content) for m in captured["messages"])


class TestParseSurface:
    def test_a_bare_surface_has_no_symbol(self):
        assert parse_surface("web") == Surface("web", None)

    def test_a_parameterized_surface_splits_on_the_colon(self):
        assert parse_surface("market_view:AAPL") == Surface("market_view", "AAPL")

    def test_a_dotted_ticker_survives_the_split(self):
        """Only the first colon separates. Dots and dashes belong to the symbol."""
        assert parse_surface("market_view:BRK.B") == Surface("market_view", "BRK.B")
        assert parse_surface("market_view:002851.SZ") == Surface(
            "market_view", "002851.SZ"
        )

    @pytest.mark.parametrize("value", [None, "", "   ", 42, {"platform": "web"}])
    def test_nothing_usable_parses_to_a_pair_of_nones(self, value):
        assert parse_surface(value) == Surface(None, None)

    def test_an_unknown_surface_still_parses(self):
        """Parsing is not admission control; the allowlist only decides logging."""
        assert parse_surface("carrier_pigeon:AAPL") == Surface("carrier_pigeon", "AAPL")
        assert is_known_surface("carrier_pigeon") is False

    def test_the_older_name_still_unpacks_as_a_tuple(self):
        assert split_surface("market_view:AAPL") == ("market_view", "AAPL")


class TestThePointerReachesTheModel:
    @pytest.mark.asyncio
    async def test_surface_symbol_and_origin_all_render(self):
        text = await _rendered_request(platform="market_view:AAPL", origin="automation")

        assert "market_view" in text
        assert "AAPL" in text
        assert "automation" in text

    @pytest.mark.asyncio
    async def test_the_symbol_is_its_own_value_not_part_of_the_surface(self):
        """The contract is keyed on the surface alone, so the compound string
        must never reach the model as the surface name."""
        text = await _rendered_request(platform="market_view:AAPL")

        assert "market_view:AAPL" not in text
        assert "market_view" in text
        assert "AAPL" in text

    @pytest.mark.asyncio
    async def test_an_unknown_surface_still_renders(self):
        """The caller may add a surface before this build learns it. Dropping
        the pointer would silently downgrade the turn to the default shape."""
        text = await _rendered_request(platform="carrier_pigeon")

        assert "carrier_pigeon" in text

    @pytest.mark.asyncio
    async def test_no_platform_renders_no_run_line(self):
        text = await _rendered_request()

        assert "Surface `" not in text
        assert "started by" not in text
        assert "subagent reporting to a parent agent" not in text


class TestTheLowDiskLine:
    @pytest.mark.asyncio
    async def test_a_low_disk_is_stated_in_megabytes(self):
        text = await _rendered_request(disk_free_mb=180)

        assert "The computer's disk is nearly full: 180 MB free" in text

    @pytest.mark.asyncio
    async def test_no_reading_states_nothing(self):
        assert "nearly full" not in await _rendered_request()


#: What a channel gateway sends for its own surface, since langalpha ships no
#: wording for one.
CALLER_RULES = "Surface slack: plain text, a few short paragraphs, no widgets."


def _rules(surface: str, surface_rules: str | None = None) -> str:
    from ptc_agent.agent.prompts import guidance_template_vars, init_loader

    return init_loader().render(
        "envelope/surface_rules.md.j2",
        **guidance_template_vars("lean"),
        surface=surface,
        surface_rules=surface_rules,
    )


class TestTheReadersAgree:
    @pytest.mark.parametrize("surface", sorted(KNOWN_SURFACES))
    def test_every_known_surface_passes_the_request_validator(self, surface):
        from src.server.models.chat import ChatRequest

        assert ChatRequest(platform=surface).platform == surface
        assert parse_surface(ChatRequest(platform=surface).platform).name == surface

    @pytest.mark.parametrize("surface", ["web", "market_view"])
    def test_the_surfaces_langalpha_renders_state_a_line_of_their_own(self, surface):
        """These two are drawn by langalpha's own frontend, so langalpha is the
        one that knows what they accept and ships the wording."""
        rendered = _rules(surface)

        assert rendered.strip()
        assert surface in rendered

    def test_a_channel_states_nothing_until_its_client_sends_rules(self):
        """The gateway owns what its channel renders, so langalpha carries no
        wording for it. Until the gateway sends one, the row names the surface
        and states no rules under it."""
        assert not _rules("slack").strip()

    def test_a_channel_states_exactly_what_its_client_sent(self):
        """Verbatim, with no wrapper and no label: the text is the paragraph."""
        assert _rules("slack", CALLER_RULES).strip() == CALLER_RULES

    def test_caller_rules_replace_the_built_in_line(self):
        """Even on a surface langalpha has a line for. The client that renders
        the reply is the authority on it."""
        rendered = _rules("web", CALLER_RULES).strip()

        assert rendered == CALLER_RULES
        assert "full markdown, no length cap" not in rendered

    def test_the_thread_create_route_accepts_the_same_grammar(self):
        """A MarketView thread carries its tag from creation, because the
        per-message path never updates platform on an existing row."""
        from src.server.models.conversation import ThreadCreateRequest

        request = ThreadCreateRequest(
            workspace_id="ws-1", first_query="what moved?", platform="market_view:AAPL"
        )
        assert parse_surface(request.platform) == Surface("market_view", "AAPL")
