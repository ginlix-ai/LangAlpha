"""A bot-message turn hands its chain position to the tools through config."""

from __future__ import annotations

from src.server.handlers.chat.request_prep import build_graph_config
from src.server.models.chat import BotMessageSource, ChatRequest


def test_a_bot_message_chain_reaches_the_graph_configurable():
    request = ChatRequest(
        bot_message_source=BotMessageSource(
            from_bot_id="b-1",
            from_name="Quant",
            message_kind="request",
            root_id="99999999-9999-9999-9999-999999999999",
            depth=2,
        )
    )
    config = build_graph_config(
        thread_id="t-1",
        user_id="u-1",
        workspace_id="w-1",
        timezone_str="UTC",
        token_callback=None,
        request=request,
        effective_model=None,
    )
    assert config["configurable"]["bot_message_root_id"] == (
        "99999999-9999-9999-9999-999999999999"
    )
    assert config["configurable"]["bot_message_depth"] == 2


def test_a_user_turn_carries_no_chain():
    config = build_graph_config(
        thread_id="t-1",
        user_id="u-1",
        workspace_id="w-1",
        timezone_str="UTC",
        token_callback=None,
        request=ChatRequest(),
        effective_model=None,
    )
    assert "bot_message_root_id" not in config["configurable"]
    assert "bot_message_depth" not in config["configurable"]
