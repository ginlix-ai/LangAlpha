"""The market-insight research graph: what it may touch and which model runs it."""

from unittest.mock import MagicMock, patch

from ptc_agent.config import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.agent.insight_graph import background_client, insight_tools


def _config(background: str | None = "background-model") -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(name="main-model", background=background),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        cache_key="thread-1",
    )


def test_a_brief_binds_research_tools_and_nothing_on_the_account():
    """A brief runs as the deployment or as a user with no workspace open, so
    nothing it binds may reach that user's workspaces, threads or charts."""
    search = MagicMock()
    search.name = "WebSearch"
    with patch(
        "ptc_agent.agent.insight_graph.get_web_search_tool",
        return_value=search,
    ):
        names = {tool.name for tool in insight_tools(_config())}

    assert names == {
        "WebSearch",
        "WebFetch",
        "get_sec_filing",
        "get_quote",
        "get_daily_prices",
        "get_company_overview",
        "get_market_overview",
        "get_options_chain",
        "screen_stocks",
    }


def test_the_background_model_is_built_by_name_with_the_cache_key():
    built = MagicMock()
    with (
        patch("src.llms.llm.ensure_model_in_manifest"),
        patch("src.llms.create_llm", return_value=built) as create,
    ):
        assert background_client(_config()) is built

    create.assert_called_once_with("background-model", cache_key="thread-1")


def test_a_client_the_resolver_built_wins_over_the_name():
    """BYOK and OAuth arrive as a built client; building from the name would
    use the deployment's own key instead."""
    config = _config()
    config.llm_client = prebuilt = MagicMock()
    with patch("src.llms.create_llm") as create:
        assert background_client(config) is prebuilt

    create.assert_not_called()


def test_no_background_model_runs_the_main_one():
    built = MagicMock()
    with (
        patch("src.llms.llm.ensure_model_in_manifest"),
        patch("src.llms.create_llm", return_value=built) as create,
    ):
        assert background_client(_config(background=None)) is built

    create.assert_called_once_with("main-model", cache_key="thread-1")
