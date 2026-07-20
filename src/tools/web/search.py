import logging
from importlib import import_module
from typing import Optional

from src.config import SELECTED_SEARCH_ENGINE
from src.tools.decorators import create_logged_tool
from src.tools.web.manifest import (
    CAPABILITY_SEARCH,
    get_capability,
    providers_with_capability,
)

logger = logging.getLogger(__name__)


def _lazy_builder(provider: str):
    """Deferred import so provider modules load only when selected."""

    def build(**kwargs):
        module = import_module(f"src.tools.web.providers.{provider}")
        return module.build_web_search_tool(**kwargs)

    return build


# Provider name -> tool builder. Adding a provider = one entry here, one
# provider module with build_web_search_tool, one manifest entry.
_PROVIDER_BUILDERS = {
    name: _lazy_builder(name) for name in ("tavily", "serper", "bocha", "exa", "parallel")
}


def get_web_search_tool(
    max_search_results: int,
    time_range: Optional[str] = None,
    verbose: bool = True,
    provider: Optional[str] = None,
    depth: Optional[str] = None,
):
    """Get web search tool with verbosity and time range control.

    Args:
        max_search_results: Maximum number of results to return.
        time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
            LLM can still override by specifying a different time_range.
        verbose: Control verbosity of search results.
            True (default): Include images in results.
            False: Exclude images (lightweight for planning).
        provider: Search engine override (per-user preference). Falls back to
            the deployment default (SELECTED_SEARCH_ENGINE) when unset or invalid.
        depth: Depth level name from the provider's manifest entry. Falls back
            to the provider's default_depth when unset or not offered.
    """
    engine = provider or SELECTED_SEARCH_ENGINE
    # User overrides degrade gracefully — including a manifest entry with no
    # builder yet (e.g. a deployment-edited manifest ahead of the module).
    # A bad deployment default still fails fast below.
    if engine != SELECTED_SEARCH_ENGINE and (
        get_capability(engine, CAPABILITY_SEARCH) is None
        or engine not in _PROVIDER_BUILDERS
    ):
        logger.warning(
            "Unknown search provider %r; falling back to default %r", engine, SELECTED_SEARCH_ENGINE
        )
        engine = SELECTED_SEARCH_ENGINE

    cap = get_capability(engine, CAPABILITY_SEARCH)
    if cap is None or engine not in _PROVIDER_BUILDERS:
        raise ValueError(
            f"Unsupported search engine: {engine}. "
            f"Supported engines: "
            f"{sorted(set(providers_with_capability(CAPABILITY_SEARCH)) & set(_PROVIDER_BUILDERS))}"
        )

    depth_spec = cap.level(depth) or cap.default_level_spec
    if depth and depth_spec.name != depth:
        logger.debug(
            "Search depth %r not offered by provider %r; using default %r",
            depth, engine, depth_spec.name,
        )

    tool_fn = _PROVIDER_BUILDERS[engine](
        max_results=max_search_results,
        default_time_range=time_range,
        verbose=verbose,
        **depth_spec.native_params,
    )

    return create_logged_tool(
        tool_fn, name="WebSearch", tracking_name=cap.tracking_key(depth_spec)
    )
