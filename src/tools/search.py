import logging
from typing import Optional

from src.config import SELECTED_SEARCH_ENGINE
from src.tools.decorators import create_logged_tool
from src.tools.search_manifest import get_search_provider_spec, get_search_providers

logger = logging.getLogger(__name__)


def _build_tavily(**kwargs):
    from src.tools.search_services.tavily import build_web_search_tool

    return build_web_search_tool(**kwargs)


def _build_serper(**kwargs):
    from src.tools.search_services.serper import build_web_search_tool

    return build_web_search_tool(**kwargs)


def _build_bocha(**kwargs):
    from src.tools.search_services.bocha import build_web_search_tool

    return build_web_search_tool(**kwargs)


# Provider name -> tool builder. Adding a provider = one entry here, one
# provider module with build_web_search_tool, one manifest entry.
_PROVIDER_BUILDERS = {
    "tavily": _build_tavily,
    "serper": _build_serper,
    "bocha": _build_bocha,
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
        engine not in get_search_providers() or engine not in _PROVIDER_BUILDERS
    ):
        logger.warning(
            "Unknown search provider %r; falling back to default %r", engine, SELECTED_SEARCH_ENGINE
        )
        engine = SELECTED_SEARCH_ENGINE

    spec = get_search_provider_spec(engine)
    if spec is None or engine not in _PROVIDER_BUILDERS:
        raise ValueError(
            f"Unsupported search engine: {engine}. "
            f"Supported engines: {sorted(set(get_search_providers()) & set(_PROVIDER_BUILDERS))}"
        )

    depth_spec = spec.depth(depth) or spec.default_depth_spec
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

    # Depth-qualified billing key for multi-depth providers so each level
    # bills at its own rate; single-depth providers keep the bare key. Adding
    # a second depth to a single-depth provider re-keys its new usage rows
    # (bare -> qualified) — check anything reading raw tracking keys first;
    # service-level keys are stable via suffix-stripping.
    tracking_name = (
        f"{spec.tracking_name}:{depth_spec.name}"
        if len(spec.depths) > 1
        else spec.tracking_name
    )
    return create_logged_tool(tool_fn, name="WebSearch", tracking_name=tracking_name)


def get_research_tool():
    """Get deep research tool (Tavily Research API).

    Always available regardless of SELECTED_SEARCH_ENGINE since research
    is a distinct capability from web search. The tool tracks credits
    dynamically inside based on the model used (mini vs pro).
    """
    from src.tools.search_services.tavily import configure_research, deep_research

    configure_research()
    # Don't use create_logged_tool — the tool tracks credits dynamically inside
    return deep_research
