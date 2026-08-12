"""Provider-side deep research: normalized adapters over exa/tavily/parallel.

Each provider runs its own agentic research loop server-side and returns a
synthesized report with citations; adapters map that into ``ResearchResult``.
Not yet bound to an agent tool — ``run_research`` is the dispatch surface: it
resolves the level's native_params from the manifest and calls the provider's
research adapter.
"""

import logging
from typing import Optional

from src.tools.web.manifest import CAPABILITY_RESEARCH, get_capability
from src.tools.web.providers.exa import ExaResearchAdapter
from src.tools.web.providers.parallel import ParallelResearchAdapter
from src.tools.web.providers.tavily import TavilyResearchAdapter
from src.tools.web.types import (
    ResearchRequest,
    ResearchResult,
    WebError,
    WebErrorType,
    WebToolError,
)

logger = logging.getLogger(__name__)

_ADAPTERS = {
    adapter.name: adapter
    for adapter in (ExaResearchAdapter(), TavilyResearchAdapter(), ParallelResearchAdapter())
}


def research_providers() -> tuple:
    """Provider names that offer research, in adapter registry order."""
    return tuple(name for name in _ADAPTERS if get_capability(name, CAPABILITY_RESEARCH))


async def run_research(
    req: ResearchRequest, provider: str, level: Optional[str] = None
) -> ResearchResult:
    """Run one research request on a provider at a manifest level.

    ``level`` of None uses the capability's default level. Raises WebToolError
    for unknown provider/level and for any provider-side failure.
    """
    adapter = _ADAPTERS.get(provider)
    cap = get_capability(provider, CAPABILITY_RESEARCH)
    if adapter is None or cap is None:
        raise WebToolError(
            WebError(
                type=WebErrorType.PROVIDER_ERROR,
                message=f"Provider {provider!r} does not offer research "
                f"(available: {', '.join(research_providers())})",
                retryable=False,
            )
        )
    level_spec = cap.level(level) if level is not None else cap.default_level_spec
    if level_spec is None:
        names = [lv.name for lv in cap.levels]
        raise WebToolError(
            WebError(
                type=WebErrorType.PROVIDER_ERROR,
                message=f"Unknown research level {level!r} for {provider!r} "
                f"(available: {', '.join(names)})",
                retryable=False,
            )
        )

    logger.info(
        f"Research dispatch: provider={provider} level={level_spec.name} "
        f"native_params={level_spec.native_params}"
    )
    return await adapter.research(req, dict(level_spec.native_params))
