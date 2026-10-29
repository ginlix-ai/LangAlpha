"""The tail middleware: the carrier for persisted rows.

Every durable row in the request is history written in a provider-neutral form,
so this middleware asks the carrier to compose the request in the shape the
model in hand reads, and takes back the site breakpoint 4 may go on: the newest
content the next call still carries.

Nothing is added for a single call. Text that rode one request and was gone on
the next would sit in front of the thinking produced after it, and a model that
binds thinking to its prefix would lose that reasoning on every later call, so
every harness-authored line is a row written once into history (``turn.py``,
``durable.py``, ``market_watch.py``). A row true for one call only goes out
turn-scoped where the model takes that (``carrier.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse

from ptc_agent.agent.middleware.provider_cache import breakpoint_marker
from ptc_agent.agent.middleware.runtime_context.carrier import (
    apply_breakpoint,
    compose_request,
    operator_window,
    resolve_carrier,
)
from ptc_agent.agent.middleware.runtime_context.durable import is_runtime_update_message

logger = logging.getLogger(__name__)


class RowCarrierMiddleware(AgentMiddleware):
    """Carries the durable rows in this model's shape and pins breakpoint 4.

    Must sit innermost (just before the reasoning sanitizer) so the content it
    pins is the true tail of the request.
    """

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        original = request.messages or []
        if not any(is_runtime_update_message(m) for m in original):
            return await handler(request)

        model = getattr(request, "model", None)
        try:
            carrier = resolve_carrier(model)
            composed = compose_request(original, carrier)
        except Exception:  # noqa: BLE001 - context is never worth failing a turn for
            logger.warning(
                "[Envelope] composing the request failed; sending it as it arrived",
                exc_info=True,
            )
            return await handler(request)

        marker = breakpoint_marker(model)
        settings = request.model_settings
        if marker is not None and composed.pin is not None:
            apply_breakpoint(composed.messages, composed.pin, *marker)
            settings = _without_top_level_marker(settings, marker[0])
        # The system shape is honored by the Anthropic client only inside this
        # window, so the opt-in is per composed call, never per client.
        with operator_window(carrier.shape):
            return await handler(
                request.override(messages=composed.messages, model_settings=settings)
            )


def _without_top_level_marker(settings: dict, key: str) -> dict:
    """The model settings with the provider's request-level breakpoint removed.

    The provider caching middleware also puts the marker on the request itself
    (Anthropic's top-level ``cache_control``), which the direct API turns into
    a breakpoint of its own on the last block. With breakpoint 4 pinned here
    that would be a fifth, and the request is refused outright; the pin is the
    one measured to be read back, so it is the one that stays.
    """
    if key not in (settings or {}):
        return settings
    return {k: v for k, v in settings.items() if k != key}
