"""ChatOpenAI subclass for Codex store=false backends.

Layers three Codex-specific fixes on ChatOpenAI: system messages go out as
developer messages, the shape codex-rs sends; a module-level guard coerces a
null ``response.output``, which the chatgpt.com backend has sent on terminal
stream frames, to ``[]`` before langchain iterates it; and the client marks its
own streams so the Responses stream bridge replays the arguments of parallel
tool calls, which that backend sends only in ``.done`` events. Cross-turn
reasoning continuity relies on ``include: ["reasoning.encrypted_content"]``;
langchain-openai (>=1.3.4) drops unpersisted ids under store=false and keeps
encrypted reasoning items itself, so no request-side id stripping is done here.
"""

import logging

from src.llms.extension.dashscope import ScopedResponsesStream, _codex_pending_calls

logger = logging.getLogger(__name__)

CODEX_ORIGINATOR = "codex_cli_rs"
CODEX_PROVIDER = "codex-oauth"


def codex_client_version() -> str:
    """The Codex CLI version this server presents itself as.

    One value, ``client_version`` on ``codex-oauth`` in providers.json, for the
    request path and the model list: the backend gates which models an account
    sees on it (each model names a minimum: 0.164.0 hid GPT-6.2, which needs
    0.166.0), so the two must never disagree and the value must keep up with
    the newest model shipped.
    """
    from src.llms.llm import LLM

    return LLM.get_model_config().get_provider_info(CODEX_PROVIDER)["client_version"]


def codex_identity_headers() -> dict[str, str]:
    """The first-party client identity the codex backend requires.

    Without an originator naming a known client and a User-Agent carrying the
    matching ``<originator>/`` prefix, newer models 404 "Model not found".
    """
    return {"originator": CODEX_ORIGINATOR, "User-Agent": f"{CODEX_ORIGINATOR}/{codex_client_version()}"}


class ChatCodexOpenAI(ScopedResponsesStream):
    """ChatOpenAI for Codex store=false backends.

    Encrypted reasoning items are replayed as-is for cross-turn continuity
    (langchain-openai handles store=false id dropping).
    """

    def _stream_scope(self):
        # A fresh record per stream, so ids are never compared across streams.
        return _codex_pending_calls, {}

    def _get_request_payload(self, input_, *, stop=None, **kwargs):
        """Send each system message as a developer message where it stands.

        Developer is what codex-rs sends, so it is the shape this backend is
        built for; it refused ``role:"system"`` before and accepts it today only
        unannounced. Never the top-level ``instructions`` field: GPT-6.2 leaves
        it out of the cached prefix (7-33% warm hits against 95%).
        """
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        for item in payload.get("input") or ():
            if isinstance(item, dict) and item.get("role") == "system":
                item["role"] = "developer"
        return payload


def _install_responses_output_guard() -> None:
    """Coerce a null Responses-API ``output`` to ``[]`` before langchain iterates it.

    The chatgpt.com Codex backend ships ``response.output = null`` on terminal
    stream frames. langchain_openai (through 1.3.5, latest) iterates it unguarded
    in ``_construct_lc_result_from_responses_api`` and raises
    ``TypeError('NoneType' object is not iterable)``, killing the turn. The
    backend rejects the request-side ``exclude`` workaround with HTTP 400, so we
    patch the single chokepoint all read paths (sync, async, streaming) funnel
    through. Streamed text is already captured from ``output_text.delta`` events,
    so an empty terminal output loses no content.

    Remove once langchain_openai guards ``response.output`` upstream — the guard
    then becomes a no-op (it only mutates when ``output`` is None).
    """
    try:
        import langchain_openai.chat_models.base as _base

        orig = _base._construct_lc_result_from_responses_api
    except (ImportError, AttributeError):
        # langchain_openai internals moved. Degrade to no guard rather than
        # breaking module import (which would take down all codex-oauth).
        logger.warning(
            "[codex] Responses output guard not installed — "
            "langchain_openai._construct_lc_result_from_responses_api is gone; "
            "codex-oauth may crash on a null terminal output frame"
        )
        return

    if getattr(orig, "_codex_output_guarded", False):
        return

    def _guarded(response, *args, **kwargs):
        if getattr(response, "output", None) is None:
            logger.debug("[codex] coerced null Responses output to [] (backend sent output=null)")
            try:
                response.output = []
            except Exception:
                # Response isn't frozen today, so the line above succeeds. Kept as
                # insurance: a frozen SDK model would raise pydantic ValidationError
                # (not TypeError), and object.__setattr__ bypasses the frozen check.
                object.__setattr__(response, "output", [])
        return orig(response, *args, **kwargs)

    _guarded._codex_output_guarded = True
    _base._construct_lc_result_from_responses_api = _guarded


_install_responses_output_guard()
