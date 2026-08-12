"""ChatOpenAI subclass for Codex store=false backends.

Layers two Codex-specific fixes on ChatOpenAI: system messages are promoted to
the top-level ``instructions`` field (the Codex API rejects ``role:"system"`` in
the input array), and a module-level guard coerces a null ``response.output`` —
which the chatgpt.com backend sends on terminal stream frames — to ``[]`` before
langchain iterates it. Cross-turn reasoning continuity relies on
``include: ["reasoning.encrypted_content"]``; langchain-openai (>=1.3.4) drops
unpersisted ids under store=false and keeps encrypted reasoning items itself, so
no request-side id stripping is done here.
"""

import logging

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)


def _extract_system_to_instructions(payload: dict) -> None:
    """Move system messages from input to the top-level ``instructions`` field.

    The Codex API rejects ``role:"system"`` in the input array. The Responses
    API equivalent is the top-level ``instructions`` parameter (what the
    official Codex CLI uses). Mutates *payload* in place.
    """
    items = payload.get("input")
    if not items:
        return

    system_parts: list[str] = []
    filtered: list = []

    for item in items:
        if isinstance(item, dict) and item.get("role") == "system":
            content = item.get("content", "")
            if isinstance(content, str):
                system_parts.append(content)
            elif isinstance(content, list):
                # langchain-openai emits {"type": "input_text", "text": "..."}
                system_parts.extend(
                    block["text"]
                    for block in content
                    if isinstance(block, dict) and block.get("type") in ("text", "input_text")
                )
        else:
            filtered.append(item)

    if len(filtered) < len(items):
        # Always strip system messages from input (Codex rejects them)
        payload["input"] = filtered
        if system_parts:
            extracted = "\n\n".join(system_parts)
            existing = payload.get("instructions")
            # Append any model-level instructions after the system prompt
            payload["instructions"] = f"{extracted}\n\n{existing}" if existing else extracted
            logger.debug("[codex] Promoted %d system message(s) to instructions", len(system_parts))
        else:
            logger.debug("[codex] Stripped system message(s) with no text content")


class ChatCodexOpenAI(ChatOpenAI):
    """ChatOpenAI for Codex store=false backends.

    Promotes system messages to the ``instructions`` field the Codex API
    requires. Encrypted reasoning items are replayed as-is for cross-turn
    continuity (langchain-openai handles store=false id dropping).
    """

    def _get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        # Codex API rejects role:"system" — promote to the instructions field.
        _extract_system_to_instructions(payload)
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
