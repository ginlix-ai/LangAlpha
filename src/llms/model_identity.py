"""How a catalog model is named to a reader: the agent, a transcript, `la`.

A leaf on purpose. The transcript renderer and the Claude Code runtime need
the response-metadata key without loading the middleware that stamps it, and
the manifest is read only when a label is asked for.
"""

from __future__ import annotations

from typing import Any

#: The ``response_metadata`` key naming the catalog model (its models.json
#: name) that wrote an answer, which a transcript records. The provider's own
#: ``model_name`` is an API id, and the same id can sit behind several rows.
CATALOG_MODEL_KEY = "catalog_model"


def model_label(name: str | None, effort: str | None = None) -> str:
    """A catalog model as a reader is told of it, empty for no model.

    The manifest's display name where the model has an entry, followed by the
    name ``la models list`` and the ``model`` arguments take, which a reader
    otherwise has no way to match to it; a user-defined model has only its
    name. Read off the raw entry rather than the listed-model metadata, which
    leaves out unlisted models.
    """
    if not name:
        return ""
    from src.llms.llm import LLM
    from src.llms.reasoning import OFF_LEVELS

    entry = LLM.get_model_config().get_model_config(name) or {}
    shown = entry.get("display_name")
    display = f"{shown} ({name})" if shown and shown != name else name
    if not effort:
        return display
    if effort in OFF_LEVELS:
        return f"{display}, reasoning off"
    return f"{display}, reasoning effort {effort}"


def client_label(client: Any, name: str | None = None) -> str:
    """``model_label`` for a built client, from what the factory stamped on it.

    The stamp is the model and effort the client actually calls, which a name
    captured elsewhere may not be; ``name`` stands in for a client the factory
    never built.
    """
    metadata = getattr(client, "metadata", None) or {}
    return model_label(catalog_model(client) or name, metadata.get("reasoning_effort"))


def catalog_model(client: Any) -> str | None:
    """The catalog model a built client runs, as ``LLM.get_llm()`` stamped it;
    None for a client the factory never built."""
    metadata = getattr(client, "metadata", None)
    value = metadata.get("manifest_model") if isinstance(metadata, dict) else None
    return value if isinstance(value, str) and value else None


def api_model_id(model: Any) -> str | None:
    """The id a client sends its provider, None when it names none.

    A binding (``bind_tools``) is read through to the client it wraps. Each
    client ``src.llms`` builds sets one of these attributes, or several to the
    same value, so the order decides nothing for them.
    """
    from langchain_core.runnables.base import RunnableBindingBase

    target =model.bound if isinstance(model, RunnableBindingBase) else model
    for attr in ("model_name", "model", "model_id", "deployment_name"):
        value = getattr(target, attr, None)
        if isinstance(value, str) and value:
            return value
    return None


def is_claude(model: Any) -> bool:
    """Whether ``model`` is a Claude model, on whichever route serves it: read
    off the id the client sends, which goes with the model, not the host."""
    return "claude" in (api_model_id(model) or "").lower()
