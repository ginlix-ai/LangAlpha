"""Curated additions to a vendor's tool descriptions, applied before the model reads them.

A vendor's schema is the contract and ships verbatim; this only fills the
holes that cost the model tool calls in practice. The first case: moomoo's
simulated order tool documents ``order_side`` as "see Side enum" and defines
the enum nowhere the model can reach, so it burned nine calls grepping the
generated docs for it before guessing. Applied once, at composite install, so
the sandbox docs, the generated wrappers and the direct JSON tools all carry
the same words.
"""

from __future__ import annotations

from copy import deepcopy

# vendor -> tool -> parameter -> text appended to that parameter's description
_PARAM_OVERLAYS: dict[str, dict[str, dict[str, str]]] = {
    "moomoo": {
        "sim_trade_input_order": {
            "order_side": "Side enum: 1=BUY, 2=SELL, 3=SELL_SHORT, 4=BUY_BACK.",
        },
        "sim_trade_modify_order": {
            "order_side": "Side enum: 1=BUY, 2=SELL, 3=SELL_SHORT, 4=BUY_BACK.",
        },
    },
}


def overlay_tool_schemas(vendor: str | None, tools: list[dict]) -> list[dict]:
    """The same list with the curated text folded into parameter descriptions.

    Copies only what it changes; a vendor with no overlay gets its list back
    untouched.
    """
    per_tool = _PARAM_OVERLAYS.get(vendor or "")
    if not per_tool:
        return tools
    out: list[dict] = []
    for tool in tools:
        params = per_tool.get(tool.get("name", ""))
        props = (tool.get("input_schema") or {}).get("properties") or {}
        if not params or not props:
            out.append(tool)
            continue
        patched = deepcopy(tool)
        for param, text in params.items():
            prop = patched["input_schema"]["properties"].get(param)
            # The vendor writes this schema, and JSON Schema lets a property
            # be a bare boolean. There is nothing to append a description to
            # then, and raising here would cost the server its whole tool set.
            if not isinstance(prop, dict):
                continue
            existing = (prop.get("description") or "").rstrip()
            prop["description"] = f"{existing} {text}".strip() if existing else text
        out.append(patched)
    return out
