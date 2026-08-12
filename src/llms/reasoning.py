"""
Unified reasoning effort mapper.

Maps abstract levels ("low", "medium", "high", "xhigh") to provider-specific
parameters. Detection-based: checks which reasoning keys exist in the model's
parameters/extra_body from models.json, then adjusts values for those specific keys.

"xhigh" is only natively honored by Anthropic adaptive thinking (Opus 4.7+)
and GLM 5.2+ (mapped to "max"); for all other providers it is clamped to "high".
"""

REASONING_LEVELS = ("low", "medium", "high", "xhigh")

# Anthropic thinking budgets per level
_ANTHROPIC_BUDGETS = {"low": 5000, "medium": 10000, "high": 32000}

# Gemini numeric thinking budgets per level (for thinking_budget pattern)
_GEMINI_BUDGETS = {"low": 1024, "medium": 8192, "high": 32768}

# GLM 5.2+ native effort levels. "low" skips thinking entirely (consistent with
# the thinking.type pattern); the server collapses "medium" to "high".
_GLM_EFFORT = {"low": "none", "medium": "medium", "high": "high", "xhigh": "max"}


def _clamp(level: str) -> str:
    """Clamp xhigh down to high for providers that don't support xhigh."""
    return "high" if level == "xhigh" else level


def apply_reasoning_effort(
    level: str,
    parameters: dict,
    extra_body: dict,
) -> tuple[dict, dict]:
    """Apply reasoning effort override to model parameters.

    Detects which reasoning pattern the model uses by checking existing keys
    in parameters and extra_body, then adjusts their values.

    Args:
        level: One of "low", "medium", "high", "xhigh". "xhigh" only affects
            Anthropic adaptive thinking; elsewhere it is clamped to "high".
        parameters: Model parameters dict (will be mutated).
        extra_body: Extra body dict (will be mutated).

    Returns:
        Tuple of (parameters, extra_body) — same objects, mutated in place.
    """
    if level not in REASONING_LEVELS:
        return parameters, extra_body

    # --- parameters-based patterns ---

    # OpenAI: parameters.reasoning.effort
    if "reasoning" in parameters:
        if isinstance(parameters["reasoning"], dict):
            parameters["reasoning"]["effort"] = _clamp(level)
        else:
            parameters["reasoning"] = {"effort": _clamp(level)}

    # Anthropic adaptive: control via output_config.effort (supports xhigh natively)
    elif "output_config" in parameters or (
        "thinking" in parameters
        and isinstance(parameters["thinking"], dict)
        and parameters["thinking"].get("type") == "adaptive"
    ):
        parameters.setdefault("output_config", {})["effort"] = level

    # Anthropic enabled: control via budget_tokens
    elif "thinking" in parameters:
        budget = _ANTHROPIC_BUDGETS[_clamp(level)]
        if isinstance(parameters["thinking"], dict):
            parameters["thinking"]["budget_tokens"] = budget
        else:
            parameters["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget,
            }

    # Gemini 3.x: parameters.thinking_level
    elif "thinking_level" in parameters:
        parameters["thinking_level"] = _clamp(level)

    # Gemini 2.x: parameters.thinking_budget (numeric)
    elif "thinking_budget" in parameters:
        parameters["thinking_budget"] = _GEMINI_BUDGETS[_clamp(level)]

    # vLLM / Groq / Cerebras: parameters.reasoning_effort
    elif "reasoning_effort" in parameters:
        parameters["reasoning_effort"] = _clamp(level)

    # --- extra_body patterns ---

    # Volcengine / Doubao: extra_body.thinking.type
    if "thinking" in extra_body:
        if isinstance(extra_body["thinking"], dict):
            extra_body["thinking"]["type"] = "disabled" if level == "low" else "enabled"
        else:
            extra_body["thinking"] = {
                "type": "disabled" if level == "low" else "enabled"
            }

    # Dashscope / Qwen: extra_body.enable_thinking
    if "enable_thinking" in extra_body:
        extra_body["enable_thinking"] = level != "low"

    # GLM 5.2+: extra_body.reasoning_effort (merged top-level into the request body)
    if "reasoning_effort" in extra_body:
        extra_body["reasoning_effort"] = _GLM_EFFORT[level]

    return parameters, extra_body
