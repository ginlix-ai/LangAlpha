"""
Pricing calculation utilities for LLM token usage with support for both flat and tiered pricing.
"""

from typing import Dict, Any, Optional, List, Tuple
import logging
import re

logger = logging.getLogger(__name__)


def extract_base_model(model_name: str) -> str:
    """
    Extract base model name from versioned model ID by stripping version suffixes.

    This handles various version suffix patterns used by different providers:
    - OpenAI style: -MMDD or -YYYY-MM-DD (e.g., "gpt-5-0905", "gpt-5-2025-08-07")
    - Claude style: -YYYYMMDD (e.g., "claude-opus-4-1-20250805")
    - Volcengine style: -YYMMDD (e.g., "doubao-seed-1-6-250615")

    Args:
        model_name: Model name that may include version suffix

    Returns:
        Base model name without version suffix. If no version pattern is detected,
        returns the original model_name unchanged.

    Examples:
        >>> extract_base_model("gpt-5-0905")
        "gpt-5"
        >>> extract_base_model("gpt-5-2025-08-07")
        "gpt-5"
        >>> extract_base_model("claude-opus-4-1-20250805")
        "claude-opus-4-1"
        >>> extract_base_model("doubao-seed-1-6-250615")
        "doubao-seed-1-6"
        >>> extract_base_model("minimax-m2")
        "minimax-m2"
    """
    # Pattern 1: Strip date-based suffixes (OpenAI style)
    # Matches: -MMDD (4 digits) or -YYYY-MM-DD (date format)
    # Examples: gpt-5-0905, gpt-5-2025-08-07
    pattern1 = r'-(\d{4}(-\d{2}-\d{2})?|\d{4})$'
    base = re.sub(pattern1, '', model_name)
    if base != model_name:
        return base

    # Pattern 2: Strip long date suffixes (Claude/Volcengine style)
    # Matches: -YYMMDD or -YYYYMMDD at end
    # Examples: doubao-seed-1-6-250615, claude-opus-4-1-20250805
    pattern2 = r'-\d{6,8}$'
    base = re.sub(pattern2, '', model_name)
    if base != model_name:
        return base

    # No version pattern detected, return as-is
    return model_name


def detect_provider_for_model(model_name: str) -> Optional[str]:
    """
    Detect provider by reverse-looking up models.json.

    Searches models.json to find which provider a model_id belongs to.
    Uses case-insensitive matching to handle mismatches between API responses
    and configuration (e.g., "MiniMax-M2" vs "minimax-m2").

    Searches both:
    - Custom model names (keys in models.json)
    - model_id values (the actual model identifier sent to APIs)

    Args:
        model_name: Model name from token usage (e.g., "MiniMax-M2", "gpt-5-0905")

    Returns:
        Provider name if found in models.json, None otherwise

    Examples:
        >>> detect_provider_for_model("MiniMax-M2")  # Case mismatch
        "minimax"
        >>> detect_provider_for_model("gpt-5")
        "openai"
        >>> detect_provider_for_model("unknown-model")
        None
    """
    from .llm import ModelConfig

    try:
        model_config = ModelConfig()
        config_data = model_config.llm_config  # Load models.json
    except Exception as e:
        logger.debug(f"Failed to load models.json for provider detection: {e}")
        return None

    if not config_data:
        return None

    # Normalize for case-insensitive comparison
    model_name_lower = model_name.lower()

    # Search through all custom model configurations
    for custom_name, config in config_data.items():
        # Check 1: Match against custom name (the key in models.json)
        if custom_name.lower() == model_name_lower:
            provider = config.get("provider")
            if provider:
                logger.debug(f"Provider detected for '{model_name}' via custom_name: {provider}")
                return provider

        # Check 2: Match against model_id field (case-insensitive)
        model_id = config.get("model_id", "")
        if model_id.lower() == model_name_lower:
            provider = config.get("provider")
            if provider:
                logger.debug(f"Provider detected for '{model_name}' via model_id: {provider}")
                return provider

    # Not found in models.json
    logger.debug(f"No provider found for model '{model_name}' in models.json")
    return None


def find_model_pricing(model_name: str, provider: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Find pricing information for a model with provider-aware lookup and fallback strategies.

    This is the centralized pricing lookup function used by both workflows and benchmarks.

    Features:
    - Provider-aware search (searches specific provider first if given)
    - Case-insensitive matching (handles "MiniMax-M2" vs "minimax-m2")
    - Alias support (e.g., "gpt-4o" → "gpt-4o-2024-05-13")
    - Pattern-based fallback (handles version snapshots like "gpt-5-0905" → "gpt-5")
    - Comprehensive logging for debugging

    Lookup chain:
    1. If provider specified: Search that provider's models first
    2. Exact ID match (case-insensitive)
    3. Alias match (case-insensitive)
    4. Pattern-based fallback (extract base model, recursive lookup)
    5. If provider not specified: Search all providers

    Args:
        model_name: Name or ID of the model from token usage
                   (e.g., "gpt-5", "MiniMax-M2", "gpt-5-0905")
        provider: Optional provider name for scoped search
                 (e.g., "openai", "minimax", "anthropic")
                 If None, searches all providers.

    Returns:
        Pricing dictionary if found, None otherwise

    Examples:
        >>> find_model_pricing("gpt-5")
        {'input': 1.25, 'output': 10.0, ...}

        >>> find_model_pricing("MiniMax-M2", provider="minimax")  # Case mismatch
        {'input': 0.30, 'output': 1.20, ...}

        >>> find_model_pricing("gpt-5-0905", provider="openai")  # Version fallback
        {'input': 1.25, 'output': 10.0, ...}  # Falls back to gpt-5 pricing
    """
    from .llm import ModelConfig

    try:
        model_config = ModelConfig()
        manifest = model_config.manifest
    except Exception as e:
        logger.warning(f"Failed to load model manifest for pricing lookup: {e}")
        return None

    if not manifest or 'models' not in manifest:
        logger.warning("Model manifest is empty or missing 'models' key")
        return None

    # Normalize for case-insensitive comparison
    model_name_lower = model_name.lower()

    # Normalize punctuation (dots ↔ hyphens) for fuzzy matching.
    # Some providers return model IDs with hyphens (e.g., "doubao-seed-2-0-pro")
    # while providers.json uses dots (e.g., "doubao-seed-2.0-pro").
    def _normalize(s: str) -> str:
        return s.lower().replace('.', '-')

    model_name_normalized = _normalize(model_name)

    # Determine which providers to search (provider → parent → global)
    if provider:
        providers_to_search = []
        if provider in manifest['models']:
            providers_to_search.append((provider, manifest['models'][provider]))
        # Fall back to parent provider's pricing if available
        parent = model_config.get_parent_provider(provider)
        if parent != provider and parent in manifest['models']:
            providers_to_search.append((parent, manifest['models'][parent]))
            logger.debug(f"Including parent provider '{parent}' in pricing search for '{provider}'")
        if not providers_to_search:
            # Neither provider nor parent has models listed, search all
            providers_to_search = list(manifest['models'].items())
            logger.debug(f"Provider '{provider}' and parent not found in manifest, searching all providers")
        else:
            logger.debug(f"Searching for '{model_name}' in provider(s): {[p for p, _ in providers_to_search]}")
    else:
        # Global search across all providers
        providers_to_search = list(manifest['models'].items())

    # STEP 1 & 2: Exact ID and alias matching (case-insensitive)
    for prov, models in providers_to_search:
        for model in models:
            # Check exact ID match (case-insensitive)
            model_id = model.get('id', '')
            if model_id.lower() == model_name_lower:
                logger.debug(f"Found pricing for '{model_name}' via exact ID match in provider '{prov}'")
                return model.get('pricing')

            # Check aliases (case-insensitive)
            aliases = model.get('alias', [])
            if isinstance(aliases, list):
                for alias in aliases:
                    if alias.lower() == model_name_lower:
                        logger.debug(
                            f"Found pricing for '{model_name}' via alias '{alias}' "
                            f"(model_id: {model_id}) in provider '{prov}'"
                        )
                        return model.get('pricing')

    # STEP 2.5: Normalized matching (dots ↔ hyphens)
    for prov, models in providers_to_search:
        for model in models:
            model_id = model.get('id', '')
            if _normalize(model_id) == model_name_normalized:
                logger.debug(
                    f"Found pricing for '{model_name}' via normalized match "
                    f"(model_id: {model_id}) in provider '{prov}'"
                )
                return model.get('pricing')

            aliases = model.get('alias', [])
            if isinstance(aliases, list):
                for alias in aliases:
                    if _normalize(alias) == model_name_normalized:
                        logger.debug(
                            f"Found pricing for '{model_name}' via normalized alias '{alias}' "
                            f"(model_id: {model_id}) in provider '{prov}'"
                        )
                        return model.get('pricing')

    # STEP 3: Pattern-based fallback for version snapshots
    base_model = extract_base_model(model_name)
    if base_model != model_name:
        logger.info(
            f"No exact match for '{model_name}', trying base model '{base_model}' "
            f"(extracted via pattern matching)"
        )
        # Recursive lookup with base model (keep same provider context)
        pricing = find_model_pricing(base_model, provider)
        if pricing:
            logger.warning(
                f"Using pricing for base model '{base_model}' for snapshot version '{model_name}'. "
                f"This is a fallback strategy. Consider adding '{model_name}' as an alias in "
                f"providers.json if this model is frequently used."
            )
            return pricing
        else:
            logger.debug(f"Base model '{base_model}' also not found in manifest")

    # STEP 4: Not found - log comprehensive error
    if provider:
        logger.warning(
            f"No pricing found for model: {model_name} (provider: {provider}). "
            f"Tried: exact ID match, alias match, base model '{base_model}'. "
            f"Please add this model to providers.json under provider '{provider}'."
        )
    else:
        logger.warning(
            f"No pricing found for model: {model_name} (searched all providers). "
            f"Tried: exact ID match, alias match, base model '{base_model}'. "
            f"Please add this model to providers.json."
        )

    return None


def calculate_tiered_cost(tokens: int, tiers: List[Dict[str, Any]]) -> float:
    """
    Calculate cost for tiered pricing based on cumulative token count.

    For tiered pricing, each tier has a max_tokens threshold and a rate.
    Cost is calculated by applying the rate to tokens in each tier range.

    Example tiers:
    [
        {"max_tokens": 32000, "rate": 0.80},
        {"max_tokens": 128000, "rate": 1.20},
        {"max_tokens": null, "rate": 2.40}  # null means infinity
    ]

    For 50,000 tokens:
    - First 32,000 tokens: 32,000 / 1M * 0.80 = $0.0256
    - Next 18,000 tokens: 18,000 / 1M * 1.20 = $0.0216
    - Total: $0.0472

    Args:
        tokens: Total number of tokens
        tiers: List of tier dictionaries with max_tokens and rate

    Returns:
        Total cost in dollars
    """
    if not tiers or tokens <= 0:
        return 0.0

    total_cost = 0.0
    remaining_tokens = tokens
    previous_max = 0

    for tier in tiers:
        max_tokens = tier.get('max_tokens')
        rate = tier.get('rate', 0)

        if max_tokens is None:
            # Last tier (infinite)
            tier_tokens = remaining_tokens
        else:
            # Calculate tokens in this tier
            tier_tokens = min(remaining_tokens, max_tokens - previous_max)

        if tier_tokens <= 0:
            break

        # Calculate cost for this tier (rate is per 1M tokens)
        tier_cost = (tier_tokens / 1_000_000) * rate
        total_cost += tier_cost

        remaining_tokens -= tier_tokens
        previous_max = max_tokens if max_tokens is not None else previous_max

        if remaining_tokens <= 0:
            break

    return total_cost


def find_2d_pricing_rates(
    input_tokens: int,
    output_tokens: int,
    pricing_matrix: List[Dict[str, Any]]
) -> Optional[Dict[str, float]]:
    """
    Find the applicable rate set from a 2D pricing matrix.

    2D pricing matrices define rates based on both input and output token counts.
    Used by models like GLM-4.6 where pricing varies by input AND output ranges.

    Matrix entries are checked in order, returning the first match where:
    - input_tokens <= entry['input_max'] (or input_max is None)
    - output_tokens <= entry['output_max'] (or output_max is None)

    Args:
        input_tokens: Total input tokens
        output_tokens: Total output tokens
        pricing_matrix: List of pricing entries with input_max, output_max, and rates

    Returns:
        Dictionary with 'input', 'output', 'cached_input' rates, or None if no match

    Example matrix:
        [
            {"input_max": 32000, "output_max": 200, "input": 0.29, "output": 1.14, "cached_input": 0.057},
            {"input_max": 32000, "output_max": null, "input": 0.43, "output": 2.00, "cached_input": 0.086},
            {"input_max": null, "output_max": null, "input": 0.57, "output": 2.29, "cached_input": 0.11}
        ]

        For input=50k, output=100: Matches entry 3 (input_max=null) -> rates: 0.57, 2.29, 0.11
        For input=20k, output=100: Matches entry 1 (input≤32k, output≤200) -> rates: 0.29, 1.14, 0.057
        For input=20k, output=500: Matches entry 2 (input≤32k, output>200) -> rates: 0.43, 2.00, 0.086
    """
    for entry in pricing_matrix:
        input_max = entry.get('input_max')
        output_max = entry.get('output_max')

        # Check if input tokens match this entry
        input_match = (input_max is None) or (input_tokens <= input_max)

        # Check if output tokens match this entry
        output_match = (output_max is None) or (output_tokens <= output_max)

        if input_match and output_match:
            # Found matching entry
            return {
                'input': entry.get('input', 0),
                'output': entry.get('output', 0),
                'cached_input': entry.get('cached_input', 0)
            }

    # No match found
    logger.warning(
        f"No matching entry in 2D pricing matrix for input={input_tokens}, output={output_tokens}"
    )
    return None


def get_input_cost(
    tokens: int,
    pricing: Dict[str, Any],
    cached_tokens: int = 0,
    output_tokens: int = 0
) -> Tuple[float, float]:
    """
    Calculate input cost with support for flat, tiered, and 2D matrix pricing.

    Args:
        tokens: Total input tokens (including cached)
        pricing: Pricing dictionary (may contain 'input', 'input_tiers', or 'matrix')
        cached_tokens: Number of tokens served from cache
        output_tokens: Total output tokens (for 2D matrix pricing)

    Returns:
        Tuple of (regular_input_cost, cached_input_cost)
    """
    regular_tokens = tokens - cached_tokens
    regular_cost = 0.0
    cached_cost = 0.0

    # Check for 2D matrix pricing mode
    pricing_mode = pricing.get('pricing_mode', 'standard')

    if pricing_mode == '2d_matrix' and 'matrix' in pricing:
        # 2D matrix pricing: Find applicable rate set
        rates = find_2d_pricing_rates(tokens, output_tokens, pricing['matrix'])
        if not rates:
            return 0.0, 0.0

        # Calculate costs using matrix rates
        if regular_tokens > 0:
            regular_cost = (regular_tokens / 1_000_000) * rates['input']

        if cached_tokens > 0:
            cached_cost = (cached_tokens / 1_000_000) * rates['cached_input']

        return regular_cost, cached_cost

    # Standard pricing modes (flat or tiered)
    # Calculate regular (non-cached) input cost
    if regular_tokens > 0:
        if 'input_tiers' in pricing:
            # Tiered pricing
            regular_cost = calculate_tiered_cost(regular_tokens, pricing['input_tiers'])
        elif 'input' in pricing:
            # Flat pricing
            regular_cost = (regular_tokens / 1_000_000) * pricing['input']

    # Calculate cached input cost (cache hits)
    # Only calculate if model has explicit cache pricing defined
    if cached_tokens > 0:
        if 'cache_hit' in pricing:
            # Use cache_hit rate (e.g., doubao)
            cached_cost = (cached_tokens / 1_000_000) * pricing['cache_hit']
        elif 'cached_input' in pricing:
            # Use cached_input rate at pricing level (e.g., OpenAI, Anthropic)
            cached_cost = (cached_tokens / 1_000_000) * pricing['cached_input']
        elif 'input_tiers' in pricing:
            # Check for per-tier cached_input rates (e.g., Qwen models)
            tiers = pricing['input_tiers']
            tier_cached_rate = None

            # Find the applicable tier based on total input tokens
            for tier in tiers:
                max_tokens = tier.get('max_tokens')
                if max_tokens is None or tokens <= max_tokens:
                    tier_cached_rate = tier.get('cached_input')
                    break

            if tier_cached_rate is not None:
                # Use per-tier cached_input rate
                cached_cost = (cached_tokens / 1_000_000) * tier_cached_rate
            # No fallback - if no cache pricing defined, model doesn't support caching

    return regular_cost, cached_cost


def get_cache_storage_cost(storage_tokens: int, pricing: Dict[str, Any]) -> float:
    """
    Calculate cache storage cost (for cache writes).

    Args:
        storage_tokens: Number of tokens written to cache
        pricing: Pricing dictionary

    Returns:
        Storage cost in dollars
    """
    if storage_tokens <= 0:
        return 0.0

    if 'cache_storage' in pricing:
        # Doubao-style cache storage pricing
        return (storage_tokens / 1_000_000) * pricing['cache_storage']

    return 0.0


def get_cache_creation_cost(
    cache_5m_tokens: int,
    cache_1h_tokens: int,
    pricing: Dict[str, Any]
) -> Tuple[float, float]:
    """
    Calculate Anthropic-style cache creation costs.

    Args:
        cache_5m_tokens: Tokens written to 5-minute cache
        cache_1h_tokens: Tokens written to 1-hour cache
        pricing: Pricing dictionary

    Returns:
        Tuple of (cache_5m_cost, cache_1h_cost)
    """
    cache_5m_cost = 0.0
    cache_1h_cost = 0.0

    if cache_5m_tokens > 0 and 'cache_5m' in pricing:
        cache_5m_cost = (cache_5m_tokens / 1_000_000) * pricing['cache_5m']

    if cache_1h_tokens > 0 and 'cache_1h' in pricing:
        cache_1h_cost = (cache_1h_tokens / 1_000_000) * pricing['cache_1h']

    return cache_5m_cost, cache_1h_cost


def get_output_cost(tokens: int, pricing: Dict[str, Any], input_tokens: int = 0) -> float:
    """
    Calculate output cost with support for flat, tiered, input-dependent, and 2D matrix pricing.

    Pricing modes:
    - Flat pricing: Single output rate for all tokens
    - Standard tiered pricing: Output rate based on OUTPUT token count
    - Input-dependent pricing: Output rate based on INPUT token count tier
      (enabled when output_pricing_mode: "input_dependent")
    - 2D matrix pricing: Rate determined by both input AND output token counts
      (enabled when pricing_mode: "2d_matrix")

    Args:
        tokens: Number of output tokens
        pricing: Pricing dictionary (may contain 'output', 'output_tiers', 'matrix', etc.)
        input_tokens: Total input tokens (for input-dependent and 2D matrix pricing)

    Returns:
        Output cost in dollars

    Examples:
        # Flat pricing
        >>> get_output_cost(10000, {'output': 1.5})
        0.015

        # Standard tiered pricing (based on output tokens)
        >>> get_output_cost(50000, {'output_tiers': [
        ...     {'max_tokens': 32000, 'rate': 1.0},
        ...     {'max_tokens': None, 'rate': 2.0}
        ... ]})
        0.068

        # Input-dependent pricing (Doubao Seed Code)
        >>> get_output_cost(10000, {
        ...     'output_pricing_mode': 'input_dependent',
        ...     'output_tiers': [
        ...         {'max_tokens': 32000, 'rate': 1.14},
        ...         {'max_tokens': 128000, 'rate': 1.71},
        ...         {'max_tokens': None, 'rate': 2.29}
        ...     ]
        ... }, input_tokens=50000)
        0.0171  # Uses 1.71 rate (input in 32k-128k tier)

        # 2D matrix pricing (GLM-4.6)
        >>> get_output_cost(100, {
        ...     'pricing_mode': '2d_matrix',
        ...     'matrix': [
        ...         {'input_max': 32000, 'output_max': 200, 'output': 1.14},
        ...         {'input_max': 32000, 'output_max': None, 'output': 2.00},
        ...         {'input_max': None, 'output_max': None, 'output': 2.29}
        ...     ]
        ... }, input_tokens=20000)
        0.000114  # Uses 1.14 rate (input≤32k, output≤200)
    """
    if tokens <= 0:
        return 0.0

    pricing_mode = pricing.get('pricing_mode', 'standard')

    # Check for 2D matrix pricing
    if pricing_mode == '2d_matrix' and 'matrix' in pricing:
        # 2D matrix pricing: Find applicable rate set
        rates = find_2d_pricing_rates(input_tokens, tokens, pricing['matrix'])
        if not rates:
            return 0.0

        return (tokens / 1_000_000) * rates['output']

    # Check for input-dependent pricing
    output_pricing_mode = pricing.get('output_pricing_mode', 'standard')

    if output_pricing_mode == 'input_dependent' and 'output_tiers' in pricing:
        # Input-dependent pricing: Output rate determined by INPUT token tier
        # Find which tier the input falls into and use that tier's output rate
        output_tiers = pricing['output_tiers']

        # Determine the tier index based on input tokens
        tier_index = 0
        previous_max = 0

        for idx, tier in enumerate(output_tiers):
            max_tokens = tier.get('max_tokens')

            if max_tokens is None:
                # Last tier (infinite)
                tier_index = idx
                break
            elif input_tokens <= max_tokens:
                # Input falls in this tier
                tier_index = idx
                break

            previous_max = max_tokens

        # Use the rate from the determined tier
        output_rate = output_tiers[tier_index].get('rate', 0)
        return (tokens / 1_000_000) * output_rate

    elif 'output_tiers' in pricing:
        # Standard tiered pricing: Output rate based on OUTPUT token count
        return calculate_tiered_cost(tokens, pricing['output_tiers'])
    elif 'output' in pricing:
        # Flat pricing
        return (tokens / 1_000_000) * pricing['output']

    return 0.0


def calculate_total_cost(
    input_tokens: int = 0,
    output_tokens: int = 0,
    cached_tokens: int = 0,
    cache_storage_tokens: int = 0,
    cache_5m_tokens: int = 0,
    cache_1h_tokens: int = 0,
    pricing: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calculate total cost with detailed breakdown.

    Supports both flat and tiered pricing for all token types.
    Works with different cache pricing models (OpenAI, Anthropic, Volcengine/Doubao).

    Args:
        input_tokens: Total input tokens
        output_tokens: Total output tokens
        cached_tokens: Tokens served from cache (cache hits)
        cache_storage_tokens: Tokens written to cache storage (doubao-style)
        cache_5m_tokens: Tokens written to 5-minute cache (anthropic-style)
        cache_1h_tokens: Tokens written to 1-hour cache (anthropic-style)
        pricing: Pricing dictionary from manifest

    Returns:
        Dictionary with cost breakdown and total
    """
    if not pricing:
        return {
            'total_cost': 0.0,
            'breakdown': {},
            'error': 'No pricing information available'
        }

    breakdown = {}
    total_cost = 0.0

    # Calculate input costs (pass output_tokens for 2D matrix pricing)
    regular_input_cost, cached_input_cost = get_input_cost(
        input_tokens, pricing, cached_tokens, output_tokens=output_tokens
    )

    if regular_input_cost > 0:
        breakdown['input'] = {
            'tokens': input_tokens - cached_tokens,
            'cost': regular_input_cost
        }
        total_cost += regular_input_cost

    if cached_input_cost > 0:
        breakdown['cached_input'] = {
            'tokens': cached_tokens,
            'cost': cached_input_cost
        }
        total_cost += cached_input_cost

    # Calculate cache storage cost (doubao-style)
    cache_storage_cost = get_cache_storage_cost(cache_storage_tokens, pricing)
    if cache_storage_cost > 0:
        breakdown['cache_storage'] = {
            'tokens': cache_storage_tokens,
            'cost': cache_storage_cost
        }
        total_cost += cache_storage_cost

    # Calculate cache creation costs (anthropic-style)
    cache_5m_cost, cache_1h_cost = get_cache_creation_cost(cache_5m_tokens, cache_1h_tokens, pricing)

    if cache_5m_cost > 0:
        breakdown['cache_5m_creation'] = {
            'tokens': cache_5m_tokens,
            'cost': cache_5m_cost
        }
        total_cost += cache_5m_cost

    if cache_1h_cost > 0:
        breakdown['cache_1h_creation'] = {
            'tokens': cache_1h_tokens,
            'cost': cache_1h_cost
        }
        total_cost += cache_1h_cost

    # Calculate output cost (pass input_tokens for input-dependent pricing)
    output_cost = get_output_cost(output_tokens, pricing, input_tokens=input_tokens)
    if output_cost > 0:
        breakdown['output'] = {
            'tokens': output_tokens,
            'cost': output_cost
        }
        total_cost += output_cost

    return {
        'total_cost': total_cost,
        'breakdown': breakdown
    }
