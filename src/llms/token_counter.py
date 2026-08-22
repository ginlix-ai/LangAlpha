"""Token usage tracking and aggregation for LLM API calls."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging

from .pricing_utils import calculate_total_cost
from .llm import LLM

logger = logging.getLogger(__name__)


def extract_cache_from_details(details: Dict[str, Any]) -> Dict[str, Any]:
    """Extract cache token counts from a LangChain input_token_details dict.

    Handles three formats produced by different providers/versions:
    - Flat: ephemeral_5m_input_tokens / ephemeral_1h_input_tokens at top level
    - Dict: cache_creation dict with ephemeral keys nested inside
    - Int:  cache_creation as total int (legacy, pre-TTL breakdown)

    Returns dict with keys cached_tokens, cache_5m_tokens, cache_1h_tokens
    (only present when value is not None and > 0).
    """
    if not details:
        return {}

    result = {}

    # Cache reads
    cache_read = details.get('cache_read')
    if cache_read is not None and cache_read > 0:
        result['cached_tokens'] = cache_read

    # Cache creation — try flat ephemeral keys first (primary format)
    cache_5m = details.get('ephemeral_5m_input_tokens')
    cache_1h = details.get('ephemeral_1h_input_tokens')

    # Override from cache_creation field if present (dict or int)
    if 'cache_creation' in details:
        cache_creation = details['cache_creation']
        if isinstance(cache_creation, dict):
            cache_5m = cache_creation.get('ephemeral_5m_input_tokens', cache_5m)
            cache_1h = cache_creation.get('ephemeral_1h_input_tokens', cache_1h)
        elif isinstance(cache_creation, int) and cache_creation > 0:
            # Legacy int format — assign to 5m if no TTL breakdown available
            if not cache_5m and not cache_1h:
                cache_5m = cache_creation

    if cache_5m is not None and cache_5m > 0:
        result['cache_5m_tokens'] = cache_5m
    if cache_1h is not None and cache_1h > 0:
        result['cache_1h_tokens'] = cache_1h

    return result


def extract_token_usage(response: Any) -> Dict[str, Any]:
    """Extract token usage information from a response object.
    
    Args:
        response: Response object from LangChain
        
    Returns:
        Dictionary containing comprehensive token usage information
    """
    token_info = {}
    
    # Try to get usage_metadata (primary source for Response API)
    if hasattr(response, 'usage_metadata'):
        usage = response.usage_metadata
        if isinstance(usage, dict):
            token_info['input_tokens'] = usage.get('input_tokens', 0)
            token_info['output_tokens'] = usage.get('output_tokens', 0)
            token_info['total_tokens'] = usage.get('total_tokens', 0)
            
            # Extract cache tokens from input_token_details (LangChain normalized format)
            if 'input_token_details' in usage:
                details = usage['input_token_details']
                token_info.update(extract_cache_from_details(details))
                if details and 'audio' in details:
                    audio = details.get('audio')
                    if audio is not None:
                        token_info['audio_input_tokens'] = audio

            # Extract output token details
            if 'output_token_details' in usage:
                details = usage['output_token_details']
                if details and 'reasoning' in details:
                    reasoning = details.get('reasoning')
                    if reasoning is not None:
                        token_info['reasoning_tokens'] = reasoning
                if details and 'audio' in details:
                    audio = details.get('audio')
                    if audio is not None:
                        token_info['audio_output_tokens'] = audio
    
    # Also check response_metadata for additional details (Standard API)
    if hasattr(response, 'response_metadata'):
        metadata = response.response_metadata
        if isinstance(metadata, dict) and 'token_usage' in metadata:
            token_usage = metadata['token_usage']

            # Fallback if usage_metadata was not available or had incomplete data
            if 'input_tokens' not in token_info or token_info.get('input_tokens', 0) == 0:
                token_info['input_tokens'] = token_usage.get('prompt_tokens', 0)
                token_info['output_tokens'] = token_usage.get('completion_tokens', 0)
                token_info['total_tokens'] = token_usage.get('total_tokens', 0)
            
            # Extract prompt token details (Standard API format)
            if 'prompt_tokens_details' in token_usage:
                details = token_usage['prompt_tokens_details']
                if details and 'cached_tokens' in details:
                    # Prefer this over cache_read if both exist
                    cached_tokens = details.get('cached_tokens')
                    if cached_tokens is not None:
                        token_info['cached_tokens'] = cached_tokens
                if details and 'audio_tokens' in details:
                    audio_tokens = details.get('audio_tokens')
                    if audio_tokens is not None:
                        token_info['audio_input_tokens'] = audio_tokens

            # Extract completion token details (Standard API format)
            if 'completion_tokens_details' in token_usage:
                details = token_usage['completion_tokens_details']
                if details and 'reasoning_tokens' in details:
                    reasoning_tokens = details.get('reasoning_tokens')
                    if reasoning_tokens is not None:
                        token_info['reasoning_tokens'] = reasoning_tokens
                if details and 'accepted_prediction_tokens' in details:
                    accepted = details.get('accepted_prediction_tokens')
                    if accepted is not None:
                        token_info['accepted_prediction_tokens'] = accepted
                if details and 'rejected_prediction_tokens' in details:
                    rejected = details.get('rejected_prediction_tokens')
                    if rejected is not None:
                        token_info['rejected_prediction_tokens'] = rejected
                if details and 'audio_tokens' in details:
                    audio_tokens = details.get('audio_tokens')
                    if audio_tokens is not None:
                        token_info['audio_output_tokens'] = audio_tokens

    # Check for Anthropic format (response_metadata.usage)
    if hasattr(response, 'response_metadata'):
        metadata = response.response_metadata
        if isinstance(metadata, dict) and 'usage' in metadata:
            usage = metadata['usage']

            # Extract basic tokens (fallback if not already set or if set to 0)
            if 'input_tokens' not in token_info or token_info.get('input_tokens', 0) == 0:
                token_info['input_tokens'] = usage.get('input_tokens', 0)
                token_info['output_tokens'] = usage.get('output_tokens', 0)
                token_info['total_tokens'] = token_info['input_tokens'] + token_info['output_tokens']

            # Anthropic: cache_read_input_tokens (cache hits & refreshes)
            if 'cache_read_input_tokens' in usage:
                cache_read = usage.get('cache_read_input_tokens')
                if cache_read is not None:
                    token_info['cached_tokens'] = cache_read

            # Anthropic: cache creation with ephemeral breakdown
            if 'cache_creation' in usage:
                cache_creation = usage['cache_creation']
                if cache_creation:
                    # 5-minute cache writes
                    cache_5m = cache_creation.get('ephemeral_5m_input_tokens')
                    if cache_5m is not None and cache_5m > 0:
                        token_info['cache_5m_tokens'] = cache_5m
                    # 1-hour cache writes
                    cache_1h = cache_creation.get('ephemeral_1h_input_tokens')
                    if cache_1h is not None and cache_1h > 0:
                        token_info['cache_1h_tokens'] = cache_1h

            # Fallback: if cache_creation_input_tokens exists but no breakdown
            elif 'cache_creation_input_tokens' in usage:
                cache_creation_tokens = usage.get('cache_creation_input_tokens')
                if cache_creation_tokens is not None and cache_creation_tokens > 0:
                    token_info['cache_creation_tokens'] = cache_creation_tokens

    return token_info


@dataclass
class TokenUsageRecord:
    """Single token usage record with comprehensive token tracking."""
    timestamp: datetime
    model: str
    operation: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    reasoning_tokens: Optional[int] = None
    cached_tokens: Optional[int] = None
    accepted_prediction_tokens: Optional[int] = None
    rejected_prediction_tokens: Optional[int] = None
    audio_input_tokens: Optional[int] = None
    audio_output_tokens: Optional[int] = None
    cache_5m_tokens: Optional[int] = None
    cache_1h_tokens: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TokenUsageTracker:
    """Tracks and aggregates token usage across multiple API calls."""
    
    def __init__(self, verbose: bool = False):
        """Initialize the token usage tracker.
        
        Args:
            verbose: If True, log token usage to console. If False, silent tracking.
        """
        self.records: List[TokenUsageRecord] = []
        self.model_totals: Dict[str, Dict[str, int]] = {}
        self.operation_totals: Dict[str, Dict[str, int]] = {}
        self.session_start = datetime.now()
        self.verbose = verbose
    
    def add_usage(
        self,
        token_info: Dict[str, Any],
        model: str = "unknown",
        operation: str = "api_call",
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a token usage record.
        
        Args:
            token_info: Dictionary with token counts
            model: Model name
            operation: Operation description
            metadata: Additional metadata
        """
        if not token_info:
            return
        
        record = TokenUsageRecord(
            timestamp=datetime.now(),
            model=model,
            operation=operation,
            input_tokens=token_info.get('input_tokens', 0),
            output_tokens=token_info.get('output_tokens', 0),
            total_tokens=token_info.get('total_tokens', 0),
            reasoning_tokens=token_info.get('reasoning_tokens'),
            cached_tokens=token_info.get('cached_tokens'),
            accepted_prediction_tokens=token_info.get('accepted_prediction_tokens'),
            rejected_prediction_tokens=token_info.get('rejected_prediction_tokens'),
            audio_input_tokens=token_info.get('audio_input_tokens'),
            audio_output_tokens=token_info.get('audio_output_tokens'),
            cache_5m_tokens=token_info.get('cache_5m_tokens'),
            cache_1h_tokens=token_info.get('cache_1h_tokens'),
            metadata=metadata or {}
        )
        
        self.records.append(record)
        self._update_totals(record)
        
        # Only log if verbose mode is enabled
        if self.verbose:
            log_msg = f"Token Usage [{model}] - {operation}: "
            log_msg += f"Input={record.input_tokens}, Output={record.output_tokens}, Total={record.total_tokens}"
            if record.reasoning_tokens is not None:
                log_msg += f", Reasoning={record.reasoning_tokens}"
            logger.info(log_msg)
    
    def _update_totals(self, record: TokenUsageRecord) -> None:
        """Update running totals."""
        # Update model totals
        if record.model not in self.model_totals:
            self.model_totals[record.model] = {
                'input_tokens': 0,
                'output_tokens': 0,
                'total_tokens': 0,
                'reasoning_tokens': 0,
                'cached_tokens': 0,
                'cache_5m_tokens': 0,
                'cache_1h_tokens': 0,
                'call_count': 0
            }

        self.model_totals[record.model]['input_tokens'] += record.input_tokens
        self.model_totals[record.model]['output_tokens'] += record.output_tokens
        self.model_totals[record.model]['total_tokens'] += record.total_tokens
        if record.reasoning_tokens:
            self.model_totals[record.model]['reasoning_tokens'] += record.reasoning_tokens
        if record.cached_tokens:
            self.model_totals[record.model]['cached_tokens'] += record.cached_tokens
        if record.cache_5m_tokens:
            self.model_totals[record.model]['cache_5m_tokens'] += record.cache_5m_tokens
        if record.cache_1h_tokens:
            self.model_totals[record.model]['cache_1h_tokens'] += record.cache_1h_tokens
        self.model_totals[record.model]['call_count'] += 1
        
        # Update operation totals
        if record.operation not in self.operation_totals:
            self.operation_totals[record.operation] = {
                'input_tokens': 0,
                'output_tokens': 0,
                'total_tokens': 0,
                'reasoning_tokens': 0,
                'cached_tokens': 0,
                'cache_5m_tokens': 0,
                'cache_1h_tokens': 0,
                'call_count': 0
            }

        self.operation_totals[record.operation]['input_tokens'] += record.input_tokens
        self.operation_totals[record.operation]['output_tokens'] += record.output_tokens
        self.operation_totals[record.operation]['total_tokens'] += record.total_tokens
        if record.reasoning_tokens:
            self.operation_totals[record.operation]['reasoning_tokens'] += record.reasoning_tokens
        if record.cached_tokens:
            self.operation_totals[record.operation]['cached_tokens'] += record.cached_tokens
        if record.cache_5m_tokens:
            self.operation_totals[record.operation]['cache_5m_tokens'] += record.cache_5m_tokens
        if record.cache_1h_tokens:
            self.operation_totals[record.operation]['cache_1h_tokens'] += record.cache_1h_tokens
        self.operation_totals[record.operation]['call_count'] += 1
    
    def get_summary(self, include_details: bool = False) -> Dict[str, Any]:
        """Get a summary of token usage.
        
        Args:
            include_details: Whether to include detailed breakdown
        
        Returns:
            Dictionary with usage summary
        """
        total_input = sum(r.input_tokens for r in self.records)
        total_output = sum(r.output_tokens for r in self.records)
        total_tokens = sum(r.total_tokens for r in self.records)
        total_reasoning = sum(r.reasoning_tokens for r in self.records if r.reasoning_tokens)
        total_cached = sum(r.cached_tokens for r in self.records if r.cached_tokens)
        total_accepted_pred = sum(r.accepted_prediction_tokens for r in self.records if r.accepted_prediction_tokens)
        total_rejected_pred = sum(r.rejected_prediction_tokens for r in self.records if r.rejected_prediction_tokens)
        
        session_duration = (datetime.now() - self.session_start).total_seconds()
        
        summary = {
            'session_duration_seconds': session_duration,
            'total_calls': len(self.records),
            'total_input_tokens': total_input,
            'total_output_tokens': total_output,
            'total_tokens': total_tokens,
            'total_reasoning_tokens': total_reasoning,
            'total_cached_tokens': total_cached,
            'average_tokens_per_call': total_tokens / len(self.records) if self.records else 0
        }
        
        # Add prediction tokens if any were used
        if total_accepted_pred > 0:
            summary['total_accepted_prediction_tokens'] = total_accepted_pred
        if total_rejected_pred > 0:
            summary['total_rejected_prediction_tokens'] = total_rejected_pred
        
        # Only include detailed breakdowns if requested
        if include_details:
            summary['by_model'] = self.model_totals
            summary['by_operation'] = self.operation_totals
            
        return summary
    
    def print_summary(self) -> None:
        """Print a formatted summary of token usage."""
        summary = self.get_summary()
        
        print("\n" + "="*60)
        print("TOKEN USAGE SUMMARY")
        print("="*60)
        
        print(f"\nSession Duration: {summary['session_duration_seconds']:.1f} seconds")
        print(f"Total API Calls: {summary['total_calls']}")
        
        # Show totals and averages
        if summary['total_calls'] > 0:
            avg_input = summary['total_input_tokens'] / summary['total_calls']
            avg_output = summary['total_output_tokens'] / summary['total_calls']
            avg_total = summary['total_tokens'] / summary['total_calls']
            
            print("\nTotal Token Usage:")
            print(f"  Total Input:      {summary['total_input_tokens']:,}")
            if summary.get('total_cached_tokens', 0) > 0:
                print(f"    - Cached:       {summary['total_cached_tokens']:,}")
                print(f"    - Regular:      {summary['total_input_tokens'] - summary['total_cached_tokens']:,}")
            else:
                # Always show cached status even if 0
                print("    - Cached:       0")
            print(f"  Total Output:     {summary['total_output_tokens']:,}")
            print(f"  Total Combined:   {summary['total_tokens']:,}")
            if summary.get('total_reasoning_tokens', 0) > 0:
                print(f"  Total Reasoning:  {summary['total_reasoning_tokens']:,}")
            
            print("\nAverage per Call:")
            print(f"  Avg Input:        {avg_input:.0f}")
            print(f"  Avg Output:       {avg_output:.0f}")
            print(f"  Avg Total:        {avg_total:.0f}")
            if summary['total_reasoning_tokens'] > 0:
                avg_reasoning = summary['total_reasoning_tokens'] / summary['total_calls']
                print(f"  Avg Reasoning:    {avg_reasoning:.0f}")
        
        print("\n" + "="*60)
    
    def export_to_json(self, filepath: str) -> None:
        """Export token usage data to JSON file.
        
        Args:
            filepath: Path to save the JSON file
        """
        data = {
            'summary': self.get_summary(),
            'records': [
                {
                    'timestamp': r.timestamp.isoformat(),
                    'model': r.model,
                    'operation': r.operation,
                    'input_tokens': r.input_tokens,
                    'output_tokens': r.output_tokens,
                    'total_tokens': r.total_tokens,
                    'reasoning_tokens': r.reasoning_tokens,
                    'metadata': r.metadata
                }
                for r in self.records
            ]
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Token usage data exported to {filepath}")
    
    def estimate_cost(self, pricing: Optional[Dict[str, Dict[str, float]]] = None, use_config: bool = True) -> Dict[str, float]:
        """Estimate cost based on token usage with support for tiered pricing.

        Args:
            pricing: Optional pricing dictionary. If not provided, uses pricing from config or defaults.
                    Format: {model: {'input': price_per_1m, 'output': price_per_1m}} or full manifest pricing
            use_config: If True, loads pricing from model config first

        Returns:
            Dictionary with cost estimates by model and total
        """
        # Load pricing from config if requested
        model_pricing_map = {}
        if use_config:
            try:
                config = LLM.get_model_config()

                for model in self.model_totals.keys():
                    # Try to find the model in config across all providers
                    for provider in ['openai', 'anthropic', 'gemini', 'volcengine', 'openrouter', 'orcarouter', 'vllm']:
                        model_info = config.get_model_info(provider, model)
                        if model_info and 'pricing' in model_info:
                            model_pricing_map[model] = model_info['pricing']
                            break
            except Exception as e:
                logger.warning(f"Failed to load pricing from config: {e}")

        # Use provided pricing if available (for backward compatibility)
        if pricing:
            for model, price_info in pricing.items():
                if model not in model_pricing_map:
                    # Convert old format (per 1K) to new format (per 1M)
                    model_pricing_map[model] = {
                        'input': price_info.get('input', 0) * 1000,
                        'output': price_info.get('output', 0) * 1000
                    }

        # Fallback to default pricing if nothing loaded
        if not model_pricing_map:
            model_pricing_map = {
                'gpt-5': {'input': 1.25, 'output': 10.00},
                'gpt-5-mini': {'input': 0.25, 'output': 2.00},
                'gpt-5-nano': {'input': 0.05, 'output': 0.40},
                'gpt-4.1': {'input': 2.00, 'output': 8.00},
                'gpt-4.1-mini': {'input': 0.40, 'output': 1.60},
                'gpt-4.1-nano': {'input': 0.10, 'output': 0.40},
            }

        total_cost = 0.0
        total_input_cost = 0.0
        total_cached_cost = 0.0
        total_output_cost = 0.0
        total_cache_storage_cost = 0.0
        total_cache_5m_cost = 0.0
        total_cache_1h_cost = 0.0

        # Calculate costs for each model
        for model, stats in self.model_totals.items():
            pricing_info = model_pricing_map.get(model)
            if not pricing_info:
                logger.warning(f"No pricing information found for model: {model}")
                continue

            # Get token counts from aggregated stats
            input_tokens = stats.get('input_tokens', 0)
            output_tokens = stats.get('output_tokens', 0)
            cached_tokens = stats.get('cached_tokens', 0)
            cache_5m_tokens = stats.get('cache_5m_tokens', 0)
            cache_1h_tokens = stats.get('cache_1h_tokens', 0)

            # Calculate cost using pricing utilities
            cost_result = calculate_total_cost(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                cache_5m_tokens=cache_5m_tokens,
                cache_1h_tokens=cache_1h_tokens,
                pricing=pricing_info
            )

            total_cost += cost_result['total_cost']

            # Aggregate breakdown by cost type
            breakdown = cost_result.get('breakdown', {})
            if 'input' in breakdown:
                total_input_cost += breakdown['input']['cost']
            if 'cached_input' in breakdown:
                total_cached_cost += breakdown['cached_input']['cost']
            if 'output' in breakdown:
                total_output_cost += breakdown['output']['cost']
            if 'cache_storage' in breakdown:
                total_cache_storage_cost += breakdown['cache_storage']['cost']
            if 'cache_5m_creation' in breakdown:
                total_cache_5m_cost += breakdown['cache_5m_creation']['cost']
            if 'cache_1h_creation' in breakdown:
                total_cache_1h_cost += breakdown['cache_1h_creation']['cost']

        # Build result dictionary
        result = {
            'total_cost': total_cost,
            'input_cost': total_input_cost,
            'output_cost': total_output_cost
        }

        # Add optional cost components
        if total_cached_cost > 0:
            result['cached_cost'] = total_cached_cost
        if total_cache_storage_cost > 0:
            result['cache_storage_cost'] = total_cache_storage_cost
        if total_cache_5m_cost > 0:
            result['cache_5m_cost'] = total_cache_5m_cost
        if total_cache_1h_cost > 0:
            result['cache_1h_cost'] = total_cache_1h_cost

        return result
    
    def print_cost_estimate(self, pricing: Optional[Dict[str, Dict[str, float]]] = None) -> None:
        """Print estimated costs with support for multiple cache types.

        Args:
            pricing: Optional pricing dictionary
        """
        costs = self.estimate_cost(pricing)

        if costs and costs['total_cost'] > 0:
            print("\n" + "="*60)
            print("ESTIMATED COST")
            print("="*60)

            # Calculate average cost per call
            total_calls = len(self.records)
            avg_cost = costs['total_cost'] / total_calls if total_calls > 0 else 0

            print(f"\nTotal Cost:     ${costs['total_cost']:.4f}")
            print(f"Average/Call:   ${avg_cost:.4f}")

            # Build breakdown with all cost types
            breakdown_parts = []
            breakdown_parts.append(f"${costs['input_cost']:.4f} (input)")

            if costs.get('cached_cost', 0) > 0:
                breakdown_parts.append(f"${costs['cached_cost']:.4f} (cache hit)")

            if costs.get('cache_storage_cost', 0) > 0:
                breakdown_parts.append(f"${costs['cache_storage_cost']:.4f} (cache storage)")

            if costs.get('cache_5m_cost', 0) > 0:
                breakdown_parts.append(f"${costs['cache_5m_cost']:.4f} (cache 5m)")

            if costs.get('cache_1h_cost', 0) > 0:
                breakdown_parts.append(f"${costs['cache_1h_cost']:.4f} (cache 1h)")

            breakdown_parts.append(f"${costs['output_cost']:.4f} (output)")

            print(f"\nBreakdown:      {' + '.join(breakdown_parts)}")

            print("="*60)


# Global token tracker instance (optional, for convenience)
_global_tracker: Optional[TokenUsageTracker] = None


def get_global_tracker() -> TokenUsageTracker:
    """Get or create the global token tracker instance."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = TokenUsageTracker()
    return _global_tracker


def reset_global_tracker() -> None:
    """Reset the global token tracker."""
    global _global_tracker
    _global_tracker = TokenUsageTracker()