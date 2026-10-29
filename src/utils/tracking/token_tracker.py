"""Token tracking initialization for workflow runs."""

import logging

from .per_call_token_tracker import PerCallTokenTracker

logger = logging.getLogger(__name__)


class TokenTrackingManager:
    """Builds the per-call token callback a workflow run bills from."""

    @staticmethod
    def initialize_tracking(
        thread_id: str,
        track_tokens: bool = True
    ) -> PerCallTokenTracker:
        """
        Initialize token tracking for a workflow run.

        Args:
            thread_id: Thread identifier for logging
            track_tokens: Whether to enable token tracking (always True, kept for compatibility)

        Returns:
            PerCallTokenTracker instance
        """
        token_callback = PerCallTokenTracker()
        logger.debug(f"Token tracking started for thread_id={thread_id}")
        return token_callback


# Public API
__all__ = [
    'TokenTrackingManager',
]
