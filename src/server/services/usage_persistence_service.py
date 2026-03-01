"""
Centralized Usage Persistence Service.

This service provides a unified interface for tracking and persisting ALL usage data:
- LLM token usage and costs
- Infrastructure usage (tools, cache, storage)
- Credit calculation and conversion
- Database persistence

Usage:
    # Initialize service for a workflow
    service = UsagePersistenceService(
        thread_id="abc-123",
        workspace_id="ws-456",
        user_id="user-789"
    )

    # Track LLM usage
    token_usage = await service.track_llm_usage(per_call_records)

    # Track infrastructure usage
    await service.record_tool_usage("TavilySearchTool", count=5)

    # Persist to database
    await service.persist_usage(response_id="resp-123")

    # Get usage summary for backward compatibility
    summary = service.get_usage_summary()
"""

import logging
from decimal import Decimal
from typing import Dict, Any, Optional
from uuid import uuid4
from datetime import datetime, timezone

from src.utils.tracking.infrastructure_costs import (
    calculate_infrastructure_credits,
    format_infrastructure_usage
)

logger = logging.getLogger(__name__)

# Credit conversion rate (USD to credits)
USD_TO_CREDITS_RATE = 1000  # 1 USD = 1000 credits


class UsagePersistenceService:
    """
    Centralized service for tracking and persisting usage data.

    This service is responsible for:
    1. Token counting integration (via PerCallTokenTracker)
    2. Infrastructure usage tracking (tools, cache, storage)
    3. Cost calculation (USD) via existing pricing utilities
    4. Credit conversion (USD → credits)
    5. Database persistence (conversation_usage table)
    6. Backward compatibility (returns token_usage format)
    """

    def __init__(
        self,
        thread_id: str,
        workspace_id: str,
        user_id: str,
        credit_conversion_rate: Optional[float] = None
    ):
        """
        Initialize usage tracking service for a workflow execution.

        Args:
            thread_id: Thread identifier
            workspace_id: Workspace identifier
            user_id: User identifier
            credit_conversion_rate: Optional custom conversion rate (default: 1000)
        """
        self.thread_id = thread_id
        self.workspace_id = workspace_id
        self.user_id = user_id
        self.credit_conversion_rate = credit_conversion_rate or USD_TO_CREDITS_RATE

        # Internal tracking
        self._token_usage: Optional[Dict[str, Any]] = None
        self._tool_usage: Dict[str, int] = {}
        self._infrastructure_credits: Decimal = Decimal("0.0")
        self._token_credits: Decimal = Decimal("0.0")

        logger.debug(
            f"[UsagePersistence] Initialized service for thread_id={thread_id}, "
            f"user_id={user_id}, conversion_rate={self.credit_conversion_rate}"
        )

    # ========== LLM Token Tracking ==========

    async def track_llm_usage(
        self,
        per_call_records: list
    ) -> Dict[str, Any]:
        """
        Track LLM token usage and calculate costs.

        This method integrates with the existing token tracking infrastructure:
        - Uses calculate_cost_from_per_call_records() for cost calculation
        - Converts USD costs to credits
        - Stores token_usage for persistence

        Args:
            per_call_records: List of per-call token records from PerCallTokenTracker

        Returns:
            Token usage dict (compatible with current format):
            {
                "by_model": {...},
                "total_cost": float,
                "cost_breakdown": {...},
                "per_call_costs": [...]
            }
        """
        from src.utils.tracking.core import calculate_cost_from_per_call_records

        if not per_call_records:
            logger.debug("[UsagePersistence] No per-call records to track")
            self._token_usage = {
                "by_model": {},
                "total_cost": 0.0,
                "cost_breakdown": {
                    "input_cost": 0.0,
                    "output_cost": 0.0,
                    "cached_cost": 0.0
                }
            }
            self._token_credits = Decimal("0.0")
            return self._token_usage

        try:
            # Calculate costs using existing infrastructure
            token_usage_with_cost = calculate_cost_from_per_call_records(per_call_records)

            # Store for persistence
            self._token_usage = token_usage_with_cost

            # Convert USD to credits
            total_cost_usd = token_usage_with_cost.get("total_cost", 0.0)
            self._token_credits = Decimal(str(total_cost_usd)) * Decimal(str(self.credit_conversion_rate))

            logger.debug(
                f"[UsagePersistence] Tracked LLM usage: "
                f"total_cost=${total_cost_usd:.4f}, "
                f"credits={float(self._token_credits):.2f}"
            )

            return token_usage_with_cost

        except Exception as e:
            logger.error(
                f"[UsagePersistence] Failed to track LLM usage: {e}",
                exc_info=True
            )
            # Return empty structure on error
            self._token_usage = {
                "by_model": {},
                "total_cost": 0.0,
                "cost_breakdown": {"input_cost": 0.0, "output_cost": 0.0, "cached_cost": 0.0}
            }
            self._token_credits = Decimal("0.0")
            return self._token_usage

    # ========== Infrastructure Usage Tracking ==========

    def record_tool_usage(self, tool_name: str, count: int = 1) -> None:
        """
        Record infrastructure tool usage.

        Args:
            tool_name: Tool class name (e.g., "TavilySearchTool")
            count: Number of times tool was used (default: 1)
        """
        if count <= 0:
            return

        self._tool_usage[tool_name] = self._tool_usage.get(tool_name, 0) + count

        logger.debug(
            f"[UsagePersistence] Recorded tool usage: {tool_name} x{count}"
        )

    def record_tool_usage_batch(self, tool_usage: Dict[str, int]) -> None:
        """
        Record multiple tool usages at once.

        Args:
            tool_usage: Dict mapping tool names to usage counts
        """
        for tool_name, count in tool_usage.items():
            self.record_tool_usage(tool_name, count)

    def _calculate_infrastructure_credits(self) -> Decimal:
        """
        Calculate infrastructure credits from recorded tool usage.

        Returns:
            Total infrastructure credits as Decimal
        """
        if not self._tool_usage:
            return Decimal("0.0")

        try:
            result = calculate_infrastructure_credits(self._tool_usage)
            total_credits = result.get("total_credits", 0.0)

            logger.debug(
                f"[UsagePersistence] Calculated infrastructure credits: "
                f"{total_credits:.2f} from {len(self._tool_usage)} tool types"
            )

            return Decimal(str(total_credits))

        except Exception as e:
            logger.error(
                f"[UsagePersistence] Failed to calculate infrastructure credits: {e}",
                exc_info=True
            )
            return Decimal("0.0")

    # ========== Persistence ==========

    async def persist_usage(
        self,
        response_id: str,
        timestamp: Optional[datetime] = None,
        msg_type: Optional[str] = None,
        deepthinking: bool = False,
        status: str = "completed",
        conn: Optional[Any] = None,
        is_byok: bool = False
    ) -> bool:
        """
        Persist usage data to conversation_usage table.

        This method:
        1. Calculates infrastructure credits
        2. Formats data for database storage
        3. Inserts into conversation_usage table

        Args:
            response_id: Response identifier
            timestamp: Optional timestamp (defaults to now)
            msg_type: Base workflow type (chat, technical_analysis, etc.)
            deepthinking: Whether deepthinking was enabled
            status: Workflow completion status (completed, error, cancelled, interrupted)
            conn: Optional database connection for transaction support

        Returns:
            True if successful, False otherwise
        """
        from src.server.database import conversation as qr_db

        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        try:
            # Calculate infrastructure credits
            self._infrastructure_credits = self._calculate_infrastructure_credits()

            # Calculate total credits
            total_credits = self._token_credits + self._infrastructure_credits

            # Format infrastructure usage for JSONB
            infrastructure_usage = None
            if self._tool_usage:
                infrastructure_usage = format_infrastructure_usage(self._tool_usage)

            final_msg_type = msg_type or 'ptc'

            # Build usage record (write-once data, no updates)
            usage_data = {
                "conversation_usage_id": str(uuid4()),
                "conversation_response_id": response_id,
                "user_id": self.user_id,
                "conversation_thread_id": self.thread_id,
                "workspace_id": self.workspace_id,
                "msg_type": final_msg_type,
                "status": status,
                "token_usage": self._token_usage,
                "infrastructure_usage": infrastructure_usage,
                "token_credits": float(self._token_credits),
                "infrastructure_credits": float(self._infrastructure_credits),
                "total_credits": float(total_credits),
                "is_byok": is_byok,
                "created_at": timestamp
            }

            # Persist to database (use provided conn for transaction support)
            if conn:
                await qr_db.create_usage_record(usage_data, conn=conn)
            else:
                async with qr_db.get_db_connection() as new_conn:
                    await qr_db.create_usage_record(usage_data, conn=new_conn)

            logger.info(
                f"[UsagePersistence] Persisted usage for response_id={response_id}, "
                f"msg_type={final_msg_type}, status={status}, "
                f"total_credits={float(total_credits):.2f} "
                f"(tokens={float(self._token_credits):.2f}, "
                f"infra={float(self._infrastructure_credits):.2f})"
            )

            return True

        except Exception as e:
            logger.error(
                f"[UsagePersistence] Failed to persist usage for response_id={response_id}: {e}",
                exc_info=True
            )
            return False

    # ========== Aggregation & Summary ==========

    def get_usage_summary(self) -> Dict[str, Any]:
        """
        Get usage summary for backward compatibility and display.

        Returns:
            Dict with structure:
            {
                "token_usage": {...},  # Original token_usage format
                "infrastructure_usage": {...},
                "credits": {
                    "token_credits": float,
                    "infrastructure_credits": float,
                    "total_credits": float
                }
            }
        """
        # Calculate latest infrastructure credits
        infrastructure_credits = self._calculate_infrastructure_credits()
        total_credits = self._token_credits + infrastructure_credits

        return {
            "token_usage": self._token_usage or {},
            "infrastructure_usage": format_infrastructure_usage(self._tool_usage) if self._tool_usage else {},
            "credits": {
                "token_credits": float(self._token_credits),
                "infrastructure_credits": float(infrastructure_credits),
                "total_credits": float(total_credits),
                "conversion_rate": self.credit_conversion_rate
            }
        }

    def get_token_usage(self) -> Dict[str, Any]:
        """
        Get token usage in original format for backward compatibility.

        This method returns the token_usage structure that is stored in
        conversation_usage.token_usage.

        Returns:
            Token usage dict or empty dict if not tracked
        """
        return self._token_usage or {}

    def get_total_credits(self) -> float:
        """
        Get total calculated credits (token + infrastructure) without persisting.

        This method is useful for SSE streaming to show credits before persistence.
        Calculates infrastructure credits on demand.

        Returns:
            Total credits as float
        """
        # Calculate latest infrastructure credits (may have changed since last calculation)
        infrastructure_credits = self._calculate_infrastructure_credits()
        total_credits = self._token_credits + infrastructure_credits

        return float(total_credits)

    # ========== Utility Methods ==========

    def reset(self) -> None:
        """Reset all tracked usage data."""
        self._token_usage = None
        self._tool_usage.clear()
        self._infrastructure_credits = Decimal("0.0")
        self._token_credits = Decimal("0.0")

        logger.debug(f"[UsagePersistence] Reset usage tracking for thread_id={self.thread_id}")

    def __repr__(self) -> str:
        return (
            f"UsagePersistenceService(thread_id={self.thread_id}, "
            f"user_id={self.user_id}, "
            f"total_credits={self.get_total_credits():.2f})"
        )


# Singleton pattern for easy access (optional, can also use direct instantiation)
_service_instances: Dict[str, UsagePersistenceService] = {}


def get_usage_service(
    thread_id: str,
    workspace_id: str,
    user_id: str,
    create_if_missing: bool = True
) -> Optional[UsagePersistenceService]:
    """
    Get or create UsagePersistenceService instance for a thread.

    Args:
        thread_id: Thread identifier
        workspace_id: Workspace identifier
        user_id: User identifier
        create_if_missing: Create new instance if not found (default: True)

    Returns:
        UsagePersistenceService instance or None
    """
    if thread_id in _service_instances:
        return _service_instances[thread_id]

    if create_if_missing:
        service = UsagePersistenceService(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id
        )
        _service_instances[thread_id] = service
        return service

    return None


def cleanup_usage_service(thread_id: str) -> None:
    """
    Clean up UsagePersistenceService instance for a thread.

    Args:
        thread_id: Thread identifier
    """
    if thread_id in _service_instances:
        del _service_instances[thread_id]
        logger.debug(f"[UsagePersistence] Cleaned up service for thread_id={thread_id}")
