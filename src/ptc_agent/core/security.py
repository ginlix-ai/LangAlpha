"""Security and monitoring utilities for code execution."""

import hashlib
import time
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class ExecutionMonitor:
    """Monitors code execution for security and performance."""

    MAX_HISTORY = 1000

    def __init__(self) -> None:
        """Initialize execution monitor."""
        self.execution_history: list[dict[str, Any]] = []
        self.active_executions: dict[str, dict[str, Any]] = {}

    def start_execution(
        self,
        execution_id: str,
        code: str,
        sandbox_id: str,
    ) -> None:
        """Start monitoring an execution.

        Args:
            execution_id: Unique execution identifier
            code: Code being executed
            sandbox_id: Sandbox identifier
        """
        code_hash = hashlib.sha256(code.encode()).hexdigest()

        execution_info = {
            "execution_id": execution_id,
            "code_hash": code_hash,
            "sandbox_id": sandbox_id,
            "start_time": time.time(),
            "code_length": len(code),
        }

        self.active_executions[execution_id] = execution_info

        logger.info(
            "Execution started",
            execution_id=execution_id,
            sandbox_id=sandbox_id,
            code_hash=code_hash[:8],
        )

    def end_execution(
        self,
        execution_id: str,
        *,
        success: bool,
        output: str | None = None,
        error: str | None = None,
    ) -> None:
        """End monitoring an execution.

        Args:
            execution_id: Execution identifier
            success: Whether execution was successful
            output: Execution output
            error: Error message if failed
        """
        if execution_id not in self.active_executions:
            logger.warning("Execution not found in active list", execution_id=execution_id)
            return

        execution_info = self.active_executions[execution_id]
        execution_info["end_time"] = time.time()
        execution_info["duration"] = execution_info["end_time"] - execution_info["start_time"]
        execution_info["success"] = success
        execution_info["output_length"] = len(output) if output else 0
        execution_info["error"] = error

        # Move to history
        self.execution_history.append(execution_info)
        # Prune oldest entries to prevent unbounded memory growth
        if len(self.execution_history) > self.MAX_HISTORY:
            self.execution_history = self.execution_history[-self.MAX_HISTORY:]
        del self.active_executions[execution_id]

        logger.info(
            "Execution completed",
            execution_id=execution_id,
            success=success,
            duration=execution_info["duration"],
        )

    def get_execution_stats(self) -> dict[str, Any]:
        """Get execution statistics.

        Returns:
            Dictionary with execution statistics
        """
        total_executions = len(self.execution_history)
        successful_executions = sum(1 for ex in self.execution_history if ex["success"])
        failed_executions = total_executions - successful_executions

        avg_duration = 0
        if total_executions > 0:
            avg_duration = sum(ex["duration"] for ex in self.execution_history) / total_executions

        return {
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": successful_executions / total_executions if total_executions > 0 else 0,
            "average_duration": avg_duration,
            "active_executions": len(self.active_executions),
        }

    def get_recent_executions(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get recent execution history.

        Args:
            limit: Maximum number of executions to return

        Returns:
            List of recent executions
        """
        return self.execution_history[-limit:]


class RateLimiter:
    """Rate limiter for code execution."""

    def __init__(self, max_executions: int = 100, window_seconds: int = 3600) -> None:
        """Initialize rate limiter.

        Args:
            max_executions: Maximum executions per window
            window_seconds: Time window in seconds
        """
        self.max_executions = max_executions
        self.window_seconds = window_seconds
        self.execution_timestamps: list[float] = []

        logger.info(
            "Initialized RateLimiter",
            max_executions=max_executions,
            window_seconds=window_seconds,
        )

    def check_rate_limit(self) -> tuple[bool, str | None]:
        """Check if rate limit is exceeded.

        Returns:
            Tuple of (is_allowed, error_message)
        """
        now = time.time()

        # Remove old timestamps outside the window
        self.execution_timestamps = [
            ts for ts in self.execution_timestamps if now - ts < self.window_seconds
        ]

        # Check if limit is exceeded
        if len(self.execution_timestamps) >= self.max_executions:
            wait_time = self.window_seconds - (now - self.execution_timestamps[0])
            return False, f"Rate limit exceeded. Try again in {int(wait_time)} seconds."

        return True, None

    def record_execution(self) -> None:
        """Record a new execution."""
        self.execution_timestamps.append(time.time())

        logger.debug(
            "Execution recorded",
            current_count=len(self.execution_timestamps),
            max_executions=self.max_executions,
        )


class ResourceMonitor:
    """Monitors resource usage in sandboxes."""

    def __init__(self) -> None:
        """Initialize resource monitor."""
        self.sandbox_resources: dict[str, dict[str, Any]] = {}

    def track_sandbox(self, sandbox_id: str) -> None:
        """Start tracking a sandbox.

        Args:
            sandbox_id: Sandbox identifier
        """
        self.sandbox_resources[sandbox_id] = {
            "created_at": time.time(),
            "execution_count": 0,
            "total_code_length": 0,
            "files_created": 0,
        }

        logger.info("Started tracking sandbox", sandbox_id=sandbox_id)

    def record_execution(self, sandbox_id: str, code_length: int) -> None:
        """Record an execution in a sandbox.

        Args:
            sandbox_id: Sandbox identifier
            code_length: Length of executed code
        """
        if sandbox_id not in self.sandbox_resources:
            self.track_sandbox(sandbox_id)

        self.sandbox_resources[sandbox_id]["execution_count"] += 1
        self.sandbox_resources[sandbox_id]["total_code_length"] += code_length

    def record_file_operation(self, sandbox_id: str, operation: str) -> None:
        """Record a file operation in a sandbox.

        Args:
            sandbox_id: Sandbox identifier
            operation: Operation type (create, read, write, delete)
        """
        if sandbox_id not in self.sandbox_resources:
            self.track_sandbox(sandbox_id)

        if operation == "create":
            self.sandbox_resources[sandbox_id]["files_created"] += 1

    def get_sandbox_stats(self, sandbox_id: str) -> dict[str, Any] | None:
        """Get statistics for a sandbox.

        Args:
            sandbox_id: Sandbox identifier

        Returns:
            Statistics dictionary or None if not tracked
        """
        if sandbox_id not in self.sandbox_resources:
            return None

        stats = self.sandbox_resources[sandbox_id].copy()
        stats["age_seconds"] = time.time() - stats["created_at"]

        return stats

    def cleanup_sandbox(self, sandbox_id: str) -> None:
        """Stop tracking a sandbox.

        Args:
            sandbox_id: Sandbox identifier
        """
        if sandbox_id in self.sandbox_resources:
            del self.sandbox_resources[sandbox_id]
            logger.info("Stopped tracking sandbox", sandbox_id=sandbox_id)


class SecurityLogger:
    """Specialized logger for security events."""

    MAX_EVENTS = 1000

    def __init__(self) -> None:
        """Initialize security logger."""
        self.security_events: list[dict[str, Any]] = []

    def log_validation_failure(
        self,
        code_hash: str,
        reason: str,
        blocked_pattern: str | None = None,
    ) -> None:
        """Log a code validation failure.

        Args:
            code_hash: Hash of the code
            reason: Reason for failure
            blocked_pattern: Blocked pattern if applicable
        """
        event = {
            "type": "validation_failure",
            "timestamp": time.time(),
            "code_hash": code_hash,
            "reason": reason,
            "blocked_pattern": blocked_pattern,
        }

        self.security_events.append(event)
        if len(self.security_events) > self.MAX_EVENTS:
            self.security_events = self.security_events[-self.MAX_EVENTS:]

        logger.warning(
            "Code validation failed",
            code_hash=code_hash[:8],
            reason=reason,
            blocked_pattern=blocked_pattern,
        )

    def log_execution_timeout(self, execution_id: str, duration: float) -> None:
        """Log an execution timeout.

        Args:
            execution_id: Execution identifier
            duration: Execution duration before timeout
        """
        event = {
            "type": "execution_timeout",
            "timestamp": time.time(),
            "execution_id": execution_id,
            "duration": duration,
        }

        self.security_events.append(event)
        if len(self.security_events) > self.MAX_EVENTS:
            self.security_events = self.security_events[-self.MAX_EVENTS:]

        logger.error("Execution timeout", execution_id=execution_id, duration=duration)

    def log_suspicious_activity(
        self,
        activity_type: str,
        details: dict[str, Any],
    ) -> None:
        """Log suspicious activity.

        Args:
            activity_type: Type of suspicious activity
            details: Additional details
        """
        event = {
            "type": "suspicious_activity",
            "timestamp": time.time(),
            "activity_type": activity_type,
            "details": details,
        }

        self.security_events.append(event)
        if len(self.security_events) > self.MAX_EVENTS:
            self.security_events = self.security_events[-self.MAX_EVENTS:]

        logger.warning("Suspicious activity detected", activity_type=activity_type, details=details)

    def get_security_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get recent security events.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of security events
        """
        return self.security_events[-limit:]
