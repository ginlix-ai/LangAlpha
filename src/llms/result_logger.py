"""
Result logger for saving workflow execution results with token usage and agent configurations.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class ResultLogger:
    """Logs workflow execution results including token usage and agent configurations."""

    def __init__(self, log_dir: str = "./log"):
        """
        Initialize the result logger.

        Args:
            log_dir: Directory to save log files
        """
        self.log_dir = Path(log_dir)
        # File logging enabled by default (can be disabled with RESULT_LOG_ENABLED env var)
        self.enabled = os.getenv("RESULT_LOG_ENABLED", "true").lower() == "true"

        # Override log directory from environment if set
        env_log_dir = os.getenv("RESULT_LOG_DIR")
        if env_log_dir:
            self.log_dir = Path(env_log_dir)

    def get_log_path(self, thread_id: str, timestamp: Optional[datetime] = None) -> Path:
        """
        Get the log file path for a given thread ID.

        Args:
            thread_id: The thread ID (execution identifier)
            timestamp: Optional timestamp, defaults to now

        Returns:
            Path to the log file with format: HHMM-last4.json
        """
        if timestamp is None:
            timestamp = datetime.now()

        date_dir = self.log_dir / timestamp.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        # Extract hour and minute (HHMM format)
        time_prefix = timestamp.strftime("%H%M")

        # Extract last 4 characters of thread_id
        last4 = thread_id[-4:] if len(thread_id) >= 4 else thread_id

        # Use format: HHMM-last4.json
        return date_dir / f"{time_prefix}-{last4}.json"
    
    def _create_initial_log(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create initial log structure with first query-response pair.

        Args:
            session_data: Session data with query_id, query, response_id, response

        Returns:
            Structured log with query_response_pairs array
        """
        query_response_pair = {
            "turn_index": 0,
            "query_id": session_data["query_id"],
            "query": session_data["query"],
            "response_id": session_data["response_id"],
            "response": session_data["response"]
        }

        return {
            "thread_id": session_data["thread_id"],
            "conversation_id": session_data.get("conversation_id"),
            "user_id": session_data.get("user_id"),
            "query_response_pairs": [query_response_pair],
            "summary": {
                "total_queries": 1,
                "total_responses": 1,
                "total_execution_time": session_data["response"]["execution_time"],
                "aggregated_token_usage": session_data["response"]["token_usage"],
                "final_status": session_data["response"]["status"]
            },
            "created_at": session_data["query"]["timestamp"],
            "updated_at": session_data["response"]["timestamp"]
        }

    def _merge_execution(self, log_path: Path, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Append new query-response pair to existing log file.

        Args:
            log_path: Path to existing log file
            session_data: Session data with query_id, query, response_id, response

        Returns:
            Merged log data with updated query_response_pairs
        """
        # Load existing log
        with open(log_path, 'r', encoding='utf-8') as f:
            existing_log = json.load(f)

        # Calculate next turn_index based on existing pairs
        next_index = len(existing_log["query_response_pairs"])

        # Create new query-response pair
        new_pair = {
            "turn_index": next_index,
            "query_id": session_data["query_id"],
            "query": session_data["query"],
            "response_id": session_data["response_id"],
            "response": session_data["response"]
        }

        # Append pair
        existing_log["query_response_pairs"].append(new_pair)

        # Update summary
        existing_log["summary"]["total_queries"] += 1
        existing_log["summary"]["total_responses"] += 1
        existing_log["summary"]["total_execution_time"] += session_data["response"]["execution_time"]
        existing_log["summary"]["final_status"] = session_data["response"]["status"]

        # Aggregate token usage
        self._aggregate_token_usage(
            existing_log["summary"]["aggregated_token_usage"],
            session_data["response"]["token_usage"]
        )

        # Update top-level fields from latest execution
        existing_log["conversation_id"] = session_data.get("conversation_id") or existing_log.get("conversation_id")
        existing_log["user_id"] = session_data.get("user_id") or existing_log.get("user_id")
        existing_log["updated_at"] = session_data["response"]["timestamp"]

        pair_count = len(existing_log["query_response_pairs"])
        logger.info(f"Appended query-response pair #{pair_count} for thread_id={existing_log['thread_id']}")

        return existing_log

    def _aggregate_token_usage(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """
        Aggregate token usage from source into target.

        Args:
            target: Target token usage dictionary to update
            source: Source token usage dictionary to aggregate from
        """
        if not source:
            return

        # Initialize target structure if needed
        if "by_model" not in target:
            target["by_model"] = {}
        if "total_cost" not in target:
            target["total_cost"] = 0.0

        # Aggregate by_model tokens
        for model, usage in source.get("by_model", {}).items():
            if model not in target["by_model"]:
                # Deep copy the usage dict
                target["by_model"][model] = {
                    "input_tokens": usage.get("input_tokens", 0),
                    "output_tokens": usage.get("output_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0)
                }
                # Copy other fields if present
                for key in ["input_token_details", "output_token_details"]:
                    if key in usage:
                        target["by_model"][model][key] = usage[key]
            else:
                # Aggregate token counts
                target["by_model"][model]["input_tokens"] += usage.get("input_tokens", 0)
                target["by_model"][model]["output_tokens"] += usage.get("output_tokens", 0)
                target["by_model"][model]["total_tokens"] += usage.get("total_tokens", 0)

        # Sum total cost
        target["total_cost"] += source.get("total_cost", 0.0)

        # Aggregate cost_breakdown if present
        if "cost_breakdown" in source:
            if "cost_breakdown" not in target:
                target["cost_breakdown"] = {}
            for key, value in source["cost_breakdown"].items():
                target["cost_breakdown"][key] = target["cost_breakdown"].get(key, 0.0) + value

    def save_result(self, session_data: Dict[str, Any]) -> bool:
        """
        Save workflow result to JSON file with query-response structure.

        For initial queries, creates a new log file. For subsequent queries on the same
        thread_id, appends a new query-response pair.

        Args:
            session_data: Dictionary with query_id, query, response_id, response, thread_id

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        try:
            # Ensure thread_id exists
            if "thread_id" not in session_data:
                logger.warning("No thread_id provided in session_data")
                return False

            thread_id = session_data["thread_id"]

            # Determine timestamp from response (primary) or query (fallback)
            response_timestamp = session_data.get("response", {}).get("timestamp")
            query_timestamp = session_data.get("query", {}).get("timestamp")

            if response_timestamp:
                timestamp = datetime.fromisoformat(response_timestamp)
            elif query_timestamp:
                timestamp = datetime.fromisoformat(query_timestamp)
            else:
                timestamp = datetime.now()

            # Get the date directory
            date_str = timestamp.strftime("%Y-%m-%d")
            date_dir = self.log_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)

            # Search for existing log file with same thread_id (by last 4 chars)
            # This ensures all query-response pairs for a thread stay in one file,
            # even if queries happen at different times (different HHMM prefixes)
            last4 = thread_id[-4:] if len(thread_id) >= 4 else thread_id
            existing_files = list(date_dir.glob(f"*-{last4}.json"))

            if existing_files:
                # Use the first existing file (reuse same file for all pairs in this thread)
                log_path = existing_files[0]
                logger.info(f"Existing log found for thread_id={thread_id} at {log_path}, appending query-response pair")
                merged_data = self._merge_execution(log_path, session_data)
            else:
                # First query in this thread - create new log structure with current timestamp
                log_path = self.get_log_path(thread_id, timestamp)
                merged_data = self._create_initial_log(session_data)

            # Save merged/new data to JSON file
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Result saved to {log_path}")
            return True

        except Exception as e:
            logger.warning(f"Failed to save result: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    async def save_result_async(
        self,
        session_data: Dict[str, Any],
    ) -> bool:
        """
        Async version of save_result.

        Args:
            session_data: Dictionary containing all session data

        Returns:
            True if the save succeeded
        """
        success_count = 0

        # Save to JSON file
        if self.enabled:
            if self.save_result(session_data):
                success_count += 1

        return success_count > 0
    
    def load_result(self, thread_id: str, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Load a result by thread ID.

        Args:
            thread_id: The thread ID to load
            date: Optional date string (YYYY-MM-DD), if not provided searches all dates

        Returns:
            The session data if found, None otherwise
        """
        try:
            # Extract last 4 characters of thread_id for searching
            last4 = thread_id[-4:] if len(thread_id) >= 4 else thread_id

            if date:
                date_dir = self.log_dir / date
                if date_dir.exists():
                    # Search for files matching pattern *-{last4}.json
                    matching_files = list(date_dir.glob(f"*-{last4}.json"))
                    if matching_files:
                        # Return first match (there should typically be only one)
                        with open(matching_files[0], 'r', encoding='utf-8') as f:
                            return json.load(f)
            else:
                # Search all date directories
                for date_dir in sorted(self.log_dir.iterdir(), reverse=True):
                    if date_dir.is_dir():
                        # Search for files matching pattern *-{last4}.json
                        matching_files = list(date_dir.glob(f"*-{last4}.json"))
                        if matching_files:
                            # Return first match
                            with open(matching_files[0], 'r', encoding='utf-8') as f:
                                return json.load(f)

            return None

        except Exception as e:
            logger.error(f"Failed to load result {thread_id}: {e}")
            return None
    
    def query_by_date(self, date: str) -> List[Dict[str, Any]]:
        """
        Query all results for a specific date.
        
        Args:
            date: Date string (YYYY-MM-DD)
            
        Returns:
            List of session data dictionaries
        """
        results = []
        date_dir = self.log_dir / date
        
        if not date_dir.exists():
            return results
        
        try:
            for log_file in date_dir.glob("*.json"):
                with open(log_file, 'r', encoding='utf-8') as f:
                    results.append(json.load(f))
            
            # Sort by timestamp
            results.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to query results for date {date}: {e}")
        
        return results
    
    def calculate_daily_token_usage(self, date: str) -> Dict[str, Any]:
        """
        Calculate total token usage for a specific date.
        
        Args:
            date: Date string (YYYY-MM-DD)
            
        Returns:
            Dictionary with token usage summary by model
        """
        results = self.query_by_date(date)
        token_summary = {}
        
        for result in results:
            if "token_usage" in result and result["token_usage"]:
                for model, usage in result["token_usage"].items():
                    if model not in token_summary:
                        token_summary[model] = {
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "total_tokens": 0,
                            "session_count": 0
                        }
                    
                    token_summary[model]["input_tokens"] += usage.get("input_tokens", 0)
                    token_summary[model]["output_tokens"] += usage.get("output_tokens", 0)
                    token_summary[model]["total_tokens"] += usage.get("total_tokens", 0)
                    token_summary[model]["session_count"] += 1
        
        # Calculate grand totals
        grand_total = {
            "input_tokens": sum(m["input_tokens"] for m in token_summary.values()),
            "output_tokens": sum(m["output_tokens"] for m in token_summary.values()),
            "total_tokens": sum(m["total_tokens"] for m in token_summary.values()),
            "total_sessions": len(results),
            "models": token_summary
        }
        
        return grand_total
    
    def export_to_csv(self, date: str, output_file: str) -> bool:
        """
        Export logs for a specific date to CSV format.
        
        Args:
            date: Date string (YYYY-MM-DD)
            output_file: Path to output CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import csv
            
            results = self.query_by_date(date)
            if not results:
                logger.warning(f"No results found for date {date}")
                return False
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'run_id', 'timestamp', 'success', 'execution_time',
                    'total_tokens', 'user_input_preview', 'output_preview'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results:
                    # Calculate total tokens
                    total_tokens = 0
                    if result.get("token_usage"):
                        for usage in result["token_usage"].values():
                            total_tokens += usage.get("total_tokens", 0)
                    
                    # Get previews (first 100 chars)
                    user_input = result.get("user_input", "")[:100]
                    final_output = result.get("final_output", "")[:100]
                    
                    writer.writerow({
                        'run_id': result.get("run_id", ""),
                        'timestamp': result.get("timestamp", ""),
                        'success': result.get("metadata", {}).get("success", False),
                        'execution_time': result.get("metadata", {}).get("execution_time", 0),
                        'total_tokens': total_tokens,
                        'user_input_preview': user_input,
                        'output_preview': final_output
                    })
            
            logger.info(f"Exported {len(results)} results to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            return False


# Global instance for convenience
_global_logger: Optional[ResultLogger] = None


def get_result_logger(log_dir: str = "./log") -> ResultLogger:
    """Get or create the global result logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = ResultLogger(log_dir)
    return _global_logger