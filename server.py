"""
Server script
"""

import argparse
import logging
import uvicorn

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the server")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload (default: True except on Windows)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind the server to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the server to (default: 8000)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Log level (default: info)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of uvicorn worker processes (default: 1)",
    )

    args = parser.parse_args()

    # Configure SSE event logger independently
    # This allows viewing ONLY SSE events by setting SSE_EVENT_LOG_LEVEL=info
    # and server --log-level=error
    import os
    sse_event_log_level = os.getenv("SSE_EVENT_LOG_LEVEL", "info").upper()
    sse_logger = logging.getLogger("sse_events")
    sse_logger.setLevel(getattr(logging, sse_event_log_level))
    # Add dedicated handler so SSE logs output independently of root logger level
    sse_handler = logging.StreamHandler()
    sse_handler.setLevel(getattr(logging, sse_event_log_level))
    sse_handler.setFormatter(logging.Formatter("%(message)s"))
    sse_logger.addHandler(sse_handler)
    # Prevent duplicate logs by not propagating to root logger
    sse_logger.propagate = False


    # Determine reload setting
    reload = False
    if args.reload:
        reload = True

    # SINGLE-WORKER invariant — authoritative rationale. The concurrent-PTC
    # report-back system (report_back.py) serializes dispatch caps with an
    # in-process asyncio.Lock and drains its queue with LINDEX-peek + LREM, and
    # background_task_manager.py's compaction guard is in-memory — none of these
    # are atomic across processes, so N workers can double-drain report-backs or
    # overshoot caps. Warn (don't crash): multi-worker deploys that never use
    # report-back still boot fine. Checked here, not in the lifespan, because
    # uvicorn never exposes the worker count to the app.
    if args.workers > 1:
        logger.warning(
            "Starting with %d uvicorn workers: the concurrent-PTC report-back "
            "serialization (in-process dispatch caps + LINDEX/LREM queue drain) "
            "assumes a SINGLE worker and is not atomic across processes — it can "
            "double-drain report-backs or overshoot caps, and the /status BTM "
            "liveness cross-check can heal a run held by another worker to "
            "COMPLETED. Use 1 worker if this deployment relies on report-back.",
            args.workers,
        )

    # Uvicorn never exposes the worker count to the app, so hand it over via
    # env: the lifespan refuses --workers>1 when the WriterGuard fence cannot
    # activate (non-Postgres checkpointer or split app/checkpoint databases).
    os.environ["LANGALPHA_WORKERS"] = str(args.workers)

    try:
        logger.info(f"Starting server on {args.host}:{args.port}")
        uvicorn.run(
            "src.server.app:app",
            host=args.host,
            port=args.port,
            reload=reload,
            workers=args.workers,
            log_level=args.log_level,
            timeout_keep_alive=300,  # 5 minutes - for long-running workflows
            timeout_graceful_shutdown=60,  # 60 seconds for graceful shutdown
        )
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        exit(1)
