"""
Server script
"""

import argparse
import contextlib
import logging
import sys
import tempfile
import uvicorn

logger = logging.getLogger(__name__)


def listen_url(host: str, port: int) -> str | None:
    """The URL this process reaches itself at when bound to *host*:*port*, or None for port 0.

    A wildcard bind is reached on loopback; any other bind on its own address.
    """
    if not port:
        return None
    if host in ("", "0.0.0.0"):
        host = "127.0.0.1"
    elif host == "::":
        host = "::1"
    if ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{port}"

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
    args.workers = max(1, args.workers)

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

    # Uvicorn never exposes the worker count to the app, so hand it over via
    # env: the lifespan refuses --workers>1 when the WriterGuard fence cannot
    # activate (non-Postgres checkpointer or split app/checkpoint databases).
    # That hard gate is the whole multi-worker admission story — the old
    # single-worker warning's mechanisms (in-process report-back caps/drain,
    # in-memory compaction guard, BTM liveness healing) are all distributed
    # or deleted as of v4 Phase 2 (accepted residual: same-pair dispatch
    # lifecycle overlap across processes, gen-CAS-bounded last-writer-wins).
    os.environ["LANGALPHA_WORKERS"] = str(args.workers)
    # The address this process's calls to itself dial when SERVER_LOOPBACK_URL
    # is unset: its own listener, under a name of its own so an explicit
    # setting always wins.
    if listen := listen_url(args.host, args.port):
        os.environ["LANGALPHA_LISTEN_URL"] = listen
    else:
        os.environ.pop("LANGALPHA_LISTEN_URL", None)
    # Prompt templates are re-read on change only where the code reloads too:
    # checking each one's mtime on every render costs a stat per template
    # per turn on the event loop.
    os.environ["LANGALPHA_RELOAD"] = "1" if reload else "0"
    # edgartools rate-limits SEC requests per process at 9/s, under SEC's 10/s
    # per client, so N workers share that budget rather than multiply it. Its
    # rate is a whole number per second, so past 9 workers the floor of 1/s
    # each adds up to more than SEC allows.
    os.environ.setdefault("EDGAR_RATE_LIMIT_PER_SEC", str(max(1, 9 // args.workers)))
    if args.workers > 9 and os.environ["EDGAR_RATE_LIMIT_PER_SEC"] == "1":
        logger.warning(f"{args.workers} workers at 1 SEC request/s each exceed SEC's 10/s per client")
    # uvloop resolves host names on libuv's thread pool, 4 threads unless this
    # says otherwise, so a few lookups stuck on an unanswering resolver would
    # hold every connect in the worker. Sized like asyncio's own executor, which
    # did the lookups before.
    os.environ.setdefault("UV_THREADPOOL_SIZE", str(min(32, (os.process_cpu_count() or 1) + 4)))

    # A setting still under a renamed name fails here, in the supervisor, not in
    # each worker it would start again and again.
    import src.config.env  # noqa: F401

    # A worker whose startup is refused before any worker started writes why
    # here and stops this supervisor, which would otherwise replace it for ever
    # (see the lifespan in src/server/app/setup.py); the server then exits as one
    # worker would. --reload runs one worker whatever --workers says, and on
    # Windows a signal to the supervisor kills it outright.
    refusals = None
    if args.workers > 1 and not reload and os.name == "posix":
        fd, refusals = tempfile.mkstemp(prefix="langalpha-startup-refusals-")
        os.close(fd)
        os.environ["LANGALPHA_STARTUP_REFUSALS"] = refusals

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
            # TCP_NODELAY on every connection; see the module.
            http="src.server.http_protocol:NoDelayHTTPProtocol",
            # How long open streams may hold shutdown. The lifespan's shutdown
            # runs only after, so whoever kills this process must wait longer.
            timeout_graceful_shutdown=int(
                os.environ.get("LANGALPHA_GRACEFUL_SHUTDOWN_SECONDS", "60")
            ),
        )
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        exit(1)
    finally:
        reason = ""
        if refusals:
            with contextlib.suppress(FileNotFoundError), open(refusals) as f:
                reason = f.read().strip()
            for path in (refusals, f"{refusals}.ready"):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(path)
    if reason:
        logger.error(f"A worker refused to start, so the server stops: {reason}")
        sys.exit(3)
