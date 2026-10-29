"""
Utility endpoints for TTS, podcast, RAG, config, health checks, and WebSocket.

This module handles miscellaneous endpoints:
- Text-to-speech conversion
- Podcast generation
- RAG configuration and resources
- Server configuration
- WebSocket chat
- Health checks
- Custom metrics
"""

import logging
from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
INTERNAL_SERVER_ERROR_DETAIL = "Internal Server Error"

# Create router (health checks are unversioned at /health)
health_router = APIRouter(tags=["Health"])

@health_router.get("/health")
async def health_check():
    """Health check endpoint with checkpointer pool stats.

    A built-in remote MCP server that failed boot discovery makes this a 503,
    not only a body field: readiness probes read the status code, and this
    process serves without that server's tools until its background reconnect
    gets through. Each worker discovers and reconnects on its own, so the
    answer is this worker's.
    """
    from ptc_agent.core.mcp_registry import get_global_registry
    from src.server.app.setup import checkpointer
    from src.server.utils.checkpointer import get_checkpointer_health

    result = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "0.1.0",
        "service": "ptc-agent",
    }

    # Include checkpointer pool health if configured
    try:
        checkpointer_health = await get_checkpointer_health(checkpointer)
        result["checkpointer"] = checkpointer_health
        if checkpointer_health.get("status") == "unhealthy":
            result["status"] = "degraded"
    except Exception as e:
        result["checkpointer"] = {"status": "error", "error": str(e)}
        result["status"] = "degraded"

    registry = get_global_registry()
    if registry is not None and registry.failed_builtin_remote:
        result["mcp"] = {
            "status": "degraded",
            "failed_servers": sorted(registry.failed_builtin_remote),
        }
        result["status"] = "unhealthy"
        return JSONResponse(result, status_code=503)

    return result