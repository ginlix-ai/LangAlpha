"""LLM domain services: request-time config resolution (config), model
availability filtering (availability), and the server-side completion
entrypoint (service). One domain, one shape — moved here from
handlers/chat and services/llm_service.py."""

import logging

# The hard-coded name request_prep uses: existing log routing keys off it, so
# every module in this package logs under it rather than under ``__name__``.
logger = logging.getLogger("src.server.handlers.chat_handler")
