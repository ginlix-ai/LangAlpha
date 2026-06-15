"""Provenance middleware — traces external data the agent reads.

Wraps tool calls and emits a custom ``provenance`` stream event per accessed
source (web result, fetched page, SEC filing, market datum, file/memo/memory
read, sandbox MCP call). See :class:`ProvenanceMiddleware`.
"""

from ptc_agent.agent.middleware.provenance.middleware import ProvenanceMiddleware

__all__ = ["ProvenanceMiddleware"]
