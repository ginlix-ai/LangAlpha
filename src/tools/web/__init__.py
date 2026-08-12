"""Shared web provider protocol: search / fetch / crawl across providers.

Normalized response and error types live in ``types``; the provider ×
capability × level manifest loader lives in ``manifest``; provider adapters
live under ``providers/``; the search dispatcher lives in ``search``, the
WebFetch tool + FetchService in ``fetch``, and the WebCrawl/WebMap tool
factory in ``crawl``. The zero-key in-house fetch engine lives under
``inhouse/`` and the shared circuit breaker in ``breaker``. Agent-facing tool
schemas stay provider-native — only responses and errors are normalized
(decision 2026-07-08).
"""
