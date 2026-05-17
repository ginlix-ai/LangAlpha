"""DB-backed ObservableCounter callbacks for LLM token + cost metrics.

Sync psycopg connection (reused, lazy) because OTel callbacks are sync; DB
failures swallow to debug-log so a transient blip doesn't break export.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Iterable

from opentelemetry.metrics import CallbackOptions, Observation

logger = logging.getLogger(__name__)


_conn = None  # lazy module-level sync psycopg connection
_conn_lock = threading.Lock()  # callbacks fire from the OTel exporter thread


def _connect():
    """Open a sync psycopg connection from DB_* env vars (autocommit).

    Uses kwargs (not a DSN string) so passwords containing spaces, '=', or
    backslashes don't silently break libpq parsing.
    """
    import psycopg

    return psycopg.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD") or None,
        autocommit=True,
    )


def _get_conn():
    """Return the shared sync connection, opening or reopening as needed.

    The lock prevents two concurrent callbacks both seeing ``_conn is None``
    and leaking a connection.
    """
    global _conn
    with _conn_lock:
        if _conn is None or _conn.closed:
            _conn = _connect()
        return _conn


def _reset_conn() -> None:
    """Drop the cached connection so the next callback reopens it."""
    global _conn
    with _conn_lock:
        _conn = None


# input_tokens in the source JSONB already includes the cached portion
# (OpenAI/Anthropic convention) — subtract cached so input + cached + output
# don't double-count.
_TOKENS_QUERY = """
SELECT
  m.key AS model,
  CASE WHEN is_byok THEN 'byok' ELSE 'platform' END AS billing_type,
  COALESCE(SUM(GREATEST(
    0,
    (m.value->>'input_tokens')::bigint
      - COALESCE((m.value->>'cached_tokens')::bigint, 0)
  )), 0) AS input_fresh,
  COALESCE(SUM(COALESCE((m.value->>'cached_tokens')::bigint, 0)), 0) AS cached,
  COALESCE(SUM(COALESCE((m.value->>'output_tokens')::bigint, 0)), 0) AS output
FROM conversation_usages,
     jsonb_each(token_usage->'by_model') AS m
WHERE token_usage IS NOT NULL
  AND token_usage ? 'by_model'
GROUP BY m.key, is_byok
"""


def llm_tokens_observe(options: CallbackOptions) -> Iterable[Observation]:
    """Yield one Observation per (model, billing_type, kind) tuple."""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(_TOKENS_QUERY)
            rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.debug("llm_tokens_observe DB read failed: %s", exc)
        _reset_conn()
        return []

    out: list[Observation] = []
    for model, billing_type, input_fresh, cached, output in rows:
        base = {"model": model or "unknown", "billing_type": billing_type}
        if input_fresh:
            out.append(Observation(int(input_fresh), {**base, "kind": "input"}))
        if cached:
            out.append(Observation(int(cached), {**base, "kind": "cached"}))
        if output:
            out.append(Observation(int(output), {**base, "kind": "output"}))
    return out


# Credits by billing_type + kind. token_credits applies only to platform-billed
# rows (BYOK keys pay externally); infrastructure_credits applies to both.
_CREDITS_QUERY = """
SELECT
  CASE WHEN is_byok THEN 'byok' ELSE 'platform' END AS billing_type,
  'token' AS kind,
  COALESCE(SUM(token_credits), 0)::float AS credits
FROM conversation_usages
GROUP BY is_byok
UNION ALL
SELECT
  CASE WHEN is_byok THEN 'byok' ELSE 'platform' END AS billing_type,
  'infrastructure' AS kind,
  COALESCE(SUM(infrastructure_credits), 0)::float AS credits
FROM conversation_usages
GROUP BY is_byok
"""


def credits_observe(options: CallbackOptions) -> Iterable[Observation]:
    """Yield one Observation per (billing_type, kind) tuple."""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(_CREDITS_QUERY)
            rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.debug("credits_observe DB read failed: %s", exc)
        _reset_conn()
        return []

    out: list[Observation] = []
    for billing_type, kind, credits in rows:
        if credits and float(credits) > 0:
            out.append(Observation(float(credits), {"billing_type": billing_type, "kind": kind}))
    return out
