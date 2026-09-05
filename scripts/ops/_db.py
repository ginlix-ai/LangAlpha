"""The Postgres URI the operator scripts under ``scripts/ops/`` connect with.

Read from the environment directly rather than importing ``src.config``: that
fires the app's ``load_dotenv()`` and would silently retarget a mutating script
at whatever ``.env`` happens to be on disk.
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def build_db_uri(prefix: str = "DB_") -> str:
    """Assemble ``<prefix>HOST``/``PORT``/``NAME``/``USER``/``PASSWORD`` into a URI.

    ``prefix`` picks the pool: the app data database is ``DB_``, the LangGraph
    checkpointer ``MEMORY_DB_``. TLS policy is one setting for the server, so
    ``DB_SSLMODE`` is read unprefixed either way.
    """
    host = os.getenv(f"{prefix}HOST", "localhost")
    port = os.getenv(f"{prefix}PORT", "5432")
    name = os.getenv(f"{prefix}NAME", "postgres")
    user = os.getenv(f"{prefix}USER", "postgres")
    password = os.getenv(f"{prefix}PASSWORD", "postgres")
    sslmode = os.getenv("DB_SSLMODE", "prefer")
    return (
        f"postgresql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{name}?sslmode={sslmode}"
    )
