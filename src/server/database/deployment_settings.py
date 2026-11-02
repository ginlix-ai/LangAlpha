"""The settings a self-hosted deployment's setup step saves, encrypted at rest.

Postgres is the truth for these: each worker holds them only as its copy in
``os.environ``, re-read from here (see ``services/setup_settings``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

from psycopg.errors import ExternalRoutineInvocationException

from src.server.database.encryption import encryption_configured, get_encryption_key
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)


async def read_deployment_settings() -> dict[str, str]:
    """Every readable setting, by name.

    One row at a time, because a value encrypted under a key since replaced
    must cost only that value: the setup step can save it again, which it could
    not if the bad row failed every read.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT name FROM deployment_settings ORDER BY name")
            names = [row[0] for row in await cur.fetchall()]
            if not names:
                return {}
            if not encryption_configured():
                logger.warning(
                    "Saved deployment settings are ignored: BYOK_ENCRYPTION_KEY is not set"
                )
                return {}
            enc_key = get_encryption_key()
            settings: dict[str, str] = {}
            for name in names:
                try:
                    await cur.execute(
                        "SELECT pgp_sym_decrypt(value, %s) FROM deployment_settings "
                        "WHERE name = %s",
                        (enc_key, name),
                    )
                except ExternalRoutineInvocationException:
                    # pgcrypto's wrong key or corrupt data. Anything else, a
                    # dropped connection say, would leave the dict short and
                    # unset the rows it missed, so it fails the whole read.
                    logger.warning(
                        "Saved deployment setting %s does not decrypt; ignoring it", name
                    )
                    continue
                row = await cur.fetchone()
                if row:
                    settings[name] = row[0]
            return settings


async def write_deployment_settings(values: Mapping[str, str | None]) -> None:
    """Save the values and remove the names set to None, all or none of them."""
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                for name, value in values.items():
                    if value is None:
                        await cur.execute(
                            "DELETE FROM deployment_settings WHERE name = %s", (name,)
                        )
                        continue
                    await cur.execute(
                        """
                        INSERT INTO deployment_settings (name, value, updated_at)
                        VALUES (%s, pgp_sym_encrypt(%s, %s), NOW())
                        ON CONFLICT (name) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = NOW()
                        """,
                        (name, value, enc_key),
                    )


async def add_deployment_setting(name: str, value: str) -> None:
    """Save ``value`` unless the name is already saved, so workers that race to
    mint one all end up reading the same row."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO deployment_settings (name, value)
                VALUES (%s, pgp_sym_encrypt(%s, %s))
                ON CONFLICT (name) DO NOTHING
                """,
                (name, value, get_encryption_key()),
            )

