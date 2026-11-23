"""A setting that does not decrypt costs only itself; any other failure fails
the read, since a short result unsets the settings it left out."""

from __future__ import annotations

from contextlib import asynccontextmanager

import psycopg
import pytest
from psycopg.errors import ExternalRoutineInvocationException

from src.server.database import deployment_settings


class _Cursor:
    def __init__(self, fail: dict[str, Exception]):
        self._fail = fail
        self._result: list[tuple[str]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params=None):
        if params is None:
            self._result = [("A_KEY",), ("B_KEY",), ("C_KEY",)]
            return
        name = params[1]
        if name in self._fail:
            raise self._fail[name]
        self._result = [(f"value-of-{name}",)]

    async def fetchall(self):
        return self._result

    async def fetchone(self):
        return self._result[0]


def _read(monkeypatch, fail: dict[str, Exception]):
    class _Conn:
        def cursor(self):
            return _Cursor(fail)

    @asynccontextmanager
    async def _get_db_connection():
        yield _Conn()

    monkeypatch.setattr(deployment_settings, "get_db_connection", _get_db_connection)
    monkeypatch.setattr(deployment_settings, "encryption_configured", lambda: True)
    monkeypatch.setattr(deployment_settings, "get_encryption_key", lambda: "k")
    return deployment_settings.read_deployment_settings()


@pytest.mark.asyncio
async def test_a_row_that_does_not_decrypt_is_skipped(monkeypatch):
    settings = await _read(
        monkeypatch, {"B_KEY": ExternalRoutineInvocationException("Wrong key or corrupt data")}
    )
    assert settings == {"A_KEY": "value-of-A_KEY", "C_KEY": "value-of-C_KEY"}


@pytest.mark.asyncio
async def test_a_lost_connection_fails_the_read(monkeypatch):
    with pytest.raises(psycopg.OperationalError):
        await _read(monkeypatch, {"B_KEY": psycopg.OperationalError("connection lost")})
