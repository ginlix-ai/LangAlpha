"""Bootstrap tests for ``init_otel()``.

Covers:
- No-op path when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is unset.
- Init survives a broken contrib instrumentor (via try/except wrap).
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk", reason="observability extra not installed")

from unittest.mock import patch  # noqa: E402



def test_init_otel_noop_when_env_unset(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    from src.observability import otel

    # Force reinit by clearing the phase-1 singleton flag.
    otel._classpatches_installed = False
    enabled = otel.init_otel()
    assert enabled is False


def test_init_otel_survives_broken_instrumentor(monkeypatch):
    """If a contrib instrumentor raises during install, init_otel must NOT
    propagate the exception. The app continues with no spans rather than failing
    to start."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    from src.observability import otel

    def boom():
        raise RuntimeError("simulated broken instrumentor")

    otel._classpatches_installed = False
    with patch.object(otel, "_install_classpatches", side_effect=boom):
        # Must not raise.
        result = otel.init_otel()
        assert result is True  # tried to enable, even though classpatches failed
