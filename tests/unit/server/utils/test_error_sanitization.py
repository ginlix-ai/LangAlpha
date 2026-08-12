"""What ``sanitize_error_text`` must scrub before exception text is delivered."""

from __future__ import annotations

import pytest

from src.server.utils.error_sanitization import sanitize_error_text


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://svc:hunter2@db.example:5432/app",
        "redis://default:s3cret@cache.example:6379/0",
        "amqp://guest:guest@broker.example:5672/",
    ],
)
def test_a_connection_dsn_never_carries_its_password_through(dsn: str) -> None:
    """The exceptions this scrubber exists for are connection failures, and
    those quote a DSN rather than an http(s) URL."""
    scrubbed = sanitize_error_text(f"connection failed: {dsn}")

    assert "hunter2" not in scrubbed
    assert "s3cret" not in scrubbed
    assert ":guest@" not in scrubbed
    # The host survives — it is the diagnostic, and the credential is not.
    assert ".example" in scrubbed


def test_the_scheme_survives_so_the_message_still_reads() -> None:
    assert sanitize_error_text("GET https://u:p@api.example/v1 failed") == (
        "GET https://api.example/v1 failed"
    )


def test_ordinary_text_with_an_at_sign_is_left_alone() -> None:
    """Only userinfo directly after a scheme is credential-shaped; an address
    or a decorator in a traceback is not."""
    text = "raised in @retry wrapper; contact ops@example.com"

    assert sanitize_error_text(text) == text
