"""The base classifier must read ``FileNotFoundError`` as absence.

``DaytonaProvider`` is the only provider whose SDK reports a machine-readable
``FILE_NOT_FOUND``; every other provider raises Python's ``FileNotFoundError``
and overrides ``classify_error`` not at all. Without a type-based absence signal
on the base, an ordinary missing file falls through to ``UNKNOWN``, the liveness
probe finds the sandbox perfectly alive, and the caller is handed a transient —
so "this file isn't there" reaches the user as a 503 instead of a 404.

Exercised against ``DockerProvider`` rather than a stub because it is the
shipping provider that inherits the default, and the ordering below is only
load-bearing given its real message-scanning ``is_transient_error``.

``DaytonaProvider`` overrides ``classify_error`` outright rather than reusing
that ordering, so the base tests say nothing about it and it is pinned
separately at the bottom of this module.
"""

from urllib.error import HTTPError

from daytona.common.errors import DaytonaNotFoundError

from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
from ptc_agent.core.sandbox.providers.docker import DockerProvider
from ptc_agent.core.sandbox.runtime import SandboxFailureKind


def _provider() -> DockerProvider:
    """A provider bypassing ``__init__``.

    Both methods under test read only the exception, so the docker client and
    config a real one needs would be dead weight.
    """
    return DockerProvider.__new__(DockerProvider)


def test_file_not_found_is_absence() -> None:
    assert (
        _provider().classify_error(FileNotFoundError("File not found: /data/report.csv"))
        is SandboxFailureKind.PATH_ABSENT
    )


def test_absence_outranks_the_transient_message_scan() -> None:
    """A filename is not a diagnosis.

    ``is_transient_error`` matches "connection" anywhere in the message, so
    checking it first would classify a file whose own path contains the word as
    a connection fault — and that file could never be reported missing.
    """
    assert (
        _provider().classify_error(
            FileNotFoundError("File not found: /data/connection.log")
        )
        is SandboxFailureKind.PATH_ABSENT
    )


def test_a_real_transient_still_classifies_as_transient() -> None:
    assert (
        _provider().classify_error(RuntimeError("connection refused"))
        is SandboxFailureKind.TRANSIENT
    )


def test_an_unclassifiable_failure_stays_unknown() -> None:
    """``UNKNOWN`` is the honest answer, not a synonym for absent.

    It routes to the liveness probe; collapsing it into ``PATH_ABSENT`` is the
    conflation that reported live files as deleted.
    """
    assert (
        _provider().classify_error(RuntimeError("something we have never seen"))
        is SandboxFailureKind.UNKNOWN
    )


def _daytona() -> DaytonaProvider:
    """A provider bypassing ``__init__`` — ``classify_error`` reads only *exc*."""
    return DaytonaProvider.__new__(DaytonaProvider)


def _miss(path: str) -> DaytonaNotFoundError:
    """A per-path miss shaped exactly as the live API returns it.

    Message and metadata transcribed from a probe against SDK 0.200.1 (the
    field was renamed ``error_code`` -> ``code`` in 0.201): the
    server echoes the requested path into the message, which is precisely what
    makes the message scan dangerous here.
    """
    return DaytonaNotFoundError(
        f"Failed to download file: file not found: {path}",
        status_code=404,
        code="FILE_NOT_FOUND",
    )


def test_daytona_file_not_found_is_absence() -> None:
    assert (
        _daytona().classify_error(_miss("/home/workspace/report.csv"))
        is SandboxFailureKind.PATH_ABSENT
    )


def test_daytona_absence_outranks_the_transient_message_scan() -> None:
    """The override had this backwards, and the path is in the message.

    ``is_transient_daytona_error`` matches "timeout" as a bare substring, so
    scanning first made a missing ``session_timeout.log`` a transient: a plain
    404 surfaced as a 503 retry card for a file that was never coming back.
    """
    exc = _miss("/home/workspace/session_timeout.log")

    from ptc_agent.core.sandbox.providers.daytona_secrets import (
        is_transient_daytona_error,
    )

    # The scan really does fire; only the ordering keeps it from deciding.
    assert is_transient_daytona_error(exc) is True
    assert _daytona().classify_error(exc) is SandboxFailureKind.PATH_ABSENT


def test_daytona_a_real_transient_still_classifies_as_transient() -> None:
    """Carrying no error code, it must still reach the scan."""
    assert (
        _daytona().classify_error(RuntimeError("Read timed out"))
        is SandboxFailureKind.TRANSIENT
    )


def test_daytona_a_missing_sandbox_is_still_gone() -> None:
    """A sandbox-level 404 keeps authorizing replacement; only files moved."""
    assert (
        _daytona().classify_error(
            DaytonaNotFoundError(
                "Sandbox not found", status_code=404, code="NOT_FOUND"
            )
        )
        is SandboxFailureKind.SANDBOX_GONE
    )


def test_daytona_an_int_code_on_a_wrapper_does_not_hide_the_sdk_string() -> None:
    """``code`` is a common attribute name, and the outermost one is not the SDK's.

    A transport error raised over the SDK's carries its own ``code``, an
    integer: ``HTTPError`` is 404 the number. Stopping the chain walk at the
    first exception that merely has the attribute loses ``FILE_NOT_FOUND``,
    and the 404 left behind routes to ``SANDBOX_GONE``, so a missing file
    authorizes destroying and replacing the sandbox it lives in.
    """
    from ptc_agent.core.sandbox.providers.daytona_secrets import daytona_error_code

    wrapped = HTTPError("http://daytona/files", 404, "Not Found", None, None)
    wrapped.__cause__ = _miss("/home/workspace/report.csv")

    assert daytona_error_code(wrapped) == "FILE_NOT_FOUND"
    assert _daytona().classify_error(wrapped) is SandboxFailureKind.PATH_ABSENT
