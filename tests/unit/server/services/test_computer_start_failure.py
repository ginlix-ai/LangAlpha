"""What a failed start leaves on its computer for the owner to act on.

The server decides the repair, so the client never reads a code it does not
know as a reinstall; and the message is one the owner may read, never a
provider's own text.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, SSHFolderMarked
from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError
from src.server.services import computer_start_failure as sf

OWNER = "3cd2aabcfce04ecf83f69a2e2952866c"


@pytest.mark.parametrize(
    ("code", "kind", "remedy"),
    [
        ("folder_marked", "ssh", "reinstall"),
        ("root_dir", "ssh", "reinstall"),
        ("not_connected", "ssh", "reconnect"),
        ("auth_failed", "ssh", "reconnect"),
        ("host_key_mismatch", "ssh", "reconnect"),
        ("unreachable", "ssh", "retry"),
        ("gateway_unreachable", "ssh", "retry"),
        ("start_failed", "ssh", "retry"),
        # Only a machine its owner brought can be reinstalled or paired again.
        ("root_dir", "daytona", "retry"),
        ("auth_failed", "docker", "retry"),
    ],
)
def test_the_server_chooses_the_repair(code, kind, remedy):
    assert sf.remedy_for(code, kind=kind) == remedy


def test_a_marked_folder_names_the_computer_whose_mark_it_is():
    exc = SSHFolderMarked("/home/dev/langalpha on box is the folder of ...", OWNER)

    failure = sf.describe_start_failure(exc, kind="ssh")

    assert failure == {
        "code": "folder_marked",
        "message": str(exc),
        "owner": OWNER,
        "remedy": "reinstall",
    }


def test_a_machine_that_did_not_answer_is_found_behind_its_wrapper():
    """A start names an unanswered machine as a host outage, with the SSH
    failure chained; the code is the SSH one."""
    cause = SSHConnectError("not_connected", "The machine's tunnel is not connected.")
    try:
        try:
            raise cause
        except SSHConnectError as e:
            raise SandboxHostUnavailableError("abc", e.for_owner) from e
    except SandboxHostUnavailableError as wrapped:
        failure = sf.describe_start_failure(wrapped, kind="ssh")

    assert failure["code"] == "not_connected"
    assert failure["message"] == cause.for_owner
    assert failure["remedy"] == "reconnect"
    assert failure["owner"] is None


def test_any_other_failure_never_shows_the_providers_text():
    exc = RuntimeError("POST https://admin:hunter2@api.vendor/sandboxes/sbx-1 500")

    failure = sf.describe_start_failure(exc, kind="daytona")

    assert failure["code"] == "start_failed"
    assert failure["remedy"] == "retry"
    assert "hunter2" not in failure["message"]
    assert "sbx-1" not in failure["message"]


@pytest.mark.asyncio
async def test_a_write_that_fails_never_replaces_the_starts_own_error():
    with patch.object(
        sf, "record_computer_start_failure", AsyncMock(side_effect=OSError("db down"))
    ) as write:
        await sf.record_start_failure("c-1", RuntimeError("boom"), kind="ssh")

    write.assert_awaited_once()


@pytest.mark.asyncio
async def test_nothing_is_written_for_a_project_on_no_computer():
    with patch.object(sf, "record_computer_start_failure", AsyncMock()) as write:
        await sf.record_start_failure(None, RuntimeError("boom"), kind="ssh")

    write.assert_not_awaited()


def test_a_row_that_does_not_parse_reads_as_none():
    assert sf.start_failure_from_row({"start_failure": None}) is None
    assert sf.start_failure_from_row({"start_failure": {"code": "x"}}) is None
    parsed = sf.start_failure_from_row(
        {
            "start_failure": {
                "code": "folder_marked",
                "message": "m",
                "owner": OWNER,
                "remedy": "reinstall",
                "at": "2026-10-25T16:33:00+00:00",
            }
        }
    )
    assert parsed is not None and parsed.owner == OWNER


def test_every_computer_response_carries_it(sample_computer_dict):
    from src.server.app.computers import computer_to_response

    row = sample_computer_dict(
        kind="ssh",
        status="stopped",
        start_failure={
            "code": "root_dir",
            "message": "Choose a new or empty folder.",
            "owner": None,
            "remedy": "reinstall",
            "at": "2026-10-25T16:33:00+00:00",
        },
    )

    body = computer_to_response(row).model_dump(mode="json")

    assert body["start_failure"]["code"] == "root_dir"
    assert body["start_failure"]["remedy"] == "reinstall"
    assert body["start_failure"]["at"].startswith("2026-10-25T16:33:00")
    assert computer_to_response({**row, "start_failure": None}).start_failure is None
