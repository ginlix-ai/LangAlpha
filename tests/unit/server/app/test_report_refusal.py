"""What every report route answers when the platform did not take a call; the
web reads the status and ``detail.error`` to word the refusal."""

from __future__ import annotations

import json

import pytest

from src.server.app.reports import report_refusal
from src.server.services.reports import PlatformResult


@pytest.mark.parametrize(
    ("status", "code", "error"),
    [
        ("rate_limited", 429, "report_rate_limited"),
        ("unavailable", 503, "report_unavailable"),
        ("closed", 409, "report_closed"),
        ("not_found", 409, "report_closed"),
        ("note_limit", 409, "report_note_limit"),
    ],
)
def test_each_refusal_maps_to_its_status_and_error(status, code, error):
    resp = report_refusal(PlatformResult(status))
    assert resp.status_code == code
    assert json.loads(resp.body)["detail"]["error"] == error


def test_a_wait_the_platform_named_is_passed_on():
    resp = report_refusal(PlatformResult("rate_limited", retry_after=30))
    assert resp.headers["retry-after"] == "30"
    assert json.loads(resp.body)["detail"]["retry_after"] == 30


def test_no_wait_named_sends_no_header():
    assert "retry-after" not in report_refusal(PlatformResult("unavailable")).headers


def test_a_failed_turns_report_needs_an_error_type():
    from pydantic import ValidationError

    from src.server.models.report import ReportError

    with pytest.raises(ValidationError):
        ReportError(type="", message="x")
