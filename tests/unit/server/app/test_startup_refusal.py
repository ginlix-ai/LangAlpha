"""A worker refused at launch stops the multi-worker supervisor; one refused
once others serve is left to uvicorn to start again."""

import os
import signal
from unittest import mock

from src.server.app.setup import _stop_supervisor


def test_a_refusal_before_any_worker_started_stops_the_supervisor(tmp_path, monkeypatch):
    refusals = tmp_path / "refusals"
    refusals.touch()
    monkeypatch.setenv("LANGALPHA_STARTUP_REFUSALS", str(refusals))
    with mock.patch("src.server.app.setup.os.kill") as kill:
        _stop_supervisor(RuntimeError("SERVER_SANDBOX_URL is not set"))
    kill.assert_called_once_with(os.getppid(), signal.SIGTERM)
    assert refusals.read_text() == "RuntimeError: SERVER_SANDBOX_URL is not set\n"


def test_a_refusal_once_a_worker_started_is_left_to_uvicorn(tmp_path, monkeypatch):
    refusals = tmp_path / "refusals"
    refusals.touch()
    (tmp_path / "refusals.ready").touch()
    monkeypatch.setenv("LANGALPHA_STARTUP_REFUSALS", str(refusals))
    with mock.patch("src.server.app.setup.os.kill") as kill:
        _stop_supervisor(TimeoutError("pool timeout"))
    kill.assert_not_called()
    assert refusals.read_text() == ""


def test_a_single_worker_has_no_supervisor_to_stop(monkeypatch):
    monkeypatch.delenv("LANGALPHA_STARTUP_REFUSALS", raising=False)
    with mock.patch("src.server.app.setup.os.kill") as kill:
        _stop_supervisor(RuntimeError("refused"))
    kill.assert_not_called()
