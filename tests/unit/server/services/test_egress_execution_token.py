"""The execution token: one call, one set of arguments, one short window.

Everything the token signs is recomputed by the relay from the frame and the
ledger row, so the tests here are the ways that recomputation must fail: a
different call, a different tool, different arguments, a tampered signature, a
foreign secret, and an expiry that has passed.
"""

from __future__ import annotations

import pytest

from src.server.services.egress.execution_token import (
    DEFAULT_TTL_SECONDS,
    EXECUTION_HEADER,
    ExecutionTokenError,
    mint_execution_token,
    parse_execution_token,
    verify_execution_token,
)

SECRET = "unit-test-relay-secret-0000000000"
OTHER_SECRET = "unit-test-relay-secret-1111111111"

ATTEMPT_ID = "11111111-2222-3333-4444-555555555555"
TOOL_CALL_ID = "call_exec_unit_0001"
TOOL = "sim_trade_input_order"
ARGS_SHA = "a" * 64
NOW = 1_800_000_000


def _mint(**overrides) -> str:
    kwargs = {
        "attempt_id": ATTEMPT_ID,
        "tool_call_id": TOOL_CALL_ID,
        "tool": TOOL,
        "args_sha256": ARGS_SHA,
        "now": NOW,
    }
    kwargs.update(overrides)
    return mint_execution_token(SECRET, **kwargs)


def _verify(token: str, **overrides):
    kwargs = {
        "tool_call_id": TOOL_CALL_ID,
        "tool": TOOL,
        "args_sha256": ARGS_SHA,
        "now": NOW,
    }
    kwargs.update(overrides)
    return verify_execution_token(SECRET, token, **kwargs)


def test_the_header_name_is_the_one_the_allowlist_must_never_carry():
    # Spelled here so a rename has to be deliberate: the relay's request
    # allowlist keeps this header from reaching the vendor by not naming it.
    assert EXECUTION_HEADER == "X-Relay-Execution"


def test_round_trip():
    token = _mint()
    parsed = _verify(token)

    assert parsed.attempt_id == ATTEMPT_ID
    assert parsed.expires_at == NOW + DEFAULT_TTL_SECONDS
    assert token.split(".")[0] == ATTEMPT_ID


def test_the_attempt_id_is_readable_before_the_token_is_trusted():
    # The relay needs the row to learn the tool_call_id the MAC covers, so the
    # id has to be parseable without the secret.
    assert parse_execution_token(_mint()).attempt_id == ATTEMPT_ID


@pytest.mark.parametrize(
    "token",
    ["", "   ", "no-dots", f"{ATTEMPT_ID}.9999", f"{ATTEMPT_ID}..sig", "a.b.c.d"],
)
def test_malformed_tokens_are_refused(token):
    with pytest.raises(ExecutionTokenError):
        parse_execution_token(token)


def test_a_non_numeric_expiry_is_refused():
    with pytest.raises(ExecutionTokenError):
        parse_execution_token(f"{ATTEMPT_ID}.soon.sig")


@pytest.mark.parametrize(
    "attempt_id",
    ["not-a-uuid", f"urn:uuid:{ATTEMPT_ID}", f"{{{ATTEMPT_ID}}}", ATTEMPT_ID.replace("-", "")],
)
def test_an_attempt_id_not_in_the_minted_form_is_refused(attempt_id):
    # The relay reads the row by this id before the MAC is checked, so a string
    # the uuid column cannot take would surface as a 500, not a refusal.
    with pytest.raises(ExecutionTokenError):
        parse_execution_token(f"{attempt_id}.{NOW}.sig")


def test_a_tampered_signature_is_refused():
    attempt_id, expires, signature = _mint().split(".")
    flipped = ("B" if signature[0] != "B" else "C") + signature[1:]
    with pytest.raises(ExecutionTokenError):
        _verify(f"{attempt_id}.{expires}.{flipped}")


def test_a_stretched_expiry_is_refused():
    attempt_id, expires, signature = _mint().split(".")
    with pytest.raises(ExecutionTokenError):
        _verify(f"{attempt_id}.{int(expires) + 3600}.{signature}", now=NOW + 3600)


def test_another_secret_cannot_mint_one():
    foreign = mint_execution_token(
        OTHER_SECRET,
        attempt_id=ATTEMPT_ID,
        tool_call_id=TOOL_CALL_ID,
        tool=TOOL,
        args_sha256=ARGS_SHA,
        now=NOW,
    )
    with pytest.raises(ExecutionTokenError):
        _verify(foreign)


def test_an_expired_token_is_refused():
    token = _mint()
    with pytest.raises(ExecutionTokenError, match="expired"):
        _verify(token, now=NOW + DEFAULT_TTL_SECONDS + 1)


def test_it_is_live_up_to_its_expiry():
    assert _verify(_mint(), now=NOW + DEFAULT_TTL_SECONDS)


def test_different_arguments_are_refused():
    with pytest.raises(ExecutionTokenError, match="does not match"):
        _verify(_mint(), args_sha256="b" * 64)


def test_a_token_cannot_be_moved_onto_another_call():
    with pytest.raises(ExecutionTokenError):
        _verify(_mint(), tool_call_id="call_exec_unit_0002")


def test_a_token_cannot_be_moved_onto_another_tool():
    with pytest.raises(ExecutionTokenError):
        _verify(_mint(), tool="trading_order_place")


def test_missing_arguments_do_not_collapse_onto_the_empty_hash():
    # A frame with no arguments hashes to something; a token minted for one
    # with arguments must not verify against it.
    empty = mint_execution_token(
        SECRET,
        attempt_id=ATTEMPT_ID,
        tool_call_id=TOOL_CALL_ID,
        tool=TOOL,
        args_sha256=None,
        now=NOW,
    )
    with pytest.raises(ExecutionTokenError):
        _verify(empty)


def test_without_a_relay_secret_nothing_mints_or_verifies():
    with pytest.raises(ExecutionTokenError, match="not configured"):
        mint_execution_token(
            "",
            attempt_id=ATTEMPT_ID,
            tool_call_id=TOOL_CALL_ID,
            tool=TOOL,
            args_sha256=ARGS_SHA,
        )
    with pytest.raises(ExecutionTokenError, match="not configured"):
        verify_execution_token(
            "",
            _mint(),
            tool_call_id=TOOL_CALL_ID,
            tool=TOOL,
            args_sha256=ARGS_SHA,
            now=NOW,
        )
