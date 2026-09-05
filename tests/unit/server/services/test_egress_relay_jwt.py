"""Relay JWT mint/validate — the sandbox's only credential.

The validator is a security boundary, so the arms that matter are the
rejections: a token is accepted only when it is HS256-signed with our secret,
addressed to this relay, carries every identity claim, and is inside its
validity window (plus a small clock-skew leeway).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import jwt
import pytest

from src.server.services.egress.relay_jwt import (
    ALGORITHM,
    AUDIENCE,
    DEFAULT_TTL_SECONDS,
    ISSUER,
    LEEWAY_SECONDS,
    REMINT_THRESHOLD_SECONDS,
    RelayJwtError,
    mint_relay_jwt,
    needs_remint,
    validate_relay_jwt,
)

# Long enough that PyJWT does not warn about HMAC key length (>= 64 bytes,
# which also covers the HS512 algorithm-allowlist arm).
SECRET = "unit-test-relay-secret-000000000000000000000000000000000000000000000"
OTHER_SECRET = "unit-test-relay-secret-rotated-11111111111111111111111111111111111"

USER_ID = "u-relay-unit"
WORKSPACE_ID = "ws-relay-unit"
SANDBOX_ID = "sbx-relay-unit"


def _payload(**overrides) -> dict:
    now = int(time.time())
    payload = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": USER_ID,
        "workspace_id": WORKSPACE_ID,
        "sandbox_id": SANDBOX_ID,
        "iat": now,
        "nbf": now,
        "exp": now + 600,
        "jti": "jti-relay-unit",
    }
    payload.update(overrides)
    return payload


def _encode(payload: dict, secret: str = SECRET, algorithm: str = ALGORITHM) -> str:
    return jwt.encode(payload, secret, algorithm=algorithm)


def _segment(obj: dict) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _handcrafted(header: dict, payload: dict, *, signature: bytes = b"") -> str:
    """Assemble a token PyJWT would refuse to mint (alg confusion arms)."""
    signing_input = f"{_segment(header)}.{_segment(payload)}"
    sig = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{signing_input}.{sig}"


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_minted_token_validates_and_carries_the_identity_claims(self):
        token = mint_relay_jwt(
            SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        claims = validate_relay_jwt(SECRET, token)

        assert claims.user_id == USER_ID
        assert claims.workspace_id == WORKSPACE_ID
        assert claims.sandbox_id == SANDBOX_ID
        assert claims.jti
        assert claims.expires_at > int(time.time())

    def test_ttl_is_honored_and_each_mint_gets_a_fresh_jti(self):
        first = validate_relay_jwt(
            SECRET,
            mint_relay_jwt(
                SECRET,
                user_id=USER_ID,
                workspace_id=WORKSPACE_ID,
                sandbox_id=SANDBOX_ID,
                ttl_seconds=90,
            ).token,
        )
        second = validate_relay_jwt(
            SECRET,
            mint_relay_jwt(
                SECRET,
                user_id=USER_ID,
                workspace_id=WORKSPACE_ID,
                sandbox_id=SANDBOX_ID,
                ttl_seconds=90,
            ).token,
        )

        assert first.expires_at - int(time.time()) <= 90
        assert first.jti != second.jti

    def test_header_declares_the_fixed_algorithm(self):
        token = mint_relay_jwt(
            SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        assert jwt.get_unverified_header(token)["alg"] == "HS256"


# ---------------------------------------------------------------------------
# Signature / algorithm
# ---------------------------------------------------------------------------


class TestSignatureAndAlgorithm:
    def test_different_hs256_key_is_rejected(self):
        token = mint_relay_jwt(
            OTHER_SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_alg_none_token_is_rejected(self):
        token = _handcrafted({"alg": "none", "typ": "JWT"}, _payload())
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_rs256_header_over_an_hmac_signature_is_rejected(self):
        """Classic algorithm confusion: the allowlist is fixed, not read from
        the header, so a token claiming RS256 never reaches verification."""
        header = {"alg": "RS256", "typ": "JWT"}
        payload = _payload()
        signing_input = f"{_segment(header)}.{_segment(payload)}".encode("ascii")
        forged = hmac.new(
            SECRET.encode("utf-8"), signing_input, hashlib.sha256
        ).digest()
        token = _handcrafted(header, payload, signature=forged)

        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_other_hmac_variant_is_rejected(self):
        token = _encode(_payload(), algorithm="HS512")
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_tampered_signature_is_rejected(self):
        token = mint_relay_jwt(
            SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        head, body, sig = token.split(".")
        # Flip a middle character: every bit there is signature-significant,
        # whereas the final char's low bits are base64 padding the decoder
        # ignores (flipping only those yields the same 32 bytes back).
        flipped = "B" if sig[5] != "B" else "C"
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, f"{head}.{body}.{sig[:5]}{flipped}{sig[6:]}")

    def test_tampered_payload_invalidates_the_signature(self):
        token = mint_relay_jwt(
            SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        head, _, sig = token.split(".")
        swapped = _segment(_payload(workspace_id="ws-somebody-else"))
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, f"{head}.{swapped}.{sig}")

    def test_garbage_is_rejected(self):
        for token in ("", "not-a-jwt", "a.b.c"):
            with pytest.raises(RelayJwtError):
                validate_relay_jwt(SECRET, token)


# ---------------------------------------------------------------------------
# Audience / issuer / required claims
# ---------------------------------------------------------------------------


class TestClaimGating:
    def test_wrong_audience_is_rejected(self):
        token = _encode(_payload(aud="some-other-service"))
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_wrong_issuer_is_rejected(self):
        token = _encode(_payload(iss="not-langalpha"))
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    @pytest.mark.parametrize(
        "claim",
        ["iss", "aud", "sub", "workspace_id", "sandbox_id", "iat", "nbf", "exp", "jti"],
    )
    def test_every_required_claim_is_required(self, claim):
        payload = _payload()
        payload.pop(claim)
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, _encode(payload))

    @pytest.mark.parametrize(
        "overrides",
        [
            {"sub": ""},
            {"workspace_id": ""},
            {"sandbox_id": ""},
            {"jti": ""},
            {"workspace_id": 42},
            {"sandbox_id": None},
        ],
    )
    def test_identity_claims_must_be_non_empty_strings(self, overrides):
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, _encode(_payload(**overrides)))


# ---------------------------------------------------------------------------
# Validity window
# ---------------------------------------------------------------------------


class TestValidityWindow:
    def test_expired_token_is_rejected(self):
        now = int(time.time())
        token = _encode(
            _payload(iat=now - 7200, nbf=now - 7200, exp=now - LEEWAY_SECONDS - 60)
        )
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_future_nbf_beyond_leeway_is_rejected(self):
        now = int(time.time())
        token = _encode(
            _payload(iat=now, nbf=now + LEEWAY_SECONDS + 60, exp=now + 3600)
        )
        with pytest.raises(RelayJwtError):
            validate_relay_jwt(SECRET, token)

    def test_expiry_just_inside_the_skew_window_is_accepted(self):
        now = int(time.time())
        token = _encode(
            _payload(iat=now - 600, nbf=now - 600, exp=now - (LEEWAY_SECONDS - 10))
        )
        assert validate_relay_jwt(SECRET, token).user_id == USER_ID

    def test_nbf_just_inside_the_skew_window_is_accepted(self):
        now = int(time.time())
        token = _encode(
            _payload(iat=now, nbf=now + (LEEWAY_SECONDS - 10), exp=now + 3600)
        )
        assert validate_relay_jwt(SECRET, token).sandbox_id == SANDBOX_ID


# ---------------------------------------------------------------------------
# Remint threshold
# ---------------------------------------------------------------------------


class TestNeedsRemint:
    def test_fresh_token_does_not_need_a_remint(self):
        now = 1_700_000_000
        assert needs_remint(now + DEFAULT_TTL_SECONDS, now=now) is False

    def test_near_expiry_token_needs_a_remint(self):
        now = 1_700_000_000
        assert needs_remint(now + 600, now=now) is True

    def test_threshold_is_exclusive_at_the_boundary(self):
        now = 1_700_000_000
        assert needs_remint(now + REMINT_THRESHOLD_SECONDS, now=now) is False
        assert needs_remint(now + REMINT_THRESHOLD_SECONDS - 1, now=now) is True

    def test_already_expired_token_needs_a_remint(self):
        now = 1_700_000_000
        assert needs_remint(now - 1, now=now) is True

    def test_defaults_to_the_wall_clock(self):
        fresh = mint_relay_jwt(
            SECRET,
            user_id=USER_ID,
            workspace_id=WORKSPACE_ID,
            sandbox_id=SANDBOX_ID,
        ).token
        assert needs_remint(validate_relay_jwt(SECRET, fresh).expires_at) is False
