"""The batch profile read: one odd vendor row must not cost the other
listings of the batch their answers."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.cache import profile_cache_service as service

_NVDA = {"companyName": "NVIDIA Corporation", "sector": "Technology"}


async def _read(answers: dict[str, object]) -> tuple[service.Profiles, AsyncMock]:
    """``get_profiles`` over a cold cache and a vendor answering ``answers``."""
    cache = SimpleNamespace(
        mget=AsyncMock(return_value=[None] * len(answers)), set_many=AsyncMock()
    )
    financial = SimpleNamespace(
        get_company_profile=AsyncMock(side_effect=lambda listing: answers[listing])
    )

    async def names(listings: list[str]) -> dict[str, tuple[None, None]]:
        return {s: (None, None) for s in listings}

    with (
        patch.object(service, "get_cache_client", return_value=cache),
        patch.object(
            service,
            "get_financial_data_provider",
            AsyncMock(return_value=SimpleNamespace(financial=financial)),
        ),
        patch.object(service, "display_names_many", names),
    ):
        return await service.get_profiles(list(answers)), cache.set_many


@pytest.mark.asyncio
async def test_an_empty_vendor_row_is_a_listing_with_no_profile():
    profiles, saved = await _read({"NVDA": [_NVDA], "ODD": [{}], "NONE": []})

    assert profiles.failed == {}
    assert profiles.found["NVDA"]["name"] == "NVIDIA Corporation"
    assert profiles.found["ODD"] is None
    assert profiles.found["NONE"] is None
    # Each is remembered, the ones with no profile for the shorter time.
    ttl = {key: ttl for key, _value, ttl in saved.await_args.args[0]}
    assert ttl["profile:NVDA"] > ttl["profile:ODD"] == ttl["profile:NONE"]


@pytest.mark.asyncio
async def test_a_vendor_error_body_fails_its_listing_and_is_not_remembered():
    profiles, saved = await _read(
        {"NVDA": [_NVDA], "BAD": [{"Error Message": "Limit reached"}]}
    )

    assert set(profiles.found) == {"NVDA"}
    assert set(profiles.failed) == {"BAD"}
    assert [key for key, _value, _ttl in saved.await_args.args[0]] == ["profile:NVDA"]


@pytest.mark.asyncio
async def test_a_listing_whose_vendor_hangs_fails_alone_and_is_not_remembered(
    monkeypatch,
):
    monkeypatch.setattr(service, "_FETCH_TIMEOUT_S", 0.01)

    async def profile(listing: str) -> object:
        if listing == "SLOW":
            await asyncio.sleep(1)
        return [_NVDA]

    cache = SimpleNamespace(mget=AsyncMock(return_value=[None, None]), set_many=AsyncMock())
    financial = SimpleNamespace(get_company_profile=profile)

    async def names(listings: list[str]) -> dict[str, tuple[None, None]]:
        return {}

    with (
        patch.object(service, "get_cache_client", return_value=cache),
        patch.object(
            service,
            "get_financial_data_provider",
            AsyncMock(return_value=SimpleNamespace(financial=financial)),
        ),
        patch.object(service, "display_names_many", names),
    ):
        profiles = await service.get_profiles(["NVDA", "SLOW"])

    # The name lookup answering nothing for NVDA still leaves its profile.
    assert profiles.found["NVDA"]["name"] == "NVIDIA Corporation"
    assert set(profiles.failed) == {"SLOW"}
    assert [key for key, _v, _t in cache.set_many.await_args.args[0]] == ["profile:NVDA"]
