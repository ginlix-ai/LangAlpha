"""``market_of``: the routing market every venue resolves to."""

import pytest

from market_protocol import market_of, to_canonical


@pytest.mark.parametrize(
    "symbol,market",
    [
        ("AAPL", "us"), ("MSFT", "us"), ("SPX", "us"),
        ("0700.HK", "hk"), ("^HSI", "hk"),
        ("600519.SS", "cn"), ("600519.SH", "cn"), ("000001.SZ", "cn"), ("830799.BJ", "cn"),
        ("VOD.L", "uk"), ("^FTSE", "uk"),
        ("7203.T", "jp"), ("^N225", "jp"),
        ("SHOP.TO", "ca"), ("BHP.AX", "au"),
        ("MC.PA", "eu"), ("SAP.DE", "eu"), ("ASML.AS", "eu"), ("ENI.MI", "eu"),
        ("SAN.MC", "eu"), ("NESN.SW", "eu"), ("^GDAXI", "eu"),
        ("005930.KS", "kr"), ("035720.KQ", "kr"),
        ("2330.TW", "tw"), ("D05.SI", "sg"),
        ("RELIANCE.NS", "in"), ("500325.BO", "in"),
    ],
)
def test_market_of_each_venue(symbol, market):
    assert market_of(to_canonical(symbol)) == market


def test_pairs_route_as_their_own_markets():
    assert market_of(to_canonical("BTC-USD.CRYPTO")) == "crypto"
    assert market_of(to_canonical("EURUSD=X")) == "fx"
    assert market_of(to_canonical("JPY=X")) == "fx"
