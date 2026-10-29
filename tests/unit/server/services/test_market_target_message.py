"""The <market-alert> block and the alert row's payload name a target as the
tool and the thread API do."""

from datetime import datetime, timezone

from src.server.services.market_targets.message import notice_payload, render_notice

# Made-up levels only.
_INDEX_TARGET = {
    "target_id": "tg-1",
    "symbol": "SPX",
    "market": "index",
    "kind": "pct_down",
    "value": 1.0,
    "pct_from": "previous_close",
    "armed_price": 6100.0,
    "fired_price": 6030.0,
    "fired_at": datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc),
    "expires_at": datetime(2026, 10, 13, 20, 0, tzinfo=timezone.utc),
}


def test_an_index_alert_uses_the_spelling_the_agent_set_it_with():
    block = render_notice(_INDEX_TARGET, "hit")
    assert "^GSPC hit your target (down 1% from the previous close)" in block
    assert "SPX" not in block
    assert notice_payload(_INDEX_TARGET, "hit")["symbol"] == "^GSPC"


def test_an_expiring_index_alert_names_it_the_same_way():
    block = render_notice(_INDEX_TARGET, "expiring", 6080.0)
    assert "Your target on ^GSPC (down 1% from the previous close) has not hit" in block


def test_a_stock_alert_keeps_its_symbol():
    stock = {**_INDEX_TARGET, "symbol": "NVDA", "market": "stock"}
    assert render_notice(stock, "hit").splitlines()[2].startswith("NVDA hit your target")
    assert notice_payload(stock, "hit")["symbol"] == "NVDA"
