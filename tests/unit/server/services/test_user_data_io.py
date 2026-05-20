"""Pure-function tests for user_data_io.

Exercises serialize → parse → diff round-trips without touching the database.
The async fetch/count/apply functions need a live DB and live in the
integration suite (``tests/integration/test_user_data_backend.py``), which
requires Postgres.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.server.services import user_data_io as io
from src.server.services.user_data_io import UserDataValidationError


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------


def _portfolio_row(**overrides):
    row = {
        "user_portfolio_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "user-1",
        "symbol": "AAPL",
        "instrument_type": "stock",
        "exchange": "NASDAQ",
        "name": "Apple Inc.",
        "quantity": Decimal("100.0"),
        "average_cost": Decimal("150.25"),
        "currency": "USD",
        "account_name": "Main",
        "notes": "Long-term hold",
        "first_purchased_at": None,
        "metadata": {},
        "created_at": datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 17, 8, 34, 11, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


class TestPortfolioSerialize:
    def test_empty_state(self):
        payload = io.serialize_portfolio([])
        assert payload["holdings"] == []
        assert payload["__version__"].startswith("sha256:")

    def test_decimal_precision_preserved(self):
        row = _portfolio_row(quantity=Decimal("100.00012345"), average_cost=Decimal("150.2575"))
        payload = io.serialize_portfolio([row])
        # Serialized as strings to preserve precision through JSON
        assert payload["holdings"][0]["quantity"] == "100.00012345"
        assert payload["holdings"][0]["average_cost"] == "150.2575"

    def test_version_is_content_hash(self):
        """__version__ is sha256 of business content — not derived from updated_at."""
        row = _portfolio_row()
        payload = io.serialize_portfolio([row])
        assert payload["__version__"].startswith("sha256:")
        assert len(payload["__version__"]) == len("sha256:") + 16

    def test_id_field_never_exposed(self):
        """DB UUIDs are server-managed; the agent should never see them."""
        row = _portfolio_row()
        payload = io.serialize_portfolio([row])
        assert "id" not in payload["holdings"][0]


class TestPortfolioParseAndDiff:
    def _make_payload(self, version, holdings):
        import json
        return json.dumps({"__version__": version, "holdings": holdings})

    def test_parse_error_invalid_json(self):
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio("not json{", [])
        assert exc.value.error_type == "parse_error"

    def test_missing_version_accepted(self):
        """Agent JSON no longer carries __version__; the backend tracks the
        version server-side, so parsing a payload without __version__ must
        succeed."""
        diff = io.parse_and_diff_portfolio('{"holdings": []}', [])
        assert diff.is_empty()

    def test_holdings_must_be_array(self):
        payload = self._make_payload("v1", "not an array")
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.error_type == "schema_error"
        assert exc.value.field_path == "holdings"

    def test_insert_new_holding(self):
        payload = self._make_payload("v1", [{
            "symbol": "TSLA", "instrument_type": "stock",
            "quantity": "10", "account_name": "Main",
        }])
        diff = io.parse_and_diff_portfolio(payload, [])
        assert len(diff.inserts) == 1
        assert diff.inserts[0]["symbol"] == "TSLA"
        assert diff.inserts[0]["quantity"] == Decimal("10")
        assert diff.updates == []
        assert diff.deletes == []

    def test_update_existing_by_unique_key(self):
        existing = _portfolio_row()
        payload = self._make_payload("v1", [{
            "symbol": "AAPL", "instrument_type": "stock",
            "quantity": "200",  # changed
            "average_cost": "150.25", "account_name": "Main",
        }])
        diff = io.parse_and_diff_portfolio(payload, [existing])
        assert diff.inserts == []
        assert len(diff.updates) == 1
        # The DB UUID is internal — set by the applier on the update row, but
        # the agent never supplied it.
        assert diff.updates[0]["id"] == "11111111-1111-1111-1111-111111111111"
        assert diff.updates[0]["quantity"] == Decimal("200")
        assert diff.deletes == []

    def test_no_op_when_unchanged(self):
        existing = _portfolio_row()
        payload = self._make_payload("v1", [{
            "symbol": "AAPL", "instrument_type": "stock",
            "exchange": "NASDAQ", "name": "Apple Inc.",
            "quantity": "100.0", "average_cost": "150.25",
            "currency": "USD", "account_name": "Main",
            "notes": "Long-term hold",
        }])
        diff = io.parse_and_diff_portfolio(payload, [existing])
        assert diff.is_empty()

    def test_agent_supplied_id_is_silently_ignored(self):
        """If the agent writes back an `id` (e.g. from a stale cache or hallucinated),
        the parser ignores it. Matching falls back to the unique key."""
        existing = _portfolio_row()
        payload = self._make_payload("v1", [{
            "id": "deadbeef-dead-beef-dead-beefdeadbeef",  # not a real DB id
            "symbol": "AAPL", "instrument_type": "stock",
            "quantity": "200", "average_cost": "150.25", "account_name": "Main",
        }])
        diff = io.parse_and_diff_portfolio(payload, [existing])
        # The fake id was ignored; (symbol,type,account) still resolved to the
        # existing row, so this is an UPDATE not an INSERT.
        assert diff.inserts == []
        assert len(diff.updates) == 1
        assert diff.updates[0]["id"] == "11111111-1111-1111-1111-111111111111"

    def test_delete_missing_row(self):
        existing = _portfolio_row()
        payload = self._make_payload("v1", [])  # agent removed everything
        diff = io.parse_and_diff_portfolio(payload, [existing])
        assert diff.deletes == ["11111111-1111-1111-1111-111111111111"]
        assert diff.inserts == []
        assert diff.updates == []

    def test_match_by_unique_key(self):
        existing = _portfolio_row()
        payload = self._make_payload("v1", [{
            "symbol": "AAPL", "instrument_type": "stock", "account_name": "Main",
            "quantity": "300", "average_cost": "150.25",
        }])
        diff = io.parse_and_diff_portfolio(payload, [existing])
        # Should match by (symbol, instrument_type, account_name) and update
        assert diff.inserts == []
        assert len(diff.updates) == 1

    def test_invalid_quantity_type(self):
        payload = self._make_payload("v1", [{
            "symbol": "AAPL", "instrument_type": "stock",
            "quantity": ["not", "a", "number"],
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.error_type == "schema_error"
        assert "quantity" in exc.value.field_path


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------


def _watchlist_row(**overrides):
    row = {
        "watchlist_id": "22222222-2222-2222-2222-222222222222",
        "user_id": "user-1",
        "name": "Tech",
        "description": "Big tech",
        "is_default": False,
        "display_order": 0,
        "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 17, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def _watchlist_item(**overrides):
    item = {
        "watchlist_item_id": "33333333-3333-3333-3333-333333333333",
        "watchlist_id": "22222222-2222-2222-2222-222222222222",
        "user_id": "user-1",
        "symbol": "MSFT",
        "instrument_type": "stock",
        "exchange": "NASDAQ",
        "name": "Microsoft",
        "notes": "",
        "alert_settings": {},
        "metadata": {},
        "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 15, tzinfo=timezone.utc),
    }
    item.update(overrides)
    return item


class TestWatchlistSerialize:
    def test_empty(self):
        payload = io.serialize_watchlist([], {})
        assert payload["watchlists"] == []

    def test_with_items(self):
        wl = _watchlist_row()
        item = _watchlist_item()
        payload = io.serialize_watchlist([wl], {str(wl["watchlist_id"]): [item]})
        assert payload["watchlists"][0]["name"] == "Tech"
        assert len(payload["watchlists"][0]["items"]) == 1
        assert payload["watchlists"][0]["items"][0]["symbol"] == "MSFT"

    def test_id_fields_never_exposed(self):
        wl = _watchlist_row()
        item = _watchlist_item()
        payload = io.serialize_watchlist([wl], {str(wl["watchlist_id"]): [item]})
        assert "id" not in payload["watchlists"][0]
        assert "id" not in payload["watchlists"][0]["items"][0]


class TestWatchlistParseAndDiff:
    def _make_payload(self, version, watchlists):
        import json
        return json.dumps({"__version__": version, "watchlists": watchlists})

    def test_insert_new_watchlist_with_item(self):
        payload = self._make_payload("v1", [{
            "name": "Growth", "is_default": False,
            "items": [{"symbol": "NVDA", "instrument_type": "stock"}],
        }])
        diff = io.parse_and_diff_watchlist(payload, [], {})
        assert len(diff.wl_inserts) == 1
        assert diff.wl_inserts[0]["name"] == "Growth"
        assert len(diff.item_inserts) == 1
        assert diff.item_inserts[0]["symbol"] == "NVDA"

    def test_delete_watchlist(self):
        wl = _watchlist_row()
        payload = self._make_payload("v1", [])
        diff = io.parse_and_diff_watchlist(payload, [wl], {})
        assert diff.wl_deletes == ["22222222-2222-2222-2222-222222222222"]

    def test_update_item_notes(self):
        wl = _watchlist_row()
        item = _watchlist_item()
        items = {str(wl["watchlist_id"]): [item]}
        # No id fields supplied — server matches watchlist by name and item by
        # (symbol, instrument_type) within the watchlist.
        payload = self._make_payload("v1", [{
            "name": wl["name"],
            "description": wl["description"],
            "is_default": False,
            "items": [{
                "symbol": item["symbol"],
                "instrument_type": item["instrument_type"],
                "exchange": item["exchange"],
                "name": item["name"],
                "notes": "watch closely",  # changed
            }],
        }])
        diff = io.parse_and_diff_watchlist(payload, [wl], items)
        assert diff.wl_inserts == []
        assert diff.wl_deletes == []
        assert len(diff.item_updates) == 1
        assert diff.item_updates[0]["notes"] == "watch closely"

    def test_rename_watchlist_becomes_delete_plus_insert(self):
        """Renaming a watchlist creates a new one and drops the old, since the
        agent has no stable id to track the rename. The agent must re-include
        items under the new name in the same write."""
        wl = _watchlist_row()  # name="Tech"
        item = _watchlist_item()
        items = {str(wl["watchlist_id"]): [item]}
        payload = self._make_payload("v1", [{
            "name": "Growth",  # renamed
            "description": wl["description"],
            "is_default": False,
            "items": [{
                "symbol": item["symbol"],
                "instrument_type": item["instrument_type"],
                "exchange": item["exchange"],
                "name": item["name"],
                "notes": item["notes"],
            }],
        }])
        diff = io.parse_and_diff_watchlist(payload, [wl], items)
        assert len(diff.wl_inserts) == 1
        assert diff.wl_inserts[0]["name"] == "Growth"
        assert diff.wl_deletes == [str(wl["watchlist_id"])]
        # New items go under the freshly-generated watchlist id
        assert len(diff.item_inserts) == 1
        assert diff.item_inserts[0]["watchlist_id"] == diff.wl_inserts[0]["id"]

    def test_agent_supplied_id_is_silently_ignored(self):
        wl = _watchlist_row()
        item = _watchlist_item()
        items = {str(wl["watchlist_id"]): [item]}
        payload = self._make_payload("v1", [{
            "id": "deadbeef-dead-beef-dead-beefdeadbeef",  # bogus
            "name": wl["name"],
            "description": wl["description"],
            "is_default": False,
            "items": [{
                "id": "deadbeef-dead-beef-dead-beefdeadbe01",  # bogus
                "symbol": item["symbol"],
                "instrument_type": item["instrument_type"],
                "exchange": item["exchange"],
                "name": item["name"],
                "notes": "updated",
            }],
        }])
        diff = io.parse_and_diff_watchlist(payload, [wl], items)
        # Watchlist matched by name despite bogus id → no insert/delete
        assert diff.wl_inserts == []
        assert diff.wl_deletes == []
        # Item matched by (symbol, instrument_type) despite bogus id → update
        assert diff.item_inserts == []
        assert len(diff.item_updates) == 1
        assert diff.item_updates[0]["notes"] == "updated"


# ---------------------------------------------------------------------------
# Preferences
# ---------------------------------------------------------------------------


class TestPreferences:
    def test_serialize_none(self):
        payload = io.serialize_preferences(None)
        assert payload["risk_preference"] == {}
        assert payload["investment_preference"] == {}
        assert payload["agent_preference"] == {}
        # other_preference is server-managed and not exposed to the agent.
        assert "other_preference" not in payload
        assert payload["__version__"].startswith("sha256:")

    def test_serialize_passthrough_excludes_other_preference(self):
        prefs = {
            "risk_preference": {"tolerance": "aggressive"},
            "investment_preference": {"style": "growth"},
            "agent_preference": {},
            # Even when the DB row has a populated other_preference,
            # the serialized payload omits it.
            "other_preference": {"onboarding_step": 3, "internal_flag": True},
            "updated_at": datetime(2026, 5, 17, tzinfo=timezone.utc),
        }
        payload = io.serialize_preferences(prefs)
        assert payload["risk_preference"] == {"tolerance": "aggressive"}
        assert payload["investment_preference"] == {"style": "growth"}
        assert "other_preference" not in payload

    def test_parse_missing_version_accepted(self):
        """Same as portfolio — version is server-tracked; payload doesn't need it."""
        values = io.parse_preferences('{"risk_preference": {}}')
        assert values["risk_preference"] == {}

    def test_parse_passthrough(self):
        payload = '{"__version__": "v1", "risk_preference": {"tolerance": "moderate"}}'
        values = io.parse_preferences(payload)
        assert values["risk_preference"] == {"tolerance": "moderate"}
        # Missing keys default to empty dicts (no None)
        assert values["investment_preference"] == {}
        # other_preference is never in the parsed values dict.
        assert "other_preference" not in values

    def test_parse_silently_drops_other_preference(self):
        """Even if the agent writes back other_preference, parse drops it
        so the applier never overwrites the server-managed column."""
        payload = (
            '{"__version__": "v1", "risk_preference": {"tolerance": "low"}, '
            '"other_preference": {"agent_should_not_set_this": true}}'
        )
        values = io.parse_preferences(payload)
        assert "other_preference" not in values

    def test_parse_non_dict_rejected(self):
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_preferences('{"__version__": "v1", "risk_preference": "not a dict"}')
        assert exc.value.error_type == "schema_error"
        assert exc.value.field_path == "risk_preference"


# ---------------------------------------------------------------------------
# Strict validation — unknown fields, duplicates, value bounds
# ---------------------------------------------------------------------------


class TestPortfolioStrictValidation:
    def _make_payload(self, holdings):
        import json
        return json.dumps({"__version__": "v1", "holdings": holdings})

    def test_unknown_field_rejected_with_suggestion(self):
        """Typo'd `symbo` should be flagged AND get a `symbol` suggestion."""
        payload = self._make_payload([{
            "symbo": "AAPL", "instrument_type": "stock", "quantity": "10",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.error_type == "schema_error"
        assert exc.value.field_path == "holdings[0]"
        assert "unknown field" in exc.value.hint
        assert "'symbol'" in exc.value.hint  # suggestion

    def test_unknown_field_no_suggestion_when_unrelated(self):
        payload = self._make_payload([{
            "symbol": "AAPL", "instrument_type": "stock", "quantity": "10",
            "totally_made_up": "value",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert "totally_made_up" in exc.value.hint

    def test_id_field_tolerated_silently(self):
        """Pre-existing carve-out: agent-supplied `id` is dropped, not rejected."""
        payload = self._make_payload([{
            "id": "deadbeef-dead-beef-dead-beefdeadbeef",
            "symbol": "AAPL", "instrument_type": "stock", "quantity": "10",
        }])
        diff = io.parse_and_diff_portfolio(payload, [])
        assert len(diff.inserts) == 1

    def test_duplicate_holding_rejected(self):
        payload = self._make_payload([
            {"symbol": "AAPL", "instrument_type": "stock", "account_name": "Main", "quantity": "10"},
            {"symbol": "AAPL", "instrument_type": "stock", "account_name": "Main", "quantity": "20"},
        ])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.error_type == "schema_error"
        assert "duplicate holding" in exc.value.hint
        assert exc.value.field_path == "holdings[1]"

    def test_duplicate_allowed_when_account_differs(self):
        payload = self._make_payload([
            {"symbol": "AAPL", "instrument_type": "stock", "account_name": "Main", "quantity": "10"},
            {"symbol": "AAPL", "instrument_type": "stock", "account_name": "IRA", "quantity": "20"},
        ])
        diff = io.parse_and_diff_portfolio(payload, [])
        assert len(diff.inserts) == 2

    def test_empty_symbol_rejected(self):
        payload = self._make_payload([{
            "symbol": "   ", "instrument_type": "stock", "quantity": "10",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.field_path == "holdings[0].symbol"
        assert "empty" in exc.value.hint

    def test_negative_quantity_rejected(self):
        payload = self._make_payload([{
            "symbol": "AAPL", "instrument_type": "stock", "quantity": "-5",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert ">= 0" in exc.value.hint

    def test_negative_average_cost_rejected(self):
        payload = self._make_payload([{
            "symbol": "AAPL", "instrument_type": "stock", "quantity": "10",
            "average_cost": "-100",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert exc.value.field_path == "holdings[0].average_cost"

    def test_overlong_symbol_rejected(self):
        payload = self._make_payload([{
            "symbol": "X" * 51,  # max is 50
            "instrument_type": "stock", "quantity": "10",
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_portfolio(payload, [])
        assert "too long" in exc.value.hint
        assert exc.value.field_path == "holdings[0].symbol"


class TestWatchlistStrictValidation:
    def _make_payload(self, watchlists):
        import json
        return json.dumps({"__version__": "v1", "watchlists": watchlists})

    def test_unknown_field_on_watchlist_rejected(self):
        payload = self._make_payload([{
            "name": "Tech", "color": "blue", "items": [],
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert "unknown field" in exc.value.hint
        assert "color" in exc.value.hint

    def test_unknown_field_on_item_rejected(self):
        payload = self._make_payload([{
            "name": "Tech",
            "items": [{
                "symbol": "AAPL", "instrument_type": "stock",
                "target_price": 250,  # not a real column
            }],
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert "target_price" in exc.value.hint
        assert "items[0]" in exc.value.field_path

    def test_duplicate_watchlist_name_rejected(self):
        payload = self._make_payload([
            {"name": "Tech", "items": []},
            {"name": "Tech", "items": []},
        ])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert "duplicate watchlist name" in exc.value.hint
        assert exc.value.field_path == "watchlists[1].name"

    def test_duplicate_item_within_watchlist_rejected(self):
        payload = self._make_payload([{
            "name": "Tech",
            "items": [
                {"symbol": "AAPL", "instrument_type": "stock"},
                {"symbol": "AAPL", "instrument_type": "stock"},
            ],
        }])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert "duplicate item" in exc.value.hint
        assert "watchlists[0].items[1]" in exc.value.field_path

    def test_same_symbol_in_different_watchlists_allowed(self):
        payload = self._make_payload([
            {"name": "Tech", "items": [{"symbol": "AAPL", "instrument_type": "stock"}]},
            {"name": "Mega", "items": [{"symbol": "AAPL", "instrument_type": "stock"}]},
        ])
        diff = io.parse_and_diff_watchlist(payload, [], {})
        assert len(diff.wl_inserts) == 2
        assert len(diff.item_inserts) == 2

    def test_empty_watchlist_name_rejected(self):
        payload = self._make_payload([{"name": "  ", "items": []}])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert exc.value.field_path == "watchlists[0].name"
        assert "empty" in exc.value.hint

    def test_overlong_watchlist_name_rejected(self):
        payload = self._make_payload([{"name": "X" * 101, "items": []}])
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_and_diff_watchlist(payload, [], {})
        assert "too long" in exc.value.hint


class TestPreferenceStrictValidation:
    def test_unknown_root_key_rejected(self):
        payload = '{"__version__": "v1", "risk_preference": {}, "rizz_preference": {}}'
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_preferences(payload)
        assert "rizz_preference" in exc.value.hint
        # Whether the suggester fires depends on how close the typo is; what
        # matters is the agent gets the allowed list to recover.
        assert "allowed" in exc.value.hint

    def test_typo_close_to_allowed_triggers_suggestion(self):
        payload = '{"__version__": "v1", "risk_pref": {}}'
        with pytest.raises(UserDataValidationError) as exc:
            io.parse_preferences(payload)
        assert "'risk_preference'" in exc.value.hint  # substring match: risk_pref ⊂ risk_preference

    def test_other_preference_tolerated_in_root(self):
        """other_preference is in the allowed root set but parse drops it from values."""
        payload = '{"__version__": "v1", "risk_preference": {}, "other_preference": {"x": 1}}'
        values = io.parse_preferences(payload)
        assert "other_preference" not in values  # still dropped from output


class TestContentHashVersion:
    """__version__ derives from agent-visible content, not updated_at.

    These tests pin down the behavior that fixes the version_conflict false
    positives: writes that bump updated_at without changing what the agent
    sees must not invalidate the agent's known version.
    """

    def test_same_content_different_updated_at_yields_same_version(self):
        """Two reads of the same business state produce the same hash even when
        an unrelated writer has bumped updated_at in between."""
        row_v1 = _portfolio_row(updated_at=datetime(2026, 5, 17, 8, 34, 11, tzinfo=timezone.utc))
        row_v2 = _portfolio_row(updated_at=datetime(2026, 5, 18, 19, 22, 0, tzinfo=timezone.utc))
        v1 = io.serialize_portfolio([row_v1])["__version__"]
        v2 = io.serialize_portfolio([row_v2])["__version__"]
        assert v1 == v2

    def test_changed_content_yields_different_version(self):
        row_v1 = _portfolio_row(quantity=Decimal("100"))
        row_v2 = _portfolio_row(quantity=Decimal("200"))
        v1 = io.serialize_portfolio([row_v1])["__version__"]
        v2 = io.serialize_portfolio([row_v2])["__version__"]
        assert v1 != v2

    def test_preferences_same_content_yields_same_version(self):
        """Skill tools that write identical values must not trigger version
        conflicts — same content → same hash."""
        prefs_v1 = {
            "risk_preference": {"tolerance": "moderate"},
            "investment_preference": {"style": "balanced"},
            "agent_preference": {},
            "other_preference": {"onboarding_step": 1},  # server-managed, excluded from hash
            "updated_at": datetime(2026, 5, 17, tzinfo=timezone.utc),
        }
        prefs_v2 = dict(prefs_v1)
        prefs_v2["updated_at"] = datetime(2026, 5, 18, tzinfo=timezone.utc)
        prefs_v2["other_preference"] = {"onboarding_step": 2}  # different server-managed state
        v1 = io.serialize_preferences(prefs_v1)["__version__"]
        v2 = io.serialize_preferences(prefs_v2)["__version__"]
        assert v1 == v2

    def test_watchlist_same_content_yields_same_version(self):
        wl1 = _watchlist_row(updated_at=datetime(2026, 5, 17, tzinfo=timezone.utc))
        wl2 = _watchlist_row(updated_at=datetime(2026, 5, 18, tzinfo=timezone.utc))
        item1 = _watchlist_item(updated_at=datetime(2026, 5, 15, tzinfo=timezone.utc))
        item2 = _watchlist_item(updated_at=datetime(2026, 5, 19, tzinfo=timezone.utc))
        v1 = io.serialize_watchlist([wl1], {str(wl1["watchlist_id"]): [item1]})["__version__"]
        v2 = io.serialize_watchlist([wl2], {str(wl2["watchlist_id"]): [item2]})["__version__"]
        assert v1 == v2

    def test_version_is_stable_across_dict_key_order(self):
        """Hash uses sort_keys=True so JSON serialization order can't shift it."""
        v1 = io.serialize_portfolio([_portfolio_row()])["__version__"]
        v2 = io.serialize_portfolio([_portfolio_row()])["__version__"]
        assert v1 == v2

    def test_portfolio_version_invariant_to_row_order(self):
        """Two fetches that return holdings in different orders must hash to
        the same version. Without this, an ORDER BY drift between the
        pre-check helper and the in-transaction recheck helper produces a
        phantom version_conflict — exactly the watchlist bug from the
        feat/user-data-backend rollout."""
        row_a = _portfolio_row(symbol="AAPL")
        row_b = _portfolio_row(
            user_portfolio_id="22222222-2222-2222-2222-222222222222",
            symbol="MSFT",
        )
        v_asc = io.serialize_portfolio([row_a, row_b])["__version__"]
        v_desc = io.serialize_portfolio([row_b, row_a])["__version__"]
        assert v_asc == v_desc

    def test_watchlist_version_invariant_to_item_order(self):
        wl = _watchlist_row()
        wl_id = str(wl["watchlist_id"])
        item_a = _watchlist_item(symbol="AAPL")
        item_b = _watchlist_item(
            watchlist_item_id="44444444-4444-4444-4444-444444444444",
            symbol="MSFT",
        )
        v_asc = io.serialize_watchlist([wl], {wl_id: [item_a, item_b]})["__version__"]
        v_desc = io.serialize_watchlist([wl], {wl_id: [item_b, item_a]})["__version__"]
        assert v_asc == v_desc

    def test_watchlist_version_invariant_to_watchlist_order(self):
        wl_a = _watchlist_row(name="Tech")
        wl_b = _watchlist_row(
            watchlist_id="55555555-5555-5555-5555-555555555555",
            name="Growth",
        )
        v1 = io.serialize_watchlist([wl_a, wl_b], {})["__version__"]
        v2 = io.serialize_watchlist([wl_b, wl_a], {})["__version__"]
        assert v1 == v2


class TestSuggestField:
    def test_substring_suggestion(self):
        from src.server.services.user_data_io import _suggest_field
        allowed = frozenset({"symbol", "instrument_type", "quantity"})
        assert _suggest_field("symbo", allowed) == "symbol"
        assert _suggest_field("quant", allowed) == "quantity"

    def test_no_suggestion_for_unrelated(self):
        from src.server.services.user_data_io import _suggest_field
        allowed = frozenset({"symbol", "quantity"})
        assert _suggest_field("totally_unrelated", allowed) is None


class TestReadmePointer:
    """`message` auto-appends a README pointer for schema/parse errors so the
    agent has a path to recovery without us inlining the hint at every raise."""

    def test_schema_error_message_points_to_readme(self):
        exc = UserDataValidationError(
            error_type="schema_error",
            file="portfolio.json",
            field_path="holdings[0].quantity",
            hint="must be >= 0",
        )
        assert "README.md" in exc.message
        assert ".agents/user/profile/README.md" in str(exc)

    def test_parse_error_message_points_to_readme(self):
        exc = UserDataValidationError(
            error_type="parse_error",
            file="watchlist.json",
            field_path="line 3 col 7",
            hint="invalid JSON",
        )
        assert "README.md" in exc.message

    def test_version_conflict_does_not_point_to_readme(self):
        exc = UserDataValidationError(
            error_type="version_conflict",
            file="portfolio.json",
            field_path="",
            hint="file was modified by another writer",
        )
        assert "README.md" not in exc.message

    def test_constraint_error_does_not_point_to_readme(self):
        exc = UserDataValidationError(
            error_type="constraint_error",
            file="watchlist.json",
            field_path="watchlists[0].name",
            hint="duplicate key",
        )
        assert "README.md" not in exc.message

    def test_readme_self_referential_error_does_not_point_to_readme(self):
        """Editing README.md → schema_error. Don't tell the agent to read README to fix it."""
        exc = UserDataValidationError(
            error_type="schema_error",
            file="README.md",
            field_path="",
            hint="is documentation, not data — it cannot be edited.",
        )
        assert "README.md" not in exc.message.removeprefix("schema_error:README.md:")

    def test_hint_field_unchanged(self):
        """The README pointer goes on `.message` only — `.hint` is unchanged so
        callers reading the structured field still get the original wording."""
        exc = UserDataValidationError(
            error_type="schema_error",
            file="portfolio.json",
            field_path="holdings[0]",
            hint="unknown field 'symbo'",
        )
        assert exc.hint == "unknown field 'symbo'"
        assert "README.md" not in exc.hint
