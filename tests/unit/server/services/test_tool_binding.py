"""The one function that decides which path a granted tool takes."""

from __future__ import annotations

import pytest

from src.server.services.brokerage_capabilities import (
    ALL_BINDINGS,
    ORDER_APPROVAL_DEFAULTS,
    _CURATION,
    _ORDER_TOOLS,
    GROUPS,
    CapabilityGroup,
    OrderMode,
    group_keys_for,
    order_modes,
)
from src.server.services.egress import fold_tool_name
from src.server.services.tool_binding import (
    BindingInputs,
    PRESET_PTC_ONLY,
    Resolved,
    allowed_bindings,
    inputs_from_row,
    merge_overrides,
    order_approval_map,
    order_approval_overrides,
    order_policy,
    resolve_plan,
    resolve_tool,
    strip_disallowed_overrides,
    validate_overrides,
)

MOOMOO = "moomoo"
ALL = ("market_data", "watchlists", "account", "paper_trading", "trading")
#: Every tool that mutates an order at any brokerage, in any mode.
ORDER_TOOLS = [
    (vendor, tool)
    for vendor, tools in sorted(_ORDER_TOOLS.items())
    for tool in sorted(tools)
]
#: The staged instruction, the mode the ``trading`` group never covered.
STAGED_ORDER = ("ibkr", "create_order_instruction")
#: A read filed under a rung group, which stays exactly where its group puts it.
PAPER_READ = "sim_trade_position_list"

ASKS_ALL = {"live": True, "paper": True, "staged": True}
ASKS_NOTHING = {"live": False, "paper": False, "staged": False}


def _answer(resolved) -> tuple[str, str, bool]:
    """The three things every reader takes off a resolution."""
    return resolved.binding, resolved.source, resolved.approval


def _asks(plan) -> frozenset[str]:
    """The plan's tools whose every call is put to the user."""
    return frozenset(t for t, r in plan.by_tool.items() if r.approval)


def test_group_default_binds_paper_trading_direct_and_the_rest_ptc():
    plan = resolve_plan(MOOMOO, ALL, BindingInputs())
    assert "sim_trade_account_list" in plan.direct
    assert "sim_trade_account_list" in plan.sandbox_excluded
    assert plan.by_tool["sim_trade_account_list"].source == "group"
    assert resolve_tool(MOOMOO, "trade_account_list", BindingInputs()).binding == "ptc"


def test_consent_still_gates_what_the_plan_covers():
    plan = resolve_plan(MOOMOO, ("paper_trading",), BindingInputs())
    assert "sim_trade_account_list" in plan.direct
    assert "trading_order_place" not in plan.by_tool


def test_precedence_override_beats_preset_beats_group():
    inputs = BindingInputs(
        overrides={"sim_trade_account_list": "direct"},
        preset=PRESET_PTC_ONLY,
    )
    plan = resolve_plan(MOOMOO, ALL, inputs)
    assert plan.by_tool["sim_trade_account_list"].binding == "direct"
    assert plan.by_tool["sim_trade_account_list"].source == "override"
    assert plan.by_tool[PAPER_READ].source == "preset"
    cleared = resolve_plan(MOOMOO, ALL, BindingInputs(preset=None))
    assert cleared.by_tool[PAPER_READ].source == "group"
    # The ladder still ran for the order tool; the pin is what it answered to.
    assert cleared.by_tool["sim_trade_input_order"].source == "policy"


@pytest.mark.parametrize("stale", ["order_direct", "no_such_preset"])
def test_a_stored_preset_the_resolver_does_not_know_means_group_defaults(stale):
    """``order_direct`` used to be the switch's off position and may still sit
    in a column. It has to resolve exactly as a cleared row does, or a row the
    user never touched again would carry a binding the page cannot explain."""
    plan = resolve_plan(MOOMOO, ALL, BindingInputs(preset=stale))
    assert plan == resolve_plan(MOOMOO, ALL, BindingInputs())
    assert plan.by_tool[PAPER_READ] == Resolved("direct", "group")
    assert _answer(plan.by_tool["sim_trade_input_order"]) == ("direct", "policy", False)
    assert _answer(plan.by_tool["trading_order_place"]) == ("direct", "policy", True)


def test_ptc_only_preset_sends_everything_it_can_through_the_sandbox():
    """Everything the policy allows into the sandbox, which is now every tool
    that mutates no order: a paper order is pinned direct the same way a live
    one is, so the preset no longer reaches it either."""
    plan = resolve_plan(MOOMOO, ALL, BindingInputs(preset=PRESET_PTC_ONLY))
    orders = frozenset(_ORDER_TOOLS[MOOMOO])
    assert plan.direct == orders
    assert plan.sandbox_excluded == orders
    assert all(r.binding == "ptc" for t, r in plan.by_tool.items() if t not in orders)


def test_both_keeps_a_wrapper_and_a_json_tool():
    inputs = BindingInputs(overrides={"sim_trade_account_list": "both"})
    plan = resolve_plan(MOOMOO, ALL, inputs)
    assert "sim_trade_account_list" in plan.direct
    assert "sim_trade_account_list" not in plan.sandbox_excluded


@pytest.mark.parametrize(
    "approval", [None, ASKS_ALL, ASKS_NOTHING, {"live": False}, {"paper": True}]
)
@pytest.mark.parametrize("preset", [None, PRESET_PTC_ONLY])
@pytest.mark.parametrize("stored", ["direct", "both", "ptc", None])
@pytest.mark.parametrize("vendor,tool", ORDER_TOOLS)
def test_an_order_tool_is_always_bound_directly(
    vendor, tool, stored, preset, approval
):
    """The clamp nothing on the row can move, in every mode. An order is one
    tool call, the shape a per-call stop and a UI can see, so no map, no
    preset and no approval switch can put one back into a sandbox execution
    where any number can be placed inside a single ``execute_code``."""
    overrides = {tool: stored} if stored else {}
    inputs = BindingInputs(
        overrides=overrides,
        preset=preset,
        **({} if approval is None else {"order_approval": approval}),
    )
    plan = resolve_plan(vendor, group_keys_for(vendor), inputs)
    resolved = plan.by_tool[tool]
    # ``policy`` whatever the ladder said: the source is what the page reads
    # to decide whether a setting is the user's to change, and this is not.
    policy = order_policy(vendor, tool, order_approval=inputs.order_approval)
    assert _answer(resolved) == ("direct", "policy", policy.approval)
    # The clamp and the order itself travel with the answer, so no reader
    # downstream has to look either of them up again.
    assert resolved.allowed == frozenset({"direct"})
    assert resolved.order == policy
    assert tool in plan.direct
    assert tool in plan.sandbox_excluded


@pytest.mark.parametrize("spelling", ["TRADING_ORDER_PLACE", " Trading_Order_Place ", "\ttrading_order_place\n"])
@pytest.mark.parametrize("stored", ["ptc", "both"])
def test_a_recased_or_padded_live_order_key_cannot_slip_past_the_clamp(spelling, stored):
    inputs = BindingInputs(overrides={spelling: stored})
    plan = resolve_plan(MOOMOO, ALL, inputs)
    assert plan.by_tool["trading_order_place"].binding == "direct"
    assert plan.by_tool[spelling].binding == "direct"
    assert plan.by_tool[spelling].source == "policy"
    assert validate_overrides(MOOMOO, {spelling: stored})


def test_the_clamp_falls_back_to_the_group_default_not_to_ptc():
    """The floor it replaces could only push a tool one way. The clamp answers
    with whatever the group says its default is, so it holds in either
    direction; a live-order group defaults to ``direct`` and that is what an
    asked-for ``ptc`` becomes."""
    resolved = resolve_tool(MOOMOO, "trading_order_place", BindingInputs(preset=PRESET_PTC_ONLY))
    assert resolved == resolve_tool(MOOMOO, "trading_order_place", BindingInputs(overrides={"trading_order_place": "both"}))
    assert (resolved.binding, resolved.source) == ("direct", "policy")


def test_every_group_default_is_one_of_its_allowed_bindings():
    for group in GROUPS:
        assert group.default_binding in group.allowed_bindings, group.key
    with pytest.raises(ValueError):
        CapabilityGroup(
            key="x", order=0, tone="neutral",
            default_binding="ptc", allowed_bindings=frozenset({"direct"}),
        )


def test_a_read_filed_under_a_rung_group_is_not_clamped():
    """The pin is per tool, not per group, because ``paper_trading`` mixes
    three mutating tools with five reads and ``order_preview`` persists
    nothing. A preview and a position list keep every path their group allows."""
    restricted = {g.key for g in GROUPS if g.allowed_bindings != ALL_BINDINGS}
    assert restricted == {"trading"}
    reads = [
        ("robinhood", "review_equity_order"),
        ("ibkr", "get_order_instructions"),
        (MOOMOO, PAPER_READ),
    ]
    for vendor, tool in reads:
        assert allowed_bindings(vendor, tool) == ALL_BINDINGS
        for stored in ("ptc", "direct", "both"):
            assert validate_overrides(vendor, {tool: stored}) is None
            assert resolve_tool(
                vendor, tool, BindingInputs(overrides={tool: stored})
            ).binding == stored
    # Not a brokerage at all, so nothing here has an opinion about it.
    assert allowed_bindings(None, "no_such_tool") == ALL_BINDINGS


@pytest.mark.parametrize("vendor,tool", ORDER_TOOLS)
@pytest.mark.parametrize("stored", ["ptc", "both"])
def test_the_write_path_refuses_an_order_override_off_the_direct_path(
    stored, vendor, tool
):
    """Refused rather than stored and then overruled: a setting the resolver
    contradicts is one the user believes is in force when it never was. The
    reason names what the tool may be, not only what it may not."""
    reason = validate_overrides(vendor, {tool: stored})
    assert reason and "direct tool call" in reason
    assert validate_overrides(vendor, {tool: "direct"}) is None
    assert validate_overrides(MOOMOO, {PAPER_READ: stored}) is None


def test_a_live_order_asks_and_only_the_row_switch_for_its_mode_stops_it():
    """The gate rides on the order map and is keyed by mode, so a switch that
    stops being asked about live orders leaves paper exactly where it was. The
    binding is unaffected either way: the pin keeps the tool on the direct
    path, so turning a switch off removes the stop without opening a sandbox
    way around it."""
    plan = resolve_plan(MOOMOO, ALL, BindingInputs())
    live = frozenset(_CURATION[MOOMOO]["trading"])
    assert _asks(plan) == live
    assert _asks(plan) <= plan.direct

    off = resolve_plan(MOOMOO, ALL, BindingInputs(order_approval={"live": False}))
    assert _asks(off) == frozenset()
    assert off.direct == plan.direct
    assert off.sandbox_excluded == plan.sandbox_excluded

    paper_on = resolve_plan(MOOMOO, ALL, BindingInputs(order_approval={"paper": True}))
    assert _asks(paper_on) == live | frozenset(
        t for t, e in _ORDER_TOOLS[MOOMOO].items() if e.mode is OrderMode.PAPER
    )


@pytest.mark.parametrize("stored", ["ptc", "both"])
def test_a_stored_path_around_the_gate_collapses_back(stored):
    """``both`` would keep a wrapper as the way around the stop and ``ptc``
    would be only that way. The clamp settles both before the gate is read, so
    a row stored before the group named one path still asks."""
    plan = resolve_plan(MOOMOO, ALL, BindingInputs(overrides={"trading_order_place": stored}))
    assert plan.by_tool["trading_order_place"].binding == "direct"
    assert plan.by_tool["trading_order_place"].source == "policy"
    assert "trading_order_place" in _asks(plan)
    assert "trading_order_place" in plan.sandbox_excluded


def test_a_paper_order_is_direct_and_asks_only_when_the_row_says_so():
    """A simulated account cannot touch real money, so the mode's default is
    not to ask. Pinned direct all the same: the ledger and the receipt want
    one interceptable event per attempt whoever's money it is."""
    plan = resolve_plan(MOOMOO, ALL, BindingInputs())
    assert "sim_trade_input_order" in plan.direct
    assert "sim_trade_input_order" in plan.sandbox_excluded
    assert "sim_trade_input_order" not in _asks(plan)
    asked = resolve_plan(MOOMOO, ALL, BindingInputs(order_approval={"paper": True}))
    assert "sim_trade_input_order" in _asks(asked)


def test_a_staged_instruction_is_direct_and_asks_by_default():
    """IBKR calls an instruction not a live order, and it is not one, but it
    is written into the real account. The mode asks by default and the row can
    say otherwise; nothing on the row moves the binding."""
    vendor, tool = STAGED_ORDER
    plan = resolve_plan(vendor, group_keys_for(vendor), BindingInputs())
    assert _answer(plan.by_tool[tool]) == ("direct", "policy", True)
    assert tool in _asks(plan) and tool in plan.sandbox_excluded
    off = resolve_plan(
        vendor, group_keys_for(vendor), BindingInputs(order_approval={"staged": False})
    )
    assert _asks(off) == frozenset()
    assert off.direct == plan.direct


def test_the_mode_defaults_are_what_each_order_tool_declares():
    """One source for "what does this mode ask by default", so a row missing a
    key and a tool whose vendor map names it cannot answer differently."""
    assert set(ORDER_APPROVAL_DEFAULTS) == {m.value for m in OrderMode}
    for vendor, tools in _ORDER_TOOLS.items():
        for tool, entry in tools.items():
            assert ORDER_APPROVAL_DEFAULTS[entry.mode.value] is entry.approval, (
                f"{vendor}.{tool}"
            )
            asked = order_policy(vendor, tool, order_approval={})
            assert asked is not None and asked.approval is entry.approval


def test_only_the_modes_a_vendor_has_are_offered():
    assert order_modes(MOOMOO) == (OrderMode.LIVE, OrderMode.PAPER)
    assert order_modes("robinhood") == (OrderMode.LIVE,)
    assert order_modes("ibkr") == (OrderMode.STAGED,)
    assert order_modes("webull") == ()
    assert order_modes(None) == ()


class TestOrderApprovalMap:
    """What the column holds, filled out from the mode defaults."""

    def test_an_absent_column_is_every_mode_default(self):
        assert order_approval_map(None) == ORDER_APPROVAL_DEFAULTS
        assert inputs_from_row({}).order_approval == ORDER_APPROVAL_DEFAULTS

    def test_the_boolean_the_column_used_to_hold_answers_for_live_alone(self):
        assert order_approval_map(False) == {**ORDER_APPROVAL_DEFAULTS, "live": False}
        assert order_approval_map(True) == ORDER_APPROVAL_DEFAULTS

    def test_a_mode_the_map_does_not_name_keeps_its_default(self):
        assert order_approval_map({"live": False}) == {
            "live": False, "paper": False, "staged": True
        }

    def test_a_key_nothing_declares_is_dropped(self):
        assert order_approval_map({"live": True, "futures": True}) == (
            ORDER_APPROVAL_DEFAULTS
        )

    def test_overrides_hold_only_what_the_row_names(self):
        assert order_approval_overrides(None) == {}
        assert order_approval_overrides(False) == {"live": False}
        assert order_approval_overrides({"paper": True, "staged": None}) == {
            "paper": True
        }
        assert order_approval_overrides({"futures": True}) == {}


def test_row_inputs_read_the_untouched_defaults():
    assert inputs_from_row(None) == BindingInputs()
    row = {"tool_binding": {"x": "direct"}, "binding_preset": ""}
    inputs = inputs_from_row(row)
    assert inputs.overrides == {"x": "direct"}
    assert inputs.preset is None
    # Absent means the mode defaults: a row written before the column was a
    # map is gated exactly where a fresh one is.
    assert inputs.order_approval == ORDER_APPROVAL_DEFAULTS
    assert inputs_from_row({**row, "order_approval": {"paper": True}}).order_approval == (
        {**ORDER_APPROVAL_DEFAULTS, "paper": True}
    )


def test_an_unchanged_stored_entry_is_not_this_requests_doing():
    """A disallowed entry that was stored before the clamp existed must not
    lock the row: resubmitting it unchanged passes, asking for it afresh does
    not."""
    stored = {"trading_order_place": "ptc", "quote_stock_quote": "direct"}
    assert validate_overrides(MOOMOO, stored, stored=stored) is None
    resubmitted = {**stored, "quote_stock_quote": "both"}
    assert validate_overrides(MOOMOO, resubmitted, stored=stored) is None
    changed = {**stored, "trading_order_place": "both"}
    assert "direct tool call" in (validate_overrides(MOOMOO, changed, stored=stored) or "")
    assert "direct tool call" in (validate_overrides(MOOMOO, stored) or "")


def test_the_stored_map_drops_what_the_clamp_overrules():
    """What gets written carries no entry the resolver would answer ``policy``
    to, folded the way ``allowed_bindings`` folds, so a recased key goes too."""
    overrides = {
        "trading_order_place": "ptc",
        " Trading_Order_Cancel ": "both",
        "trading_order_confirm": "direct",
        "sim_trade_input_order": "ptc",
        PAPER_READ: "ptc",
    }
    assert strip_disallowed_overrides(MOOMOO, overrides) == {
        "trading_order_confirm": "direct",
        PAPER_READ: "ptc",
    }
    assert strip_disallowed_overrides(None, overrides) == overrides


def test_a_fold_colliding_map_reports_what_the_split_does():
    """The resolver reads the map the way the split and the relay compare
    names. Two spellings that fold together must report one binding, and it
    must be the binding the sandbox wrapper actually gets or loses."""
    from src.server.services.mcp_tool_split import split_server_tools

    upper = PAPER_READ.upper()
    inputs = BindingInputs(overrides={PAPER_READ: "both", upper: "direct"})
    plan = resolve_plan(MOOMOO, ("paper_trading",), inputs)
    reported = plan.by_tool[PAPER_READ].binding
    assert plan.by_tool[upper].binding == reported
    assert reported == "both", "sorted raw keys, later wins"
    sandbox, direct = split_server_tools(
        [{"name": PAPER_READ}], denied=None, plan=plan
    )
    kept_wrapper = any(t["name"] == PAPER_READ for t in sandbox)
    assert kept_wrapper == (reported == "both")
    assert direct is not None and direct.tools[0].name == PAPER_READ
    assert inputs.overrides == {
        PAPER_READ: "both",
        upper: "direct",
    }, "the stored bytes are never rewritten"


def test_a_new_fold_collision_is_refused_but_a_stored_one_does_not_lock_the_row():
    padded = f" {PAPER_READ.title()} "
    reason = validate_overrides(MOOMOO, {PAPER_READ: "both", padded: "direct"})
    assert reason and repr(padded) in reason
    assert repr(PAPER_READ) in reason

    stored = {PAPER_READ: "both", PAPER_READ.upper(): "direct"}
    assert validate_overrides(MOOMOO, stored, stored=stored) is None
    unrelated = {**stored, "quote_stock_quote": "both"}
    assert validate_overrides(MOOMOO, unrelated, stored=stored) is None
    changed = {**stored, PAPER_READ.upper(): "ptc"}
    assert "same tool" in (validate_overrides(MOOMOO, changed, stored=stored) or "")


def test_a_recased_override_for_a_denied_tool_is_not_reported_as_reachable():
    """Consent carries the curated spelling; an override key need not. The plan
    has to drop both, or the tools page offers a binding for a tool the relay
    refuses."""
    inputs = BindingInputs(overrides={"SIM_TRADE_INPUT_ORDER": "direct"})
    plan = resolve_plan(MOOMOO, ("market_data",), inputs)
    assert "SIM_TRADE_INPUT_ORDER" not in plan.by_tool
    assert not any(fold_tool_name(t) == "sim_trade_input_order" for t in plan.direct)


class TestUnclassifiedAtACuratedVendor:
    """A tool a brokerage publishes that no capability group names.

    Reachable, because the policy regulates what is curated and never blocks
    what is not, but only through the sandbox: the direct set at a brokerage
    is then exactly the curated set, so nothing is bound past a policy that
    has never read it. A server we hold no map for is untouched.
    """

    NEW = "quote_something_the_vendor_shipped_later"

    def test_it_can_only_be_a_sandbox_wrapper(self):
        assert allowed_bindings(MOOMOO, self.NEW) == frozenset({"ptc"})

    @pytest.mark.parametrize("asked", ["direct", "both"])
    def test_the_write_path_refuses_it_and_says_why(self, asked):
        reason = validate_overrides(MOOMOO, {self.NEW: asked})
        assert reason and "we classify" in reason and MOOMOO in reason
        assert "sandbox wrapper" in reason

    @pytest.mark.parametrize("asked", ["direct", "both"])
    def test_an_override_that_got_in_earlier_resolves_to_ptc(self, asked):
        inputs = BindingInputs(overrides={self.NEW: asked})
        resolved = resolve_tool(MOOMOO, self.NEW, inputs)
        assert _answer(resolved) == ("ptc", "policy", False)
        assert resolved.allowed == frozenset({"ptc"})
        assert strip_disallowed_overrides(MOOMOO, {self.NEW: asked}) == {}

    def test_ptc_stays_the_ordinary_default_rather_than_a_clamp(self):
        resolved = resolve_tool(MOOMOO, self.NEW, BindingInputs())
        assert _answer(resolved) == ("ptc", "default", False)

    def test_a_server_we_hold_no_map_for_is_untouched(self):
        assert allowed_bindings(None, self.NEW) == ALL_BINDINGS
        assert validate_overrides(None, {self.NEW: "direct"}) is None

    def test_a_tool_we_read_and_withheld_is_a_different_absence(self):
        """``UNCURATED`` is denied at every grant, so consent drops it from the
        plan entirely rather than leaving it a sandbox binding to argue about,
        and the refusal must not call it unclassified: this module's whole
        point is that the two absences do not collapse into one sentence."""
        plan = resolve_plan("ibkr", group_keys_for("ibkr"), BindingInputs())
        assert "provide_customer_feedback" not in plan.by_tool
        reason = validate_overrides("ibkr", {"provide_customer_feedback": "direct"})
        assert reason and "we classify" not in reason
        assert "sandbox wrapper" in reason


class TestMergeOverrides:
    """The delta the binding endpoint applies to the stored map.

    The page names the tool it changed instead of carrying the map, so the
    reconciliation the client used to do lands here: the resolver reads the
    map folded, and a stored key can be another spelling of the name being
    written.
    """

    def test_set_adds_without_disturbing_the_rest(self):
        stored = {"quote_stock_quote": "direct"}
        assert merge_overrides(stored, set_={"quote_kline": "both"}) == {
            "quote_stock_quote": "direct",
            "quote_kline": "both",
        }

    def test_unset_removes_only_the_tool_it_names(self):
        stored = {"quote_stock_quote": "direct", "quote_kline": "both"}
        assert merge_overrides(stored, unset=["quote_kline"]) == {
            "quote_stock_quote": "direct"
        }

    def test_set_replaces_every_stored_spelling_of_the_tool(self):
        stored = {" EXISTING_TOOL ": "direct", "quote_kline": "both"}
        assert merge_overrides(stored, set_={"existing_tool": "ptc"}) == {
            "quote_kline": "both",
            "existing_tool": "ptc",
        }

    def test_unset_removes_every_stored_spelling_of_the_tool(self):
        stored = {" EXISTING_TOOL ": "direct", "quote_kline": "both"}
        assert merge_overrides(stored, unset=["existing_tool"]) == {
            "quote_kline": "both"
        }

    def test_an_empty_delta_leaves_the_map_alone(self):
        stored = {"quote_stock_quote": "direct"}
        assert merge_overrides(stored) == stored


class TestStdioIsNeverRelayable:
    """A stdio server has no URL, so the relay has nothing to dial.

    The direct path is unreachable for it by construction, and the endpoint
    that offers the choice has to say so rather than accept a binding the
    resolver will not honour.
    """

    def test_a_stdio_row_offers_only_ptc(self):
        assert allowed_bindings(None, "anything", relayable=False) == frozenset({"ptc"})

    def test_an_http_row_is_unrestricted_without_a_group(self):
        assert allowed_bindings(None, "anything") == ALL_BINDINGS

    def test_an_override_asking_for_direct_resolves_to_ptc_as_policy(self):
        inputs = BindingInputs(overrides={"anything": "direct"}, relayable=False)
        resolved = resolve_tool(None, "anything", inputs)
        assert (resolved.binding, resolved.source) == ("ptc", "policy")

    def test_the_same_override_stands_on_a_relayable_row(self):
        inputs = BindingInputs(overrides={"anything": "direct"})
        resolved = resolve_tool(None, "anything", inputs)
        assert (resolved.binding, resolved.source) == ("direct", "override")

    def test_the_write_path_refuses_direct_on_a_stdio_row(self):
        reason = validate_overrides(None, {"anything": "direct"}, relayable=False)
        assert reason and "sandbox wrapper" in reason

    def test_inputs_read_relayable_off_the_row_transport(self):
        assert inputs_from_row({"transport": "stdio"}).relayable is False
        assert inputs_from_row({"transport": "http"}).relayable is True
