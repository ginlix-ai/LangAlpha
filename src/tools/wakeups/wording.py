"""Every model-facing string of the wakeup feature but one, in one place.

The tool description, the schema text, the notices a fire delivers and the
refusals are worded together and checked as a set. The one left out is the
trigger that routes a wake back to the agent, which ``transcript/classify.py``
owns beside the other triggers it tells apart. ``TEMPLATES`` holds each drafted
tool result with ``{placeholders}``, and ``TOOL_RESULTS`` the worked example it
was drafted as, filled in from ``SAMPLES``; ``BUILD_TOOL_RESULTS`` holds the
templates the build added.
"""

from __future__ import annotations

from typing import Sequence

SCHEDULE_WAKEUP_DOC = (
    "Come back to this conversation later, or when something happens, to carry on a task whose next step you already know: at a time, on a repeating schedule, on a price move, or when an order you placed fills or ends. Call with no arguments to see what is pending.\n"
    "\n"
    "A wakeup carries nothing but what fired, so before you end the turn tell the user what you set, when it stops, and what you will do when it fires; that reply is what you will pick up from. It is not for background tasks or hand-offs, which report back on their own. At most 10 can be pending in a conversation; a series counts as one.\n"
    "\n"
    "Args:\n"
    "    triggers: Wakeups to set.\n"
    '    cancel: Ids of wakeups to cancel, or "all".\n'
    "    quiet: End a turn that a wakeup started, without a reply: one short line on what you checked and found, shown to the user in the list of quiet checks.\n"
    "\n"
    "Returns:\n"
    "    What is pending afterwards: each wakeup's id (for cancel; the user knows it by its label), what it waits for, and when it next fires and stops or lapses.\n"
    "\n"
    "A wakeup that fires while you are working arrives in this turn; otherwise it starts a new turn here with no user message. When that turn finds nothing the user needs to hear, end it with quiet instead of replying. Cancel your own wakeups once their goal is met, such as an entry taken or an order done."
)

TRIGGER_MODEL_DOC = "Set exactly one of at, cron, price, order."

PRICE_PCT_FROM = 'Only with pct_up or pct_down: measure from "now", the price when you set it, instead of the previous close.'

TRIGGER_FIELDS = {
    "label": 'A few words naming what this wakeup is for, shown to the user ("NVDA pullback entry"). Optional.',
    "at": 'Fire once at this time, ISO 8601 ("2026-10-21T08:30"), read in timezone unless it carries an offset; 1 minute to 7 days ahead.',
    "cron": 'Fire repeatedly on this 5-field cron, read in timezone: "*/5 * * * *" every 5 minutes, "0 9 * * 1-5" weekdays at 9:00. Fires are at least 5 minutes apart, and market holidays still fire. A series fires at most 24 times and lasts at most 7 days, so every 5 minutes ends after 2 hours; set until to stop it sooner.',
    "until": 'With cron: stop the series after this time, ISO 8601 ("2026-10-20T16:00" for today only), read like at.',
    "timezone": "The IANA timezone at, cron and until are read in (\"America/New_York\"); the user's by default. For a market's hours, use its exchange's timezone and write the hours on that clock, so they hold for a user elsewhere and across daylight-saving changes.",
    "price": "Fire when a price level or % move hits. It lasts through its market's next full session; shortly before it lapses unhit you are woken to set it again or let it go.",
    "order": "Fire when this order first fills, even partly, or ends unfilled: the order id from the brokerage's reply to an order you placed in this conversation. It lasts until the order ends, at most 7 days.",
}

WAKE_OPEN = "<wakeup>"
WAKE_CLOSE = "</wakeup>"
WAKE_NOTE = "Automated wakeup you set with schedule_wakeup (not a user message)."

WAKE_QUIET_HINT = 'This turn started on its own: if it finds nothing the user needs to hear, end it with schedule_wakeup(quiet="<what you checked and found>") instead of replying.'

WAKE_LINES = {
    "time": "It is {fire_time}, the time you set at {set_time}.",
    "series": "Check {n} of {total}{label_paren} of the series you set at {set_time} ({cron} in {timezone}). Next check at {next_time}.",
    "series_skipped": " {skipped} checks were skipped while this one waited to reach you.",
    "series_last": "Check {n} of {total}{label_paren}, the last of the series you set at {set_time} ({cron} in {timezone}). Set a new series if the task still needs it.",
    "price_hit": "{symbol} hit your trigger ({phrase}): it traded at {price} at {fire_time}. You set it at {set_time}; it will not fire again.",
    "price_expiring": "Your trigger on {symbol} ({phrase}) has not hit and lapses at {expires_at}. Last price: {last}. Set it again to keep it; otherwise let it lapse.",
    "order_filled": "Your order filled: {order} (order {order_id}), {qty} at an average {avg} at {fire_time}.",
    "order_partial": "Your order partly filled: {order} (order {order_id}), {filled} of {qty} at an average {avg} so far, and the rest is still working. Set an order wakeup again to hear when it finishes.",
    "order_cancelled": "Your order ended unfilled: {order} (order {order_id}) was cancelled at {fire_time}.",
    "order_rejected": "Your order ended unfilled: {order} (order {order_id}) was rejected by the brokerage: {reason}.",
}

BUILD_WAKE_LINES = {
    "series_skipped_one": " 1 check was skipped while this one waited to reach you.",
    "price_expiring_unpriced": "Your trigger on {symbol} ({phrase}) has not hit and lapses at {expires_at}. Set it again to keep it; otherwise let it lapse.",
    "order_partial_cancelled": "Your order ended partly filled: {order} (order {order_id}), {filled} of {qty} at an average {avg}, then was cancelled at {fire_time}.",
    "order_failed": "Your order ended unfilled: {order} (order {order_id}) failed: {reason}.",
    "order_partial_expired": "Your order ended partly filled: {order} (order {order_id}), {filled} of {qty} at an average {avg}, then expired at {fire_time}.",
    "order_expired": "Your order ended unfilled: {order} (order {order_id}) expired at {fire_time}.",
    "order_confirmed": "Your order was confirmed at the brokerage and sent there as a live order: {order} (order {order_id}), at {fire_time}. Its fills show in the brokerage's own orders.",
    "order_handed_off": "Your staged order left the brokerage's instruction list: {order} (order {order_id}) was sent, deleted or expired there by {fire_time}, and the brokerage does not say which.",
}

WAKE_LABEL_PREFIX = "{label}: "

CANCEL_ROW_ONE = "The user cancelled a wakeup you set: {label}. Leave it cancelled unless the user asks for it again."
CANCEL_ROW_MANY = (
    "The user cancelled wakeups you set:\n"
    "{lines}\n"
    "Leave them cancelled unless the user asks for them again."
)
CANCEL_ROW_SETTING_OFF = (
    "The user turned wakeups off, which cancelled every pending wakeup:\n"
    "{lines}\n"
    "schedule_wakeup is no longer available in this conversation."
)

TEMPLATES = {
    "none": "No pending wakeups.",
    "cancelled": "Cancelled: {items}.",
    "cancel_unknown": "{id}: no pending wakeup with that id.",
    "cancel_late": "{id} already fired and is on its way to you, so it can't be cancelled.",
    "refused_header": "Not set:",
    "refused_one_of": "- set exactly one of at, cron, price, order.",
    "refused_cron_gap": "- cron {expr}: fires {gap} minutes apart; at least {least}.",
    "refused_cron_misread": "- cron {problem}",
    "refused_cron_none": "- cron {expr} in {zone}: never fires in the next 7 days.",
    "refused_timezone": "- timezone {name}: not an IANA timezone; use a name like America/New_York.",
    "refused_timezone_abbrev": "- timezone {name}: a fixed offset that ignores daylight saving; use a name like America/New_York.",
    "refused_until": "- until {s}: already past.",
    "refused_pct_from": "- price: pct_from goes with pct_up or pct_down.",
    "refused_soon": "- at {s}: under a minute from now or already past (now {now}).",
    "refused_horizon": "- at {s}: more than 7 days ahead.",
    "refused_order_unknown": "- order {id}: no open order with that id was placed in this conversation.",
    "refused_order_unreadable": "- order {id}: IBKR staged instructions can't be read back, so an order wakeup can't see them; set a timer instead.",
    "refused_label": "- label: at most 40 characters.",
    "refused_unknown_field": "- {key}: not a field; name a wakeup with label.",
    "refused_cap": "- this conversation already has 10 pending wakeups; cancel one first.",
    "quiet_refused": "quiet only ends a turn a wakeup started; this turn has a user message, so reply to it.",
    "quiet_ok": "Ended without a reply.",
}

SAMPLES = {
    "cancelled": {
        "items": "w_3f2a (NVDA pullback entry: cron */5 10-15 * * 1-5 in America/New_York), w_91c0 (NVDA breakdown: NVDA below $198.00)"
    },
    "cancel_unknown": {"id": "w_77aa"},
    "cancel_late": {"id": "w_91c0"},
    "refused_cron_gap": {"expr": "*/2 * * * *", "gap": 2, "least": 5},
    "refused_cron_misread": {
        "problem": "'0 0 9 * * 5' has 6 fields; use five: minute hour day-of-month month day-of-week (seconds aren't supported)"
    },
    "refused_cron_none": {"expr": "0 9 30 2 *", "zone": "Asia/Shanghai"},
    "refused_timezone": {"name": "Eastern"},
    "refused_timezone_abbrev": {"name": "EST"},
    "refused_until": {"s": "2026-10-20T09:00"},
    "refused_soon": {"s": "2026-10-20T15:40", "now": "15:40 EDT"},
    "refused_horizon": {"s": "2026-11-02T09:00"},
    "refused_order_unknown": {"id": "7712046"},
    "refused_order_unreadable": {"id": "5520"},
    "refused_unknown_field": {"key": "title"},
}

TOOL_RESULTS = {key: text.format(**SAMPLES.get(key, {})) for key, text in TEMPLATES.items()}

BUILD_TOOL_RESULTS = {
    "top_header": "Not done:",
    "unknown_arg": "- {k}: not an argument; schedule_wakeup takes triggers, cancel and quiet.",
    "triggers_not_list": "- triggers: a list of trigger entries.",
    "quiet_not_text": "- quiet: one short line of text.",
    "quiet_refused_acted": "quiet ends a check that found nothing to tell; this turn acted on an order or handed work to an analyst, so tell the user what you did.",
    "quiet_refused_calls": "quiet ends the turn at once, so send it on its own after you have read what your other calls returned.",
    "cancel_arg": '- cancel: one wakeup id, a list of them, or "all".',
    "until_needs_cron": "- until needs cron.",
    "at_unparseable": "- at {s}: not an ISO 8601 time.",
    "until_unparseable": "- until {s}: not an ISO 8601 time.",
    "cron_invalid": "- cron {expr}: not a valid cron expression.",
    "cron_until_none": "- cron {expr} in {zone}: never fires before until {raw}.",
    "unknown_field_other": "- {key}: not a field; a trigger takes label, at, cron, until, timezone, price, order.",
    "price_one_kind": "- price: set exactly one of above, below, pct_up, pct_down.",
    "price_no_live": '- {desc}: no live price to measure "now" from; leave pct_from out to measure from the previous close, or set a price level.',
    "save_failed": "- {desc}: could not be saved, so it is not set.",
    "order_readback_off": "- order {id}: this server doesn't read order status back, so an order wakeup can't see it; set a timer instead.",
    "order_vendor_unreadable": "- order {id}: {vendor} orders can't be read back, so an order wakeup can't see them; set a timer instead.",
    "setting_off": "- wakeups are turned off in Settings, so this was not set.",
    "cancelled_in_flight": "Cancelled: {desc}; its check {n} had already fired and still reaches you.",
    "trigger_not_object": "- each trigger is an object with one of at, cron, price, order.",
    "nested_unknown": "- {path}: not a field.",
    "required": "- {path}: required.",
    "generic": "- {path}: {message}.",
    "cron_too_long": "- cron: at most 120 characters.",
}

CANCEL_ARG_REFUSAL = BUILD_TOOL_RESULTS["cancel_arg"]

BUILD_PRICE_PCT_LINE = (
    "{symbol} {phrase}; last {last}, trigger {level}, {away}% away; lapses {when}"
)
BUILD_PCT_NOW_PHRASE = "up 3% from $200.00, its price when you set it"

BUILD_PCT_NOW_BASE = "{price}, its price when you set it"

# The price checks the wakeup kept from the market-watch tool's price targets,
# worded there. ``{desc}`` is the entry as the model wrote it (PRICE_DESC), and
# a ``{reason}`` is why it cannot be watched (LISTING_*) or would fire at once
# (PRICE_ALREADY_MET).
PRICE_REFUSALS = {
    "not_ticker": "- {label}: not a ticker symbol.",
    "not_positive": "- {desc}: {kind} must be greater than 0.",
    "fall_too_far": "- {desc}: a fall of 100% or more can never hit.",
    "reason": "- {desc}: {reason}.",
}
PRICE_DESC = {
    "above": "{symbol} above {value:g}",
    "below": "{symbol} below {value:g}",
    "pct": "{symbol} {direction} {value:g}% from {base}",
}
PRICE_DESC_BASES = {"previous_close": "the previous close", "now": "now"}
PRICE_ALREADY_MET = {
    "level": "{symbol} is already {where} {value:g} (last {last})",
    "pct_up": "{symbol} is already up {move:.2f}% from the previous close (last {last})",
    "pct_down": "{symbol} is already down {move:.2f}% from the previous close (last {last})",
}
LISTING_PAIR = "a crypto, currency or futures symbol; the price monitor cannot watch those"
LISTING_UNREADABLE = "the price monitor cannot read this symbol"
LISTING_UNKNOWN_SUFFIX = "an exchange suffix the price monitor does not know, so it would never be checked"
LISTING_NO_SUFFIX = (
    "a bare number names no market; add its exchange suffix, as in 600519.SH, 830799.BJ, "
    "0700.HK, 7203.T, 005930.KS or 2330.TW"
)


def render_wake_block(lines: Sequence[str], *, started_turn: bool) -> str:
    """The ``<wakeup>`` block for the notices a model call takes in together.

    The quiet hint is only true of a turn the wakeup started itself; inline in
    a running turn the model has a user to answer, so it never sees the hint.
    """
    body = [WAKE_OPEN, WAKE_NOTE, *lines]
    if started_turn:
        body.append(WAKE_QUIET_HINT)
    body.append(WAKE_CLOSE)
    return "\n".join(body)
