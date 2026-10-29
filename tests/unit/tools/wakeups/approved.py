"""The approved model-facing wording, copied from the reviewed drafts as literals.

A change to any string here is a change to prompt surface and needs the same
review the draft had.
"""

TOOL_RESULTS = {
    "none": "No pending wakeups.",
    "cancelled": "Cancelled: w_3f2a (NVDA pullback entry: cron */5 10-15 * * 1-5 in "
    "America/New_York), w_91c0 (NVDA breakdown: NVDA below $198.00).",
    "cancel_unknown": "w_77aa: no pending wakeup with that id.",
    "cancel_late": "w_91c0 already fired and is on its way to you, so it can't be cancelled.",
    "refused_header": "Not set:",
    "refused_one_of": "- set exactly one of at, cron, price, order.",
    "refused_cron_gap": "- cron */2 * * * *: fires 2 minutes apart; at least 5.",
    "refused_cron_misread": "- cron '0 0 9 * * 5' has 6 fields; use five: minute hour day-of-month "
    "month day-of-week (seconds aren't supported)",
    "refused_cron_none": "- cron 0 9 30 2 * in Asia/Shanghai: never fires in the next 7 days.",
    "refused_timezone": "- timezone Eastern: not an IANA timezone; use a name like America/New_York.",
    "refused_timezone_abbrev": "- timezone EST: a fixed offset that ignores daylight saving; use a "
    "name like America/New_York.",
    "refused_until": "- until 2026-10-20T09:00: already past.",
    "refused_pct_from": "- price: pct_from goes with pct_up or pct_down.",
    "refused_soon": "- at 2026-10-20T15:40: under a minute from now or already past (now 15:40 EDT).",
    "refused_horizon": "- at 2026-11-02T09:00: more than 7 days ahead.",
    "refused_order_unknown": "- order 7712046: no open order with that id was placed in this "
    "conversation.",
    "refused_order_unreadable": "- order 5520: IBKR staged instructions can't be read back, so an "
    "order wakeup can't see them; set a timer instead.",
    "refused_label": "- label: at most 40 characters.",
    "refused_unknown_field": "- title: not a field; name a wakeup with label.",
    "refused_cap": "- this conversation already has 10 pending wakeups; cancel one first.",
    "quiet_refused": "quiet only ends a turn a wakeup started; this turn has a user message, so reply "
    "to it.",
    "quiet_ok": "Ended without a reply.",
}

BUILD_TOOL_RESULTS = {
    "top_header": "Not done:",
    "unknown_arg": "- {k}: not an argument; schedule_wakeup takes triggers, cancel and quiet.",
    "triggers_not_list": "- triggers: a list of trigger entries.",
    "quiet_not_text": "- quiet: one short line of text.",
    "quiet_refused_acted": "quiet ends a check that found nothing to tell; this turn acted on an order "
    "or handed work to an analyst, so tell the user what you did.",
    "quiet_refused_calls": "quiet ends the turn at once, so send it on its own after you have read "
    "what your other calls returned.",
    "cancel_arg": '- cancel: one wakeup id, a list of them, or "all".',
    "until_needs_cron": "- until needs cron.",
    "at_unparseable": "- at {s}: not an ISO 8601 time.",
    "until_unparseable": "- until {s}: not an ISO 8601 time.",
    "cron_invalid": "- cron {expr}: not a valid cron expression.",
    "cron_until_none": "- cron {expr} in {zone}: never fires before until {raw}.",
    "unknown_field_other": "- {key}: not a field; a trigger takes label, at, cron, until, timezone, "
    "price, order.",
    "price_one_kind": "- price: set exactly one of above, below, pct_up, pct_down.",
    "price_no_live": '- {desc}: no live price to measure "now" from; leave pct_from out to measure '
    "from the previous close, or set a price level.",
    "save_failed": "- {desc}: could not be saved, so it is not set.",
    "order_readback_off": "- order {id}: this server doesn't read order status back, so an order "
    "wakeup can't see it; set a timer instead.",
    "order_vendor_unreadable": "- order {id}: {vendor} orders can't be read back, so an order wakeup "
    "can't see them; set a timer instead.",
    "setting_off": "- wakeups are turned off in Settings, so this was not set.",
    "cancelled_in_flight": "Cancelled: {desc}; its check {n} had already fired and still reaches you.",
    "trigger_not_object": "- each trigger is an object with one of at, cron, price, order.",
    "nested_unknown": "- {path}: not a field.",
    "required": "- {path}: required.",
    "generic": "- {path}: {message}.",
    "cron_too_long": "- cron: at most 120 characters.",
}

CONFIRM_RESULTS = {
    "set": "Set w_3f2a, w_4c11, w_6a08, w_91c0, w_5d1e. Pending wakeups (the user is in "
    "Asia/Shanghai, now Tue 21:57):\n"
    "- w_3f2a (NVDA pullback entry): cron */5 10-15 * * 1-5 in America/New_York. 24 checks, "
    "stopping at the 24-check limit: first Tue 2026-10-20 10:00 EDT, in 3 min (Tue 22:00 for "
    "the user); last Tue 2026-10-20 11:55 EDT (Tue 23:55 for the user).\n"
    "- w_4c11 (Hourly NVDA update): cron 40 * * * * in America/New_York until Tue 2026-10-20 "
    "16:00 EDT. 6 checks, stopping at until: first Tue 2026-10-20 10:40 EDT, in 43 min (Tue "
    "22:40 for the user); last Tue 2026-10-20 15:40 EDT (Wed 03:40 for the user).\n"
    "- w_6a08 (Hourly NVDA check): cron 0 10-16 * * 1-5 in America/New_York. 24 checks, "
    "stopping at the 24-check limit: first Tue 2026-10-20 10:00 EDT, in 3 min (Tue 22:00 for "
    "the user); last Fri 2026-10-23 12:00 EDT (Sat 00:00 for the user). By day, EDT: Tue 10:00 "
    "to 16:00 every hour (7), Wed 10:00 to 16:00 every hour (7), Thu 10:00 to 16:00 every hour "
    "(7), Fri 10:00 to 12:00 every hour (3).\n"
    "- w_91c0 (NVDA breakdown): NVDA below $198.00; lapses Wed 2026-10-21 16:00 EDT (Thu 04:00 "
    "for the user).\n"
    "- w_5d1e (Morning brief): at Wed 2026-10-21 08:30 CST in Asia/Shanghai, the user's "
    "timezone, in 10 h 33 min.",
    "pending": "Pending wakeups (the user is in Asia/Shanghai, now Tue 22:31):\n"
    "- w_3f2a (NVDA pullback entry): cron */5 10-15 * * 1-5 in America/New_York. 7 of 24 "
    "checks done; next Tue 2026-10-20 10:35 EDT, in 4 min (Tue 22:35 for the user); last "
    "Tue 2026-10-20 11:55 EDT (Tue 23:55 for the user), the 24-check limit.\n"
    "- w_91c0 (NVDA breakdown): NVDA below $198.00; lapses Wed 2026-10-21 16:00 EDT (Thu "
    "04:00 for the user).\n"
    "- w_0be7 (NVDA entry order): order 7712045 (buy 100 NVDA, limit $200.00, at moomoo) "
    "fills or ends; lapses Tue 2026-10-27 10:12 EDT if it does neither.",
}

WAKE_LINES = {
    "time": "It is {fire_time}, the time you set at {set_time}.",
    "series": "Check {n} of {total}{label_paren} of the series you set at {set_time} ({cron} in "
    "{timezone}). Next check at {next_time}.",
    "series_skipped": " {skipped} checks were skipped while this one waited to reach you.",
    "series_last": "Check {n} of {total}{label_paren}, the last of the series you set at {set_time} "
    "({cron} in {timezone}). Set a new series if the task still needs it.",
    "price_hit": "{symbol} hit your trigger ({phrase}): it traded at {price} at {fire_time}. You set "
    "it at {set_time}; it will not fire again.",
    "price_expiring": "Your trigger on {symbol} ({phrase}) has not hit and lapses at {expires_at}. "
    "Last price: {last}. Set it again to keep it; otherwise let it lapse.",
    "order_filled": "Your order filled: {order} (order {order_id}), {qty} at an average {avg} at "
    "{fire_time}.",
    "order_partial": "Your order partly filled: {order} (order {order_id}), {filled} of {qty} at an "
    "average {avg} so far, and the rest is still working. Set an order wakeup again "
    "to hear when it finishes.",
    "order_cancelled": "Your order ended unfilled: {order} (order {order_id}) was cancelled at "
    "{fire_time}.",
    "order_rejected": "Your order ended unfilled: {order} (order {order_id}) was rejected by the "
    "brokerage: {reason}.",
}

BUILD_WAKE_LINES = {
    "series_skipped_one": " 1 check was skipped while this one waited to reach you.",
    "order_partial_cancelled": "Your order ended partly filled: {order} (order {order_id}), {filled} "
    "of {qty} at an average {avg}, then was cancelled at {fire_time}.",
    "order_failed": "Your order ended unfilled: {order} (order {order_id}) failed: {reason}.",
    "order_partial_expired": "Your order ended partly filled: {order} (order {order_id}), {filled} "
    "of {qty} at an average {avg}, then expired at {fire_time}.",
    "order_expired": "Your order ended unfilled: {order} (order {order_id}) expired at {fire_time}.",
    "order_confirmed": "Your order was confirmed at the brokerage and sent there as a live "
    "order: {order} (order {order_id}), at {fire_time}. Its fills show in the brokerage's own "
    "orders.",
    "order_handed_off": "Your staged order left the brokerage's instruction list: {order} (order "
    "{order_id}) was sent, deleted or expired there by {fire_time}, and the brokerage does not "
    "say which.",
    # Not in drafts.py's BUILD_WAKE_LINES, which only says in a comment that the
    # line drops "Last price" when no price is known; the wording is that line.
    "price_expiring_unpriced": "Your trigger on {symbol} ({phrase}) has not hit and lapses at "
    "{expires_at}. Set it again to keep it; otherwise let it lapse.",
}

WAKE_QUIET_HINT = (
    "This turn started on its own: if it finds nothing the user needs to hear, end it with "
    'schedule_wakeup(quiet="<what you checked and found>") instead of replying.'
)

WAKE_NOTE = "Automated wakeup you set with schedule_wakeup (not a user message)."

CANCEL_ROW_ONE = (
    "The user cancelled a wakeup you set: {label}. Leave it cancelled unless the user asks for it "
    "again."
)

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
