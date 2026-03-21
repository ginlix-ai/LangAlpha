---
name: automation
description: Create and manage scheduled and price-triggered automations.
---

# Automation Skill

This skill provides 3 tools for creating and managing scheduled automations:
- `check_automations` - List all or inspect a specific automation
- `create_automation` - Create a new scheduled automation
- `manage_automation` - Update, pause, resume, trigger, or delete automations

You should call these tools directly instead of using ExecuteCode tool.

## Before Creating an Automation

**Always confirm with the user before calling `create_automation`.** Automations run autonomously on a schedule, so getting the details right matters. If the user's request is unclear or underspecified, ask to clarify:

- **Schedule** — "Every morning" is ambiguous. Confirm the exact time and days (e.g. "Weekdays at 9 AM in your timezone?").
- **Thread strategy** — If the task involves ongoing analysis or follow-ups, ask whether they want results in a fresh thread each time, a single persistent thread, or the current conversation.
- **Instruction** — The instruction runs without further user input. If the user gives a vague prompt like "check my portfolio", refine it: what tickers? what metrics? what format?
- **Delivery** — If the user hasn't mentioned how they want to receive results, ask if they want delivery (e.g. Slack) or just in-app.

Summarize what you're about to create and get a "yes" before calling the tool.

---

## Tool 1: check_automations

List all automations or inspect a specific one with execution history.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `automation_id` | str | No | Automation ID to inspect. Omit to list all. |

### Examples

```python
# List all automations
check_automations()

# Inspect a specific automation (includes last 5 executions)
check_automations(automation_id="abc-123")
```

---

## Tool 2: create_automation

Create a new scheduled automation.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Short name for the automation |
| `instruction` | str | Yes | The prompt the agent will execute on each run |
| `schedule` | str | Yes | Cron expression or ISO datetime (see below) |
| `description` | str | No | Optional description |
| `thread` | str | No | `"new"` (default), `"persistent"`, or `"current"` (see Thread Strategy) |
| `delivery` | str | No | Comma-separated delivery methods (e.g. `"slack"`) |

### Thread Strategy

| Mode | Behavior |
|------|----------|
| `"new"` | Fresh thread each run — no conversation history carried over (default) |
| `"persistent"` | Single dedicated thread — all runs share conversation history |
| `"current"` | Pins to the current conversation thread — automation runs continue here |

### Schedule Format

- **Recurring (cron):** Standard 5-field cron expression
  - `0 9 * * 1-5` — weekdays at 9 AM
  - `0 */4 * * *` — every 4 hours
  - `30 8 1 * *` — 1st of each month at 8:30 AM
- **One-time (ISO datetime):**
  - `2026-03-01T10:00:00` — single execution at that time

### Examples

```python
# Daily market briefing on weekdays at 9 AM
create_automation(
    name="Morning Market Brief",
    instruction="Summarize overnight market moves, top gainers/losers, and any news for my watchlist.",
    schedule="0 9 * * 1-5",
)

# One-time earnings reminder
create_automation(
    name="AAPL Earnings Reminder",
    instruction="Analyze AAPL ahead of earnings: recent price action, analyst expectations, key metrics to watch.",
    schedule="2026-04-30T08:00:00",
    description="Pre-earnings analysis for Apple Q2 2026",
)

# Daily report delivered to Slack
create_automation(
    name="Morning Market Brief",
    instruction="Summarize overnight market moves for my watchlist.",
    schedule="0 9 * * 1-5",
    delivery="slack",
)

# Automation with persistent thread (all runs share history)
create_automation(
    name="Weekly Portfolio Review",
    instruction="Review my portfolio performance and update the analysis.",
    schedule="0 9 * * 1",
    thread="persistent",
)

# Automation that continues in the current conversation
create_automation(
    name="Hourly Price Check",
    instruction="Check AAPL, MSFT, GOOGL prices and alert if any moved >2%.",
    schedule="0 * * * *",
    thread="current",
)
```

---

## Price-Triggered Automations

In addition to cron/datetime schedules, automations can trigger when a stock price meets a specific condition. Set `trigger_type="price"` and provide a `trigger_config` dict instead of (or alongside) a `schedule`.

### Condition Types

| Condition | Description |
|-----------|-------------|
| `price_above` | Fires when price rises above the given value |
| `price_below` | Fires when price drops below the given value |
| `pct_change_above` | Fires when percentage change exceeds the given value |
| `pct_change_below` | Fires when percentage change drops below the given (negative) value |

For percentage conditions, `reference` sets the baseline price:

| Reference | Description |
|-----------|-------------|
| `previous_close` | Prior trading day's closing price (default) |
| `day_open` | Current trading day's opening price |

### Retrigger Modes

| Mode | Behavior |
|------|----------|
| `one_shot` | Trigger once, then mark completed (default) |
| `recurring` | Re-arm after cooldown. Omit `cooldown_seconds` for once-per-trading-day default, or set `cooldown_seconds` (min 14400 = 4 hours) for custom interval. |

### Examples

```python
# Alert when AAPL drops below $150 (one-shot)
create_automation(
    name="AAPL Price Alert",
    instruction="AAPL has dropped below $150. Summarize recent news and analyst sentiment.",
    trigger_type="price",
    trigger_config={
        "symbol": "AAPL",
        "conditions": [{"type": "price_below", "value": 150}],
    },
)

# Run analysis when TSLA moves up 5% from yesterday's close
create_automation(
    name="TSLA Momentum Alert",
    instruction="TSLA is up 5% from yesterday's close. Analyze volume, technicals, and any catalysts.",
    trigger_type="price",
    trigger_config={
        "symbol": "TSLA",
        "conditions": [
            {"type": "pct_change_above", "value": 5, "reference": "previous_close"},
        ],
    },
)

# Recurring alert with 4-hour cooldown
create_automation(
    name="BTC Volatility Watch",
    instruction="BTC moved more than 3% from today's open. Summarize order flow and sentiment.",
    trigger_type="price",
    trigger_config={
        "symbol": "BTC-USD",
        "conditions": [
            {"type": "pct_change_above", "value": 3, "reference": "day_open"},
        ],
        "retrigger": {"mode": "recurring", "cooldown_seconds": 14400},
    },
)
```

### Agent Guidelines for Price Triggers

- **Confirm before creating.** Always repeat the symbol, condition, threshold value, and retrigger mode back to the user and get explicit confirmation.
- **Default to `one_shot`** retrigger mode unless the user asks for repeated alerts. For recurring, omit cooldown_seconds to default to once per trading day.
- **Use flash mode** by default for price-triggered automations (lightweight, low-latency execution).

---

## Tool 3: manage_automation

Manage an existing automation.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `automation_id` | str | Yes | Automation ID to manage |
| `action` | str | Yes | One of: `update`, `pause`, `resume`, `trigger`, `delete` |
| `name` | str | No | New name (update only) |
| `description` | str | No | New description (update only) |
| `instruction` | str | No | New prompt (update only) |
| `schedule` | str | No | New cron or ISO datetime (update only) |
| `thread` | str | No | `"new"`, `"persistent"`, or `"current"` (update only) |
| `delivery` | str | No | Comma-separated delivery methods (update only) |
| `remove_delivery` | bool | No | Set to `true` to remove delivery config (update only) |

### Action Reference

| Action | Description |
|--------|-------------|
| `update` | Change name, description, instruction, schedule, thread strategy, or delivery |
| `pause` | Temporarily stop the automation from running |
| `resume` | Re-enable a paused automation |
| `trigger` | Run the automation immediately (outside normal schedule) |
| `delete` | Permanently remove the automation |

### Examples

```python
# Pause an automation
manage_automation(automation_id="abc-123", action="pause")

# Resume it
manage_automation(automation_id="abc-123", action="resume")

# Trigger an immediate run
manage_automation(automation_id="abc-123", action="trigger")

# Update the schedule to run every Monday at 8 AM
manage_automation(
    automation_id="abc-123",
    action="update",
    schedule="0 8 * * 1",
)

# Switch an automation to a persistent thread
manage_automation(automation_id="abc-123", action="update", thread="persistent")

# Remove delivery from an automation
manage_automation(automation_id="abc-123", action="update", remove_delivery=True)

# Delete an automation
manage_automation(automation_id="abc-123", action="delete")
```
