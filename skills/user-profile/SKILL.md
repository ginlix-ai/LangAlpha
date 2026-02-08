---
name: user-profile
description: Manage user profile including watchlists, portfolio, and preferences. Also contains onboarding instructions.
---

# User Profile Skill

This skill provides 3 unified tools for managing user data:
- `get_user_data` - Read user data
- `update_user_data` - Create or update user data
- `remove_user_data` - Delete user data

The tools are hidden by default and will be available to you after loading the skill.
To load the skill, use the `LoadSkill` tool with the skill name "user-profile".
You should call the tools being loaded directly instead of using ExecuteCode tool.

For first-time user setup, see [onboarding.md](./onboarding.md).

---

## Tool 1: get_user_data

Retrieve user data by entity type.

### Entities

| Entity | Description | entity_id |
|--------|-------------|-----------|
| `all` | Complete user data (profile, preferences, watchlists with items, portfolio) | Not used |
| `profile` | User info (name, timezone, locale) | Not used |
| `preferences` | All preferences (risk, investment, agent) | Not used |
| `watchlists` | List of all watchlists | Not used |
| `watchlist_items` | Items in a specific watchlist | Optional watchlist_id |
| `portfolio` | All portfolio holdings | Not used |

### Examples

```python
# Get complete user data (recommended for initial context)
get_user_data(entity="all")
# Returns: {
#   "profile": {"name": "John", "timezone": "America/New_York", "locale": "en-US"},
#   "preferences": {"risk_preference": {...}, "investment_preference": {...}, ...},
#   "watchlists": [{"name": "Tech Stocks", "items": [...], ...}],
#   "portfolio": [{"symbol": "AAPL", "quantity": 50, ...}]
# }

# Get user profile
get_user_data(entity="profile")
# Returns: {"name": "John", "timezone": "America/New_York", "locale": "en-US"}

# Get all preferences
get_user_data(entity="preferences")
# Returns: {"risk_preference": {...}, "investment_preference": {...}, "agent_preference": {...}}

# Get all watchlists
get_user_data(entity="watchlists")
# Returns: [{"watchlist_id": "abc", "name": "Tech Stocks", "is_default": true}, ...]

# Get items from default watchlist
get_user_data(entity="watchlist_items")
# Returns: [{"symbol": "AAPL", "notes": "..."}, {"symbol": "NVDA", ...}]

# Get items from specific watchlist
get_user_data(entity="watchlist_items", entity_id="abc-123")

# Get portfolio holdings
get_user_data(entity="portfolio")
# Returns: [{"symbol": "AAPL", "quantity": 50, "average_cost": 175.0}, ...]
```

---

## Tool 2: update_user_data

Create or update user data (upsert semantics).

### Common Options for Preferences

All preference entities (`risk_preference`, `investment_preference`, `agent_preference`) support:

| Parameter | Type | Description |
|-----------|------|-------------|
| `replace` | bool | If `True`, completely replace the preference instead of merging with existing data |

The `data` dict also accepts extra fields like `notes`, `instruction`, `avoid_sectors`, etc.

```python
# Merge with existing (default behavior)
update_user_data(entity="agent_preference", data={"output_style": "quick", "notes": "User prefers brevity"})

# Replace entire preference (delete all existing fields, set only new ones)
update_user_data(entity="agent_preference", data={"output_style": "deep_dive"}, replace=True)
```

### Entity: profile

Update user profile info.

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Display name |
| `timezone` | str | e.g., "America/New_York" |
| `locale` | str | Preferred language, e.g., "en-US", "zh-CN" |
| `onboarding_completed` | bool | Mark onboarding done (write-only, not returned in get) |

```python
# Update display name
update_user_data(entity="profile", data={"name": "John Doe"})

# Mark onboarding complete
update_user_data(entity="profile", data={"onboarding_completed": True})
```

### Entity: risk_preference

Set risk tolerance settings.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `risk_tolerance` | str | **Yes** | "low", "medium", "high", "long_term_focus" |

```python
# Set risk preference
update_user_data(
    entity="risk_preference",
    data={"risk_tolerance": "medium"}
)

# Long-term investor comfortable with volatility
update_user_data(
    entity="risk_preference",
    data={"risk_tolerance": "long_term_focus"}
)
```

### Entity: investment_preference

Set investment style settings. At least one field is required.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `company_interest` | str | No* | "growth", "stable", "value", "esg" |
| `holding_period` | str | No* | "short_term", "mid_term", "long_term", "flexible" |
| `analysis_focus` | str | No* | "growth", "valuation", "moat", "risk" |

*At least one field must be provided.

```python
# Set company interest type
update_user_data(
    entity="investment_preference",
    data={"company_interest": "growth"}
)

# Full investment profile
update_user_data(
    entity="investment_preference",
    data={
        "company_interest": "value",
        "holding_period": "long_term",
        "analysis_focus": "moat"
    }
)
```

### Entity: agent_preference

Set agent behavior settings.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `output_style` | str | **Yes** | "summary", "data", "deep_dive", "quick" |

```python
# Quick summaries for busy user
update_user_data(
    entity="agent_preference",
    data={"output_style": "quick"}
)

# Detailed analysis with data focus
update_user_data(
    entity="agent_preference",
    data={"output_style": "deep_dive"}
)
```

### Entity: watchlist

Create or update a watchlist.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | str | Yes | Watchlist name (used as key for upsert) |
| `description` | str | No | Purpose of the watchlist |
| `is_default` | bool | No | Set as default watchlist |

```python
# Create a watchlist
update_user_data(
    entity="watchlist",
    data={"name": "AI Companies", "description": "Companies focused on AI"}
)

# Create and set as default
update_user_data(
    entity="watchlist",
    data={"name": "My Watchlist", "is_default": True}
)
```

### Entity: watchlist_item

Add or update an item in a watchlist.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | str | Yes | Stock symbol (used as key) |
| `watchlist_id` | str | No | Target watchlist (uses default if omitted) |
| `instrument_type` | str | No | "stock", "etf", "index", "crypto" (default: "stock") |
| `exchange` | str | No | e.g., "NASDAQ" |
| `name` | str | No | Company name |
| `notes` | str | No | Why you're watching |

```python
# Add to default watchlist
update_user_data(
    entity="watchlist_item",
    data={"symbol": "NVDA", "notes": "Watching for AI chip growth"}
)

# Add to specific watchlist with full details
update_user_data(
    entity="watchlist_item",
    data={
        "symbol": "AAPL",
        "watchlist_id": "abc-123",
        "name": "Apple Inc.",
        "exchange": "NASDAQ",
        "notes": "iPhone revenue growth"
    }
)

# Add an ETF
update_user_data(
    entity="watchlist_item",
    data={"symbol": "QQQ", "instrument_type": "etf", "notes": "Tech exposure"}
)
```

### Entity: portfolio_holding

Add or update a portfolio holding.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | str | Yes | Stock symbol (used as key) |
| `quantity` | float | Yes | Number of shares |
| `average_cost` | float | No | Cost per share |
| `account_name` | str | No | e.g., "Robinhood", "Fidelity IRA" (part of key) |
| `instrument_type` | str | No | Default: "stock" |
| `currency` | str | No | Default: "USD" |
| `notes` | str | No | Additional notes |

```python
# Add basic holding
update_user_data(
    entity="portfolio_holding",
    data={"symbol": "AAPL", "quantity": 50, "average_cost": 175.0}
)

# Add holding with account
update_user_data(
    entity="portfolio_holding",
    data={
        "symbol": "VTI",
        "quantity": 100,
        "average_cost": 220.50,
        "account_name": "Fidelity 401k",
        "instrument_type": "etf",
        "notes": "Long-term retirement holding"
    }
)

# Same symbol in different accounts
update_user_data(
    entity="portfolio_holding",
    data={"symbol": "MSFT", "quantity": 25, "account_name": "Robinhood"}
)
update_user_data(
    entity="portfolio_holding",
    data={"symbol": "MSFT", "quantity": 50, "account_name": "Schwab IRA"}
)
```

---

## Tool 3: remove_user_data

Delete user data by entity type.

### Entity: watchlist

Delete an entire watchlist.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `watchlist_id` | str | Either | Watchlist ID |
| `name` | str | Either | Watchlist name |

```python
# Delete by ID
remove_user_data(
    entity="watchlist",
    identifier={"watchlist_id": "abc-123"}
)

# Delete by name
remove_user_data(
    entity="watchlist",
    identifier={"name": "Tech Stocks"}
)
```

### Entity: watchlist_item

Remove an item from a watchlist.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | str | Yes | Stock symbol |
| `watchlist_id` | str | No | Uses default if omitted |

```python
# Remove from default watchlist
remove_user_data(
    entity="watchlist_item",
    identifier={"symbol": "NVDA"}
)

# Remove from specific watchlist
remove_user_data(
    entity="watchlist_item",
    identifier={"symbol": "AAPL", "watchlist_id": "abc-123"}
)
```

### Entity: portfolio_holding

Remove a portfolio holding.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | str | Yes | Stock symbol |
| `account_name` | str | No | For disambiguation if same symbol in multiple accounts |

```python
# Remove holding (when only one account)
remove_user_data(
    entity="portfolio_holding",
    identifier={"symbol": "AAPL"}
)

# Remove from specific account
remove_user_data(
    entity="portfolio_holding",
    identifier={"symbol": "MSFT", "account_name": "Robinhood"}
)
```

---

## Conversation Tips

1. **Be conversational** - Don't ask all questions at once. Let the conversation flow naturally.

2. **Provide context** - When asking about risk tolerance, explain what each level means:
   - Conservative: Prefer stability, avoid volatile stocks
   - Moderate: Balance between growth and stability
   - Aggressive: Willing to accept high volatility for growth potential

3. **Handle partial info** - If user mentions "I own some AAPL", ask follow-up:
   - "How many shares do you have?"
   - "What's your average cost per share?"
   - "Which brokerage account is it in?"

4. **Confirm entries** - After saving, confirm what was added:
   - "Added AAPL (50 shares @ $175) to your portfolio."

5. **Use defaults** - If user doesn't have a watchlist, items go to the default one automatically.

---

## Error Handling

- If a stock is already in a watchlist, inform the user and offer alternatives
- If a holding already exists, offer to update it instead of creating a duplicate
- If user_id is not available, inform that the user needs to be logged in
