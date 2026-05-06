---
name: user-profile
description: Manage user profile including watchlists, portfolio, and preferences.
---

# User Profile Skill

This skill provides 3 unified tools for managing user data:
- `get_user_data` - Read user data
- `update_user_data` - Create or update user data
- `remove_user_data` - Delete user data

You should call these tools directly instead of using ExecuteCode tool.

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

The `data` dict accepts any fields. Extra fields like `notes`, `instruction`, `avoid_sectors` are stored alongside named fields.

```python
# Merge with existing (default behavior)
update_user_data(entity="agent_preference", data={
    "output_style": "Balanced summary with key numbers highlighted",
    "notes": "User prefers brevity"
})

# Replace entire preference (delete all existing fields, set only new ones)
update_user_data(entity="agent_preference", data={
    "output_style": "In-depth deep dive with full analysis"
}, replace=True)
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

Set risk tolerance settings. All fields accept any descriptive string.

| Field | Type | Description |
|-------|------|-------------|
| `risk_tolerance` | str | Risk tolerance description (any text) |
| *(extra fields)* | any | Additional context (notes, constraints, etc.) |

```python
# Descriptive risk preference
update_user_data(
    entity="risk_preference",
    data={
        "risk_tolerance": "Moderate - comfortable with market swings but avoids concentrated bets",
        "notes": "Prefers diversification after 2022 tech losses"
    }
)
```

### Entity: investment_preference

Set investment style settings. All fields accept any descriptive string. At least one field is required.

| Field | Type | Description |
|-------|------|-------------|
| `company_interest` | str | Type of companies interested in (any text) |
| `holding_period` | str | Preferred holding period (any text) |
| `analysis_focus` | str | Primary analysis focus area (any text) |
| *(extra fields)* | any | Additional context (avoid_sectors, focus_sectors, notes, etc.) |

```python
# Full investment profile with rich descriptions
update_user_data(
    entity="investment_preference",
    data={
        "company_interest": "Dividend-paying blue chips and REITs for income",
        "holding_period": "Long-term (5+ years), rarely sells",
        "analysis_focus": "Dividend sustainability and balance sheet strength",
        "avoid_sectors": "Crypto, speculative biotech"
    }
)
```

### Entity: agent_preference

Set agent behavior settings. All fields accept any descriptive string.

| Field | Type | Description |
|-------|------|-------------|
| `output_style` | str | Preferred output style (any text) |
| `data_visualization` | str | Chart/visualization preferences (any text) |
| `proactive_questions` | str | When to ask clarifying questions (any text) |
| *(extra fields)* | any | Additional context (instruction, notes, etc.) |

```python
# Rich agent preferences
update_user_data(
    entity="agent_preference",
    data={
        "output_style": "Balanced summary with key numbers highlighted",
        "data_visualization": "Include charts when comparing multiple stocks",
        "proactive_questions": "Use your judgment, only ask when critical"
    }
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
| `instrument_type` | str | No | Free-form. Common: "stock", "etf", "index", "crypto", "future", "commodity", "currency". Other values accepted (default: "stock") |
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
| `instrument_type` | str | No | Free-form. Common: "stock", "etf", "index", "crypto", "future", "commodity", "currency". Other values accepted (default: "stock") |
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

## Error Handling

- If a stock is already in a watchlist, inform the user and offer alternatives
- If a holding already exists, offer to update it instead of creating a duplicate
- If user_id is not available, inform that the user needs to be logged in
