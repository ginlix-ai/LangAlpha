---
name: onboarding
description: First-time user onboarding to set up investment profile, watchlists, portfolio, and preferences.
---

# Onboarding Skill

## Purpose

Help new users set up their investment profile through a natural, conversational flow. The agent gathers preferences and stores them as rich, descriptive text that future conversations can reference for personalized advice.

This skill provides 5 tools:
- `get_user_data` - Read user data
- `update_user_data` - Create or update user data
- `remove_user_data` - Delete user data
- `manage_workspaces` - Create workspaces (via action="create")
- `ptc_agent` - Dispatch a research question to a workspace

You should call these tools directly instead of using ExecuteCode tool.

---

## Tool Reference

### Tool 1: get_user_data

Retrieve user data by entity type.

| Entity | Description | entity_id |
|--------|-------------|-----------|
| `all` | Complete user data (profile, preferences, watchlists with items, portfolio) | Not used |
| `profile` | User info (name, timezone, locale) | Not used |
| `preferences` | All preferences (risk, investment, agent) | Not used |
| `watchlists` | List of all watchlists | Not used |
| `watchlist_items` | Items in a specific watchlist | Optional watchlist_id |
| `portfolio` | All portfolio holdings | Not used |

```python
# Get complete user data (recommended at start of onboarding)
get_user_data(entity="all")
```

### Tool 2: update_user_data

Create or update user data (upsert semantics). Preference entities merge by default.

| Entity | Description |
|--------|-------------|
| `profile` | User info (name, timezone, locale, onboarding_completed) |
| `risk_preference` | Risk tolerance settings |
| `investment_preference` | Investment style settings |
| `agent_preference` | Agent behavior settings |
| `watchlist` | Create or update a watchlist |
| `watchlist_item` | Add or update item in watchlist |
| `portfolio_holding` | Add or update a portfolio holding |

All preference fields accept **any descriptive string**. Extra fields are allowed and persisted.

```python
# Good - rich context that helps future conversations
update_user_data(entity="risk_preference", data={
    "risk_tolerance": "Moderate - comfortable with market swings but avoids concentrated bets",
    "notes": "Lost money in 2022 tech crash, now prefers diversification"
})

# Bad - keyword with no context
update_user_data(entity="risk_preference", data={"risk_tolerance": "medium"})
```

### Tool 3: remove_user_data

Delete user data by entity type.

| Entity | Identifier fields |
|--------|-------------------|
| `watchlist` | `watchlist_id` or `name` |
| `watchlist_item` | `symbol` (+ optional `watchlist_id`) |
| `portfolio_holding` | `symbol` (+ optional `account_name`) |

### Tool 4: manage_workspaces (action="create")

Create the user's first workspace. This requires user approval — the user sees a card and must approve.

| Parameter | Type | Description |
|-----------|------|-------------|
| `action` | string | Must be `"create"` |
| `name` | string | Name for the workspace (e.g. "My Portfolio Analysis") |
| `description` | string | Brief description of the workspace purpose |

```python
manage_workspaces(action="create", name="My Portfolio Analysis", description="Track and analyze my stock portfolio")
```

Returns `{ success: true, workspace_id: "...", workspace_name: "..." }` on approval, or `"User declined workspace creation."` on rejection.

### Tool 5: ptc_agent

Dispatch a personalized research question to a workspace. This requires user approval — the user sees the question and can approve to start the analysis.

| Parameter | Type | Description |
|-----------|------|-------------|
| `question` | string | An actionable question related to the user's interests |
| `workspace_id` | string | The workspace ID (from `manage_workspaces` result) |

```python
ptc_agent(
    question="Analyze my NVDA position — what's the current technical setup and any upcoming catalysts I should watch for?",
    workspace_id="abc-123"
)
```

Returns `{ success: true, workspace_id: "...", thread_id: "...", status: "dispatched" }` on approval, or `"User declined research dispatch."` on rejection.

---

## What to Gather

### Stocks (Required, Structured)

At least one stock must be added to the watchlist or portfolio before onboarding can complete. Use the structured `watchlist_item` or `portfolio_holding` entities.

```python
# Watchlist item
update_user_data(entity="watchlist_item", data={
    "symbol": "NVDA", "notes": "Watching for AI chip growth"
})

# Portfolio holding
update_user_data(entity="portfolio_holding", data={
    "symbol": "AAPL", "quantity": 50, "average_cost": 175.0
})
```

### Risk & Investment Profile (Required, Flexible)

Gather enough context so future conversations can give personalized advice. At minimum, capture `risk_tolerance` on `risk_preference`. Topics to explore:

- **Risk comfort** - How much volatility can they handle? Any past experiences that shaped their risk view?
- **Investment style** - Growth, value, income, ESG? Any sectors they avoid or focus on?
- **Time horizon** - Short-term trading, long-term holding, or flexible?
- **Analysis preference** - Do they care most about growth metrics, valuation, competitive moat, or risk factors?

Store these as descriptive text across `risk_preference` and `investment_preference`:

```python
update_user_data(entity="risk_preference", data={
    "risk_tolerance": "Conservative - prioritizes capital preservation, uncomfortable with >10% drawdowns",
    "notes": "Nearing retirement in 5 years, shifting from growth to income"
})

update_user_data(entity="investment_preference", data={
    "company_interest": "Dividend-paying blue chips and REITs for income",
    "holding_period": "Long-term (5+ years), rarely sells",
    "analysis_focus": "Dividend sustainability, payout ratio, and balance sheet strength",
    "avoid_sectors": "Crypto, speculative biotech"
})
```

### Agent Preferences (Optional, Flexible)

How does the user want the agent to behave? Topics to explore:

- **Output style** - Quick bullet points, balanced summaries, or deep dives?
- **Visualization** - Always include charts, only when helpful, or prefer text?
- **Proactive questions** - Should the agent ask before acting, use its judgment, or only ask when critical?
- **Anything else** - Notes, instructions, preferences the user wants remembered.

```python
update_user_data(entity="agent_preference", data={
    "output_style": "Balanced summary with key numbers highlighted",
    "data_visualization": "Include charts when comparing multiple stocks",
    "proactive_questions": "Use your judgment, only ask when the decision significantly impacts the analysis",
    "instruction": "Always mention if a stock has upcoming earnings within 2 weeks"
})
```

---

## Conversation Guide

### Always Use AskUserQuestion

Present options so the user can tap instead of type. Options are **starting points for richer conversation**, not rigid mappings. After the user selects an option, capture the full context of their choice (including any follow-up detail) as descriptive text.

Example AskUserQuestion options by topic:

**Risk comfort:**
- "Conservative - protect my capital"
- "Moderate - balanced risk and reward"
- "Aggressive - maximize growth potential"
- "I have a nuanced view"

**Investment style:**
- "Growth companies with strong momentum"
- "Stable dividend payers for income"
- "Undervalued opportunities"
- "ESG / sustainable investing"

**Time horizon:**
- "Short-term (under 1 year)"
- "Medium-term (1-5 years)"
- "Long-term (5+ years)"
- "Flexible - depends on the opportunity"

**Analysis preference:**
- "Focus on growth metrics (revenue, earnings growth)"
- "Focus on valuation (P/E, DCF)"
- "Focus on competitive moat and market position"
- "Focus on risk factors and downside protection"

**Output style:**
- "Quick bullet points - just the highlights"
- "Balanced summary with supporting data"
- "In-depth deep dive with full analysis"
- "Data-heavy with charts and numbers"

**Visualization:**
- "Always include charts and visuals"
- "Include when it helps explain something"
- "Prefer text-only analysis"

**Proactive questions:**
- "Ask me before making decisions"
- "Use your judgment most of the time"
- "Only ask when it's critical"

### Storing Responses

After the user selects an option (or provides a custom answer), store the **descriptive text**, not a keyword:

```python
# User selected "Moderate - balanced risk and reward" and added
# "but I get nervous during big market drops"
update_user_data(entity="risk_preference", data={
    "risk_tolerance": "Moderate - balanced risk and reward, but gets nervous during big market drops"
})
```

### Conversation Flow

1. **Start** - Greet the user, explain what you'll set up, and ask about stocks they're watching or own.
2. **Stocks** - Add their stocks to watchlist/portfolio. Ask follow-up for holdings (quantity, cost basis).
3. **Risk & Investment** - Use AskUserQuestion for each topic. Follow up naturally for more detail.
4. **Agent Preferences** - Optional. Ask about output style, visualization, proactive questions.
5. **Open-ended** - "Anything else I should know about how you like to work?"
6. **Complete** - Summarize what was set up, mark onboarding complete.
7. **Workspace & Question** - After completing onboarding, create a workspace using `manage_workspaces(action="create", name="...", description="...")` with a name and description that fits the user's interests. Then use `ptc_agent(question="...", workspace_id="...")` with the returned `workspace_id` to dispatch an actionable starter question based on the user's stocks or interests. The question should be specific and immediately useful (e.g. "Analyze my NVDA position — what are the key technical levels and upcoming catalysts?" rather than "Tell me about stocks").

Don't ask all questions at once. Let the conversation flow naturally. If the user wants to skip optional topics, respect that.

### Not Exhaustive

The listed topics are a starting point. If the conversation naturally reveals other preferences (e.g., specific sectors to avoid, earnings season behavior, news sensitivity), store those too. Any extra fields are accepted via `extra="allow"` on the models.

---

## Completion Requirements

Before marking onboarding complete, verify:
1. At least one stock was added (watchlist or portfolio)
2. Risk preference was set (any truthy value in `risk_preference`)

```python
# Mark onboarding complete
update_user_data(entity="profile", data={"onboarding_completed": true})
```

If missing:
- **No stocks:** "Before we finish, let's add at least one stock you're interested in. What's a stock you're watching or own?"
- **No risk preference:** "One more thing - I'd like to understand your risk comfort level so I can tailor my advice."

---

## Tips

1. **Be conversational** - Don't interrogate. Let topics flow naturally and combine related questions.
2. **Use AskUserQuestion for choices** - Always present options as selectable buttons. Only use plain text for open-ended input (stock symbols, quantities, notes).
3. **Handle partial info** - If the user says "I own some AAPL", follow up for quantity and cost basis.
4. **Confirm entries** - After saving, briefly confirm: "Added AAPL (50 shares @ $175) to your portfolio."
5. **Capture context, not keywords** - The user's words and nuances are more valuable than a one-word category.
6. **Use defaults** - If user doesn't specify a watchlist, items go to the default one automatically.
7. **Respect skips** - Investment preferences and agent preferences are optional. Don't push if the user wants to move on.

---

## Error Handling

- If a stock is already in a watchlist, inform the user and offer alternatives
- If a holding already exists, offer to update it instead of creating a duplicate
- If user_id is not available, inform that the user needs to be logged in
