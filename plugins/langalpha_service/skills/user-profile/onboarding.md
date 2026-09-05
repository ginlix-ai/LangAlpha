# User Onboarding Procedure

This document describes the step-by-step procedure for onboarding new users via chat.

## Overview

Onboarding helps users set up their investment profile so the agent can provide personalized advice.

### Required Information (MUST HAVE)

1. **At least one stock** (in watchlist or portfolio)
2. **Risk preference** (tolerance level)

The user **must** provide both before onboarding can be completed.

### Optional Preferences

Company Interest, Holding Period, Analysis Focus, and Output Style are **optional**. The user may skip any or all of these.

### Preference Entities and API Reference

Settings are stored via `update_user_data`. API details: `docs/api/50-users/update-preferences.yml` and `docs/api/50-users/get-preferences.yml`.

| Setting | Entity | Field | Valid Values |
|---------|--------|-------|--------------|
| Risk Tolerance | `risk_preference` | `risk_tolerance` | "low", "medium", "high", "long_term_focus" |
| Company Interest | `investment_preference` | `company_interest` | "growth", "stable", "value", "esg" |
| Holding Period | `investment_preference` | `holding_period` | "short_term", "mid_term", "long_term", "flexible" |
| Analysis Focus | `investment_preference` | `analysis_focus` | "growth", "valuation", "moat", "risk" |
| Output Style | `agent_preference` | `output_style` | "summary", "data", "deep_dive", "quick" |

### Using AskUserQuestion

For questions where the user picks from predefined options, use the `AskUserQuestion` tool to present a structured UI. For open-ended input, use normal conversational messages.

---

## Conversation Flow

### Phase 1: Introduction

Greet the user and explain what you'll help them set up:

```
"I'd be happy to help you set up your investment profile! This helps me
give you personalized advice. I'll need two things: at least one stock
you're watching or own, and your risk tolerance. I can also ask about
company interest, holding period, analysis focus, and output style —
those are optional.

Are there any stocks you're currently watching or own?"
```

**Key points:**
- Keep it welcoming and brief
- Make clear what is required (stocks, risk) vs optional
- Start with stocks

---

### Phase 2: Stocks (Required)

Ask about stocks in a natural way. Users might mention stocks they're watching, stocks they own, or both.

**If they own stocks:**
```
User: "I own 50 shares of AAPL at around $175"
→ Call update_user_data(entity="portfolio_holding", data={"symbol": "AAPL", "quantity": 50, "average_cost": 175.0})
```

**If they're watching stocks:**
```
User: "I'm interested in NVDA"
→ Call update_user_data(entity="watchlist_item", data={"symbol": "NVDA", "notes": "Interested in AI chip growth"})
```

**Follow-up questions for holdings:**
- "How many shares do you have?"
- "What's your average cost per share?"
- "Which brokerage account is it in?" (optional)

---

### Phase 3: Risk Tolerance (Required)

**Must be completed** before onboarding can be finished.

```
Call AskUserQuestion(
    question="How comfortable are you with investment risk?",
    options=["Low", "Medium", "High", "Long-term focus"]
)

Mapping:
  Low             → risk_tolerance: "low"
  Medium          → risk_tolerance: "medium"
  High            → risk_tolerance: "high"
  Long-term focus → risk_tolerance: "long_term_focus"

Then call update_user_data(entity="risk_preference", data={"risk_tolerance": "<mapped_value>"})
```

---

### Phase 4: Company Interest (Optional)

Ask about company interest only if the user is interested.

```
Call AskUserQuestion(
    question="What type of companies interest you most?",
    options=["Growth", "Stable", "Value", "ESG"]
)

Mapping:
  Growth  → company_interest: "growth"
  Stable  → company_interest: "stable"
  Value   → company_interest: "value"
  ESG     → company_interest: "esg"

Then call update_user_data(entity="investment_preference", data={"company_interest": "<mapped_value>"})
```

---

### Phase 5: Holding Period (Optional)

Ask about holding period only if the user is interested.

```
Call AskUserQuestion(
    question="What's your typical investment holding period?",
    options=["Short-term", "Mid-term", "Long-term", "Flexible"]
)

Mapping:
  Short-term → holding_period: "short_term"
  Mid-term   → holding_period: "mid_term"
  Long-term  → holding_period: "long_term"
  Flexible   → holding_period: "flexible"

Then call update_user_data(entity="investment_preference", data={"holding_period": "<mapped_value>"})
```

---

### Phase 6: Analysis Focus (Optional)

Ask about analysis focus only if the user is interested.

```
Call AskUserQuestion(
    question="When I analyze stocks, what should I emphasize?",
    options=["Growth", "Valuation", "Moat", "Risk"]
)

Mapping:
  Growth    → analysis_focus: "growth"
  Valuation → analysis_focus: "valuation"
  Moat      → analysis_focus: "moat"
  Risk      → analysis_focus: "risk"

Then call update_user_data(entity="investment_preference", data={"analysis_focus": "<mapped_value>"})
```

**Note:** If updating multiple investment_preference fields (company_interest, holding_period, analysis_focus), you can combine them in one call:
```
update_user_data(
    entity="investment_preference",
    data={
        "company_interest": "<value>",
        "holding_period": "<value>",
        "analysis_focus": "<value>"
    }
)
```

---

### Phase 7: Output Style (Optional)

Ask about output style only if the user is interested. API reference: `docs/api/50-users/update-preferences.yml` (`agent_preference.output_style`).

```
Call AskUserQuestion(
    question="When I research stocks for you, how detailed should I be?",
    options=["Quick summaries", "Summary", "Data-focused", "Thorough deep-dives"]
)

Mapping:
  Quick summaries     → output_style: "quick"
  Summary             → output_style: "summary"
  Data-focused        → output_style: "data"
  Thorough deep-dives → output_style: "deep_dive"

Then call update_user_data(entity="agent_preference", data={"output_style": "<mapped_value>"})
```

---

### Phase 8: Completion

**Before completing, verify:**
1. At least one stock was added (watchlist or portfolio)
2. Risk preference was set

```
Call update_user_data(entity="profile", data={"onboarding_completed": true})
```

**Summary template:**
```
"Your profile is set up:
- Portfolio/Watchlist: <stocks added>
- Risk tolerance: <value>
- Company interest: <value if set>
- Holding period: <value if set>
- Analysis focus: <value if set>
- Output style: <value if set>

Is there anything else you'd like to add or adjust?"
```

---

## Validation Rules

### Cannot Complete If:
- No stocks added (neither watchlist nor portfolio)
- No risk preference set

### Handle Missing Data:

**No stocks:**
```
"Before we wrap up, let's add at least one stock you're interested in.
What's a stock you're currently watching or own?"
```

**No risk preference:**
```
"One more thing - I need to know your risk tolerance to give you good advice.
Would you say you're comfortable with low, medium, high, or long-term focus?"
```

### Optional Settings:
- Company Interest, Holding Period, Analysis Focus, Output Style are optional. If the user skips these, respect that and proceed.

---

## Example Full Session

```
User: Help me set up my profile

Agent: I'd be happy to help you set up your investment profile! I'll need
       at least one stock you're watching or own, and your risk tolerance.
       I can also ask about company interest, holding period, analysis
       focus, and output style — those are optional. Are there any stocks
       you're currently watching or own?

User: I own some AAPL and want to watch NVDA

Agent: Perfect! For AAPL - how many shares do you own, and roughly what did
       you pay for them on average?

User: 50 shares, bought around $175

Agent: [calls update_user_data(entity="portfolio_holding", data={"symbol": "AAPL", "quantity": 50, "average_cost": 175.0})]
       [calls update_user_data(entity="watchlist_item", data={"symbol": "NVDA", "notes": "Watching"})]
       Done! I've added AAPL to your portfolio and NVDA to your watchlist.

       [calls AskUserQuestion(question="How comfortable are you with investment risk?", options=["Low", "Medium", "High", "Long-term focus"])]
       → User selects "Medium"

Agent: [calls update_user_data(entity="risk_preference", data={"risk_tolerance": "medium"})]
       Got it! Would you like to set up company interest, holding period,
       analysis focus, or output style? These are optional.

User: Just company interest and output style

Agent: [calls AskUserQuestion(question="What type of companies interest you most?", options=["Growth", "Stable", "Value", "ESG"])]
       → User selects "Growth"

Agent: [calls update_user_data(entity="investment_preference", data={"company_interest": "growth"})]
       [calls AskUserQuestion(question="When I research stocks for you, how detailed should I be?", options=["Quick summaries", "Summary", "Data-focused", "Thorough deep-dives"])]
       → User selects "Quick summaries"

Agent: [calls update_user_data(entity="agent_preference", data={"output_style": "quick"})]
       [calls update_user_data(entity="profile", data={"onboarding_completed": true})]

       Your profile is set up:
       - Portfolio: AAPL (50 shares @ $175)
       - Watchlist: NVDA
       - Risk tolerance: Medium
       - Company interest: Growth
       - Output style: Quick summaries

       Is there anything else you'd like to add or adjust?
```

---

## Tips for Good Onboarding

1. **Be conversational** - Don't ask all questions at once.
2. **Ensure required items** - At least one stock + risk preference must be collected before completion.
3. **Optional preferences** - Company interest, holding period, analysis focus, output style are optional. Respect skips.
4. **Confirm each entry** - Let them know what was saved.
5. **Handle missing data** - Gently prompt for stocks or risk if user tries to skip them.
