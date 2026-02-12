# User Onboarding Procedure

This document describes the step-by-step procedure for onboarding new users.

## Overview

Onboarding helps users set up their investment profile so the agent can provide personalized advice.

### Required Information
1. **At least one stock** (in watchlist or portfolio)
2. **Risk preference** (tolerance level)

### Optional Information
- Portfolio holdings with quantity/cost
- Investment preference (style, sectors)
- Agent preference (research style, detail level)

### Using AskUserQuestion

For questions where the user picks from predefined options (risk tolerance, investment style,
research preference), use the `AskUserQuestion` tool to present a structured UI. For open-ended
questions (stock names, share counts, cost basis), use normal conversational messages.

---

## Conversation Flow

### Phase 1: Introduction

Greet the user and explain what you'll help them set up:

```
"I'd be happy to help you set up your investment profile! This helps me
give you personalized advice. Let's start with what matters most to you.

Are there any stocks you're currently watching or own?"
```

**Key points:**
- Keep it welcoming and brief
- Ask what they'd like to set up first
- Adapt based on their response

---

### Phase 2: Stocks (Natural Discovery)

Ask about stocks in a natural way. Users might mention:
- Stocks they're watching
- Stocks they own
- Both

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

### Phase 3: Risk Assessment

This is **required** before completing onboarding.

```
Call AskUserQuestion(
    question="How comfortable are you with investment risk?",
    options=["Conservative", "Moderate", "Aggressive"]
)

Mapping:
  Conservative → risk_tolerance: "low"
  Moderate     → risk_tolerance: "medium"
  Aggressive   → risk_tolerance: "high"

Then call update_user_data(entity="risk_preference", data={"risk_tolerance": "<mapped_value>"})
```

**Optional follow-up:**
- "What's your investment time horizon? Short-term (< 1 year), medium (1-5 years), or long-term (5+ years)?"

---

### Phase 4: Optional Preferences

Only ask if the user seems engaged and interested. Don't force these.

**Investment Style:**
```
Call AskUserQuestion(
    question="Do you have a preferred investment style?",
    options=["Growth", "Value", "Income", "Balanced"]
)

Then call update_user_data(entity="investment_preference", data={"company_interest": "<selected_value>"})
```

**Agent Style:**
```
Call AskUserQuestion(
    question="When I research stocks for you, how detailed should I be?",
    options=["Quick summaries", "Balanced analysis", "Thorough deep-dives"]
)

Mapping:
  Quick summaries    → output_style: "quick"
  Balanced analysis  → output_style: "summary"
  Thorough deep-dives → output_style: "deep_dive"

Then call update_user_data(entity="agent_preference", data={"output_style": "<mapped_value>"})
```

**Visualizations:**
```
Call AskUserQuestion(
    question="How often should I include charts and visualizations in my analysis?",
    options=["Always", "When useful", "Prefer not to", "Never"]
)

Mapping:
  Always       → enable_charts: "always"
  When useful  → enable_charts: "auto"
  Prefer not to → enable_charts: "prefer_not"
  Never        → enable_charts: "never"

Then call update_user_data(entity="agent_preference", data={"enable_charts": "<mapped_value>"})
```

**Proactive Questions:**
```
Call AskUserQuestion(
    question="Should I ask clarifying questions before starting work?",
    options=["Always", "When unclear", "Prefer not to", "Never"]
)

Mapping:
  Always        → proactive_questions: "always"
  When unclear  → proactive_questions: "auto"
  Prefer not to → proactive_questions: "prefer_not"
  Never         → proactive_questions: "never"

Then call update_user_data(entity="agent_preference", data={"proactive_questions": "<mapped_value>"})
```

---

### Phase 5: Completion

Summarize what was set up and mark onboarding complete.

**Before completing, verify:**
1. At least one stock was added (watchlist or portfolio)
2. Risk preference was set

```
Call update_user_data(entity="profile", data={"onboarding_completed": true})
```

**Summary template:**
```
"Your profile is set up:

- Portfolio: AAPL (50 shares @ $175)
- Watchlist: NVDA
- Risk preference: Moderate
- Research style: Quick summaries
- Charts: When useful
- Proactive questions: Always

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
Would you say you're conservative, moderate, or aggressive with investments?"
```

---

## Example Full Session

```
User: Help me set up my profile

Agent: I'd be happy to help you set up your investment profile! This helps me
       give you personalized advice.

       Are there any stocks you're currently watching or own?

User: I own some AAPL and want to watch NVDA

Agent: Perfect! For AAPL - how many shares do you own, and roughly what did
       you pay for them on average?

User: 50 shares, bought around $175

Agent: [calls update_user_data(entity="portfolio_holding", data={"symbol": "AAPL", "quantity": 50, "average_cost": 175.0})]
       [calls update_user_data(entity="watchlist_item", data={"symbol": "NVDA", "notes": "Watching"})]

       Done! I've added:
       - AAPL to your portfolio (50 shares @ $175)
       - NVDA to your watchlist

       [calls AskUserQuestion(question="How comfortable are you with investment risk?", options=["Conservative", "Moderate", "Aggressive"])]
       → User selects "Moderate"

Agent: [calls update_user_data(entity="risk_preference", data={"risk_tolerance": "medium"})]

       Got it! I've set your risk preference to moderate.

       [calls AskUserQuestion(question="When I research stocks for you, how detailed should I be?", options=["Quick summaries", "Balanced analysis", "Thorough deep-dives"])]
       → User selects "Quick summaries"

Agent: [calls update_user_data(entity="agent_preference", data={"output_style": "quick"})]

       [calls AskUserQuestion(question="How often should I include charts and visualizations in my analysis?", options=["Always", "When useful", "Prefer not to", "Never"])]
       → User selects "When useful"

Agent: [calls update_user_data(entity="agent_preference", data={"enable_charts": "auto"})]

       [calls AskUserQuestion(question="Should I ask clarifying questions before starting work?", options=["Always", "When unclear", "Prefer not to", "Never"])]
       → User selects "Always"

Agent: [calls update_user_data(entity="agent_preference", data={"proactive_questions": "always"})]
       [calls update_user_data(entity="profile", data={"onboarding_completed": True})]
       # Or: onboarding will auto-complete when risk_preference + at least one stock are set

       Perfect! Your profile is set up:

       - Portfolio: AAPL (50 shares @ $175)
       - Watchlist: NVDA
       - Risk preference: Moderate
       - Research style: Quick summaries
       - Charts: When useful
       - Proactive questions: Always

       Is there anything else you'd like to add or adjust?
```

---

## Tips for Good Onboarding

1. **Be conversational** - Don't ask all questions at once
2. **Respect their time** - If they seem brief, skip optional preferences
3. **Confirm each entry** - Let them know what was saved
4. **Handle errors gracefully** - If a stock exists, offer to update it
5. **End with a summary** - Show what was set up
