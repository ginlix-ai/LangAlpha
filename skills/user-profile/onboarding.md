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
```python
# User: "I own 50 shares of AAPL at around $175"
update_user_data(
    entity="portfolio_holding",
    data={"symbol": "AAPL", "quantity": 50, "average_cost": 175.0}
)
```

**If they're watching stocks:**
```python
# User: "I'm interested in NVDA"
update_user_data(
    entity="watchlist_item",
    data={"symbol": "NVDA", "notes": "Interested in AI chip growth"}
)
```

**Follow-up questions for holdings:**
- "How many shares do you have?"
- "What's your average cost per share?"
- "Which brokerage account is it in?" (optional)

---

### Phase 3: Risk Assessment

This is **required** before completing onboarding.

```
"How comfortable are you with investment risk? Would you say you're:
- Conservative (prefer stability, avoid volatility)
- Moderate (balance between growth and stability)
- Aggressive (willing to accept volatility for growth)"
```

```python
# Based on response
update_user_data(
    entity="risk_preference",
    data={"tolerance": "moderate"}  # or "conservative", "aggressive"
)
```

**Optional follow-up:**
- "What's your investment time horizon? Short-term (< 1 year), medium (1-5 years), or long-term (5+ years)?"

---

### Phase 4: Optional Preferences

Only ask if the user seems engaged and interested. Don't force these.

**Investment Style:**
```
"Do you have a preferred investment style?
- Growth (focus on companies with high growth potential)
- Value (focus on undervalued companies)
- Income (focus on dividend-paying stocks)
- Balanced (mix of everything)"
```

```python
update_user_data(
    entity="investment_preference",
    data={"style": "growth"}
)
```

**Agent Style:**
```
"When I research stocks for you, do you prefer:
- Quick summaries (just the key points)
- Balanced analysis (moderate detail)
- Thorough deep-dives (comprehensive data)"
```

```python
update_user_data(
    entity="agent_preference",
    data={"research_style": "quick", "detail_level": "intermediate"}
)
```

---

### Phase 5: Completion

Summarize what was set up and mark onboarding complete.

**Before completing, verify:**
1. At least one stock was added (watchlist or portfolio)
2. Risk preference was set

```python
# Mark onboarding complete
update_user_data(
    entity="profile",
    data={"onboarding_completed": True}
)
```

**Summary template:**
```
"Your profile is set up:

- Portfolio: AAPL (50 shares @ $175)
- Watchlist: NVDA
- Risk preference: Moderate
- Research style: Quick summaries

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

       Now, how comfortable are you with investment risk? Would you say:
       - Conservative (prefer stability)
       - Moderate (balance of growth and stability)
       - Aggressive (willing to accept volatility for growth)

User: Moderate I guess

Agent: [calls update_user_data(entity="risk_preference", data={"tolerance": "moderate"})]

       Got it! I've set your risk preference to moderate.

       One more question - when I research stocks for you, do you prefer:
       - Quick summaries (just the key points)
       - Detailed analysis (comprehensive with all the data)

User: Quick summaries, I don't have much time

Agent: [calls update_user_data(entity="agent_preference", data={"research_style": "quick", "detail_level": "intermediate"})]
       [calls update_user_data(entity="profile", data={"onboarding_completed": True})]

       Perfect! Your profile is set up:

       - Portfolio: AAPL (50 shares @ $175)
       - Watchlist: NVDA
       - Risk preference: Moderate
       - Research style: Quick summaries

       Is there anything else you'd like to add or adjust?
```

---

## Tips for Good Onboarding

1. **Be conversational** - Don't ask all questions at once
2. **Respect their time** - If they seem brief, skip optional preferences
3. **Confirm each entry** - Let them know what was saved
4. **Handle errors gracefully** - If a stock exists, offer to update it
5. **End with a summary** - Show what was set up
