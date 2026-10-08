---
name: onboarding
description: First-time setup. Import holdings and watchlists from a connected brokerage, learn the markets the user follows, and save it all to their profile files.
---

# Onboarding

Set up a new user's profile in one short conversation. Everything you learn goes into the files under `.agents/user/profile/`, which the app and every later conversation read. Read `.agents/user/profile/README.md` before your first write: it has each file's schema and the reasons a save is refused.

## 1. Start from what is there

Read `user.json`, `portfolio.json`, `watchlist.json` and `preference.json`. Skip any question they already answer, and keep the rows the user already has when you write a file back.

## 2. Import from the brokerage

A connected brokerage is listed in `<mcp-servers>`. Read its tool docs, then fetch every account's positions and every watchlist in one `execute_code` call, printing a compact summary rather than the raw answers.

- Show the user what you found: the accounts, how many positions, the largest few, and the watchlists. Ask with AskUserQuestion whether to import all of it, pick, or skip.
- Holdings go into `portfolio.json`. Set `account_name` to the brokerage's name followed by the account's last four digits (`moomoo 1234`), or the name alone when the brokerage reports no account number, so the name stays the same when a later import finds more accounts. Replace the rows with that `account_name` and keep every other row, so a later import updates the account instead of doubling it.
- Map each position to an `instrument_type` (`stock`, `etf`, `option`, `crypto`, `bond`, `fund`) and carry quantity, average cost and currency as the brokerage reports them. Leave out a field the brokerage does not give rather than guessing it.
- Each brokerage watchlist becomes its own list in `watchlist.json`, named after it with the brokerage in front (`moomoo: Favorites`), and is replaced the same way on a later import. The dashboard shows only the default list, so when the user's default list is empty, make the largest imported list the default.
- If a call fails or there is nothing to import, say so in one line and move on.

With no brokerage connected, ask which stocks the user owns or follows. Add them to the default watchlist, or to the portfolio when they give a quantity.

## 3. Learn the markets they follow

Ask with AskUserQuestion, a few related choices at a time, each leaving room for their own answer:

- Markets: regions (US, Hong Kong, mainland China, Japan, Europe) and asset classes (stocks, ETFs, options, crypto, bonds).
- Focus: the sectors and themes they watch, and any they avoid.
- Style and horizon: growth, value, income or trading; holding for days, months or years.
- Risk: how deep a drawdown they can sit through, and what shaped that.
- Answers: brief or in depth, charts or text.

Save their words, not a label: "moderate, but sold everything in March 2020 and regretted it" tells a later conversation more than "moderate". Markets, focus and style go in `investment_preference`, risk in `risk_preference`, and how they want answers in `agent_preference`.

## 4. Who they are

If `user.json` has no name, ask what to call them. Confirm the timezone it holds, or ask for their city when it is empty: their turns and new automations run in that zone.

## 5. Finish

1. Sum up in a few lines what you saved.
2. Set `onboarding_completed` to `true` in `user.json`.
3. Offer a first piece of work built on what they told you, tied to their largest holding or main market. If they accept, create a workspace for it with `manage_workspaces(action="create")` and hand the question to its analyst with `delegate_to_analyst`.

Keep it a conversation, not a form: combine related questions, follow up when an answer is vague, and let the user skip anything.
