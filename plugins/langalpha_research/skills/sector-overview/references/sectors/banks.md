# Banks

Deferred reference for `sector-overview`. Load it before the metric and valuation steps when the subject earns money from a spread on its own balance sheet, from custody and processing fees, or from capital-markets activity.

Template and how to add a sector: `.agents/skills/sector-overview/references/sectors/README.md`.

## Archetypes

| Archetype | Earns money by | The trap |
|---|---|---|
| Spread lender | lending at a yield above the cost of deposits | reading loan growth as good news without the funding cost and the credit cost that came with it |
| Fee and processing bank | custody, servicing, payments and trust fees on assets it does not own | benchmarking its efficiency ratio against a spread lender's, when the cost base is a different business |
| Capital-markets bank | trading, underwriting and advisory, so revenue tracks volume and volatility | extrapolating a strong trading quarter into a run rate |
| Consumer and card lender | a high asset yield less a credit cost that arrives on a lag | reading peak earnings at the point in the cycle where provisions are lowest |
| Digital deposit gatherer | a thin balance sheet and fast deposit growth | treating deposit growth as franchise value when the deposits are rate-shopping and leave at the next repricing |

The whole sector shares one trap that dwarfs the archetype traps: a revenue-growth-and-margin frame. A bank's revenue is net interest income plus fees, its debt is raw material rather than financing, and it has no margin that means the same thing as an industrial margin.

## KPIs that matter

- **Net interest margin, and net interest income.** Reconstruct NIM from the average-balance table rather than quoting it: earning-asset yield less funding cost, on average earning assets.
- **Deposit beta.** The share of a rate move that passes into deposit costs. It is the single input that decides whether a rate cycle helps or hurts.
- **Non-interest income mix.** How much of revenue does not depend on the balance sheet.
- **Efficiency ratio.** Non-interest expense over revenue, comparable only within an archetype.
- **Provision, net charge-offs, non-performing loans, and reserve coverage (ACL to loans).** Provision is the flow, the reserve is the stock, and coverage against a rising NPL balance is the signal.
- **CET1 ratio and risk-weighted assets.** The constraint on growth and on buybacks.
- **Loan-to-deposit ratio, uninsured deposit share, and the held-to-maturity mark.** The three that decide funding fragility together, never separately.
- **Tangible book value per share, ROTCE and ROA.** The return the valuation is a function of.

## Model architecture

Model the balance sheet first and the income statement off it. Average earning assets times yield gives interest income; average interest-bearing liabilities times cost gives interest expense; the difference is net interest income and NIM falls out of it. Build the reserve rather than the provision: beginning ACL plus provision less net charge-offs equals ending ACL, with the charge-off rate as the driver. Carry capital as its own schedule, CET1 over risk-weighted assets, and let it constrain the buyback rather than sizing the buyback first. Bridge tangible book value each period including accumulated other comprehensive income, so a securities mark shows up where it actually lands.

## Valuation lenses, in order

1. **P/TBV against ROTCE.** The primary lens: the multiple a bank earns is a function of its return on tangible equity against its cost of equity. Plot the set, and a name off the line needs a reason.
2. **P/E on normalised credit.** Useful once the provision is normalised to a through-cycle charge-off rate rather than to the current one.
3. **Dividend discount or excess-return model.** For a name where the capital return, not the growth, is the story.

Wrong here: **EV/EBITDA and EV/revenue** have no clean meaning, because enterprise value treats debt as financing and a bank's debt is its inventory. **Revenue growth and margin** frames the wrong two variables. **Free cash flow** as normally computed is not a bank concept; the distributable figure is capital generation above the CET1 requirement.

## Red flags

- NIM expansion with flat asset yields: check for a one-off, a securities repositioning or a day-count effect.
- Deposit costs rising faster than asset yields: the beta is catching up and the next quarters compress.
- Loan growth well above peers in one category: check the concentration table and the underwriting commentary.
- Reserve coverage falling while non-performing loans rise: check the charge-off outlook against the reserve build.
- A large held-to-maturity book with unrealised losses beside a high uninsured deposit share: check the liquidity and the borrowing capacity.
- Securities gains or a legal reversal carrying the quarter: recompute the efficiency ratio without them.
- Commercial real estate concentration in one property type: check the maturity schedule and the appraisal vintage.

## Source hierarchy

On top of the general tiers, for this sector specifically: the quarterly financial supplement is the primary working document, because the average-balance and yield tables are what make NIM, deposit beta and the earning-asset mix reconstructable, and the 10-Q text alone is not. The 10-K and 10-Q carry the loan portfolio, the reserve roll-forward, the concentration tables and the held-to-maturity disclosure. Regulatory capital disclosures and published stress-test results carry the capital position on a comparable basis. Deposit beta guidance and the forward NIM path come from the earnings deck and the call and are `company claim`. The rate path behind any NIM forecast comes from `get_treasury_rates`, stated with its as-of.

## Rules that hold in this sector

- Value a bank on P/TBV against ROTCE, never on an enterprise-value multiple: EV has no defensible meaning where debt is raw material.
- Build the provision from the reserve roll-forward, never as a percentage of revenue: a margin-style provision breaks in exactly the quarter it matters.
- Compare an efficiency ratio inside one archetype, and say so when the table mixes them.
- Read tangible book value together with accumulated other comprehensive income and the held-to-maturity mark: book value alone missed the last funding crisis in the sector.
- Restate NIM and the balance-sheet series across a merger before carrying the trend, or start the series after the close.
