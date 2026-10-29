# Transmission Channels

Deferred reference for `impact-analysis`. Read it when the event runs through a channel not already mapped, or when the first affected line item is not obvious from the disclosure.

Four fields per channel, and they are the four the exposure table needs: the line item it **reaches first**, the **lag** and the mechanism that sets it, the **tell** that says it is happening before the print does, and the **sizing disclosure** that turns the claim into a number. Lag is never a property of the event; it is a property of the buffer between the event and the line item, which is why two companies hit by one shock report it two quarters apart.

**One row per channel per name.** A shock usually runs through two or three channels at once with different signs and different lags, so a name reached twice is two rows in the exposure table and never a merged one.

## Rates and discounting

- **Reaches first**: the multiple, immediately and through no line item at all, because a discount rate is a valuation input rather than an operating cost. Then interest expense on the floating-rate share of debt and on anything refinancing inside the window, then capex through the hurdle rate, then working-capital financing cost. For banks and lenders, net interest income first, and the sign depends on deposit beta rather than on the direction of the move.
- **Lag**: the multiple moves the same session. Interest expense moves at the next reset or maturity, which is quarters to years depending on the fixed and floating mix. Capex decisions move one to three quarters out, and the capex spend itself later than that.
- **Tell**: the weighted-average interest rate in the interest-expense note moving before the total does; a refinancing pulled forward or postponed; a buyback slowed while the revolver is drawn.
- **Sizes from**: the debt note, specifically the maturity schedule and the floating-rate percentage.

## Input costs

- **Reaches first**: cost of goods sold, so gross margin, while revenue has not moved at all. Opex second where the input is labour or energy consumed outside production.
- **Lag**: inventory turns plus contract coverage. A company holding ninety days of inventory under annual supply contracts carries the old cost for two or three quarters after the new one exists.
- **Tell**: cost per unit rising while inventory days rise with it; the cost-of-sales commentary naming the input for the first time; a list-price increase announcement, which is the pass-through attempt rather than the pass-through.
- **Sizes from**: the input's share of cost of sales, disclosed directly or derived from the segment and cost-structure notes.

## FX

Two effects that behave differently and are worth splitting before either is sized.

- **Translation**: foreign results restated into the reporting currency. Reaches reported revenue and reported earnings with no cash consequence and no margin effect.
- **Transaction**: costs and revenue sitting in different currencies. Reaches gross margin, and it is real money.
- **Lag**: translation lands in the next reported period. Transaction lands as the hedge book rolls off, typically two to four quarters, and a company with a rolling twelve-month hedge program can show none of it for a year.
- **Tell**: constant-currency growth diverging from reported growth; the hedging note's notional and tenor changing; guidance revised for currency with no change to volume.
- **Sizes from**: the geographic revenue split against the cost footprint, and the hedging note's notional, tenor and rate.

## Demand

A shock to end-market volume: a fiscal change, a confidence shock, a rate move that reaches the household, a rule that opens or closes a market.

- **Reaches first**: revenue through units. Gross margin second, through operating leverage and through the discounting that shows up when volume disappoints. Working capital third, as inventory ordered for the old demand arrives against the new one.
- **Lag**: weeks in consumables and staples, where the shelf is the buffer. Quarters to years wherever a backlog insulates the print, which is why an industrial can report record revenue through the first year of a demand shock.
- **Tell**: order intake or book-to-bill turning before revenue; channel inventory building; promotional cadence changing; unit growth and price growth diverging.
- **Sizes from**: the disclosed unit and price split, backlog with its coverage period, and the end-market revenue mix.

## Regulation and policy

A rule, an approval, a subsidy, a tax change, an antitrust action.

- **Reaches first**: set by the instrument rather than by the sector. A subsidy reaches revenue or capex, a tax change reaches the tax line, a compliance rule reaches opex and capex, a market ban or approval reaches revenue directly.
- **Lag**: the longest and the most variable, because the announcement date and the effective date are different facts and only the second one moves a line item. Comment periods, transition provisions, phase-ins and litigation all sit between them, and the multiple often moves on the announcement while the line item waits years.
- **Tell**: the effective date and transition provisions in the rule text itself; the company's risk-factor language changing between filings; a compliance line appearing in opex; a lobbying or litigation disclosure.
- **Sizes from**: the rule text's own scope (the covered products, thresholds and rates) against the company's segment disclosure.

## Supply chain

A disruption, a capacity loss, a logistics constraint, an export control.

- **Reaches first**: revenue, through units that cannot ship. Cost second, through expedited freight and spot capacity. Reaching for cost first is the common error here, and it dates the impact one quarter late.
- **Lag**: immediate where no buffer exists, one to two quarters where inventory or dual sourcing covers it.
- **Tell**: lead times extending; expedite and spot freight named in cost commentary; a supplier's own guidance moving first; inventory falling while backlog rises.
- **Sizes from**: the supplier and geographic concentration disclosure, the single-source risk factors, and the inventory position against normal turns.

## Credit conditions

Lending standards, spread widening, a funding market closing.

- **Reaches first**: interest expense and refinancing capacity for the borrower. Capex second. Revenue third, for anyone whose customer finances the purchase, which is where the effect is largest and least anticipated.
- **Lag**: the spread moves immediately, the borrower feels it at its next funding need, and the demand effect on financed goods lands one to two quarters after standards tighten.
- **Tell**: lending-standards and credit-conditions series from `get_economic_indicator`; issuance calendars going quiet; a drawn revolver, a cancelled buyback, a dividend held flat; captive-finance approval rates falling.
- **Sizes from**: the maturity schedule against the funding need in the window, and the share of revenue sold on credit or through a captive finance arm.

## Commodity pass-through

An oil, gas, metal or agricultural price move. The one channel most often merged into a single row when it deserves at least two, because one event moves producers and consumers in opposite directions on different clocks.

- **Reaches first**: revenue for the producer, through realised price. Cost of goods sold for the consumer. Neither, for a regulated utility or a toller whose contract or tariff passes the cost straight through, where only working capital moves and the exposure claim is usually wrong.
- **Lag**: about a quarter for the producer, since realisations lag spot by the contract terms. The hedge book's tenor for the consumer. A pass-through name shows the effect only where the contractual reset is slower than the price move.
- **Tell**: realised price disclosed against the spot benchmark; the hedging note; a fuel or materials surcharge appearing or being withdrawn; a regulated tariff filing. Where a physical spot assessment is not in hand, a public producer-price series from `get_economic_indicator` is the proxy and is labelled as one.
- **Sizes from**: production volumes against realisations for the producer, the commodity's share of cost of sales for the consumer, and the hedge notional and tenor for both.

## A channel not on this list

Build the entry with the same four fields before writing the claim: name the first line item, take the lag from the buffer that creates it (inventory turns, contract length, a reset date, a regulatory effective date, a hedge tenor), find one observable tell that moves before the print, and name the disclosure that would size it. A channel whose lag cannot be traced to a named buffer is a guess about timing, and the timing bucket in `.agents/skills/impact-analysis/SKILL.md` is what the reader acts on.
