# Energy

Deferred reference for `sector-overview`. Load it before the metric and valuation steps when the subject produces, moves, refines or sells energy, or sells services to those who do.

Template and how to add a sector: `.agents/skills/sector-overview/references/sectors/README.md`.

## Archetypes

| Archetype | Earns money by | The trap |
|---|---|---|
| Upstream producer | selling a commodity it does not price, less the cost of lifting it | reading revenue growth as execution when it is the price deck moving |
| Midstream | fees on volume moved or stored, often contracted | treating it as commodity-exposed when it is a toll road, or the reverse where the contracts are percent-of-proceeds |
| Refiner | a spread between crude and product, not a price | comparing its EBITDA margin to a producer's, when one is a spread on throughput and one is a price on a barrel |
| Integrated | all three at once | analysing at the consolidated level, where a strong refining quarter masks upstream decline |
| Oilfield services | activity, which is the producers' capex | modelling it off the commodity price rather than off the capex budgets it actually bills |
| Contracted power and renewables | long-dated contracted offtake | valuing it as a growth company when the cash flows are an infrastructure annuity with a recontracting date |

Classify on how the barrel or the electron turns into cash, not on the sector tag. An integrated name needs the segment split before any of the metrics below mean anything.

## KPIs that matter

- **Production and mix.** Volumes in barrels of oil equivalent per day, with the liquids share, because a boe of gas and a boe of oil are not the same revenue.
- **Realisations and differentials.** What the company actually received against the benchmark, per basin.
- **Cost per boe.** Lifting cost, cash operating cost, and all-in sustaining cost, stated on the same basis across the set.
- **Maintenance capex.** The capex that holds production flat against the decline curve. It is the most important derived number in the sector and it is rarely disclosed directly.
- **Reserves.** Reserve life (R/P), reserve replacement ratio, the proved developed share, and PV-10.
- **The hedge book.** Percentage hedged by year, at what price, and in what instrument.
- **Free cash flow at a stated price, and the dividend breakeven.** Both meaningless without the price they are stated at.
- **Net debt to EBITDA at a stated price deck.** For midstream, add contracted percentage and distributable cash flow coverage. For refiners, add throughput, utilisation, capture rate against the benchmark crack, and the turnaround schedule.

## Model architecture

Model volume and price on separate lines, and state the price deck with its source and its date on the face of the model: a producer model with the deck buried is not auditable. Start from the decline curve, so base production falls and capex buys new volume, which makes maintenance capex a model output rather than a guess. Put hedges on their own line offsetting realisations, so the outlook is visible under the hedge. Model differentials per basin rather than one blended discount. Carry cash flow before working capital as the comparable figure across the set. For midstream, model contracted revenue and the recontracting schedule separately from commodity-exposed volume. For refiners, model the spread and the capture rate, never a product price alone.

## Valuation lenses, in order

1. **EV/EBITDA at one stated price deck, plus free cash flow yield at strip.** The primary cross-sectional lens, and the deck has to be identical for every name in the table or the table is measuring the deck.
2. **Net asset value.** A discounted type-curve or reserve build against market capitalisation, which is the only lens that prices the resource rather than the year.
3. **EV per flowing barrel, or EV per boe of proved developed reserves.** A fast cross-check on whether the first two agree.
4. **Distribution or dividend coverage at a stated price**, for midstream and for the payout-led names.

Wrong here: **P/E on a producer**, because earnings swing with price and depend on whether the company reports on successful efforts or full cost, so the same barrels produce different earnings. **Revenue growth as a quality signal**, when it is mostly a price move. **EV/revenue on a refiner**, where revenue is throughput times the crude price and carries no information about the spread the company earns.

## Red flags

- Production growth funded by spending above cash flow: check the outspend and the borrowing base.
- Reserve replacement below 100% for consecutive years: check whether the drilling inventory is being consumed.
- A rising proved undeveloped share: check the capital required to convert it and the five-year development rule.
- The hedge book rolling off into a lower strip: recompute the next-year cash flow unhedged.
- Differentials widening in one basin: check takeaway capacity and the contracted transport.
- An impairment after a price-deck change: check what the remaining carrying values assume.
- A dividend covered only well above the current strip: state the breakeven price beside the yield.
- For midstream, a recontracting cliff inside the horizon; for refiners, a turnaround deferred into the next year.

## Source hierarchy

On top of the general tiers, for this sector specifically: the 10-K reserve tables and the standardised measure are the reported reserve figures, and PV-10 there is computed on a trailing twelve-month average price set by disclosure convention, so it is a disclosure rather than a market value; a strip-based net asset value is `model-derived` and says so. The quarterly operations report or supplement carries per-basin volumes, well results and per-unit costs. The hedge table in the 10-Q carries the hedge book. Agency data for inventories, production and rig counts comes through `get_economic_indicator` or the issuing agency's own release, with its release date. Commodity price levels come from the price tools with their as-of, never from a level quoted inside an article.

## Rules that hold in this sector

- State the price deck on every producer comparison: an EV/EBITDA table built on mixed decks ranks the decks, not the companies.
- Treat PV-10 as a disclosure computed on a fixed convention, and build a separate strip case when a value is what the reader needs.
- Compare per-boe costs only after showing the liquids mix: a gas-weighted producer looks cheap per boe on cost and on value alike.
- Read a hedged realisation as a hedge outcome rather than as the price outlook: state the unhedged view beside it.
- Name the accounting convention when comparing earnings across producers, or compare cash flow instead, which is convention-free.
