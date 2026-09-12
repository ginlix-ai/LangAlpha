# Sector Drivers

Deferred reference for `dcf-model`. Load it while building the revenue and margin lines, when the subject sits in one of the sectors below, or when a refresh has to move operating metrics rather than the three statements (`.agents/skills/model-update/SKILL.md` routes here for that).

A revenue line that is a single growth percentage says nothing about the business. Each sector below names the drivers the build has to carry explicitly, so the growth rate is an output of something rather than an input to everything, and the tell that the model was built by someone who had not looked at the company.

## How to use this file

Model the drivers for the sector as their own rows above revenue, then let revenue be the formula over them. Two or three drivers is usually the whole gain: past that the model gets harder to audit without getting more right. Where a driver is not disclosed, say so in the provenance comment and carry the assumption label rather than inventing a disclosure.

## Software and subscription

- Ending customers or seats, split new and existing.
- Net revenue retention, which is the single line most subscription models get wrong by assuming it holds flat forever. Fade it toward the cohort's observed floor.
- Gross retention separately, since a high net number can hide churn that expansion is masking.
- Price per seat or per unit of consumption, and its direction.
- Sales and marketing as the cost of new bookings rather than a percent of total revenue, so the payback period is visible.
- Capitalised software: check whether development costs sit in capex or operating expense, because the free cash flow differs even when the margin does not.

## Consumer and retail

- Store count, openings and closures, and the productivity of a new store against the fleet average.
- Same-store or comparable sales, split into traffic and average ticket. A comp built only on ticket is a price story with a volume risk under it.
- Gross margin split into product margin and occupancy or fulfilment cost.
- Inventory turns, since working capital is a real cash driver in this sector and not a plug.
- Channel mix, where digital and wholesale carry different margins on the same revenue.

## Industrials and capital goods

- Backlog and book-to-bill, plus the conversion rate from backlog into revenue and the period it converts over.
- Aftermarket or service revenue as its own line: it carries a different margin and a different cycle from equipment.
- Capacity utilisation and the incremental margin on the next unit of volume.
- Input cost pass-through, with the lag between the input moving and price following it.
- Maintenance capex against growth capex, because the two have different claims on the terminal year.

## Energy, mining and materials

- Volume and realised price separately, with the realised price bridged to the benchmark, so a differential or a hedge is visible rather than buried in an average.
- Reserve life and decline rate, which cap the terminal assumption more tightly than any perpetuity growth rate.
- All-in sustaining cost per unit, and where the asset sits on the industry cost curve.
- Sustaining capex as the cost of holding volume flat, stated per unit.
- Hedge book: which volumes are hedged, at what price, through when.

## Healthcare and biopharma

- Per product: patients or prescriptions, net price after rebates, and the gross-to-net gap, which moves independently of list price.
- Loss of exclusivity dates by product, with the erosion curve after each, modelled rather than assumed away.
- Pipeline treated as probability-weighted, with the probability stated per asset and the set never rolled into the base case unlabelled.
- Research and development as a commitment rather than a percent of revenue, since it does not fall when revenue does.

## Financials

A bank or an insurer does not take an unlevered free cash flow model: debt is raw material, not financing, so an enterprise value is not a meaningful quantity. Value the equity directly, with a dividend or excess-return model discounted at the cost of equity, and drive it with:

- Earning assets and the net interest margin, or premiums earned and the combined ratio.
- Fee income as its own line with its own driver.
- Provisions or losses through the cycle rather than at the current point in it.
- The capital ratio the business has to hold, which sets what can be paid out and therefore what the equity is worth.

Say in the model that the method changed and why, so a reader does not go looking for the WACC sheet.

## Real estate and infrastructure

- Occupancy, rent per unit, and the spread between in-place rent and market rent.
- Lease expiry schedule, and the re-leasing spread on rolls.
- Maintenance capex and leasing costs, which are real and recurring.
- Cap rate for the terminal value, cross-checked against the perpetuity growth rate the model implies.

## Semiconductors and hardware

- Units and average selling price, with the ASP path stated: this sector deflates, and a flat ASP assumption is a decision that has to be defended.
- Content per device or attach rate, where growth comes from more silicon in the same unit.
- Fab utilisation and the fixed-cost absorption that drives gross margin through the cycle.
- Inventory in the channel, which turns a demand story into a correction one quarter later.

## Transport and logistics

- Volume, measured the way the business measures it, and yield per unit of volume.
- Load factor or utilisation, and the incremental margin on filling the next slot.
- Fuel or energy cost with its pass-through mechanism and lag.
- Fleet age and the replacement capex it forces.

## Model shape by company type

The driver set is one decision; the shape of the model around it is another.

| Company type | Horizon | What changes |
|---|---|---|
| High growth, early profitability | 7 to 10 years | The explicit period runs until margins reach something defensible, since a terminal value taken off an unstable margin is the whole valuation |
| Mature and stable | 3 to 5 years | Growth converges on the economy quickly; the work moves to capital allocation and the durability of the margin |
| Cyclical | through a full cycle | Normalise the terminal year at mid-cycle margins rather than at the current point, and say which point of the cycle the last actual sits at |
| Multi-segment | per segment | Build each segment's drivers separately and sum, then value the corporate line and the unallocated costs explicitly rather than spreading them |
