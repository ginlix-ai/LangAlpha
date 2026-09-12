# Software and Internet

Deferred reference for `sector-overview`. Load it before the metric and valuation steps when the subject earns money from software licences, subscriptions, cloud consumption, a marketplace take rate or advertising.

Template and how to add a sector: `.agents/skills/sector-overview/references/sectors/README.md`.

## Archetypes

| Archetype | Earns money by | The trap |
|---|---|---|
| Seat subscription | seats times price per seat, renewed annually | reading net retention as demand when the expansion came from a price increase, and reading ARR as revenue |
| Consumption | metered usage of compute, storage, queries or messages | comparing its net retention to a seat model's; consumption retention moves with customer workloads and is a usage signal, not a renewal signal |
| Marketplace take rate | gross merchandise value times a take rate | treating GMV as revenue, and comparing a gross-billings margin to a net-revenue margin |
| Advertising | impressions times price, sold against attention | reading user growth as revenue growth while the mix shifts to lower-ARPU geographies |
| Software with embedded payments | mostly the payment spread, with software attached | applying a software multiple to what is largely a transaction spread business |
| Licence in transition to subscription | perpetual licences today, ratable revenue later | reading the optical revenue trough as lost demand rather than a recognition change |

Classify on the revenue recognition note and the segment note, not on the sector tag. Two names both called "software" can be a seat model and a payments spread.

## KPIs that matter

- **ARR and the ARR bridge.** Beginning ARR, plus new, plus expansion, less contraction, less churn. The bridge is where growth quality lives; the single ARR number hides it.
- **Net retention (NDR) and gross retention (GRR).** GRR caps at 100% and measures whether customers stay. NDR includes expansion and can exceed it. A set that mixes the two is not a comparison.
- **RPO and current RPO, and billings.** The link between what is booked and what will be recognised. cRPO growth diverging from revenue growth is a leading signal in both directions.
- **Gross margin, split subscription against services.** A rising services share drags the blended margin without anything changing in the software.
- **Rule of 40, magic number, CAC payback.** Definitions and benchmark bands: `.agents/skills/competitive-analysis/references/unit-economics.md`.
- **Stock compensation as a share of revenue, and diluted share growth.** The cost that an adjusted margin removes and the share count keeps.
- **Free cash flow margin and its working-capital component.** Annual prepay flatters cash flow in the year the customer switches to it.

## Model architecture

Build revenue from the ARR bridge rather than from a growth rate: each bridge line has its own driver, and a single blended rate hides a collapse in new business behind strong expansion. Split subscription and services into separate lines with separate gross margins. Carry deferred revenue and RPO so billings, revenue and cash collection stay reconciled. Model stock compensation twice, once as an expense and once as a share-count path, because the dilution is the part that reaches the equity holder. Where development costs are capitalised, keep the capitalised amount visible: it moves both margin and capex.

## Valuation lenses, in order

1. **EV/NTM revenue against growth plus margin.** The primary cross-sectional lens while margins are still being invested away. The multiple is a function of forward growth and the Rule of 40 score, so plot the set rather than quoting one multiple.
2. **EV/gross profit.** Use it when the set mixes gross margins widely, which a marketplace and a seat model always do.
3. **EV/FCF or EV/EBITDA.** Available once the free cash flow margin is durable rather than a working-capital artefact.
4. **DCF with an explicit steady-state margin and share-count path.** The only lens that forces a view on where margins actually land.

Wrong here: **P/E**, on a name whose earnings are suppressed by growth spend or dominated by stock compensation, prices an accounting outcome rather than the business. **EV/EBITDA**, where a large share of development cost is capitalised, rewards the capitalisation choice. **EV/revenue on gross billings** for a marketplace values the merchant's revenue, not the company's.

## Red flags

- Net retention falling while billings hold up: check for pull-forward into multi-year prepay.
- cRPO growth well below revenue growth: check the renewal cohort and the average contract length.
- Customer count or ARR definition changed in the quarter: get the restated series from the transcript before carrying the trend.
- Services growing as a share of revenue: check whether the product needs implementation it did not need before.
- Stock compensation growing faster than revenue with a flat adjusted margin: check diluted share growth.
- Adjusted gross margin that excludes hosting cost: recompute on the reported basis.
- Receivables days rising with unchanged terms: check for quarter-end deal timing.

## Source hierarchy

On top of the general tiers, for this sector specifically: the 10-K and 10-Q carry revenue, RPO, deferred revenue and the segment split and are the only reported figures here. ARR, net retention, customer counts and bookings live in the shareholder letter or the quarterly supplement, are not defined by accounting standards, and change definition without a restatement, so they carry the `company claim` label with the venue and date. The transcript is where a definition change is usually admitted. Third-party download, traffic and app-spend estimates are directional evidence about a trend and never a revenue figure.

## Rules that hold in this sector

- Model and quote revenue from reported revenue, and label ARR as ARR: an ARR figure carried into a revenue line overstates the year by the ramp.
- Compare retention only within one archetype: a consumption net retention set beside a seat net retention is two different measurements in one column.
- Restate a customer or ARR series before carrying it across a definition change, or drop the earlier points and say why.
- Value a marketplace on net revenue: gross billings times a revenue multiple prices volume the company never keeps.
- Show the diluted share path beside any margin that adds back stock compensation.
