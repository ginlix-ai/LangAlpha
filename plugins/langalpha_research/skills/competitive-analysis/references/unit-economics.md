# Unit Economics Benchmarks

Reference for SaaS, subscription and other recurring-revenue names. Use it when the competitive set is judged on customer economics rather than on units shipped or stores opened, and use the same definitions for every company in the set: a comparison of one company's net retention against another's gross retention is not a comparison.

Public filings rarely give CAC or LTV directly. Derive them from sales and marketing expense, net customer adds and disclosed churn, then label the figure as derived and show the inputs beside it. A derived number presented as a disclosed one is the fastest way to lose the room.

## Metric definitions

| Metric | Definition |
|---|---|
| ARR bridge | Beginning ARR, plus new, plus expansion, less contraction, less churn, equals ending ARR |
| CAC | Total sales and marketing spend divided by new customers acquired in the period |
| LTV | (ARPU times gross margin) divided by churn rate |
| LTV:CAC | Lifetime value per dollar of acquisition cost |
| CAC payback | Months of gross profit needed to recover one customer's acquisition cost |
| Gross retention | Percentage of beginning ARR retained, expansion excluded; caps at 100% |
| Net retention (NDR) | Percentage of beginning ARR retained with expansion included; can exceed 100% |
| Logo churn | Percentage of customers lost |
| Dollar churn | Percentage of revenue lost, often very different from logo churn |
| Rule of 40 on EBITDA margin | Revenue growth rate plus EBITDA margin. Always named with its margin basis, since `.agents/skills/comps-analysis/SKILL.md` runs the FCF-margin variant |
| Magic number | Net new ARR divided by prior-period sales and marketing spend |

## Benchmark bands

| Metric | Best in class | Good | Concerning |
|---|---|---|---|
| Rule of 40 on EBITDA margin | above 60 | above 40 | below 30 |
| Magic number | above 1.0x | above 0.75x | below 0.5x |
| Net retention (NDR) | above 120% | above 110% | below 100% |
| Gross retention | above 95% | above 90% | below 85% |
| LTV:CAC | above 5x | above 3x | below 2x |
| CAC payback | under 12 months | under 18 months | over 24 months |

The bands are calibrated on enterprise software. Move them before applying them elsewhere: SMB and consumer subscription businesses churn faster and are read against gross retention nearer 80%, while infrastructure and usage-based names run higher NDR and lower gross retention at the same quality. Say which convention you used.

## Cohort matrix

The single most informative view of revenue quality, and the one to ask for first. Rows are acquisition vintages, columns are years since acquisition:

| Cohort | Year 0 | Year 1 | Year 2 | Year 3 | Year 4 |
|---|---|---|---|---|---|
| 2022 | $1.0mm | $1.1mm | $1.2mm | $1.1mm | |
| 2023 | $1.5mm | $1.7mm | $1.8mm | | |
| 2024 | $2.0mm | $2.3mm | | | |
| 2025 | $3.0mm | | | | |

Show it twice, in dollars and indexed to Year 0 equals 100%. The dollar view sizes the business, the indexed view is the only one where cohorts of different sizes are comparable, and a vintage that peaks in Year 2 and rolls over is visible in the indexed view alone.

## Caveats

- **NDR above 100% can hide heavy gross churn** when expansion inside the surviving base is strong enough to cover it. Never publish NDR without gross retention beside it: 130% net on 82% gross is a very different business from 130% net on 96% gross, and the first one stops working the moment expansion slows.
- **Contracted ARR is not recognised revenue.** Say which one a chart is drawn from and keep it consistent across the competitive set.
- **Professional services revenue belongs in its own line.** It is not recurring and it carries lower margin, so blending it into ARR flatters growth and dilutes the margin story at the same time.
- **Usage-based models do not have a clean ARR.** Read consumption trends, net revenue retention on active accounts and expansion within cohorts instead of forcing subscription metrics onto them.
- **Blended CAC hides the segment that is failing.** Split enterprise, mid-market and SMB whenever the disclosure allows it; a healthy blended payback often covers an SMB cohort that never pays back at all.
- **Aggregates hide the problem the cohorts show.** When customer-level data is available, build the cohorts from it rather than trusting the summary metrics a company chose to present.
