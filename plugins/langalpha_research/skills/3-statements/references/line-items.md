# Line Items: Canonical Labels and Aliases

Deferred reference for `3-statements`. Load it while transcribing historicals, when a filing's label does not match the model's row, when two companies name the same line differently, or when a subtotal will not tie to its components. `.agents/skills/comps-analysis/SKILL.md` routes here for the same reason: a peer table only aligns if both sides mapped to the same canonical row.

One canonical label per line. Map every reported label to one of them at ingestion and record the mapping beside the figure, so a reviewer can see what was renamed. Where a company reports something that maps to no canonical row, add a row rather than forcing it into the nearest one, and say what it holds.

## Income statement

| Canonical | Reported as | Watch for |
|---|---|---|
| Revenue | net sales, net revenue, total revenues, turnover, total net sales | gross billings against net revenue changes the whole margin structure; agent against principal recognition decides which one is reported |
| Cost of revenue | cost of sales, cost of goods sold, cost of services | whether depreciation and stock compensation sit inside it, which moves gross margin without moving anything real |
| Gross profit | gross margin (as a dollar line) | some filers never present it; compute it and say you did |
| Research and development | technology and development, product development, engineering | capitalised development moves out of here and into capex, and the two firms are then not comparable |
| Selling and marketing | sales and marketing, advertising and promotion, distribution costs | shipping sometimes sits here and sometimes in cost of revenue |
| General and administrative | administrative expenses, corporate expenses, overhead | a corporate line that also holds unallocated segment costs |
| Stock-based compensation | share-based payments, equity compensation | usually allocated across the expense lines rather than shown separately; the cash flow statement is where the total lives |
| Operating income | operating profit, income from operations, EBIT | an EBIT the company presents may already exclude items; check against the reconciliation |
| EBITDA | adjusted EBITDA, adjusted operating income | almost never a reported line under the accounting standards; build it and state the build |
| Interest expense | finance costs, interest and debt expense | net against gross of interest income; capitalised interest is excluded from the expense and belongs in capex |
| Other income (expense) | non-operating income, other net | FX, equity-method income and one-off gains hide here and should be separated from anything recurring |
| Pre-tax income | income before income taxes, profit before tax, EBT | |
| Income tax expense | provision for income taxes, tax charge | current against deferred split matters for the cash flow bridge |
| Net income | profit for the period, net earnings | to the company against to all shareholders: subtract non-controlling interests before EPS |
| Diluted shares | weighted average shares, diluted | period-weighted, so a buyback shows up in the count slowly and in the balance sheet immediately |

## Balance sheet

| Canonical | Reported as | Watch for |
|---|---|---|
| Cash and equivalents | cash and cash equivalents, cash and short-term deposits | short-term investments are a separate line and are not always available to pay debt |
| Short-term investments | marketable securities, current investments | |
| Accounts receivable | trade receivables, trade and other receivables, debtors | net of allowance; a jump in unbilled or contract assets is a revenue-quality signal |
| Inventory | inventories, stock | LIFO reserve, where applicable, has to be added back before comparing across companies |
| Prepaid and other current | other current assets | |
| Property, plant and equipment | fixed assets, PP&E net, right-of-use assets | operating lease right-of-use assets arrive with a matching lease liability; keep both or neither |
| Goodwill | goodwill | separate from other intangibles: only one of them amortises |
| Intangible assets | other intangibles, acquired intangibles | |
| Accounts payable | trade payables, trade and other payables, creditors | |
| Accrued liabilities | accrued expenses, other accruals | |
| Deferred revenue | contract liabilities, unearned revenue, customer deposits | the current and non-current split matters for the working-capital calculation |
| Short-term debt | current portion of long-term debt, notes payable, current borrowings | the current portion belongs in the debt schedule, not in working capital |
| Long-term debt | borrowings, senior notes, term loan, non-current lease liabilities | face against carrying value: discounts, premiums and issuance costs sit in carrying |
| Deferred tax | deferred tax asset, deferred tax liability | net presentation hides one side of it |
| Non-controlling interest | minority interest | inside equity, and a claim on enterprise value |
| Common stock and APIC | share capital, share premium, contributed capital | |
| Retained earnings | accumulated deficit, retained profits | negative is an accumulated deficit; the roll-forward is the same |
| Treasury stock | own shares held | negative inside equity |

## Cash flow statement

| Canonical | Reported as | Watch for |
|---|---|---|
| Depreciation and amortisation | depreciation, amortisation of intangibles, DD&A | tie the total to the PP&E and intangible schedules, not to the income statement alone |
| Stock-based compensation | share-based payment expense | the reliable place to find the total |
| Change in working capital | changes in operating assets and liabilities | the movement here rarely equals the balance-sheet movement once acquisitions and FX are in; the difference is a real reconciling item, not an error |
| Capital expenditure | purchases of property and equipment, additions to PP&E, purchase of intangibles | capitalised software sits here for some filers and in operating expense for others |
| Acquisitions, net of cash | business combinations | |
| Debt issued / repaid | proceeds from borrowings, repayment of borrowings | gross, not net, or the maturity ladder cannot be modelled |
| Share repurchases | purchase of treasury stock, buyback | |
| Dividends paid | distributions to shareholders | including any paid to non-controlling interests, which is a separate line |
| Effect of FX on cash | exchange rate effects | it belongs in the cash roll-forward and in no other subtotal |

## Traps that produce a tie-out failure

- **A subtotal that is a reported line, not a sum.** Some filers present a subtotal that excludes a line printed above it. Transcribe the components and let the model compute the subtotal, then check it against the reported one and record the difference.
- **The same word for two things.** "Other" appears in three statements and means something different in each. Never map two reported "other" lines onto one canonical row.
- **Restated prior periods.** A comparative column in this year's filing may not match last year's filing. Take the most recent presentation, and keep the original figure beside it per `.agents/skills/model-update/SKILL.md`.
- **Reclassification without restatement.** A company moves a cost between lines with no prior-period restatement, so the growth rate on both lines is wrong for one year. Flag it and normalise, or the margin trend is fiction.
- **Segment totals that do not sum to the group.** There is almost always an unallocated or corporate line. Model it explicitly rather than spreading it.
- **Currency and scale.** Thousands, millions and units appear in one filing across different tables. Normalise at ingestion and record the factor applied.
