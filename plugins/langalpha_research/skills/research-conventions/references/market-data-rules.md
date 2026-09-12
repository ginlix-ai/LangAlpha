# Market-Data Correctness Rules

Deferred reference for `research-conventions`. Load it before the first market-data pull of any deliverable that prints a period label, a multiple, a growth rate, a margin or a return.

These bind every number in the artifact. They are cheap to follow while the data is in front of you and expensive to retrofit once the artifact is written.

## Fiscal periods

Take the fiscal quarter and fiscal year verbatim from the name of the reporting event, and read neither from the calendar date of the report. A company whose fiscal year ends in January reports its Q4 in February, so a February print is Q4 FY2026, not Q4 2025 and not Q1 2026. Carry that label unchanged into the title, the headers, the tables and every reference in the body. When the event name is ambiguous, settle it against the period labels on the fundamentals MCP server statements rather than guessing.

Across companies, match the period before comparing the number: two names with different fiscal year ends do not share a "Q3" unless the calendar quarters behind them line up, and a table that mixes them says so in the column header.

## LTM and NTM, never trailing and forward

Every valuation ratio carries an explicit LTM (last twelve months) or NTM (next twelve months) label, because "trailing" and "forward" leave the reader guessing which twelve months.

- LTM is the sum of the most recent four **reported** quarters, taken per company. If a peer has reported one quarter further than the subject, that peer's LTM includes it. One fixed calendar window applied across a table is wrong for every name that reports on a different schedule.
- NTM EPS is the **sum of the next four quarterly consensus mean estimates**, not a single annual estimate. When fewer than four forward quarters exist for a name, mark its NTM multiple `n/a` rather than annualising something shorter.
- Record which four quarters went into each figure, per company.

## One base date for every return

Comparative returns (YTD, one-year, ninety-day, thirty-day) use the same start date and the same end date for every ticker in the table. After pulling prices, find the first trading date present in all of them and use it as the common base. A ticker whose history starts later moves the base for the whole table rather than getting a base of its own. State the common base date beside the returns.

## Show the arithmetic

For any multi-step figure (implied quarterly numbers from annual guidance, an LTM or NTM multiple, a growth rate, a segment change), write each step out and check the intermediate before it feeds the next one. When a stated sum does not equal the components printed next to it, the artifact is wrong: recompute from the raw pull rather than reusing a remembered intermediate.

## Rate changes and level changes

A change in a rate (a margin, a yield, a share, a tax rate) is stated in basis points or percentage points. A change in a level (revenue, income, a price) is stated in percent or in currency. "Margin rose 12%" carries two readings and both mislead; write "margin rose 120bp to 24.0%", or "operating income rose 12% to the figure shown". The same split applies to a growth rate that itself moves: growth going from 8% to 11% is three percentage points, not 37.5%.

## Margins name their numerator and denominator

A margin is a pair. Fix which numerator (gross, EBITDA, EBIT, operating, net, free cash flow) and which denominator (total revenue, net revenue, revenue excluding pass-through) before comparing across companies, and hold one pair for the whole table.

Reported and adjusted margins are different metrics. Label which one the artifact uses, and where it uses adjusted, show the bridge from reported at least once. Where two companies report on different bases, one on net revenue and one on gross billings, recompute both onto the basis the table declares, or mark the peer not comparable and say why.

## No year-over-year without the year-ago period

A growth rate needs both sides. When the prior-year period is missing from the pull, print "y/y not available" and leave it out of the chart rather than estimating the base to fill the cell.

## Date-stamp market data

Price, market cap and every multiple built on them move daily. Record the retrieval date with the figure, and say so in the text when it differs from the publication date, otherwise a reader cannot tell a stale multiple from a live one. The full list of always-timestamped figures, and the freshness threshold for each data type, is in `.agents/skills/research-conventions/references/evidence.md`.
