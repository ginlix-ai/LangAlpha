---
name: earnings-analysis
description: "Post-print earnings update for a covered name: beat/miss decomposition, EPS quality, transcript debate map, estimate revisions, thesis impact. Also the call-only ask that wants the transcript Q&A and the debate map alone. Triggers on earnings update, post-earnings report, analyze quarterly results, Q[N] update, what management said on the call."
---

# Earnings Update

A post-print report on a company already under coverage: what changed this quarter, whether the change recurs, what it does to estimates, and what it does to the thesis. Eight to twelve pages of DOCX, inside 48 hours of the release, written for a reader who already knows the company.

Route elsewhere when the request is a first-time initiation (`.agents/skills/initiating-coverage/SKILL.md`), a pre-print setup (`.agents/skills/earnings-preview/SKILL.md`), or a same-morning reaction blurb (`.agents/skills/morning-note/SKILL.md`).

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Output modes

| Mode | Fires when | Contract |
|---|---|---|
| Deep dive | the default | every phase present, the length budget below, assembled as a DOCX through `.agents/skills/docx/SKILL.md` |
| One-pager | the user asks for a one-pager, quick take or flash note | one page, in this order: decision box, beat/miss table with revenue and EPS variance, EPS-quality verdict, debate map, changed estimate lines with old against new. Delivered in chat unless the user asked for a document, and as a DOCX through `.agents/skills/docx/SKILL.md` when they did. No chart minimum |
| Debate map alone | the whole ask is the call or the Q&A | the transcript Q&A map and the debate map, each side carrying a falsifier tied to a dated catalyst. Delivered in chat, no report and no charts around them |

A mode is chosen once and holds, and a shorter mode is rebuilt at that depth rather than truncated (`.agents/skills/research-conventions/references/depth.md`). Missing inputs never shorten the note: a missing artifact stays visible as a labelled gap in the section that wanted it, using the absence vocabulary below.

The length budget, the chart count and the DOCX assembly belong to the deep dive. A short mode is complete on the contract in its own row; the freshness gate, the evidence contract and the tier 1 hard fails in `references/best-practices.md` bind every mode.

## Evidence contract

Every user-facing number and every quote carries a **findable** citation: the artifact plus a location pointer that puts a reader on the figure in under thirty seconds. A location pointer is a page plus table, a page plus section heading, a slide number, or a transcript line range with the speaker. The document name alone is not a citation.

Sources resolve down one **ladder**, highest first:

1. The filed 10-Q or 10-K for the quarter.
2. The 8-K exhibit that carried the results.
3. The earnings press release.
4. The investor deck and the prepared remarks.
5. The transcript, which is narrative support and never the source for a filed number.

When a document was reissued, cite the final version and keep the original timestamp beside it.

- Guidance that lives only in call commentary and in no filed document is labelled **call-only guidance** wherever it appears.
- Every non-GAAP figure appears with its closest GAAP comparable and the reconciliation source that bridges them.
- Consensus names the estimate set and its as-of timestamp, or states that the timestamp is unavailable.
- Absence is one of four words, never a blank and never a bare n/a: **not guided** (the company declined to guide it), **not disclosed** (the company does not publish it), **not provided** (it exists but is absent from the materials in hand), **source not found** (searched and unresolved, which is the `needs-source` label in `.agents/skills/research-conventions/references/evidence.md`).
- Every source ships as a hyperlink with display text, so a reader sees "10-Q" rather than the raw address, and SEC links point at the EDGAR viewer. The closing Sources section lists every material with its date and its link.

The delivered document is self-contained: a reader holding only the DOCX can follow every number in it without opening the model or the chart folder.

## The run

Five phases. Each ends on its stated criterion; the detail behind each lives in `references/workflow.md`.

### Phase 1: Freshness gate

Training data is old and the wrong quarter is the most expensive mistake this skill can make. Write down today's date, search for the most recent release rather than assuming which quarter is latest, and open the actual materials.

**Complete when** today's date, the release date, the transcript date and the filing date are all written down; the release is within 90 days of today; and every artifact names the same fiscal period, taken verbatim from the event name per `.agents/skills/research-conventions/references/market-data-rules.md`.

### Phase 2: Extraction and beat/miss

Pull reported results, pre-print consensus and our own prior estimates into one comparison, then decompose the variance by segment, geography, product and channel.

**Complete when** every headline metric has reported, expected and variance side by side; each cell carries a findable citation; every rate variance is stated in basis points; and reported and constant-currency figures sit in separate columns.

### Phase 3: Quality and drivers

The analytical core: the EPS-quality screen, the two or three load-bearing drivers, the cash-quality check, the guidance read, the transcript Q&A map and the debate map.

**Complete when** the EPS-quality screen has either produced a recurring-EPS bridge or recorded "no material trigger identified"; two to three drivers are named with what moved, why it moved and what it does to forward expectations; the cash-quality module reconciles earnings to cash; and the debate map carries a falsifier on each side tied to a dated catalyst.

### Phase 4: Estimates, valuation and model update

Revise forward estimates, restate or move the price target, and produce the model update in packet form unless the user supplied a workbook and asked for it to be written.

**Complete when** every changed line shows old, new and a one-clause reason; the price target is explicitly changed or explicitly maintained with its reason; and the update mode (packet or apply) is stated in the delivery message.

### Phase 5: Charts, report and gates

In the deep dive, build eight to twelve charts and assemble the DOCX through `.agents/skills/docx/SKILL.md`. In a short mode, skip the charts and the document. Either way, run the three quality gates in `references/best-practices.md`.

**Complete when** the hard-fail list is clean, the delivery checklist is ticked for everything the chosen mode produces, the judgement gate passes on a note that answers what changed rather than summarising the quarter, and one posture from the ladder in `.agents/skills/research-conventions/SKILL.md` is stated near the top.

## Length budget (deep dive)

| Dimension | Target |
|---|---|
| Pages | 8 to 12 |
| Words | 3,000 to 5,000 |
| Summary tables | 1 to 3, never a full P&L |
| Charts | 8 to 12, quarterly trends and changes |
| Typography | set by `.agents/skills/docx/SKILL.md` |

## Deliverable

`[Company]_Q[X]_[Year]_Earnings_Update.docx`, for example `Nike_Q2_FY24_Earnings_Update.docx`. Charts come from Python (matplotlib, pandas). A workbook update is optional and follows the packet-or-apply rule in Phase 4.

## Reference files

- The phase you are running, its steps and its tables: `references/workflow.md`.
- Writing a page or a section of the report, or the exact shape of the decision box, the recurring-EPS bridge, the Q&A map or the debate map: `references/report-structure.md`.
- Before delivery, and whenever a headline or a claim needs calibrating: `references/best-practices.md`.
