---
name: thesis-tracker
description: "Keep a live thesis honest: pillar status, evidence ledger, monitoring triggers, drift detection. Triggers on thesis tracker, thesis update, is the thesis still intact, post-earnings thesis check, portfolio thesis review, re-underwrite."
---

# Thesis Tracker

Two verdicts per update, because they move apart: the **company thesis** says whether the business is doing what we underwrote, and the **security call** says whether the stock is a decision we can act on today. A business can improve while the stock gets worse, since expectations rerate faster than evidence arrives, and a weakening business does not license a trim when we hold no price, no valuation frame and no position context. One status column hides both cases.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

The tracker is **append-only**. Prior pillars, thresholds and ledger rows stay as written; a row that no longer holds is marked `superseded` or `stale` with its date, or contradicted by a named later row, and the replacement is a new row. Drift is only visible against a history that was not edited, and catching drift is what the tracker is for.

## Step 1: Take the mode, then frame the thesis and its pillars

The mode comes from what the user supplies, and its default runs without asking.

| Supplied | Mode | Default behaviour |
|---|---|---|
| Nothing | blank shell | Emit the empty structure below, list the minimum inputs, populate nothing |
| A ticker only | frame | Draft candidate pillars from filings and consensus, each carrying the evidence label its support earns and a `draft` provenance until the user confirms it |
| An existing thesis | load | Parse it into pillar records, and mark each field the source did not carry as missing |
| A thesis plus a development | update | Run the remaining steps on the delta only |
| A print, transcript or filing | post-earnings | Update, with the evidence-quality parse in step 2 done first |
| Several positions | portfolio review | Per name: aggregate status and security call, then four groups, priority actions, names deteriorating, names where evidence improved but risk and reward worsened, and catalysts inside the horizon |
| A long and a short as a pair | paired | One tracker per leg, plus what breaks the pair rather than either leg |

Three to five pillars, each a claim that evidence could kill. A claim nothing could disconfirm is a preference, not a pillar.

Every pillar carries these fields:

| Field | Carries |
|---|---|
| Claim | one falsifiable sentence |
| Priority | `core` or `supporting`; core pillars drive the reconciliation rule in step 3 |
| Baseline | the figure at underwriting, with its as-of |
| Expected path | what the metric does, by when |
| Confirm / warning / break thresholds | three levels, each with a provenance label; the break level is the exit trigger the thesis was written with |
| Latest evidence | the ledger row id and date from step 2 |
| Signal | the current direction on the step 2 scale |
| Evidence quality | the label from `.agents/skills/research-conventions/references/evidence.md` |
| Provenance | `inherited`, `draft` or `approved` per the note below: mandate approval, never a restatement of evidence quality |
| Model line | the line item this pillar drives, so a break has somewhere to land |
| Implied action | what a break implies, in the verb vocabulary of step 2 |
| Next proof point | the dated release or event that tests it |
| Owner | who does the work |

**Provenance.** Every pillar, threshold and action trigger is labelled `inherited` (written in the original underwriting), `draft` (our proposal, awaiting confirmation) or `approved` (an agreed monitoring rule). A pillar or level we introduce stays `draft` until the user confirms it, so an analyst-chosen exit price reads as a proposal rather than a mandate rule. This tracks approval, not support: a pillar drawn from a filing is `fact` on the evidence labels and still `draft` until someone signs off on it.

**Blank shell.** With no thesis supplied, deliver the empty structure and ask for the minimum inputs: ticker, direction, horizon, the two or three claims the position rests on, and any thresholds the user already runs. Fill nothing from memory, and say plainly which fields are waiting.

Done when the mode is named, every pillar carries all thirteen fields or names the field as missing, every pillar and threshold carries a provenance label, and no field holds a figure without a source.

## Step 2: Append to the evidence ledger

One row per data point, appended, never rewritten:

| Field | Carries |
|---|---|
| Id and date | stable row id, the date the fact arrived |
| Reporting period | the fiscal period the fact belongs to, per `.agents/skills/research-conventions/references/market-data-rules.md` |
| Source and type | the document or tool call, and its evidence label |
| Fact | what was reported, in figures |
| Our prior expectation | what we had modelled |
| Market expectation | consensus or the visible bogey, with its vintage |
| Interpretation | what it means for the claim, in one sentence |
| Pillar | which pillar it lands on |
| Signal and magnitude | from the scale below |
| Evidence quality | per the evidence labels |
| Impact | model, valuation, confidence, and the action taken |
| Follow-up and owner | the next piece of work and who holds it |

**Signal scale**, used as a qualitative discipline: strongly confirming, confirming, mildly confirming, neutral, mildly weakening, weakening, strongly weakening, plus `mixed`, `invalidating` and `untested`. Read the distribution of signals across a pillar and state the direction in words. Summing them into a composite score invents precision the evidence does not carry.

**Evidence-quality parse.** A headline beat becomes evidence only once it is decomposed: volume against price against mix, cost actions, tax rate, share count and buyback, currency, one-time items, KPI definition quality, the shape of guidance, revisions beyond the next quarter, cash conversion. Name which component carried the beat and whether it recurs. A beat that came from tax and share count leaves every operating pillar `untested`.

**Management credibility.** Commentary earns weight when it is quantified, consistent with what was said last quarter, specific about the mechanism, and candid about what went wrong. Vague optimism, a changed KPI definition and selective disclosure earn none. Write which of the two you are looking at.

**Accounting red flags.** Each one lowers evidence quality on the pillar it touches and opens a dated follow-up: a KPI definition or disclosure change, non-GAAP adjustments growing as a share of earnings, a revenue-recognition change, receivables or inventory building faster than sales, cash conversion falling away from reported earnings, a spike in capitalised costs, a segment restatement, an auditor or CFO departure, related-party transactions, and "one-time" charges that recur.

**Action.** The verb comes from the closed vocabulary in `.agents/skills/research-conventions/references/judgment.md`, which also states the inputs each verb needs before it is available. The tracker adds no verbs: on a short leg the reader's word for `exit` is `cover`. Its two workflow outcomes, `update model` when the change lands in a line item and `escalate` when a threshold in step 7 fires, are steps the tracker takes rather than position actions, and they sit beside the verb rather than in its slot.

Done when every new data point is one ledger row naming its pillar, its signal and its evidence quality, prior rows are unchanged, and every action verb has its inputs in hand.

## Step 3: Status the pillars and reconcile the aggregate

Each pillar takes exactly one of eight values:

| Status | Means |
|---|---|
| `strengthening` | evidence beat the expected path and the path ahead is unchanged or better |
| `intact` | evidence is consistent with the expected path |
| `watch` | a warning threshold was touched, or evidence is mixed; the next proof point decides it |
| `impaired` | a break threshold was crossed here while the thesis still stands on the other pillars |
| `broken` | the claim failed and the reason to own it is gone |
| `changed` | the business is doing something other than what we underwrote, so the old claim no longer applies |
| `untested` | no evidence has reached this pillar since underwriting |
| `retired` | deliberately closed, with the date and the reason |

**Reconciliation.** The aggregate follows the core pillars. One core pillar at `impaired` with an aggregate more benign than `watch` requires an evidenced override, written as one sentence naming what offsets it and the evidence behind the offset. Two or more core pillars at `impaired` set the aggregate to `impaired`. A core pillar at `broken` or `changed` sets the aggregate to the same. This is the anti-drift mechanism: without it the aggregate sits at `intact` for a year while the pillars underneath it rot.

The scorecard the reader sees is one row per pillar, in this shape:

| Pillar | Priority | Expected path | Latest evidence | Signal | Status |
|---|---|---|---|---|---|

**Scoring honesty.** Weighted pillar scores and conviction charts appear only when the user's own method defines the weights. Absent that, the aggregate is one of the eight words plus the sentence that justifies it.

Done when every pillar carries one of the eight values, the aggregate is reconciled or the override sentence is written, and no numeric conviction score appears that the user did not define.

## Step 4: Rate the security call

| Call | Holds when |
|---|---|
| `callable` | the action verb and every input it needs are in hand |
| `conditional` | callable once one named input arrives; name it and the date it arrives |
| `re-underwrite` | the thesis changed, so the call goes back through the initiation rather than through a trim |
| `inputs missing` | an input the call needs is missing |

These four are the status of one object, the security call, not a readiness scale: the artifact still carries one posture from the ladder in `.agents/skills/research-conventions/SKILL.md`, read against its input state.

**The gate.** Without a current price, a valuation frame or the position context (size, cost basis, benchmark weight), the security call is `inputs missing` and says which of the three is missing. A weakening company thesis converts into `re-underwrite` or into a monitoring item, and a valuation-led action waits for the valuation input.

**Price action is a signal to decompose.** Before a price move enters the update as evidence, split it across fundamentals, estimate revisions, multiple change, factor and sector beta, positioning and crowding, liquidity and flows, options and hedging, and macro. Attribute what the data supports and say what stays unattributed. A move nobody can attribute is a question, not a confirmation, and price remains a statement of belief per `.agents/skills/research-conventions/references/judgment.md`.

Done when the security call carries one of the four values, any value other than `callable` names the missing input and what would supply it, and every price move cited in the update carries its decomposition.

## Step 5: Monitoring and the KPI tracker

| Metric | Threshold | Source | Window | Confirming signal | Disconfirming signal | Action if crossed | Action if not | Provenance |
|---|---|---|---|---|---|---|---|---|

That is the six columns the monitored-item table in `.agents/skills/research-conventions/references/judgment.md` requires, plus the tracker's three: the two signal columns, which are what make the row usable by whoever reads it on the day, and provenance.

KPI tracker, one row per tracked metric, with every comparison basis present:

| KPI | This period | Prior period | Our estimate | Guidance | Consensus | Threshold | Peers |
|---|---|---|---|---|---|---|---|

A comparison we cannot get is marked `n/a` with the reason in the cell. Dropping the column hides that the KPI was never compared to the thing that would have moved the thesis.

Which KPIs a sector rewards tracking, and the warning sign that shows up first in each: `.agents/skills/thesis-tracker/references/sector-signals.md`, read when the name is outside a sector you have already framed pillars for.

Done when every monitored item names the action on each side of its threshold, and every KPI row shows all six comparison bases or marks the missing one with its reason.

## Step 6: Drift and red team

Mark each drift pattern present or absent, by name, in every update:

1. **The reason changed.** Today's rationale is not the underwriting rationale, and no re-underwrite recorded the switch.
2. **Catalyst laundering.** A catalyst passed without the expected result and was replaced by qualitative rationale rather than a new dated proof point.
3. **Valuation substitution.** "It is cheap" quietly took over from the growth or margin pillar we actually underwrote.
4. **Dismissal by horizon.** Repeated disconfirming evidence is being set aside as long-term noise.
5. **Protection as reason.** Downside protection became the reason to hold a position taken for upside.

Then red-team the current view: the strongest opposing case as its holder would put it (per `.agents/skills/research-conventions/references/judgment.md`), the evidence behind it, what would make it right, what would change our recommendation, the open questions, and the next review date. Useful prompts across sectors: is the debate about growth, margin, multiple, balance sheet, management credibility or regulation; is the KPI we track leading or lagging; is the weakness cyclical, company-specific or structural; and what does the current price already assume.

Done when each of the five patterns is marked present or absent, and the red team names a view someone actually holds with the evidence that supports it.

## Step 7: Operating model and output

The artifact opens with the operating model, so the reader knows who acts and when:

| Slot | Value |
|---|---|
| PM decision owner | |
| Analyst owner | |
| Evidence and ledger owner | |
| KPI and model owner | |
| Review cadence | quarterly at minimum, and on every catalyst |
| Post-catalyst update deadline | the working days after an event by which the ledger and statuses are updated |
| Escalation triggers | a break threshold crossed, the aggregate at `impaired` or worse, or the security call `inputs missing` for two consecutive cycles |
| Next review gate | the date, and the decision that gate makes |

Order of the deliverable: header (aggregate status, security call, readiness posture, as-of), operating model, pillar scorecard, KPI tracker, monitoring table, ledger rows added since the last version, drift and red team, next gate. Markdown for a morning meeting, or Word through `.agents/skills/docx/SKILL.md` for a review pack. Save to `$WORK_DIR/work/{task}/`.

Route the work the tracker uncovers rather than doing it here: dated events to `.agents/skills/catalyst-calendar/SKILL.md`, a changed line item to `.agents/skills/model-update/SKILL.md`, a `re-underwrite` call to `.agents/skills/initiating-coverage/SKILL.md`, and a print that needs full decomposition to `.agents/skills/earnings-analysis/SKILL.md`.

Done when both verdicts and the posture are in the first screen of the artifact, the ledger delta lists every row added since the previous version, and the next review gate carries a date.
