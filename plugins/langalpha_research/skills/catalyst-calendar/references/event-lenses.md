# Event Lenses

Deferred reference for `catalyst-calendar`. Read it when a captured event falls outside the earnings and macro classes, and when a `P1` row needs its per-event action block filled.

Each lens gives the core question the event answers, the outputs the row needs before it is useful, and what to pull from the primary document. The classification decides the work, so classify by the path the money takes rather than by the headline: a court date in a merger is merger arithmetic, and a court date about a patent is litigation.

## Earnings and guidance

- **Core question**: does the print move the estimate path, and is the reaction about the quarter or about the guide?
- **Required outputs**: consensus with its vintage, our estimate, the buy-side bogey where observable, the options-implied move with its session, the two or three metrics the reaction hangs on, and the guidance shape that would change the model.
- **Extract**: the release date and session from the issuer's announcement, prior-quarter guidance language, the segment disclosure the model needs, and whether this issuer has pre-announced before, which moves the date that actually matters earlier.

## Investor day and capital-markets day

- **Core question**: are the medium-term targets credible, and what has to be true for them to hold?
- **Required outputs**: the prior target set and how it was tracking, the bridge from today's numbers to each new target, the assumptions the bridge rests on, and whether targets are reiterations, raises or quiet retreats.
- **Extract**: the agenda and presenter list, prior target slides, and any pre-announced guidance framework.

## Regulatory decision

- **Core question**: which milestone is this, and what does it decide?
- **Required outputs**: the milestone named exactly (submission, acceptance, information request, hearing or advisory committee, decision, appeal window, launch or effective date), the statutory or target clock behind the date, the outcome set with what each does to the model, and the follow-on milestone each outcome triggers.
- **Extract**: the docket or register entry, the agency's own target date, the conditions attached to a prior milestone.
- **Failure to avoid**: writing an acceptance date as an approval date. They are different events with different payoffs, and the calendar row is the place the confusion starts.

## Clinical readout

- **Core question**: which readout is this, and is it powered to settle the question the thesis asks?
- **Required outputs**: the readout stage separated (abstract acceptance, conference presentation, topline release, full data, submission, advisory committee, decision), the endpoint and whether it is primary, the comparator, enrolment status, and the window rendered as a window.
- **Extract**: the trial registry entry (phase, endpoints, enrolment, estimated completion), the company's own guided timing language, and the conference calendar.

## Merger and acquisition

- **Core question**: what is the spread, and what does it imply about the odds of closing?
- **Required outputs**: the deal terms (cash, stock, ratio, collar), the current spread and its annualised return to the expected close, the implied probability against a stated break price, the break price arithmetic and how it was derived, the conditions still open (shareholder vote, antitrust and foreign-investment clearances, financing), and the dates each condition carries.
- **Extract**: the merger agreement's outside date and termination fees, the proxy timetable, the regulatory filings made and their statutory clocks.

## Spin-off and separation

- **Core question**: what are the two pieces worth apart, and who is forced to sell?
- **Required outputs**: a parts-based value with each piece on its own peer set and valuation base, the distribution ratio and when-issued timeline, the forced-selling estimate (index membership the spinco will not inherit, mandate constraints of the holder base), the stranded-cost estimate, and the leverage each entity carries out.
- **Extract**: the Form 10 or equivalent separation document, pro-forma financials by entity, and the index treatment announcement.

## Activist campaign

- **Core question**: can the activist win, and what changes if they do?
- **Required outputs**: the vote arithmetic (shares outstanding, record date, the holder base and how it typically votes, the proxy advisers' positions once published), the nomination and meeting dates, the demands with the value each carries, and the path if the campaign settles instead.
- **Extract**: the activist's filed position and letters, the company's response, the bylaw nomination window, and the meeting timetable.

## Litigation

- **Core question**: what is the exposure, and when is it knowable?
- **Required outputs**: the merits in one paragraph with the standard applied, the timing (motions, trial, verdict, appeal), a damages range with its basis, the settlement path and comparable settlements, insurance and indemnity coverage, and the accounting treatment already taken.
- **Extract**: the docket entries and scheduling order, the company's contingency disclosure in its last filing.

## Index and passive flow

- **Core question**: how much mechanical supply or demand arrives, over how many days of volume?
- **Required outputs**: the index event and its effective date, the estimated share flow, that flow as a multiple of average daily volume, the passive share of the holder base, the announcement-to-effective window, and what typically reverses afterwards.
- **Extract**: the index provider's announcement and methodology for the change, the float and share count from `get_shares_float`.
- **Keep separate**: flow moves price without saying anything about the business. Write it in its own block rather than blending it into the fundamental case.

## Lockup expiry and secondary

- **Core question**: how many shares can come, from whom, and what do they cost?
- **Required outputs**: shares releasable and as a share of float, the holder base behind them (founders, sponsors, employees) and their basis, staggered release dates where the lockup tiers, prior selling behaviour, and the flow against average daily volume.
- **Extract**: the prospectus lockup terms and any early-release provisions, the registration statement, insider filing history.

## Debt maturity and refinancing

- **Core question**: does the refinancing change the earnings power or the equity's option value?
- **Required outputs**: the maturity ladder, the coupon on the maturing paper against the current market rate, the interest expense change per year, covenant headroom before and after, and the liquidity sources available if the market is shut.
- **Extract**: the debt schedule from the last filing, the covenant definitions, any commitment letters.

## Product launch and contract renewal

- **Core question**: how much revenue is at stake, and when does it show up in a reported line?
- **Required outputs**: the revenue at risk or in play with its source, the timing from launch or renewal to recognised revenue, the competitive set at the decision, and the observable that will confirm it before the print (channel data, an agency filing, a customer announcement).
- **Extract**: the contract disclosure or award notice, prior renewal history and terms.

## Conference presentation

- **Core question**: is management presenting, and is new disclosure likely?
- **Required outputs**: confirmation from the agenda that the issuer presents, the speaker's seniority, whether the slot is a fireside or a formal update, and whether the issuer is inside a quiet period. Note conspicuous absences from a sector conference as their own signal.
- **Extract**: the conference agenda page, and the issuer's own event announcement.
