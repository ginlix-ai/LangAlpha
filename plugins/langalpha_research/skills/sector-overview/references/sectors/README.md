# Sector Lens Template

Deferred reference for `sector-overview`. Load it when the subject sits in a sector with no file in this directory, and when adding one.

A sector lens is context, never a deliverable. It changes which metrics the report tracks, which valuation lens leads and which failure modes get checked. It never owns the artifact: the calling skill still decides what gets built.

Every file here is `<sector>.md` and carries these seven headings, in this order, so a reader who knows one sector can read any other at speed.

| Heading | What goes under it |
|---|---|
| `## Archetypes` | How companies in the sector actually earn money, one row per archetype, each with the trap that catches an analyst who classified it wrong. Classification happens before analysis. |
| `## KPIs that matter` | The operating and financial measures the sector is judged on, with the definition where two companies would define it differently. |
| `## Model architecture` | What the forecast is built from and in what order, so the build follows the economics rather than a revenue growth rate. |
| `## Valuation lenses, in order` | The lenses in priority order, each with what it is a function of, then the generic lenses that give a confidently wrong answer here and the reason. |
| `## Red flags` | Observable patterns that say the reported figures are not what they look like. Each names what to check next. |
| `## Source hierarchy` | Where this sector's numbers actually come from, on top of the general tiers in `.agents/skills/research-conventions/references/evidence.md`, including which disclosures are company claims rather than reported figures. |
| `## Rules that hold in this sector` | The practice that holds here, each rule paired with the failure it prevents. |

Where a sector has no file, build the archetype list from the filings before analysing: read the revenue recognition note and the segment note of two names in the set, write down how each actually earns money, and carry that classification through the rest of the work. Say in the delivery message that the lens was built on the spot rather than from a file.

A new file is complete when all seven headings carry content, every valuation lens names what it is a function of, and the rules list has at least three entries drawn from the traps and red flags above it.
