---
name: ui-design
description: "Design-quality reference for financial-research visual output: typography, color, composition, and avoiding generic AI aesthetics"
---

# UI Design

A design-quality reference for any visual output you produce. It exists to make that output look like it came from a research desk, not a marketing landing page or a generic AI template.

Read this **before** producing any styled output — it covers design taste: typography, color, composition, and restraint. The examples below are HTML/CSS (the most common surface), but the principles apply to any visual you render — charts and images, PDFs, on-screen layouts — not just HTML.

> **This skill exists to help you match the user's personal frontend style and UI preferences — those always win over the defaults below.** If the user has stated a taste — in this conversation, in your long-term memory, or in their saved preferences — it outranks every rule here. They want a specific brand color, a chosen font, a different accent, dark-only, no serif? Do that. The directions below are strong defaults for when the user hasn't expressed a preference; they are never a license to overrule the user's explicit style.

> **These are reference defaults, not a cage.** They anchor quality and steer you off generic, lazy output — they don't replace your own judgment. When you have a clearly better, coherent choice for the specific content, take it. The only non-negotiables are the hard constraints: WCAG AA contrast, green/red reserved for profit/loss, and the theme-variable + print mechanics. The specific palette, font pairings, and exact scale below are strong starting points you are free to improve on.

## The Tone: Research Desk, Not Marketing Page

The audience is a portfolio manager, analyst, or sophisticated investor reading a research note. They want **dense, scannable, credible** information design — the visual language of a sell-side note, a Bloomberg terminal, or a quality print newspaper's business section. Not a SaaS hero page.

This means:
- Information density over whitespace theatre. A research reader expects a high signal-per-screen ratio.
- Restraint over decoration. No hero gradients, no oversized rounded cards floating on pastel backgrounds, no "Get Started" energy.
- Numbers are the protagonist. Tables, figures, and charts carry the page; prose supports them.
- Credibility cues: sources cited, dates stamped, units labelled, precision consistent.

## Avoid AI Slop

These are the tells of generic AI-generated UI. Each one has a concrete replacement — use the "instead" column.

| Anti-pattern | Why it reads as slop | Instead |
|---|---|---|
| **Inter everywhere** (or system-font-only) for headings and body | The default AI font; signals zero typographic intent | Commit to a real pairing (see Typography). A serif or a distinctive grotesque for headings; a clean readable face for body. |
| **Purple/violet gradients** on white, or any gradient hero | The single most overused AI aesthetic | One flat accent color used sparingly. Backgrounds are paper (light) or ink (dark), not gradients. |
| **Uniform rounded card grids** — everything is a `border-radius: 16px` card with a drop shadow | Marketing-template look; wastes vertical space; flattens hierarchy | Use tables for tabular data, rules (hairline borders) to separate sections, and reserve cards for genuine KPI callouts. Small radius (4–8px) or none. |
| **Emoji as icons** (📈 💰 🚀 in headings/labels) | Looks unserious; breaks in print; inconsistent rendering | Use a real icon set sparingly (inline SVG, e.g. lucide), or just clean typographic labels. Most research UIs need no icons at all. |
| **Rainbow categorical charts** — 8 saturated hues with no logic | Looks like a default Chart.js palette; hard to read; no semantic meaning | A restrained sequential or single-hue-with-tints palette; reserve green/red strictly for profit/loss. Max 3-4 categorical colors, muted. |
| **Centered everything** with huge top margins | Landing-page composition; poor for scanning data | Left-aligned reading column, tables flush, consistent baseline grid. |
| **Generic stock-photo-style placeholder vibes** — giant empty cards, "Lorem"-feeling filler | Signals the content wasn't really designed | Fill with real numbers and real findings; size containers to the actual content. |

## Typography

**Commit to an intentional pairing** — a real headline voice plus a clean body face, loaded from the Google Fonts CDN (allowlisted). The three below are proven starting points: pick one, use the user's brand font, or choose your own with the same level of intent. What matters is the commitment — don't fall back to Inter-everywhere or the bare system stack.

### Pairings that work (starting points)

1. **Editorial / authoritative** — headings `"Source Serif 4", Georgia, serif`; body `"Inter", -apple-system, sans-serif`. Serif headings give a print-research feel; body stays clean and readable. (The only acceptable use of Inter: as the *body* of a serif-headed document — never as the headline voice.)
2. **Modern terminal** — headings & body `"IBM Plex Sans", system-ui, sans-serif`; figures/tables `"IBM Plex Mono", monospace`. Plex reads as engineered and precise; the mono companion makes numeric tables align beautifully.
3. **Refined grotesque** — headings `"Newsreader", Georgia, serif` (a true reading serif) or `"Libre Franklin", sans-serif`; body `"Libre Franklin", sans-serif`. Franklin is a workhorse news face with more character than Inter.

```html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,400;8..60,600&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
```

### Type scale

A real, restrained scale (ratio ~1.25) keeps sizing coherent. The values below are a sensible default — whatever scale you use, keep it consistent rather than sizing ad-hoc.

```css
--text-xs: 0.75rem;   /* 12px — captions, source lines, table headers */
--text-sm: 0.875rem;  /* 14px — secondary text, dense table cells */
--text-base: 1rem;    /* 16px — body */
--text-lg: 1.25rem;   /* 20px — section headings (h2/h3) */
--text-xl: 1.75rem;   /* 28px — KPI figures */
--text-2xl: 2.25rem;  /* 36px — page title (h1); typically one per document */
```

### Numbers

Financial figures **must** use tabular (monospaced) digits so columns align and numbers don't jitter when they update:

```css
.figure, td.num, .kpi-value {
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum" 1;
}
```

Right-align numeric table columns. Keep decimal precision consistent within a column (e.g. all prices to 2dp). Label units once in the header, not on every cell.

## Color

A restrained, committed palette: ink on paper with a single accent, plus the two semantic financial colors. One accent is the disciplined default — add a second hue only when it genuinely earns its place (a deliberate two-tone scheme), never as rainbow filler. The table below is a **reference palette** that meets AA on its paired backgrounds; use it, or derive your own that holds the same constraints.

| Role | Light value | Dark value | Notes |
|---|---|---|---|
| Paper (page bg) | `#fbfaf8` / `#ffffff` | `#0f1117` | Warm off-white reads as print; pure white is harsher |
| Ink (primary text) | `#1a1a1a` | `#e8e8e8` | Near-black, not pure `#000` |
| Muted ink (secondary) | `#5a5a5a` | `#9aa0aa` | Labels, captions, source lines |
| Accent | `#1f5fb4` (steel blue) | `#5b9bff` | Links, active states, single-series charts |
| Profit / positive | `#1a7f4f` | `#3fb37a` | Green — gains only |
| Loss / negative | `#b42318` | `#f0685a` | Red — losses only |
| Hairline border | `#e4e1dc` | `#262a33` | Rules between sections, table row lines; use at 1px or 0.5px |

Rules:
- **WCAG AA**: body text ≥ 4.5:1 against its background; large text/UI ≥ 3:1. The values above meet AA on their paired backgrounds. Verify any custom pairing.
- **Green/red are reserved** for profit/loss and beat/miss. Never use them as decorative categorical colors.
- **Dark-aware via the fallback pattern**: author every color as `var(--color-role, #literalFallback)` so the output themes with the app yet still renders standalone and in print.
- Categorical chart palette (when you genuinely need categories): derive 3-4 muted tints rather than full-saturation hues, e.g. the accent plus two desaturated neighbors. More than 4 series → switch to small multiples or a table.

## Composition

- **One reading column**, left-aligned, max-width ~`min(100%, 1100px)` for reading-heavy layouts (wider for dense, data-heavy views). Center the column on the page, not the text within it.
- **Hairline rules over boxes** to divide sections. A 1px top border on each section header carries hierarchy more cleanly than wrapping everything in shadowed cards.
- **A real grid** for KPI rows: `display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: ...`. Consistent gaps; aligned baselines.
- **Generous but consistent vertical rhythm** between sections; tight, dense rhythm inside tables. Density is a feature for data; air is for separating ideas.
- **Hierarchy through weight and size, not color.** A heading is bigger/heavier, not a different hue. Reserve color for semantics (links, profit/loss).
- **Tables earn their space**: zebra striping is optional and should be subtle (a 2-3% tint); a hairline under the header row and between groups is usually enough. Right-align numbers, left-align labels.

## Motion (Restrained)

Motion is the exception, not the rule, in a research document.

- Keep motion restrained and purposeful — a brief entrance (e.g. a staggered fade-in of sections on load) is plenty. Avoid scattered hover wiggles, parallax, and infinite loops in a report.
- Charts may animate their initial draw (Chart.js/ECharts default) — keep it short.
- **Respect reduced-motion** and **never let animation block print**:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; }
}
@media print {
  *, *::before, *::after { animation: none !important; transition: none !important; opacity: 1 !important; }
}
```

(The `opacity: 1 !important` in the `@media print` block above is what keeps entrance-animated elements from exporting blank — keep it.)

## Match Complexity to the Content

Spend effort on precision, number formatting, alignment, and sourcing — not decoration. Add visual complexity only when the content genuinely calls for it; a clean, dense research note beats an over-animated one.

## Apply Checklist

Before delivering any styled output:

- [ ] Committed to **one** typographic pairing from above (no Inter-as-headline, no system-font-only)
- [ ] Real type scale used — no ad-hoc font sizes
- [ ] Financial figures use `font-variant-numeric: tabular-nums`; numeric columns right-aligned, consistent precision
- [ ] **One** accent color; green/red reserved strictly for profit/loss
- [ ] All colors authored as `var(--color-role, #fallback)`; WCAG AA verified for body text
- [ ] No purple gradients, no uniform shadowed-card grids, no emoji icons, no rainbow charts
- [ ] Left-aligned reading column, hairline rules over boxes, consistent vertical rhythm
- [ ] Motion (if any) respects `prefers-reduced-motion` and is forced off / opacity-restored in `@media print`
- [ ] Reads as a research desk artifact, not a marketing page — dense, credible, scannable
