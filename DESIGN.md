# LangAlpha Web Design System — "Quiet Workspace"

The visual language of the web app (`web/`): a quiet, charcoal-and-paper research
workspace where **content is the interface** and color is reserved for meaning.
Tokens are the single source of truth — every value below lives in
`web/src/styles/tokens.css`, and every `var(--color-*)` referenced anywhere must
be declared there (enforced by `web/src/styles/__tests__/tokenRefs.test.ts`).

## Identity

- **Temperature**: dark charcoal layers / light warm paper. No decorative
  gradients anywhere (`--color-accent-gradient` is intentionally flat).
- **Accent**: burnished amber — `#E9954A` (dark) / `#D07D33` (light). It is an
  **annotation color, not a brand fill** (see Accent discipline).
- **Radius**: soft, `--radius: 0.5rem` everywhere; no per-component overrides.
- **Depth**: borders first, shadows second (`--shadow-card` is the only card
  shadow); never glows.

## Palette (both themes live in tokens.css)

| Role | Dark | Light |
|---|---|---|
| Page ground | `hsl(var(--background))` charcoal ~#191919 | white |
| Canvas under card grids | = page | `#F5F4F1` warm paper |
| Cards | `hsl(var(--card))` ~#202020 | white + `#E8E8E6` hairline |
| Elevated (menus, tooltips) | `#2A2B2E` | `#F7F7F6` |
| Primary text | `#E6E6E4` | `#1F1F1E` |
| Accent (annotation) | `#E9954A` | `#D07D33` |
| Primary button | `#ECECEA` bg / `#1A1B1D` text | `#1F1D1A` bg / `#FAF9F7` text |
| Profit / loss | `#3FB950` / `#F85149` | `#1A7F37` / `#CF222E` |
| Warning | `#D8B04C` | `#B45309` |

Background roles (`bg-page` → `bg-canvas` → `bg-card`/`bg-tool-card` →
`bg-elevated`, `bg-input`, `bg-popover`) are documented in the comment block
above the background group in `tokens.css` — pick by surface role, never by
matching a hex. Floating surfaces deliberately do not share one fill yet; don't
converge one of them in isolation.

## Typography

| Role | Face | How |
|---|---|---|
| Display / headings | **Sora** | `.title-font` (`--font-ui`, tracking −0.01em) |
| UI + body | **Geist** (+ Noto Sans SC for CJK) | `--font-ui` / `--font-content` |
| Data, numbers, kickers | **JetBrains Mono** | literal stack at call sites (`'JetBrains Mono', 'Menlo', monospace`) |

Tabular numerals (`font-variant-numeric: tabular-nums`) wherever digits align.

## Accent discipline (the rule that makes the system)

Amber is **annotation-only**: small icons, dots, chips, thin indicators, the
ascii liveness glyph. Never:

- a button or CTA fill, wash, ring, or glow;
- a **one-sided accent bar** on a card, panel, or blockquote — liveness and
  state are carried by glyphs + text, and borders stay uniform neutral. The
  only sanctioned edge markers are **nav selection** (sidebar rail edge,
  bottom-bar underline, watchlist active row).

Semantic color (profit/loss/warning/danger) is separate from the accent and
never substitutes for it.

## Status & liveness vocabulary

- **Live work** renders the shared ascii `Loader` glyph (amber) + text — never
  a spinning lucide icon.
- **Completed** on dense surfaces (nav subagent rows) renders **no glyph**:
  absence of the liveness glyph is the done state. Roomier surfaces (status
  bar) may keep a celebratory check.
- Exceptional terminal states keep informative glyphs (error = alert,
  cancelled = stop, spawned-idle = hollow circle).
- Status/icon mappings live in one table per vocabulary
  (`web/src/pages/ChatAgent/components/taskStatusUi.tsx`); surfaces declare
  overrides there instead of restating ladders.

## Motion

Entrance animations are shared utilities (`web/src/styles/animations.css`):
staggered fade-up with `cubic-bezier(0.16, 1, 0.3, 1)`, ≤ 500ms. No ambient or
looping decoration; motion signals state change only. Respect
`prefers-reduced-motion`.

## Empty states

Dot-grid texture is reserved for empty states only — never behind content.

## Working rules

1. New colors enter through `tokens.css` (both themes, plus the role comment)
   — never a hex literal in a component. Canvas painters that can't read CSS
   variables go through `web/src/lib/themeTokens.ts`.
2. Style both themes in the same change; the light theme is a first-class
   surface, not an inversion.
3. Buttons are quiet: `--color-btn-primary-*` for primary, neutral borders for
   secondary. `--color-text-on-accent` exists solely for text sitting on an
   amber accent fill (rare, annotation-scale).
4. Liveness = glyph + text, per the vocabulary above.
5. When in doubt, remove decoration rather than add it.
