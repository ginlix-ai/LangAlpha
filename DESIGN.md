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
- **Radius**: soft, growing with the element. Pick from the scale, never a new value:
  4px inline code · 6px thumbnails · 8px (`--radius`) the default for rows, tables
  and most surfaces, and the user's avatar up to 36px · 12px transcript file cards,
  the composer's send button and the 48px settings avatar · pills (half the height)
  for chips, tags and agent avatars.
  - **One exception, the chat composer at 22px**: concentric with its 12px send
    button, which sits 10px from the bottom edge (12 + 10). The todo tab tucked
    behind it takes 16px, about 0.7× the composer. Change the composer's radius
    and the send button's radius or inset together, or the corner pinches.
- **Depth**: borders first, shadows second (`--shadow-card` is the only card
  shadow); never glows.

## Palette (both themes live in tokens.css)

| Role | Dark | Light |
|---|---|---|
| Page ground | `hsl(var(--background))` charcoal ~#191919 | white |
| Canvas under filled cards (orders; the dashboard's frames sit on the page) | = page | `#F5F4F1` warm paper |
| Cards | `hsl(var(--card))` ~#202020 | white + `#E8E8E6` hairline |
| Elevated (menus, tooltips) | `#2A2B2E` | `#F7F7F6` |
| Primary text | `#E6E6E4` | `#1F1F1E` |
| Focus ring | = primary text | = primary text |
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

The rule governs the product's own surfaces. Illustrations that *depict* the
product are not one of them: the onboarding intro panels draw a miniature of the
app rather than being part of it, and they carry the accent structurally, in
washes, glows and fills. A picture of a branded thing reads as brand, not as
state. The alternative is a picture whose color has to be kept in step with the
brand by hand, which is how that panel spent a rebrand still showing the old one.

**The sign-in page is the one product surface that is exempt**, and the reason
is that the rule has nothing to bite on there. It carries no chrome, no
navigation and no data, so there is nothing for the accent to annotate. Amber
is its whole interaction vocabulary instead: the submit button has always taken the
fill on hover, and keyboard focus takes the same fill rather than inventing a
second language for the same "about to act" state. Focus is not left to hue
alone, since hue is what a color deficiency flattens: an underline by default, a
doubled edge on the boxed controls, an outline under forced colors. The exemption
is the `.login-page` surface itself, not the route it mounts on: the same build
serves the app from that route once someone is signed in.

Semantic color (profit/loss/warning/danger) is separate from the accent and
never substitutes for it.

## Status & liveness vocabulary

- **Live work** renders an amber liveness glyph beside text that says what is
  running, never a spinning lucide icon. The ascii `Loader`
  (`web/src/components/ui/loader.tsx`) marks running rows, chips and cards; the
  `FigureEightLoading` orbit sits under a reply while its text is still, the
  agent at work with nothing yet to type. Reduced motion slows both rather than
  stopping them: a glyph that changes is not movement across the screen.
- **A running label shimmers**, and it is the one looping text treatment:
  `TextShimmer` sweeps the title of a running tool call, the reasoning row's
  label while it streams, the reader's own message while it is still being
  delivered (pending, steering or queued), and a chart's loading label. It
  ends with the work, it stops under reduced motion, and nothing else loops on
  text.
- **Completed** on dense surfaces (nav subagent rows) renders **no glyph**:
  absence of the liveness glyph is the done state. Roomier surfaces (status
  bar) may keep a celebratory check.
- Exceptional terminal states keep informative glyphs (error = alert,
  cancelled = stop, spawned-idle = hollow circle).
- A running turn lists its tool calls a row per call. The status line, opt-in
  in Settings while it is being tried out (`other_preference.tool_call_layout:
  'status'`), shows one line naming the current step under the counts of the
  finished ones, in the same glyphs and shimmer. A turn shows one line, on its
  newest activity. Between steps the line holds the step that last finished,
  settled, until the next one takes its place, and it stays while the reply
  types under it; it goes when the turn ends and folds its work away. The step
  a turn opens on, a thought or a call, runs where the counts will be, at their
  height, so nothing is held empty above the line: a reply finds it where it
  ran, and the next step turns it into the count in place, with the line coming
  in under it. The line's steps are drawn at that height too.
- Status/icon mappings live in one table per vocabulary
  (`web/src/pages/ChatAgent/components/taskStatusUi.tsx`); surfaces declare
  overrides there instead of restating ladders.

## Data cards

A tool call that returns data (a quote, a chart, a filing, an order) lands in
the transcript as a card and opens a detail panel in the same language
(`web/src/pages/ChatAgent/components/charts/TermCard.tsx`, `TermPanel.tsx`).
The reader picks one of three styles in Settings, per device
(`web/src/lib/cardStyle.ts`, stamped on `<html>` as `data-card-style`); the
dashboard's widgets and windows follow it too:

- **Balanced**: Terminal's frame, its title set into the top edge,
  with words in the text face, figures in mono and tags as pills. Data reads as
  data beside the filled cards a reply opens (files, dashboards).
- **Quiet**, the default: the same type and marks in the platform's own card,
  filled, its title inside, on the warm grays, so cards change the app least.
- **Terminal**: the unfilled frame with mono throughout, bracket tags, dashed
  rules and dotted leaders, on a cool palette (the `--color-term-*` tokens) so
  a figure reads as data against the warm page.

Terminal is the cards' own stylesheets. Balanced and Quiet live in
`charts/termCardStyles.css` and only take marks away, so all three share one
layout and one set of components. A new card takes its words from the kit's
classes (`term-label`, `term-caps`, `term-fill`; `term-code` for an identifier
that stays mono), never from an inline `text-transform`, or the quieter styles
cannot reach it.

## Motion

Motion says something changed: it arrived, left, opened, closed or was pressed.
Nothing moves for decoration. A loop says something is live: the vocabulary
above, a loading skeleton, a live-data dot, a microphone listening.

**One vocabulary**, `web/src/lib/motion.ts`, mirrored for CSS in `tokens.css`:

| Token | Value | For |
|---|---|---|
| `DURATION.press` · `--motion-press` | 100ms | the press scale |
| `DURATION.quick` · `--motion-quick` | 160ms | color and hover, a menu, tooltip or chip appearing |
| `DURATION.fold` · `--motion-fold` | 240ms | a section folding, a panel growing to fit |
| `DURATION.enter` · `--motion-enter` | 280ms | a dialog, sheet or card entering |
| `DURATION.exit` · `--motion-exit` | 140ms | anything leaving |
| `DURATION.reveal` · `--motion-reveal` | 360ms | streamed text fading in as it is typed |
| `STAGGER` · `--motion-stagger` | 40ms | between the items of a batch |

Curves: `EASE_OUT` (`--ease-out`), fast out with a long settle, for nearly
everything; `EASE_REVEAL` (`--ease-reveal`) for a fade that must not pop, such
as streamed text, which `EASE_OUT` has at half opacity a tenth of the way in;
`EASE_COLLAPSE` (`--ease-collapse`) for a tall box closing, so the content
under it does not leap. Springs: `SPRING_FOLD` (disclosures, chevrons),
`SPRING_SNAPPY` (live rows), `SPRING_INDICATOR` (knobs, sliding markers) and
`SPRING_DRAG` (whatever a finger lets go of), each damped to at least 0.85 of
critical, so nothing overshoots. Tailwind's bare `transition` and
`transition-colors` already run `quick` on `EASE_OUT`, and `duration-*` and
`ease-*` take the token names.

- **Enter and settle; leave faster.** A small surface pops in at its anchor
  (`.pop-in`: menus, popovers, hover cards, tooltips), a dialog uses
  `.pop-in-center` and a sheet `.sheet-in`. One state replacing another in
  place crossfades rather than cutting, through `SwapCell`
  (`web/src/components/ui/crossfade.tsx`), which keeps the leaving state from
  screen readers and clicks.
- **Folds are measured.** A box changes height by tweening measured px, through
  the one fold, `Disclosure` (`web/src/components/ui/Disclosure.tsx`). Inside
  the chat scroller nothing hands framer `auto` or a `layout` prop: resolving
  one forces a layout that clamps the scroll offset.
- **Live rows open, then rise in.** A row of a running turn opens its space
  first, and its content rises a few px into it as it fades in once the space
  is nearly open; leaving, the content fades and lifts first and the space
  closes after (`LiveRow`). A step that replaces another on the status line
  rises in as the old one lifts out (`SwapCell`'s `riseKey`). A box opening
  over content already showing drew every glyph top first, cut off at the
  bottom. A row opening from rest is launched at its spring's own rate, so the
  space is nearly open in 160ms and the content comes in that much sooner.
- **Press, not hover.** A committing button scales to 0.97 (`PRESS_SCALE`,
  `--press-scale`) over `press`, through the shared `Button` or the `press`
  class, which carries its own transition list and so never sits beside a
  `transition-*` class. Hover changes color, background or border and never
  size. What moves on hover moves 2px at most: an arrow nudging toward where it
  leads, a chip or the sign-in buttons lifting 1px.
- **Stagger only a batch the reader asked for**, and only its first few items.
- **Reduced motion** is one block in `styles/animations.css` for every CSS
  transition and keyframe, plus framer's `MotionConfig` for transforms. What
  neither reaches (a framer opacity loop, an imperative `animate()`, a smooth
  scroll) asks `useMotionSafe()` or `scrollMotion()` in
  `web/src/lib/reducedMotion.ts`.
- **No literals.** A duration, curve or spring written outside `motion.ts`,
  `tokens.css` and `animations.css` fails lint for scripts and
  `styles/__tests__/motionTokens.test.ts` for CSS. A value no token names on
  purpose, such as a loop's period, takes an exemption with its reason.

**Pin your message, then follow.** On send the reader's message holds near the
top of the transcript and the reply fills the room under it; once the reply
outgrows that room the view glides after its tail
(`web/src/pages/ChatAgent/components/chatView/useChatScroll.ts`).

## Illustrations

The Computers dialog draws each machine a computer runs on
(`web/src/pages/ChatAgent/components/machineArt/`), and the New computer form
draws the one it will add. A machine the user brings is drawn front on from
its maker's own shots, so a MacBook Pro or a Mac Studio is recognisably that
machine; a laptop is drawn the way Apple draws its own (the `macbook` SF
Symbol), its base wider than the lid, since the true elevation, where the two
are one width, reads as a box on a box. It is kept smaller than a hosted box's
stage, so the two read at one scale. Until it has been reached and its model
read, it is the two it could be, a laptop and a Linux desk, in dashed
outline with no fill or shading, and the corner says it is drawn once
reached. A hosted computer is a small box of our
own on a desk, never a maker's product, sized by its spec on one shared scale,
so a bigger spec is a bigger box. A drawing carries its own shading, from
the `--color-art-*` tokens: lit from the upper left, a top and four side
shades, hairlines in three weights that stay 0.6px at any scale, and a soft
contact shadow. No gradients and no glow. The one colour in a drawing is the
machine's light, which says what its status line says (success while it runs,
the accent while it gets ready or goes down), and its screen shows the app in
the reader's theme while it runs. While it gets ready (made, set up or
starting) its light flickers like a disk at work and its screen types a few
lines of setup at one corner, a line a beat, then waits on a cursor: small and
faint, to be glanced at rather than read. Both stop as it comes up, the app
fading in over the log; going down, the light holds still and the screen goes
dark. Reduced motion shows the log whole and the light steady. A laptop is
always drawn open: front on, a shut one is a slab the width of its base,
which the narrower lid of this drawing cannot become, so a new one boots on
its screen rather than opening.

## Alignment

- **A menu's rows share one leading column.** Every row starts with a glyph of
  one width, an item's icon or a computer's status dot centred in that width,
  so every label starts at one x. A row with nothing to show there gets an
  icon (All computers takes the server glyph), never an empty indent.
- **What trails a row sits flush with its right edge**: a badge, a count, a
  shortcut. Nothing holds an empty column for a mark only one row shows: the
  chosen row's check takes its room at its end, and only there, so the other
  rows' badges reach the edge (the computer filter). A menu of choices alone,
  where the labels must not move as the choice does (the sort menu), keeps
  its check column.
- **Drawings side by side share one floor**: each stands on the bottom of its
  frame (§ Illustrations), so a laptop and a box beside it line up.

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
4. Liveness = glyph + text, per the vocabulary above; motion = the tokens in
   § Motion.
5. When in doubt, remove decoration rather than add it.
6. Check alignment by looking at it: a screenshot of the real surface, in both
   themes, at the widths it ships at. A reserved column or an edge a few px
   off reads fine in the markup and wrong on screen.
