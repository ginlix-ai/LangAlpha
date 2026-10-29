/**
 * The house motion vocabulary (DESIGN.md § Motion), in one place so a surface
 * that folds, grows or appears reads the same curve as every other one.
 * Motion signals a state change and nothing else; the numbers are short on
 * purpose. `styles/tokens.css` mirrors these for CSS (`--motion-*`, `--ease-*`),
 * and lint plus `styles/__tests__/motionTokens.test.ts` keep literals out of
 * the rest of the app.
 *
 * A leaf module with no imports, and it has to stay one: the e2e live-zone
 * spec reaches it from Node through `liveZoneTiming.ts`.
 */

/** The entrance curve: fast out, long settle. */
export const EASE_OUT = [0.16, 1, 0.3, 1] as const;

/**
 * Streamed text fading in as it is typed. EASE_OUT is at half opacity a tenth
 * of the way in, which on a word reads as the pop the fade is there to soften;
 * this cubic is at a quarter.
 */
export const EASE_REVEAL = [0.33, 1, 0.68, 1] as const;

/**
 * A tall box closing. Easing in as well as out keeps the content below from
 * leaping up in the first frames, which EASE_OUT does to a collapse.
 */
export const EASE_COLLAPSE = [0.45, 0, 0.2, 1] as const;

export const DURATION = {
  /** A press scaling a committing button down. */
  press: 0.1,
  /** A menu or a small element appearing. */
  quick: 0.16,
  /** A section folding open or a panel growing to fit. */
  fold: 0.24,
  /** A dialog, a sheet or a decision card entering. */
  enter: 0.28,
  /** Anything leaving; exits are always shorter than entrances. */
  exit: 0.14,
  /** Streamed text fading in as it is typed (EASE_REVEAL). */
  reveal: 0.36,
} as const;

/**
 * Each beat as a framer tween on its role's curve: everything leaves, enters
 * and folds on `EASE_OUT`, and streamed text fades on `EASE_REVEAL`. A call
 * site that needs a delay spreads one (`{ ...TWEEN.quick, delay }`).
 */
export const TWEEN = {
  press: { duration: DURATION.press, ease: EASE_OUT },
  quick: { duration: DURATION.quick, ease: EASE_OUT },
  fold: { duration: DURATION.fold, ease: EASE_OUT },
  enter: { duration: DURATION.enter, ease: EASE_OUT },
  exit: { duration: DURATION.exit, ease: EASE_OUT },
  reveal: { duration: DURATION.reveal, ease: EASE_REVEAL },
} as const;

/** The curve as a CSS `cubic-bezier(...)`, for an inline style or WAAPI, which
 *  cannot take the array framer does. */
export const cssEase = (curve: readonly [number, number, number, number]) =>
  `cubic-bezier(${curve.join(', ')})`;

/** How far apart a batch the reader asked for arrives (DESIGN.md § Motion). */
export const STAGGER = 0.04;

/** A list filling in staggers its first few rows only: past the cap the rows
 *  arrive together, so a long list never keeps the reader waiting on its tail. */
const STAGGER_CAP = 5;

export const staggerDelay = (index: number) => Math.min(index, STAGGER_CAP) * STAGGER;

/** The press scale; `styles/tokens.css` holds the same as `--press-scale`. */
export const PRESS_SCALE = 0.97;

// Springs. Every one is damped to at least 0.85 of critical
// (damping / (2 * sqrt(stiffness))), so nothing overshoots and swings back.

/** Turn and activity disclosures, and every chevron turning (ζ ≈ 0.93). */
export const SPRING_FOLD = { type: 'spring' as const, stiffness: 260, damping: 30 };

/** Live-zone rows entering, damped to just under critical (ζ ≈ 0.99) so a row
 *  never overshoots its height and swings back. */
export const SPRING_SNAPPY = { type: 'spring' as const, stiffness: 200, damping: 28 };

/** Toggle knobs and sliding selection markers: fast, and settled on arrival
 *  (ζ ≈ 0.89). */
export const SPRING_INDICATOR = { type: 'spring' as const, stiffness: 500, damping: 40 };

/** Whatever a finger lets go of: a sheet, the FAB snapping to an edge, a
 *  swipe returning (ζ = 1). */
export const SPRING_DRAG = { type: 'spring' as const, stiffness: 400, damping: 40 };
