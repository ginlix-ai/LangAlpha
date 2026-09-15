/**
 * The house motion vocabulary (DESIGN.md § Motion), in one place so a surface
 * that folds, grows or appears reads the same curve as every other one.
 * Motion signals a state change and nothing else; the numbers are short on
 * purpose.
 */

/** The entrance curve: fast out, long settle. */
export const EASE_OUT = [0.16, 1, 0.3, 1] as const;

export const DURATION = {
  /** A menu or a small element appearing. */
  quick: 0.16,
  /** A section folding open or a panel growing to fit. */
  fold: 0.24,
  /** A dialog entering. */
  enter: 0.28,
  /** Anything leaving; exits are always shorter than entrances. */
  exit: 0.14,
} as const;
