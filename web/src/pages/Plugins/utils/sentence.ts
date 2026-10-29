/**
 * Text written to sit mid-line, standing at the start of one: first letter
 * capitalized. A server's findings and the row-note clauses are written lower
 * case on purpose, so they compose into longer messages ("refused: this
 * plugin uses …"); standing alone, they read as a fragment. A script without
 * case is left as it is.
 */
export function capitalizeFirst(text: string): string {
  return text.replace(/^\p{Ll}/u, (letter) => letter.toUpperCase());
}
