// Pure scroll decision logic for the chat transcript. Kept free of DOM/layout
// so it is unit-testable under jsdom (which has no layout).

export interface ScrollMetrics {
  scrollTop: number;
  scrollHeight: number;
  clientHeight: number;
}

/** Distance from the bottom is within `threshold` px (inclusive). */
export function isNearBottom(m: ScrollMetrics, threshold = 120): boolean {
  return m.scrollHeight - m.scrollTop - m.clientHeight <= threshold;
}
