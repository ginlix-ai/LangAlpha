/**
 * Word matching for the app's local searches (the Settings rail, the command
 * palette). A query matches a text when every word of it appears somewhere in
 * the text, in any order, so "dark theme" finds "Theme: dark". Case folds;
 * nothing else does, because a fuzzier rule (initials, typos) surfaces rows a
 * reader cannot see the reason for.
 */

/** The query's words, lowercased; none for a blank query. */
export function queryWords(query: string): string[] {
  return query.trim().toLowerCase().split(/\s+/).filter(Boolean);
}

/** Whether every word appears in `text`. */
export function matchesAllWords(text: string, words: readonly string[]): boolean {
  const folded = text.toLowerCase();
  return words.every((word) => folded.includes(word));
}

/**
 * How well `label` answers the words, lower being better, or null when the
 * label and its `extra` text together miss a word. A label that starts with
 * the query beats one with a word starting with it, which beats one that only
 * contains it, which beats a match found only in `extra` (aliases, a
 * description, the section a setting sits in).
 */
export function matchRank(label: string, words: readonly string[], extra = ''): number | null {
  if (words.length === 0) return null;
  const folded = label.toLowerCase();
  if (!matchesAllWords(`${folded} ${extra}`, words)) return null;
  const phrase = words.join(' ');
  if (folded.startsWith(phrase)) return 0;
  if (words.every((word) => folded.startsWith(word) || folded.includes(` ${word}`))) return 1;
  if (matchesAllWords(folded, words)) return 2;
  return 3;
}

/** The [start, end) spans of `text` the words cover, merged and in order, for bolding a match. */
export function matchSpans(text: string, words: readonly string[]): [number, number][] {
  const folded = text.toLowerCase();
  const spans: [number, number][] = [];
  for (const word of words) {
    let from = 0;
    for (;;) {
      const at = folded.indexOf(word, from);
      if (at < 0) break;
      spans.push([at, at + word.length]);
      from = at + word.length;
    }
  }
  spans.sort((a, b) => a[0] - b[0]);
  const merged: [number, number][] = [];
  for (const span of spans) {
    const last = merged[merged.length - 1];
    if (last && span[0] <= last[1]) last[1] = Math.max(last[1], span[1]);
    else merged.push([span[0], span[1]]);
  }
  return merged;
}
