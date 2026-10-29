/**
 * Shared web search result parsing utilities.
 *
 * Used by DetailPanel (structured card rendering) to normalize results from
 * Tavily, Bocha, and Serper.
 */

/** Result types that should be displayed as search result cards.
 *  Skips: knowledge_graph, people_also_ask, related_searches (Serper),
 *         image (Tavily/Bocha verbose mode) */
function isDisplayableResult(item: Record<string, unknown>): boolean {
  return !item.type || item.type === 'page' || item.type === 'news';
}

/** Parse raw web search content into a filtered array of displayable results. */
export function parseDisplayableResults(raw: unknown): Array<Record<string, unknown>> | null {
  let results: unknown;
  try {
    results = JSON.parse(typeof raw === 'string' ? raw : String(raw));
    if (!Array.isArray(results)) return null;
  } catch {
    return null;
  }
  const filtered = (results as Array<Record<string, unknown>>).filter(isDisplayableResult);
  return filtered.length > 0 ? filtered : null;
}

/** Build a URL-keyed lookup map from artifact.results for enriching content items.
 *  Handles both `url` (Tavily/Bocha) and `link` (Serper) field names. */
export function buildRichResultMap(
  artifact: Record<string, unknown> | undefined,
): Map<string, Record<string, unknown>> {
  const map = new Map<string, Record<string, unknown>>();
  const richResults = artifact?.results as Array<Record<string, unknown>> | undefined;
  if (richResults) {
    for (const rr of richResults) {
      const url = (rr.url as string) || (rr.link as string);
      if (url) map.set(url, rr);
    }
  }
  return map;
}

/** Resolve the best available snippet for a search result item.
 *  Priority: artifact rich snippet > content body > content snippet field. */
export function resolveSnippet(
  item: Record<string, unknown>,
  richResult: Record<string, unknown> | undefined,
): string {
  return (richResult?.snippet as string) || (item.content as string) || (item.snippet as string) || '';
}
