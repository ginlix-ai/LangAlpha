export interface NewsSentiment {
  ticker: string;
  sentiment?: string | null;
  reasoning?: string | null;
}

export interface TickerImpactRow {
  ticker: string;
  sentiment: string | null;
  note?: string | null;
}

/** A story's tickers with the feed's score for each. A ticker the feed did not
 *  score stays in the list, unscored, whether its entry has an empty sentiment
 *  or it is missing from `sentiments`. `scored` is false when nothing in the
 *  story carries a score, so no surface claims an inference that was not made. */
export function tickerImpactRows(
  sentiments: ReadonlyArray<NewsSentiment> | null | undefined,
  tickers: readonly string[] | undefined,
): { rows: TickerImpactRow[]; scored: boolean } {
  const scores = sentiments ?? [];
  const rows: TickerImpactRow[] = [
    ...scores.map((s) => ({ ticker: s.ticker, sentiment: s.sentiment || null, note: s.reasoning })),
    ...(tickers ?? [])
      .filter((ticker) => !scores.some((s) => s.ticker === ticker))
      .map((ticker) => ({ ticker, sentiment: null })),
  ];
  return { rows, scored: rows.some((row) => row.sentiment) };
}
