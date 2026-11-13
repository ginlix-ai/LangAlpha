/**
 * The US index families the quote API knows by their bare name (`GSPC`, not
 * `^GSPC`), and what each is called. The desktop shell reads the same five
 * as indices, so a bare `VIX` means the same thing on both sides.
 */
export const INDEX_SYMBOLS = ['GSPC', 'IXIC', 'DJI', 'RUT', 'VIX'] as const;

const BARE = new Set<string>(INDEX_SYMBOLS);

export function isBareIndex(symbol: string): boolean {
  return BARE.has(symbol);
}

/**
 * An index as people say it: `long` in the panel and Settings (`S&P 500`),
 * `short` beside the menu bar icon (`S&P`), since `GSPC` reads as a
 * typo. Null for a symbol that is not one of the five.
 */
export function indexName(symbol: string, size: 'long' | 'short', t: (key: string) => string): string | null {
  return isBareIndex(symbol) ? t(`marketIndex.${symbol}.${size}`) : null;
}
