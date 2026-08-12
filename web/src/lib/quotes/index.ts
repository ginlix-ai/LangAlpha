/**
 * Unified frontend quote layer. See quoteBatcher.ts for the coalescing design.
 */
export { useQuote, useQuotes } from './useQuotes';
export type { UseQuoteOptions, UseQuoteResult, UseQuotesOptions, UseQuotesResult } from './useQuotes';
export { getQuoteBatcher, QuoteBatcher, quoteKey, stockKey, indexKey, BATCH_WINDOW_MS } from './quoteBatcher';
export type { QuoteRow } from './quoteBatcher';
export { snapshotToStockPrice } from './quoteAdapters';
export { writeQuoteFromWs } from './quoteWriteThrough';
export type { WsQuoteFields } from './quoteWriteThrough';
