/**
 * WS write-through into the unified quote cache.
 *
 * Real-time WS frames carry live price/change; push them into ['quote', SYMBOL]
 * so REST and WS agree and every quote consumer sees the live value. Only the
 * fields the WS frame actually carries are merged — name, previous_close, day
 * OHLC/volume and any other REST-authored fields are preserved.
 *
 * Guard: we only merge into an entry that already exists (prev !== undefined).
 * A WS frame must never *seed* a fresh entry, or it would create a partial,
 * `previous_close`-less row that reads as fresh and suppresses the initial REST
 * fetch. Symbols get their first quote from REST; WS only keeps it live.
 */
import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { stockKey, type QuoteRow } from './quoteBatcher';

/** The live fields a WS aggregate frame contributes. */
export interface WsQuoteFields {
  price: number;
  change: number;
  changePercent: number;
}

/**
 * Merge live WS fields into an existing ['quote', SYMBOL] entry. No-op if no
 * entry exists yet. Exported (rather than inlined) so the merge is unit-tested
 * independently of the WS transport.
 */
export function writeQuoteFromWs(
  queryClient: QueryClient,
  symbol: string,
  fields: WsQuoteFields,
): void {
  const key = stockKey(symbol);
  if (!key) return;
  queryClient.setQueryData<QuoteRow | null>(queryKeys.quote.detail(key), (prev) => {
    // undefined = nobody watching; null = REST resolved "unknown symbol".
    // Neither may be seeded into a partial, previous_close-less row.
    if (prev == null) return prev;
    const base: QuoteRow = prev;
    return {
      ...base,
      symbol: base.symbol ?? key,
      price: fields.price,
      change: fields.change,
      change_percent: fields.changePercent,
    };
  });
}
