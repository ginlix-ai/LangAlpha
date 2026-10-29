import { useMemo } from 'react';
import { useQuote, type QuoteRow } from '@/lib/quotes';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import type { PriceTriggerConfig } from '@/types/automation';
import { meterReading, watchedQuote, type MeterReading } from '../utils/price';

export interface PriceWatch {
  /** The symbol once typing has rested on it; null while it is still changing. */
  symbol: string | null;
  quote: QuoteRow | undefined;
  /** The lookup for `symbol` has answered, so a missing quote means there is none. */
  settled: boolean;
  /** How far the price has to go, once there is a level to measure against. */
  reading: MeterReading | null;
}

/**
 * The live quote for a price trigger that is still being written. The symbol
 * is looked up only once the typing rests, so "N", "NV", "NVD" do not each
 * cost a quote request on the way to "NVDA".
 */
export function usePriceWatch(cfg: PriceTriggerConfig): PriceWatch {
  const rested = useDebouncedValue(cfg.symbol, 400);
  const target = useMemo(() => (rested ? watchedQuote({ ...cfg, symbol: rested }) : null), [cfg, rested]);
  const { quote, isLoading } = useQuote(target?.symbol, { isIndex: target?.isIndex, enabled: !!target });
  const symbol = rested && rested === cfg.symbol ? rested : null;
  return {
    symbol,
    quote: symbol ? quote : undefined,
    settled: !!symbol && !isLoading,
    reading: symbol && cfg.conditions[0]?.value > 0 ? meterReading(cfg, quote) : null,
  };
}
