import { TickerLabel } from '@/components/TickerLogo';
import type { OrderInstrument } from '@/types/orders';
import { instrumentLabel, instrumentLogoSymbol } from './instrument';

/** An order's instrument as one line, led by its listing's logo; `fallback`
 *  stands in when the order names none (a cancel, an exercise). */
export function InstrumentLabel({
  instrument,
  fallback,
}: {
  instrument: OrderInstrument | null | undefined;
  fallback: string;
}): React.ReactElement {
  return <TickerLabel symbol={instrumentLogoSymbol(instrument)}>{instrumentLabel(instrument) || fallback}</TickerLabel>;
}
