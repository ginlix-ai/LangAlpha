import type { OrderInstrument } from '@/types/orders';

/**
 * The instrument as one line, per kind.
 *
 * Returns the empty string rather than a placeholder when there is nothing to
 * name: a cancel and an exercise carry no instrument, and the caller decides
 * what an absence looks like in its own column. The switch is exhaustive over
 * the union, so an asset class the server learns to send is a compile error
 * here rather than a blank cell on a row about somebody's trade.
 */
export function instrumentLabel(instrument: OrderInstrument | null | undefined): string {
  if (!instrument) return '';
  switch (instrument.kind) {
    case 'equity':
      return [instrument.symbol, instrument.venue].filter(Boolean).join(' · ');
    case 'option': {
      const right = instrument.right === 'C' ? 'C' : instrument.right === 'P' ? 'P' : null;
      return [instrument.underlying, instrument.expiration, instrument.strike, right]
        .filter(Boolean)
        .join(' ');
    }
    case 'combo':
      return (
        instrument.strategy ||
        (instrument.legs?.length ? `${instrument.legs.length} legs` : '')
      );
    case 'future':
      return [instrument.symbol, instrument.contract_month, instrument.venue]
        .filter(Boolean)
        .join(' ');
    case 'crypto':
      return instrument.pair || '';
    case 'opaque':
      return instrument.raw_code || '';
    default:
      return '';
  }
}
