import { describe, it, expect } from 'vitest';
import {
  isPriceTriggerConfig,
  formatPriceTrigger,
  formatRetriggerMode,
} from '../price';
import type { PriceTriggerConfig } from '@/types/automation';

const validConfig: PriceTriggerConfig = {
  symbol: 'AAPL',
  conditions: [{ type: 'price_above', value: 200 }],
  retrigger: { mode: 'one_shot' },
};

/* --------------------------------------------------------- */
/*  isPriceTriggerConfig                                      */
/* --------------------------------------------------------- */
describe('isPriceTriggerConfig', () => {
  it('returns true for a valid config', () => {
    expect(isPriceTriggerConfig(validConfig)).toBe(true);
  });

  it('returns true when optional fields are present', () => {
    const cfg = {
      ...validConfig,
      market: 'stock',
      retrigger: { mode: 'recurring', cooldown_seconds: 3600 },
    };
    expect(isPriceTriggerConfig(cfg)).toBe(true);
  });

  it('returns false for null', () => {
    expect(isPriceTriggerConfig(null)).toBe(false);
  });

  it('returns false for undefined', () => {
    expect(isPriceTriggerConfig(undefined)).toBe(false);
  });

  it('returns false for a string', () => {
    expect(isPriceTriggerConfig('AAPL')).toBe(false);
  });

  it('returns false for a number', () => {
    expect(isPriceTriggerConfig(42)).toBe(false);
  });

  it('returns false when symbol is missing', () => {
    expect(
      isPriceTriggerConfig({ conditions: [], retrigger: { mode: 'one_shot' } }),
    ).toBe(false);
  });

  it('returns false when symbol is not a string', () => {
    expect(
      isPriceTriggerConfig({ symbol: 123, conditions: [], retrigger: { mode: 'one_shot' } }),
    ).toBe(false);
  });

  it('returns false when conditions is missing', () => {
    expect(
      isPriceTriggerConfig({ symbol: 'AAPL', retrigger: { mode: 'one_shot' } }),
    ).toBe(false);
  });

  it('returns false when conditions is not an array', () => {
    expect(
      isPriceTriggerConfig({ symbol: 'AAPL', conditions: 'bad', retrigger: { mode: 'one_shot' } }),
    ).toBe(false);
  });

  it('returns false for an empty object', () => {
    expect(isPriceTriggerConfig({})).toBe(false);
  });
});

/* --------------------------------------------------------- */
/*  formatPriceTrigger                                        */
/* --------------------------------------------------------- */
describe('formatPriceTrigger', () => {
  it('returns fallback for null', () => {
    expect(formatPriceTrigger(null)).toBe('Price alert');
  });

  it('returns fallback for undefined', () => {
    expect(formatPriceTrigger(undefined)).toBe('Price alert');
  });

  it('returns fallback for a malformed object', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(formatPriceTrigger({ foo: 'bar' } as any)).toBe('Price alert');
  });

  it('returns symbol alert when conditions array is empty', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'TSLA',
      conditions: [],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg)).toBe('TSLA price alert');
  });

  it('formats price_above condition', () => {
    expect(formatPriceTrigger(validConfig)).toBe('AAPL > $200.00');
  });

  it('formats price_below condition', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'GOOG',
      conditions: [{ type: 'price_below', value: 150.5 }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg)).toBe('GOOG < $150.50');
  });

  it('formats pct_change_above with day_open reference', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'MSFT',
      conditions: [{ type: 'pct_change_above', value: 5, reference: 'day_open' }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg)).toContain('MSFT');
    expect(formatPriceTrigger(cfg)).toContain('5.00%');
    expect(formatPriceTrigger(cfg)).toContain('open');
  });

  it('formats pct_change_below with previous_close reference', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AMZN',
      conditions: [{ type: 'pct_change_below', value: 3, reference: 'previous_close' }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg)).toContain('AMZN');
    expect(formatPriceTrigger(cfg)).toContain('3.00%');
    expect(formatPriceTrigger(cfg)).toContain('close');
  });

  it('uses only the first condition', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'NVDA',
      conditions: [
        { type: 'price_above', value: 100 },
        { type: 'price_below', value: 50 },
      ],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg)).toBe('NVDA > $100.00');
  });
});

/* --------------------------------------------------------- */
/*  formatRetriggerMode                                       */
/* --------------------------------------------------------- */
describe('formatRetriggerMode', () => {
  it('returns One-shot for null', () => {
    expect(formatRetriggerMode(null)).toBe('One-shot');
  });

  it('returns One-shot for undefined', () => {
    expect(formatRetriggerMode(undefined)).toBe('One-shot');
  });

  it('returns One-shot for a malformed object', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(formatRetriggerMode({ bad: true } as any)).toBe('One-shot');
  });

  it('returns One-shot for one_shot mode', () => {
    expect(formatRetriggerMode(validConfig)).toBe('One-shot');
  });

  it('returns Recurring with hours when cooldown_seconds is set', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring', cooldown_seconds: 7200 },
    };
    expect(formatRetriggerMode(cfg)).toBe('Recurring (2h)');
  });

  it('returns Recurring when cooldown rounds to zero hours', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring', cooldown_seconds: 60 },
    };
    expect(formatRetriggerMode(cfg)).toBe('Recurring');
  });

  it('returns Recurring (daily) when no cooldown_seconds', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring' },
    };
    expect(formatRetriggerMode(cfg)).toBe('Recurring (daily)');
  });

  it('returns One-shot when retrigger is missing entirely', () => {
    // Simulate an object that passes the guard but has no retrigger
    // (retrigger is required in the type but might be absent at runtime)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const cfg = { symbol: 'AAPL', conditions: [] } as any;
    expect(formatRetriggerMode(cfg)).toBe('One-shot');
  });
});
