import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import {
  isPriceTriggerConfig,
  formatPriceTrigger,
  formatRetriggerMode,
} from '../price';
import type { PriceTriggerConfig } from '@/types/automation';

const en = i18n.getFixedT('en-US');

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
    expect(formatPriceTrigger(null, en)).toBe('Price alert');
  });

  it('returns fallback for undefined', () => {
    expect(formatPriceTrigger(undefined, en)).toBe('Price alert');
  });

  it('returns fallback for a malformed object', () => {
    expect(formatPriceTrigger({ foo: 'bar' } as any, en)).toBe('Price alert');
  });

  it('returns symbol alert when conditions array is empty', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'TSLA',
      conditions: [],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg, en)).toBe('TSLA price alert');
  });

  it('formats price_above condition', () => {
    expect(formatPriceTrigger(validConfig, en)).toBe('AAPL > $200.00');
  });

  it('formats price_below condition', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'GOOG',
      conditions: [{ type: 'price_below', value: 150.5 }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg, en)).toBe('GOOG < $150.50');
  });

  it('formats pct_change_above with day_open reference', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'MSFT',
      conditions: [{ type: 'pct_change_above', value: 5, reference: 'day_open' }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg, en)).toContain('MSFT');
    expect(formatPriceTrigger(cfg, en)).toContain('5.00%');
    expect(formatPriceTrigger(cfg, en)).toContain('open');
  });

  it('formats pct_change_below with previous_close reference', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AMZN',
      conditions: [{ type: 'pct_change_below', value: 3, reference: 'previous_close' }],
      retrigger: { mode: 'one_shot' },
    };
    expect(formatPriceTrigger(cfg, en)).toContain('AMZN');
    expect(formatPriceTrigger(cfg, en)).toContain('3.00%');
    expect(formatPriceTrigger(cfg, en)).toContain('close');
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
    expect(formatPriceTrigger(cfg, en)).toBe('NVDA > $100.00');
  });
});

/* --------------------------------------------------------- */
/*  formatRetriggerMode                                       */
/* --------------------------------------------------------- */
describe('formatRetriggerMode', () => {
  it('returns One-shot for null', () => {
    expect(formatRetriggerMode(null, en)).toBe('One-shot');
  });

  it('returns One-shot for undefined', () => {
    expect(formatRetriggerMode(undefined, en)).toBe('One-shot');
  });

  it('returns One-shot for a malformed object', () => {
    expect(formatRetriggerMode({ bad: true } as any, en)).toBe('One-shot');
  });

  it('returns One-shot for one_shot mode', () => {
    expect(formatRetriggerMode(validConfig, en)).toBe('One-shot');
  });

  it('returns Recurring with hours when cooldown_seconds is set', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring', cooldown_seconds: 7200 },
    };
    expect(formatRetriggerMode(cfg, en)).toBe('Recurring (2h)');
  });

  it('returns Recurring when cooldown rounds to zero hours', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring', cooldown_seconds: 60 },
    };
    expect(formatRetriggerMode(cfg, en)).toBe('Recurring');
  });

  it('returns Recurring (daily) when no cooldown_seconds', () => {
    const cfg: PriceTriggerConfig = {
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 200 }],
      retrigger: { mode: 'recurring' },
    };
    expect(formatRetriggerMode(cfg, en)).toBe('Recurring (daily)');
  });

  it('returns One-shot when retrigger is missing entirely', () => {
    // Simulate an object that passes the guard but has no retrigger
    // (retrigger is required in the type but might be absent at runtime)
    const cfg = { symbol: 'AAPL', conditions: [] } as any;
    expect(formatRetriggerMode(cfg, en)).toBe('One-shot');
  });
});

describe('price text in Chinese', () => {
  const zh = i18n.getFixedT('zh-CN');

  it('reads in the reader\'s language', () => {
    const move: PriceTriggerConfig = {
      symbol: 'SPX',
      conditions: [{ type: 'pct_change_above', value: 1.5, reference: 'previous_close' }],
      retrigger: { mode: 'recurring', cooldown_seconds: 14400 },
    };
    expect(formatPriceTrigger(move, zh)).toBe('SPX 较昨收 ↑1.50%');
    expect(formatRetriggerMode(move, zh)).toBe('循环（每 4 小时）');
    expect(formatRetriggerMode(validConfig, zh)).toBe('单次触发');
  });
});
