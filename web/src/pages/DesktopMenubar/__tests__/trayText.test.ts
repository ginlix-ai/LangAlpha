// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { formatTrayLine, trayPercent, trayRefreshMs, type TrayFigure } from '../trayText';

const nvda: TrayFigure = { label: 'NVDA', price: 163.99, pct: -5.6, ext: null };
const opts = (show: 'change' | 'price' | 'both', extendedHours = true) => ({ show, extendedHours });

describe('formatTrayLine', () => {
  it('prints the move, the price, or both', () => {
    expect(formatTrayLine(nvda, opts('change'), 'en-US')).toBe('NVDA ▼5.60%');
    expect(formatTrayLine(nvda, opts('price'), 'en-US')).toBe('NVDA 163.99');
    expect(formatTrayLine(nvda, opts('both'), 'en-US')).toBe('NVDA 163.99 ▼5.6%');
  });

  it('marks a rise and leaves a move that rounds to nothing unmarked', () => {
    expect(formatTrayLine({ ...nvda, pct: 2.081 }, opts('change'), 'en-US')).toBe('NVDA ▲2.08%');
    expect(formatTrayLine({ ...nvda, pct: 0.001 }, opts('change'), 'en-US')).toBe('NVDA 0.00%');
  });

  it('shows the extended move when asked for and there is one', () => {
    const ah: TrayFigure = { ...nvda, ext: { label: 'AH', price: 167.4, pct: 2.08 } };
    expect(formatTrayLine(ah, opts('change'), 'en-US')).toBe('NVDA AH ▲2.08%');
    expect(formatTrayLine(ah, opts('both'), 'en-US')).toBe('NVDA AH 167.40 ▲2.1%');
    expect(formatTrayLine(ah, opts('change', false), 'en-US')).toBe('NVDA ▼5.60%');
  });

  it('falls back to the figure that is known, and is null when none is', () => {
    // Before the open the day has no move yet, only the last close.
    expect(formatTrayLine({ ...nvda, pct: null }, opts('change', false), 'en-US')).toBe('NVDA 163.99');
    expect(formatTrayLine({ ...nvda, price: null }, opts('price'), 'en-US')).toBe('NVDA ▼5.60%');
    expect(formatTrayLine({ ...nvda, price: null, pct: null }, opts('both'), 'en-US')).toBeNull();
  });

  it('prints a portfolio as its move alone, whatever is asked', () => {
    const port: TrayFigure = { label: 'Port', price: null, pct: 0.78, ext: null, percentOnly: true };
    expect(formatTrayLine(port, opts('price'), 'en-US')).toBe('Port ▲0.78%');
    expect(formatTrayLine({ ...port, ext: { label: 'PM', price: null, pct: -0.3 } }, opts('both'), 'en-US')).toBe('Port PM ▼0.30%');
    expect(formatTrayLine({ ...port, pct: null }, opts('change'), 'en-US')).toBeNull();
  });

  it('groups a large price and keeps a fine one', () => {
    expect(formatTrayLine({ label: 'DJI', price: 47012.5, pct: null, ext: null }, opts('price'), 'en-US')).toBe('DJI 47,012.50');
    expect(formatTrayLine({ label: 'X', price: 0.0483, pct: null, ext: null, places: 4 }, opts('price'), 'en-US')).toBe('X 0.0483');
  });
});

describe('trayPercent', () => {
  it('rounds to the digits asked', () => {
    expect(trayPercent(-5.64, 1)).toBe('▼5.6%');
    expect(trayPercent(-0.04, 1)).toBe('0.0%');
  });
});

describe('trayRefreshMs', () => {
  it('asks often while a session trades and rarely when none does', () => {
    expect(trayRefreshMs('open')).toBe(15_000);
    expect(trayRefreshMs('pre')).toBe(15_000);
    expect(trayRefreshMs('after')).toBe(15_000);
    expect(trayRefreshMs('closed')).toBe(300_000);
    expect(trayRefreshMs('unknown')).toBe(60_000);
  });
});
