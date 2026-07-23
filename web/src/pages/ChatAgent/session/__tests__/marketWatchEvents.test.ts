import { describe, it, expect, vi } from 'vitest';
import { handleMarketWatchUpdate } from '../marketWatchEvents';
import type { MarketWatchState } from '../marketWatchEvents';

describe('handleMarketWatchUpdate (streaming)', () => {
  it('parses symbols + content + timestamp onto the setter', () => {
    const setMarketWatch = vi.fn<(next: MarketWatchState) => void>();
    const handled = handleMarketWatchUpdate({
      event: {
        symbols: ['NVDA', 'TSLA'],
        content: 'NVDA 900.12 (+1.2%)',
        timestamp: 1719878400,
      },
      setMarketWatch,
    });

    expect(handled).toBe(true);
    expect(setMarketWatch).toHaveBeenCalledTimes(1);
    expect(setMarketWatch).toHaveBeenCalledWith({
      symbols: ['NVDA', 'TSLA'],
      content: 'NVDA 900.12 (+1.2%)',
      timestamp: 1719878400,
    });
  });

  it('coerces a missing/non-array symbols field to an empty list (watch off)', () => {
    const setMarketWatch = vi.fn<(next: MarketWatchState) => void>();
    handleMarketWatchUpdate({
      event: { content: '', timestamp: undefined },
      setMarketWatch,
    });

    expect(setMarketWatch).toHaveBeenCalledWith({
      symbols: [],
      content: '',
      timestamp: undefined,
    });
  });

  it('drops non-string entries from the symbols list', () => {
    const setMarketWatch = vi.fn<(next: MarketWatchState) => void>();
    handleMarketWatchUpdate({
      event: { symbols: ['NVDA', 42, null, 'AAPL'] },
      setMarketWatch,
    });

    const arg = setMarketWatch.mock.calls[0][0];
    expect(arg.symbols).toEqual(['NVDA', 'AAPL']);
  });

  it('leaves content/timestamp undefined when the event omits them', () => {
    const setMarketWatch = vi.fn<(next: MarketWatchState) => void>();
    handleMarketWatchUpdate({
      event: { symbols: ['NVDA'] },
      setMarketWatch,
    });

    const arg = setMarketWatch.mock.calls[0][0];
    expect(arg.symbols).toEqual(['NVDA']);
    expect(arg.content).toBeUndefined();
    expect(arg.timestamp).toBeUndefined();
  });
});
