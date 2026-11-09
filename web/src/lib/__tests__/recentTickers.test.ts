import { afterEach, describe, expect, it } from 'vitest';
import { userLocalStorage } from '@/lib/userStorage';
import { readRecentTickers, recordRecentTicker } from '../recentTickers';

const KEY = 'palette:recent-tickers';

describe('recentTickers', () => {
  afterEach(() => userLocalStorage.removeItem(KEY));

  it('keeps the newest first, once each', () => {
    recordRecentTicker('NVDA', 1);
    recordRecentTicker('AAPL', 2);
    recordRecentTicker('NVDA', 3);
    expect(readRecentTickers()).toEqual([{ symbol: 'NVDA', at: 3 }, { symbol: 'AAPL', at: 2 }]);
  });

  it('drops a row written wrong, not the list', () => {
    userLocalStorage.setItem(
      KEY,
      JSON.stringify([{ symbol: ' TSLA ', at: 5 }, { symbol: '   ', at: 4 }, { symbol: 'X', at: null }, 'AMD', { symbol: 'MSFT', at: 1 }]),
    );
    expect(readRecentTickers()).toEqual([{ symbol: 'TSLA', at: 5 }, { symbol: 'MSFT', at: 1 }]);
  });

  it('reads a slot that is not a list as empty', () => {
    userLocalStorage.setItem(KEY, '{"symbol":"NVDA"}');
    expect(readRecentTickers()).toEqual([]);
    userLocalStorage.setItem(KEY, 'not json');
    expect(readRecentTickers()).toEqual([]);
  });
});
