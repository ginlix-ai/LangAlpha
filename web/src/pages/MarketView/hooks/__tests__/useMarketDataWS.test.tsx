import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { QuoteRow } from '@/lib/quotes/quoteBatcher';

class FakeWebSocket {
  static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
  static last: FakeWebSocket | null = null;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onclose: ((e: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(public url: string) {
    FakeWebSocket.last = this;
    setTimeout(() => { this.readyState = 1; this.onopen?.(); }, 0);
  }
  send() {}
  close() { this.readyState = 3; this.onclose?.({ code: 1006, reason: '' }); }
  push(obj: unknown) { this.onmessage?.({ data: JSON.stringify(obj) } as MessageEvent); }
}

vi.mock('@/lib/authToken', async (orig) => ({ ...(await orig<object>()), getAccessToken: async () => null }));

import useMarketDataWS from '../useMarketDataWS';

// 2026-11-03 is Eastern Standard Time, so ET is UTC-5.
const et = (h: number, m: number): number => Date.UTC(2026, 10, 3, h + 5, m);
const tick = (close: number, high: number, low: number, time: number) => ({
  type: 'aggregate',
  data: { symbol: 'AAPL', time, open: close, high, low, close, volume: 100 },
});

const ROW = {
  symbol: 'AAPL', price: 100, previous_close: 100, regular_close: 100, open: 99, high: 105, low: 98,
  change: 0, change_percent: 0, as_of: et(11, 30),
} as unknown as QuoteRow;

let qc: QueryClient;
const cachedRow = () => qc.getQueryData<QuoteRow>(queryKeys.quote.detail('AAPL')) as unknown as Record<string, number>;

async function mountHook() {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
  const hook = renderHook(() => useMarketDataWS(), { wrapper });
  await act(async () => { await vi.advanceTimersByTimeAsync(10); });
  expect(FakeWebSocket.last?.readyState).toBe(1);
  return hook;
}
const push = (frame: unknown) => act(() => { FakeWebSocket.last!.push(frame); });

describe('useMarketDataWS day figures', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(et(12, 0));
    vi.stubGlobal('WebSocket', FakeWebSocket);
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, json: async () => ({ enabled: true }) })));
    qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(queryKeys.quote.detail('AAPL'), ROW);
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('measures the change against the quote row previous close and writes it through', async () => {
    const { result } = await mountHook();
    push(tick(110, 110, 110, et(12, 0)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ change: 10, changePercent: 10 });
    push(tick(112, 112, 112, et(12, 1)));
    expect(result.current.prices.get('AAPL')?.changePercent).toBeCloseTo(12);
    expect(cachedRow()).toMatchObject({ change_percent: expect.closeTo(12), last_minute_close: 112, previous_close: 100 });
  });

  it('starts high and low from the row and lets only regular-hours prints move them', async () => {
    const { result } = await mountHook();
    push(tick(104, 104, 103, et(12, 0)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ open: 99, high: 105, low: 98 });
    push(tick(120, 121, 119, et(16, 45)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ price: 120, high: 105, low: 98 });
    push(tick(107, 107, 106, et(15, 59)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ high: 107, low: 98 });
  });

  it('takes the day open, high and low from a row that lands after the first print', async () => {
    qc.removeQueries({ queryKey: queryKeys.quote.detail('AAPL') });
    const { result } = await mountHook();
    push(tick(104, 104, 103, et(12, 0)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ open: 104, high: 104, low: 103 });
    act(() => { qc.setQueryData(queryKeys.quote.detail('AAPL'), ROW); });
    push(tick(104, 104, 103, et(12, 1)));
    expect(result.current.prices.get('AAPL')).toMatchObject({ open: 99, high: 105, low: 98 });
  });

  it('drops a print older than five minutes while keepalives keep the socket up, and clears on close', async () => {
    const { result } = await mountHook();
    push(tick(104, 104, 103, et(12, 0)));
    expect(result.current.prices.size).toBe(1);
    for (let i = 0; i < 18; i++) {
      await act(async () => { await vi.advanceTimersByTimeAsync(20000); });
      push({ type: 'keepalive' });
    }
    expect(FakeWebSocket.last!.readyState).toBe(1);
    expect(result.current.prices.size).toBe(0);

    push(tick(105, 105, 104, et(12, 10)));
    expect(result.current.prices.size).toBe(1);
    act(() => { FakeWebSocket.last!.close(); });
    expect(result.current.prices.size).toBe(0);
  });
});

describe('useMarketDataWS class shares', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(et(12, 0));
    vi.stubGlobal('WebSocket', FakeWebSocket);
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, json: async () => ({ enabled: true }) })));
    qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    vi.spyOn(console, 'info').mockImplementation(() => {});
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('files a print the feed spells BRK.B under each spelling that subscribed', async () => {
    qc.setQueryData(queryKeys.quote.detail('BRK-B'), { ...ROW, symbol: 'BRK-B', previous_close: 490 });
    const { result } = await mountHook();
    act(() => { result.current.subscribe(['BRK-B']); });
    push({ ev: 'AM', sym: 'BRK.B', o: 500, h: 501, l: 499, c: 500, v: 10, s: et(12, 0) });
    expect(result.current.prices.get('BRK-B')).toMatchObject({ symbol: 'BRK-B', price: 500 });
    expect(result.current.prices.has('BRK.B')).toBe(false);

    act(() => { result.current.subscribe(['BRK.B']); });
    push({ ev: 'AM', sym: 'BRK.B', o: 502, h: 502, l: 502, c: 502, v: 10, s: et(12, 1) });
    for (const spelling of ['BRK-B', 'BRK.B']) {
      expect(result.current.prices.get(spelling)).toMatchObject({ price: 502, change: 12, volume: 20 });
    }
  });

  it("takes the day's figures from the spelling whose quote row is from the print's day", async () => {
    qc.setQueryData(queryKeys.quote.detail('BRK-B'), { ...ROW, symbol: 'BRK-B', open: 470, as_of: et(11, 30) - 86400000 });
    qc.setQueryData(queryKeys.quote.detail('BRK.B'), { ...ROW, symbol: 'BRK.B', open: 495, high: 510, low: 490 });
    const { result } = await mountHook();
    act(() => { result.current.subscribe(['BRK-B', 'BRK.B']); });
    push({ ev: 'AM', sym: 'BRK.B', o: 500, h: 501, l: 499, c: 500, v: 10, s: et(12, 0) });
    expect(result.current.prices.get('BRK-B')).toMatchObject({ open: 495, high: 510, low: 490 });
  });
});
