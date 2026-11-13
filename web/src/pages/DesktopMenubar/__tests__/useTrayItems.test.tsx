/**
 * The lines beside the icon follow the newest ask: one the shell made while a
 * slow read was out ends first, and the older read landing late leaves them be.
 */
import { describe, expect, it, vi } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { TrayItemText } from '@/lib/desktop';

const h = vi.hoisted(() => ({
  refresh: null as (() => void) | null,
  sent: [] as TrayItemText[][],
  reads: [] as Array<(price: number | Error) => void>,
}));

vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/desktop')>()),
  canSetTrayItems: () => true,
  setTrayItems: (items: TrayItemText[]) => h.sent.push(items),
  onTrayRefresh: (cb: () => void) => {
    h.refresh = cb;
    return () => { h.refresh = null; };
  },
}));

vi.mock('@/lib/desktopSettings', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/desktopSettings')>();
  const view = { menubar: { ...actual.DEFAULT_MENUBAR, items: [{ symbol: 'NVDA', kind: 'stock' }], show: 'price' } };
  return { ...actual, useDesktopSettings: () => ({ view, failed: false, save: () => {} }) };
});

vi.mock('@/lib/marketUtils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/marketUtils')>()),
  marketStatusQuery: () => ({ queryKey: ['test-market-status'], queryFn: async () => null }),
}));

// A key per read, so each ask waits on its own answer rather than sharing one.
vi.mock('@/lib/quotes', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/quotes')>()),
  quoteQueryOptions: (_qc: unknown, key: string) => {
    const n = h.reads.length;
    let answer: (price: number | Error) => void = () => {};
    const row = new Promise<number>((resolve, reject) => {
      answer = (v) => (v instanceof Error ? reject(v) : resolve(v));
    });
    h.reads.push(answer);
    return { queryKey: ['test-quote', key, n], queryFn: () => row };
  },
}));

vi.mock('../quoteSplit', () => ({
  splitQuote: (_key: string, price: number | undefined) => ({ quoteAvailable: price !== undefined, price, pct: 0, ext: null, places: 2 }),
}));

vi.mock('../trayText', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../trayText')>()),
  formatTrayLine: (figure: { label: string; price: number }) => `${figure.label} ${figure.price}`,
}));

import { useTrayItems } from '../useTrayItems';

describe('useTrayItems', () => {
  it('keeps the newest lines when an older read lands after them', async () => {
    renderHookWithProviders(() => useTrayItems());
    await waitFor(() => expect(h.reads).toHaveLength(1));
    act(() => h.refresh?.());
    await waitFor(() => expect(h.reads).toHaveLength(2));

    await act(async () => h.reads[1](200));
    await waitFor(() => expect(h.sent.at(-1)).toEqual([{ text: 'NVDA 200' }]));
    await act(async () => h.reads[0](100));
    await act(async () => {});
    expect(h.sent.at(-1)).toEqual([{ text: 'NVDA 200' }]);
  });

  it('keeps the last line when a read fails with no row to fall back on', async () => {
    h.reads.length = 0;
    h.sent.length = 0;
    renderHookWithProviders(() => useTrayItems());
    await waitFor(() => expect(h.reads).toHaveLength(1));
    await act(async () => h.reads[0](100));
    await waitFor(() => expect(h.sent.at(-1)).toEqual([{ text: 'NVDA 100' }]));

    act(() => h.refresh?.());
    await waitFor(() => expect(h.reads).toHaveLength(2));
    await act(async () => h.reads[1](new Error('Network Error')));
    await act(async () => {});
    expect(h.sent.at(-1)).toEqual([{ text: 'NVDA 100' }]);
  });
});
