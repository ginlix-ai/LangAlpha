import { describe, expect, it } from 'vitest';
import { showsDefaultWatchlist } from '../useGlanceData';

const refused = (status: number) => ({ response: { status } });
const items = { items: [{ symbol: 'NVDA' }] };

describe('showsDefaultWatchlist', () => {
  it('shows the default with no list chosen', () => {
    expect(showsDefaultWatchlist(null, { isError: false, data: undefined, error: null })).toBe(true);
  });

  it('shows the chosen list while it loads and once it has', () => {
    expect(showsDefaultWatchlist('w2', { isError: false, data: undefined, error: null })).toBe(false);
    expect(showsDefaultWatchlist('w2', { isError: false, data: items, error: null })).toBe(false);
  });

  it('keeps a loaded list through a refresh that failed in passing', () => {
    expect(showsDefaultWatchlist('w2', { isError: true, data: items, error: new Error('Network Error') })).toBe(false);
    expect(showsDefaultWatchlist('w2', { isError: true, data: items, error: refused(503) })).toBe(false);
    expect(showsDefaultWatchlist('w2', { isError: true, data: items, error: refused(401) })).toBe(false);
  });

  it('falls back once the list is gone, or when it never loaded', () => {
    expect(showsDefaultWatchlist('w2', { isError: true, data: items, error: refused(404) })).toBe(true);
    expect(showsDefaultWatchlist('w2', { isError: true, data: undefined, error: new Error('Network Error') })).toBe(true);
  });
});
