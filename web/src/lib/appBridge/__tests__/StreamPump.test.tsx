import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';

const locale = vi.hoisted(() => ({ current: 'zh-CN' }));
const row = vi.hoisted(() => ({ symbol: 'AAPL', price: 227.15 }));

vi.mock('@/hooks/useLocale', () => ({ useLocale: () => locale.current }));
vi.mock('@/lib/quotes', () => ({
  useQuotes: (keys: string[]) => ({ quotes: Object.fromEntries(keys.map((k) => [k, row])) }),
}));
vi.mock('@/pages/MarketView/hooks/useMarketDataWS', () => ({
  default: () => ({ subscribe: vi.fn(), unsubscribe: vi.fn() }),
}));
vi.mock('../methods', () => ({
  toQuote: (key: string) => ({ symbol: key, label: locale.current }),
}));

import { StreamPump } from '../StreamPump';
import type { QuoteArgs } from '../methods';

const subs = new Map<string, QuoteArgs>([['s1', { keys: ['AAPL'], isIndex: false } as QuoteArgs]]);

describe('StreamPump', () => {
  it('sends each row again in the new language, since a closed market may not tick', () => {
    const onQuote = vi.fn();
    const view = render(<StreamPump subs={subs} onQuote={onQuote} />);
    view.rerender(<StreamPump subs={subs} onQuote={onQuote} />);
    expect(onQuote.mock.calls.map((call) => call[1].label)).toEqual(['zh-CN']);

    locale.current = 'en-US';
    view.rerender(<StreamPump subs={subs} onQuote={onQuote} />);
    expect(onQuote.mock.calls.map((call) => call[1].label)).toEqual(['zh-CN', 'en-US']);
  });
});
