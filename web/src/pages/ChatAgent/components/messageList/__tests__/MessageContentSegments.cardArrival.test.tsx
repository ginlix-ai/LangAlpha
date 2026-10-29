/**
 * A lookup card plays its arrival only when its call lands in front of the
 * reader. One the message is drawn with (a load, a replay, a turn opened
 * again) is simply there, however recently its call settled.
 */
import React, { useContext } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import { MessageContentSegments } from '../MessageContentSegments';
import { CardArrivals } from '../CardArrivals';
import { CardCallsContext } from '../cardCalls';
import { TranscriptDisplayContext } from '@/lib/transcriptDisplay';
import type { ContentSegmentRecord, ToolCallProcessRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock('../../ActivityBlock', () => ({ default: () => <div data-testid="activity-block" /> }));
vi.mock('../../charts/InlineArtifactCards', async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  INLINE_ARTIFACT_MAP: {
    quote: ({ artifact }: { artifact: { quotes: { symbol: string }[] } }) => (
      <section data-testid={artifact.quotes[0].symbol === 'AMD' ? 'quote' : `quote-${artifact.quotes[0].symbol}`} />
    ),
    stock_prices: () => <section data-testid="prices" />,
  },
}));

const segments = [{ type: 'tool_call', toolCallId: 'q1', order: 0 }] as ContentSegmentRecord[];
const call = { toolName: 'get_quote', toolCall: { id: 'q1', name: 'get_quote', args: { symbol: 'AMD' } }, order: 0 };
const running = (): ToolCallProcessRecord => ({ ...call, toolCallResult: null, isInProgress: true, _createdAt: Date.now() });
const landed = (): ToolCallProcessRecord => ({
  ...call,
  toolCallResult: { content: 'Quote', artifact: { type: 'quote', quotes: [{ symbol: 'AMD', price: 1 }] } },
  isInProgress: false,
  isComplete: true,
  _createdAt: Date.now(),
  _settledAt: Date.now(),
});

const lookup = (id: string, symbol: string, order: number, settled: boolean): ToolCallProcessRecord => ({
  toolName: 'get_quote',
  toolCall: { id, name: 'get_quote', args: { symbol } },
  order,
  _createdAt: Date.now(),
  ...(settled
    ? {
      toolCallResult: { content: 'Quote', artifact: { type: 'quote', quotes: [{ symbol, price: 1 }] } },
      isInProgress: false,
      isComplete: true,
      _settledAt: Date.now(),
    }
    : { toolCallResult: null, isInProgress: true }),
});

const view = (proc: ToolCallProcessRecord, procs: Record<string, ToolCallProcessRecord> = { q1: proc }, segs = segments) => (
  <TranscriptDisplayContext.Provider value={{ turnDisplay: 'lean', streamingMode: 'token' }}>
    <MessageContentSegments
      segments={segs}
      reasoningProcesses={{}}
      toolCallProcesses={procs}
      todoListProcesses={{}}
      subagentTasks={{}}
      isStreaming
      fold="live"
      isTurnTail
    />
  </TranscriptDisplayContext.Provider>
);

describe('a lookup card arriving', () => {
  let animate: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    // jsdom reports an unseen page, where nothing plays.
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false);
    animate = vi.fn(() => ({ finished: new Promise(() => {}), cancel: () => {} }));
    (Element.prototype as unknown as { animate: unknown }).animate = animate;
  });
  afterEach(() => {
    delete (Element.prototype as unknown as { animate?: unknown }).animate;
    vi.restoreAllMocks();
  });

  it('plays only when the call lands, not when the message is drawn with the card', () => {
    const drawn = render(view(landed()));
    expect(drawn.getByTestId('quote')).toBeTruthy();
    expect(drawn.container.querySelector('[data-arriving]')).toBeNull();
    expect(animate).not.toHaveBeenCalled();
    drawn.unmount();

    // The same card landing in a mounted message does play, so the silence
    // above is the first render's and not a test that cannot see an arrival.
    const live = render(view(running()));
    expect(live.queryByTestId('quote')).toBeNull();
    live.rerender(view(landed()));
    const card = live.getByTestId('quote');
    expect(card.parentElement?.hasAttribute('data-arriving')).toBe(true);
    expect(animate.mock.contexts).toContain(card);
  });

  // Parallel lookups land in the order they finish. The earlier call's card
  // goes in front of the later one's, which must stay the card the reader is
  // watching arrive, not be drawn again.
  it('keeps a card already drawn when a call before it lands', () => {
    const both = [
      { type: 'tool_call', toolCallId: 'q1', order: 0 },
      { type: 'tool_call', toolCallId: 'q2', order: 1 },
    ] as ContentSegmentRecord[];
    const first = lookup('q1', 'NVDA', 0, false);
    const live = render(view(first, { q1: first, q2: lookup('q2', 'TSLA', 1, true) }, both));
    const later = live.getByTestId('quote-TSLA');

    const landedFirst = lookup('q1', 'NVDA', 0, true);
    live.rerender(view(landedFirst, { q1: landedFirst, q2: lookup('q2', 'TSLA', 1, true) }, both));
    expect(live.getByTestId('quote-TSLA')).toBe(later);
    // One box: the later card joins the one now above it.
    expect(live.getByTestId('quote-NVDA').parentElement?.hasAttribute('data-card-join')).toBe(false);
    expect(later.parentElement?.hasAttribute('data-card-join')).toBe(true);
  });

  // A series of one lookup reads as one box; a different lookup under it
  // would read as part of the first.
  it('joins only lookups of one type', () => {
    const both = [
      { type: 'tool_call', toolCallId: 'q1', order: 0 },
      { type: 'tool_call', toolCallId: 'p1', order: 1 },
    ] as ContentSegmentRecord[];
    const quote = lookup('q1', 'NVDA', 0, true);
    const prices: ToolCallProcessRecord = {
      ...lookup('p1', 'NVDA', 1, true),
      toolName: 'get_stock_daily_prices',
      toolCallResult: { content: 'Prices', artifact: { type: 'stock_prices', symbol: 'NVDA', ohlcv: [{ close: 1 }] } },
    };
    const drawn = render(view(quote, { q1: quote, p1: prices }, both));
    expect(drawn.getByTestId('prices').parentElement?.hasAttribute('data-card-join')).toBe(false);
  });
});

describe('a row handing over to its card', () => {
  const Calls = () => <output>{[...useContext(CardCallsContext)].join(' ')}</output>;
  const arrivals = (settledAt: number | undefined, live = true) => (
    <CardArrivals cards={[{ key: 'compact-q1', call: 'q1', settledAt }]} live={live} motionSafe>
      <Calls />
    </CardArrivals>
  );
  beforeEach(() => { vi.spyOn(document, 'hidden', 'get').mockReturnValue(false); });
  afterEach(() => { vi.restoreAllMocks(); });

  // A row told its card takes its place gives its space up at once, so one
  // whose card will not play an arrival in it must leave as rows leave.
  it('hands over only to a card that arrives', () => {
    expect(render(arrivals(Date.now())).container.textContent).toBe('q1');
    // A subagent tab's calls, and a reconnect's replay, carry no landing.
    expect(render(arrivals(undefined)).container.textContent).toBe('');
    expect(render(arrivals(Date.now() - 60_000)).container.textContent).toBe('');
    expect(render(arrivals(Date.now(), false)).container.textContent).toBe('');
  });
});
