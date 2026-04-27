import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, fireEvent, act, screen } from '@testing-library/react';
import { createRef } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

// Mock the API helpers chat-input fetches at mount — they hit network in real
// usage and we don't need them for deck behavior.
vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('@/lib/modelCapabilities', () => ({
  supportsXhighEffort: () => false,
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

const SNAP_A: WidgetContextSnapshot = {
  widget_type: 'markets.chart',
  widget_id: 'a',
  label: 'NVDA · 1d Chart',
  description: '120 daily bars',
  captured_at: '2026-04-26T11:42:08Z',
  text: '<widget-context>chart</widget-context>',
  data: {},
};

const SNAP_B: WidgetContextSnapshot = {
  widget_type: 'news.feed',
  widget_id: 'b',
  label: 'News headline',
  description: 'Reuters · 14m ago',
  captured_at: '2026-04-26T11:43:00Z',
  text: '<widget-context>news</widget-context>',
  data: {},
};

function renderInput(onSend = vi.fn(), refOpts?: { ref?: React.Ref<ChatInputHandle> }) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput ref={refOpts?.ref} onSend={onSend} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('ChatInput — widget context deck', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('does not render the deck when no snapshots are attached', () => {
    renderInput();
    expect(screen.queryByTestId('widget-context-deck')).toBeNull();
  });

  it('renders a card on ContextBus.attach', () => {
    renderInput();
    act(() => {
      ContextBus.attach(SNAP_A);
    });
    const deck = screen.getByTestId('widget-context-deck');
    expect(deck).toBeInTheDocument();
    expect(deck.textContent).toContain('NVDA · 1d Chart');
    expect(deck.textContent).toContain('1 in context');
  });

  it('addWidgetSnapshot ref method adds a card without using the bus', () => {
    const ref = createRef<ChatInputHandle>();
    renderInput(undefined, { ref });
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    act(() => {
      ref.current?.addWidgetSnapshot(SNAP_A);
    });
    expect(screen.getByTestId('widget-context-deck').textContent).toContain('NVDA · 1d Chart');
    // No attach event was published — the ref method is local-only.
    expect(handler).not.toHaveBeenCalled();
  });

  it('removing a card publishes detach so other inputs drop it', () => {
    renderInput();
    act(() => {
      ContextBus.attach(SNAP_A);
      ContextBus.attach(SNAP_B);
    });
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    const removeButtons = screen.getAllByLabelText(/remove from context/i);
    act(() => {
      fireEvent.click(removeButtons[0]);
    });
    // detach published with one of the widget ids
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0].type).toBe('detach');
    expect(['a', 'b']).toContain(handler.mock.calls[0][0].widgetId);
  });

  it('two mounted ChatInputs mirror the same deck via ContextBus', () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <ChatInput onSend={vi.fn()} />
          <ChatInput onSend={vi.fn()} />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    act(() => {
      ContextBus.attach(SNAP_A);
    });
    const decks = screen.getAllByTestId('widget-context-deck');
    expect(decks).toHaveLength(2);
    decks.forEach((d) => expect(d.textContent).toContain('NVDA'));
  });

  it('ContextBus.clear empties every mounted deck', () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <ChatInput onSend={vi.fn()} />
          <ChatInput onSend={vi.fn()} />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    act(() => {
      ContextBus.attach(SNAP_A);
      ContextBus.attach(SNAP_B);
    });
    expect(screen.getAllByTestId('widget-context-deck')).toHaveLength(2);
    act(() => {
      ContextBus.clear();
    });
    expect(screen.queryAllByTestId('widget-context-deck')).toHaveLength(0);
  });

  it('registers chat-input root in ChatInputRegistry on mount', () => {
    expect(ChatInputRegistry.size()).toBe(0);
    const { unmount } = renderInput();
    expect(ChatInputRegistry.size()).toBe(1);
    unmount();
    expect(ChatInputRegistry.size()).toBe(0);
  });

  it('renders 1 / 3 / 5 cards with stable peek geometry', () => {
    const { rerender, unmount } = renderInput();
    const more: WidgetContextSnapshot[] = [SNAP_A, SNAP_B, { ...SNAP_A, widget_id: 'c' }, { ...SNAP_A, widget_id: 'd' }, { ...SNAP_A, widget_id: 'e' }];

    // 1 card
    act(() => ContextBus.attach(more[0]));
    let deck = screen.getByTestId('widget-context-deck');
    let cards = deck.querySelectorAll('.widget-deck-card');
    expect(cards.length).toBe(1);
    expect((cards[0] as HTMLElement).style.transform).toContain('scale(1)');

    // +2 → 3 cards. Snapshot the per-index transforms.
    act(() => {
      ContextBus.attach(more[1]);
      ContextBus.attach(more[2]);
    });
    deck = screen.getByTestId('widget-context-deck');
    cards = deck.querySelectorAll('.widget-deck-card');
    expect(cards.length).toBe(3);

    // +2 more → 5 cards. Last card uses index-4 peek (24px translate).
    act(() => {
      ContextBus.attach(more[3]);
      ContextBus.attach(more[4]);
    });
    deck = screen.getByTestId('widget-context-deck');
    cards = deck.querySelectorAll('.widget-deck-card');
    expect(cards.length).toBe(5);
    // Top card (newest, index 0): no peek translate
    const indexed = Array.from(cards) as HTMLElement[];
    const top = indexed.find((c) => c.dataset.i === '0')!;
    expect(top.style.transform).toContain('translateY(0px)');
    expect(top.style.opacity).toBe('1');
    // Last card (index 4): 24px peek translate, lower opacity
    const last = indexed.find((c) => c.dataset.i === '4')!;
    expect(last.style.transform).toContain('translateY(24px)');
    expect(parseFloat(last.style.opacity)).toBeLessThan(1);

    rerender(<></>);
    unmount();
  });

  it('clear button publishes ContextBus.clear', () => {
    renderInput();
    act(() => {
      ContextBus.attach(SNAP_A);
      ContextBus.attach(SNAP_B);
    });
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    act(() => {
      fireEvent.click(screen.getByText(/^Clear$/i));
    });
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0].type).toBe('clear');
  });
});
