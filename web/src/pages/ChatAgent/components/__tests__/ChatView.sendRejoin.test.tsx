/**
 * A reader who scrolled up and then sends is taken to their message: it is
 * pinned near the viewport top and the reply fills the room under it, which a
 * floor under the transcript holds open, so nothing scrolls while the reply
 * fits. Once it outgrows the view the follow glides after its end. The
 * turn-end landing ('reply_start') then applies to that turn too, since it
 * moves only a reader the follow was carrying.
 *
 * The one end-to-end pass over the shipped wiring, on the REAL ChatView;
 * useTranscriptFollow.test.tsx covers each turn starter and the landing rules.
 * The pin and the follow move over animation frames, which the fake timers
 * run, so each check lets them finish first.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import '@testing-library/jest-dom';
import { act, screen } from '@testing-library/react';

vi.mock('@/lib/framer', async () => (await import('./chatViewHarness')).framerMock());
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: 'en-US', resolvedLanguage: 'en-US' } }),
}));
vi.mock('@/lib/supabase', () => ({ supabase: null }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('../Markdown', async () => (await import('./chatViewHarness')).markdownMock());
vi.mock('../../../../components/ui/chat-input', async () => (await import('./chatViewHarness')).chatInputMock());
vi.mock('../../hooks/useChatMessages', async (importOriginal) =>
  (await import('./chatViewHarness')).chatMessagesMock(await importOriginal()));
vi.mock('../../hooks/useWorkspaceFiles', async () => (await import('./chatViewHarness')).workspaceFilesMock());
vi.mock('../../hooks/useNavigationData', async (importOriginal) =>
  (await import('./chatViewHarness')).navigationDataMock(await importOriginal()));
vi.mock('../../utils/api', async (importOriginal) => (await import('./chatViewHarness')).apiMock(await importOriginal()));

const prefs = vi.hoisted(() => ({ value: null as Record<string, unknown> | null }));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: prefs.value, isLoading: false }),
}));
// The fake clock passes the view's 1.5s file panel prefetch, whose import would
// still be loading when the file tears down.
vi.mock('../FilePanel', () => ({ default: () => null }));

import { assistant, chatInput, chatState, mountChatView, resetChatState, userMsg } from './chatViewHarness';
import ChatView from '../ChatView';
import { scrollMemory } from '@/lib/scrollMemory';

const reply = (id: string, content: string, isStreaming = false) =>
  assistant(id, { content, contentSegments: [{ type: 'text', content, order: 0 }], isStreaming });

// ---------------------------------------------------------------------------
// Layout: jsdom has none, so the transcript viewport and its bubbles are
// measured from these.
// ---------------------------------------------------------------------------

const VIEW_H = 500;
let contentH = 2000;
/** Each bubble's top within the transcript. */
let layout: Record<string, number> = {};
let observers: { cb: ResizeObserverCallback; targets: Element[] }[] = [];

/** The main transcript's scrolling element: the ScrollArea's inner div. */
const content = () => document.querySelector<HTMLElement>('.max-w-3xl.overflow-x-hidden')!;
const viewport = () => content().closest<HTMLElement>('.overflow-auto')!;
const floor = () => viewport().querySelector<HTMLElement>('[data-transcript-floor]')!;
/** The scroll height a browser would give it: the transcript, or the floor
 *  held open under it if that is taller. */
const scrollHeight = () => Math.max(contentH, parseFloat(floor().style.minHeight) || 0);

/** Clamped the way a browser clamps, so a request past the end lands on it. */
function setScrollTop(top: number) {
  const v = viewport();
  const clamped = Math.max(0, Math.min(top, scrollHeight() - VIEW_H));
  Object.defineProperty(v, 'scrollTop', { value: clamped, writable: true, configurable: true });
  v.dispatchEvent(new Event('scroll'));
}

/** The transcript grew to `height`, as the content's ResizeObserver reports it.
 *  It watches the padded wrapper, whose bottom padding tracks the composer. A
 *  shrink clamps the view first, as layout does before any observer runs. */
function grow(height: number) {
  contentH = height;
  if (viewport().scrollTop > scrollHeight() - VIEW_H) setScrollTop(viewport().scrollTop);
  const el = content().parentElement!;
  const observer = observers.find((o) => o.targets.includes(el))!;
  act(() => observer.cb([{ contentRect: { height } } as ResizeObserverEntry], {} as ResizeObserver));
}

/** A reader's own scroll: the wheel takes control, then the view moves. */
function userScrollTo(top: number) {
  act(() => {
    viewport().dispatchEvent(new Event('wheel'));
    setScrollTop(top);
  });
}

/** Long enough for a landing or a glide to finish. */
async function frames(ms = 1000) {
  await act(async () => { vi.advanceTimersByTime(ms); });
}

const pill = () => screen.getByLabelText('chat.jumpToLatest.aria');

describe('ChatView send while scrolled up', () => {
  const originalScrollTo = HTMLElement.prototype.scrollTo;
  const originalRect = HTMLElement.prototype.getBoundingClientRect;
  const OriginalRO = window.ResizeObserver;

  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    resetChatState();
    prefs.value = null;
    contentH = 2000;
    layout = {};
    observers = [];
    scrollMemory.clear();
    window.ResizeObserver = class {
      targets: Element[] = [];
      constructor(cb: ResizeObserverCallback) {
        observers.push({ cb, targets: this.targets });
      }
      observe(el: Element) {
        this.targets.push(el);
      }
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) setScrollTop(top);
    } as HTMLElement['scrollTo'];
    HTMLElement.prototype.getBoundingClientRect = function (this: HTMLElement) {
      // The padded wrapper is the transcript's own height, without the floor.
      const height = this === content().parentElement ? contentH : 0;
      const id = this.closest<HTMLElement>('[data-message-id]')?.dataset.messageId;
      const top = id && id in layout ? layout[id] - viewport().scrollTop : 0;
      return { top, bottom: top + height, height, left: 0, right: 0, width: 0, x: 0, y: top, toJSON: () => {} } as DOMRect;
    };
  });

  afterEach(() => {
    HTMLElement.prototype.scrollTo = originalScrollTo;
    HTMLElement.prototype.getBoundingClientRect = originalRect;
    window.ResizeObserver = OriginalRO;
    vi.useRealTimers();
  });

  /** A settled two-turn thread, opened at its end, its first frames run. */
  async function openThread() {
    chatState.messages = [userMsg('u1', 'how did the quarter go?'), reply('a1', 'Revenue rose.')];
    layout = { u1: 0, a1: 100 };
    const view = mountChatView(ChatView);
    Object.defineProperty(viewport(), 'clientHeight', { value: VIEW_H, configurable: true });
    Object.defineProperty(viewport(), 'scrollHeight', { get: scrollHeight, configurable: true });
    grow(2000);
    await frames(50);
    return view;
  }

  function send() {
    const onSend = chatInput.props!.onSend as (m: string, att: unknown[], cmds: unknown[], opts: unknown) => void;
    act(() => onSend('and the next quarter?', [], [], {}));
    expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1);
  }

  it("pins the reader's message on a send, follows the reply once it outgrows the view, and lands on its start under 'reply_start'", async () => {
    prefs.value = { other_preference: { turn_end_scroll: 'reply_start' } };
    const { update } = await openThread();
    // Opened at its end, with nothing held open under it.
    expect(viewport().scrollTop).toBe(1500);
    expect(floor().style.minHeight).toBe('');
    userScrollTo(800);
    expect(pill()).toHaveAttribute('data-state', 'open');

    send();
    expect(pill()).toHaveAttribute('data-state', 'closed');

    // The send lands its bubble, and the view glides to put it 16px under the top.
    layout = { ...layout, u2: 2000, a2: 2100 };
    update({
      isLoading: true,
      messages: [...chatState.messages, userMsg('u2', 'and the next quarter?'), reply('a2', 'Guidance', true)],
    });
    grow(2150);
    await frames(17);
    expect(viewport().scrollTop).toBeGreaterThan(800);
    expect(viewport().scrollTop).toBeLessThan(2000 - 16);
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);

    // The reply fills the room under it, and nothing scrolls.
    grow(2300);
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);

    // Past the view, the floor sits under the reply's end and moves nothing,
    // and the follow glides after the end.
    grow(3000);
    expect(floor().style.minHeight).toBe(`${2000 - 16 + VIEW_H}px`);
    expect(viewport().scrollTop).toBe(2000 - 16);
    await frames();
    expect(viewport().scrollTop).toBe(2500);

    // A row the reply loses on the way takes the view no higher than the pin,
    // and the follow carries on once the reply grows again.
    grow(2300);
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);
    grow(3000);
    await frames();
    expect(viewport().scrollTop).toBe(2500);

    update({
      isLoading: false,
      messages: [...chatState.messages.slice(0, 3), reply('a2', 'Guidance was raised.')],
    });
    await frames();

    // The reply's first line under the viewport top, the anchor's 16px gap above it.
    expect(viewport().scrollTop).toBe(2100 - 16);
  });

  it('carries the landing on through a move of the view the reader did not make', async () => {
    const { update } = await openThread();
    userScrollTo(800);
    send();
    layout = { ...layout, u2: 2000, a2: 2100 };
    update({
      isLoading: true,
      messages: [...chatState.messages, userMsg('u2', 'and the next quarter?'), reply('a2', 'Guidance', true)],
    });
    grow(2150);
    await frames(17);
    const midway = viewport().scrollTop;
    expect(midway).toBeLessThan(2000 - 16);

    // Scroll anchoring, as a row above the view shrinks: the view moves with
    // no wheel, touch, press or key of the reader's behind it.
    act(() => setScrollTop(midway - 2));
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);
    expect(floor().style.minHeight).not.toBe('');
  });

  it('keeps a short reply where it streamed, and holds the room under it when the end of the turn takes rows away', async () => {
    const { update } = await openThread();
    userScrollTo(800);
    send();
    layout = { ...layout, u2: 2000, a2: 2100 };
    update({
      isLoading: true,
      messages: [...chatState.messages, userMsg('u2', 'and the next quarter?'), reply('a2', 'Guidance', true)],
    });
    grow(2150);
    await frames();
    grow(2260);
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);

    // The turn ends and the streaming indicator leaves: the transcript is
    // shorter, and the view stays where the reader was watching.
    update({
      isLoading: false,
      messages: [...chatState.messages.slice(0, 3), reply('a2', 'Guidance was raised.')],
    });
    grow(2220);
    await frames();
    expect(viewport().scrollTop).toBe(2000 - 16);
    expect(scrollHeight()).toBe(2000 - 16 + VIEW_H);

    // Reading back up gives the room back as it leaves the view, so nothing
    // under the reader moves.
    userScrollTo(1600);
    expect(viewport().scrollTop).toBe(1600);
    expect(floor().style.minHeight).toBe('');
  });

  it('lets a reader drag the scrollbar away from a reply their send is following', async () => {
    const { update } = await openThread();
    send();
    layout = { ...layout, u2: 2000, a2: 2100 };
    update({
      isLoading: true,
      messages: [...chatState.messages, userMsg('u2', 'and the next quarter?'), reply('a2', 'Guidance', true)],
    });
    grow(2150);
    await frames();
    grow(2600);
    await frames();
    expect(viewport().scrollTop).toBe(2100);

    // A scrollbar drag: a press on the scroller, then the view moves, no wheel.
    act(() => {
      viewport().dispatchEvent(new Event('pointerdown'));
      setScrollTop(800);
    });
    grow(2800);
    await frames();

    expect(viewport().scrollTop).toBe(800);
  });
});
