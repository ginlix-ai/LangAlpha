/**
 * A notice on a composer control opens beside it once on its own, leaving
 * focus in the draft, and Tab from the control steps into it. An offer goes
 * once it closes: answered by Not now or by Escape from inside it, let go by
 * a press elsewhere, by focus leaving the composer, by its control hiding
 * with its view, or by its clock, which stops while the pointer or focus is
 * on it and keeps running through a host re-rendering under it. A warning
 * opens once a session, across every view that waited on it, and otherwise
 * only closes, since its control stays marked; one the model picker already
 * shows is not opened over it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useRef } from 'react';
import { useOnClickOutside } from '@/hooks/useOnClickOutside';
import { renderWithProviders } from '@/test/utils';
import { NoticeBody, NoticePopover, OFFER_EXPIRES_MS, type ComposerNotice } from '../chat-input.notices';

type Offer = Extract<ComposerNotice, { tone: 'offer' }>;

const CONTENT = <NoticeBody><button type="button">Act</button></NoticeBody>;

function notice(over: Partial<Offer> = {}): Offer {
  return {
    key: 'offer-1',
    tone: 'offer',
    text: 'Subagents are off in this thread.',
    content: CONTENT,
    onDismiss: () => {},
    onExpire: () => {},
    ...over,
  };
}

function warning(key: string): ComposerNotice {
  return { key, tone: 'warning', text: 'Your sign-in has expired.', content: CONTENT };
}

function Host({ shown, blocked = false, hidden = false, onKeyDown }: {
  shown: ComposerNotice | null;
  blocked?: boolean;
  /** The view is cached and hidden, as one left by Back or Forward. */
  hidden?: boolean;
  onKeyDown?: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement>(null);
  return (
    <>
      <div data-composer-box style={hidden ? { display: 'none' } : undefined}>
        <textarea aria-label="Draft" onKeyDown={onKeyDown} />
        <button ref={anchorRef} type="button">Control</button>
        <button type="button">Next</button>
        <NoticePopover notice={shown} anchorRef={anchorRef} blocked={blocked} />
      </div>
      <button type="button">Elsewhere</button>
    </>
  );
}

/** Holds every IntersectionObserver the page makes, so a test can say when a
 *  hidden view shows again, or a shown one hides. */
function watchIntersections() {
  const callbacks: IntersectionObserverCallback[] = [];
  const Real = window.IntersectionObserver;
  window.IntersectionObserver = class {
    constructor(callback: IntersectionObserverCallback) { callbacks.push(callback); }
    observe() {}
    unobserve() {}
    disconnect() {}
    takeRecords() { return []; }
  } as unknown as typeof IntersectionObserver;
  return {
    fire: (isIntersecting: boolean) => act(() => {
      callbacks.forEach((cb) => cb([{ isIntersecting } as IntersectionObserverEntry], {} as IntersectionObserver));
    }),
    count: () => callbacks.length,
    restore: () => { window.IntersectionObserver = Real; },
  };
}

/** The popover opens a frame after the notice appears. */
const settle = () => act(async () => { await new Promise((resolve) => requestAnimationFrame(resolve)); });

beforeEach(() => {
  sessionStorage.clear();
});

describe('NoticePopover', () => {
  it('opens on its own without taking focus from the draft', async () => {
    const { rerender } = renderWithProviders(<Host shown={null} />);
    const draft = screen.getByRole('textbox', { name: 'Draft' });
    draft.focus();
    rerender(<Host shown={notice()} />);
    await settle();
    expect(screen.getByRole('dialog')).toHaveTextContent('Subagents are off in this thread.');
    expect(document.activeElement).toBe(draft);
  });

  it('lets an offer go as answered on Not now and on Escape', async () => {
    const onDismiss = vi.fn();
    const onExpire = vi.fn();
    const first = renderWithProviders(<Host shown={notice({ onDismiss, onExpire })} />);
    await settle();
    fireEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(onDismiss).toHaveBeenCalledOnce();
    first.unmount();

    renderWithProviders(<Host shown={notice({ key: 'offer-2', onDismiss, onExpire })} />);
    await settle();
    const notNow = screen.getByRole('button', { name: 'Not now' });
    notNow.focus();
    fireEvent.keyDown(notNow, { key: 'Escape' });
    expect(onDismiss).toHaveBeenCalledTimes(2);
    expect(onExpire).not.toHaveBeenCalled();
  });

  // Radix hears Escape on the whole page. In the draft it shuts the @ or /
  // list, and must not answer an offer for the thread.
  it('answers an offer on Escape only from inside it, and leaves the key to the draft', async () => {
    const onDismiss = vi.fn();
    const onDraftKey = vi.fn();
    renderWithProviders(<Host shown={notice({ onDismiss })} onKeyDown={onDraftKey} />);
    await settle();
    const draft = screen.getByRole('textbox', { name: 'Draft' });
    draft.focus();
    fireEvent.keyDown(draft, { key: 'Escape' });
    expect(onDismiss).not.toHaveBeenCalled();
    expect(onDraftKey).toHaveBeenCalledOnce();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('is reached with Tab from its control, and steps back out to the control or past it', async () => {
    const onExpire = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(<Host shown={notice({ onExpire })} />);
    await settle();
    act(() => screen.getByRole('textbox', { name: 'Draft' }).focus());

    // Tab from the draft lands on the control, outside the popover, which stays.
    await user.tab();
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Control' }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();

    await user.tab();
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Not now' }));
    await user.tab({ shift: true });
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Control' }));
    await user.tab();
    await user.tab();
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Act' }));
    await user.tab();
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Next' }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(onExpire).not.toHaveBeenCalled();
  });

  it('lets an offer go once focus leaves the composer', async () => {
    const onExpire = vi.fn();
    renderWithProviders(<Host shown={notice({ onExpire })} />);
    await settle();
    act(() => screen.getByRole('button', { name: 'Elsewhere' }).focus());
    expect(onExpire).toHaveBeenCalledOnce();
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  // A floating composer (a phone's market view) collapses on a press outside
  // itself, and the popover is portaled out of it.
  it('counts as inside a composer that closes on a press outside', async () => {
    const onOutside = vi.fn();
    function Floating() {
      const ref = useRef<HTMLDivElement>(null);
      useOnClickOutside(ref, onOutside);
      return <div ref={ref}><Host shown={notice()} /></div>;
    }
    renderWithProviders(<Floating />);
    await settle();
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Act' }));
    expect(onOutside).not.toHaveBeenCalled();
  });

  it('hands focus from inside it back to its control on Escape, and leaves the draft its own', async () => {
    const first = renderWithProviders(<Host shown={notice()} />);
    await settle();
    const notNow = screen.getByRole('button', { name: 'Not now' });
    notNow.focus();
    fireEvent.keyDown(notNow, { key: 'Escape' });
    await settle();
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Control' }));
    first.unmount();

    renderWithProviders(<Host shown={notice({ key: 'offer-2' })} />);
    await settle();
    const draft = screen.getByRole('textbox', { name: 'Draft' });
    draft.focus();
    fireEvent.keyDown(draft, { key: 'Escape' });
    await settle();
    expect(document.activeElement).toBe(draft);
  });

  it('lets an offer go unanswered on a press elsewhere', async () => {
    const onDismiss = vi.fn();
    const onExpire = vi.fn();
    renderWithProviders(<Host shown={notice({ onDismiss, onExpire })} />);
    await settle();
    await userEvent.click(screen.getByRole('button', { name: 'Elsewhere' }));
    expect(onExpire).toHaveBeenCalledOnce();
    expect(onDismiss).not.toHaveBeenCalled();
  });

  it('opens a warning once a session, and only closes it', async () => {
    const lapsed = warning('lapsed:model-a');
    const first = renderWithProviders(<Host shown={lapsed} />);
    await settle();
    expect(screen.getByRole('dialog')).toHaveTextContent('Your sign-in has expired.');
    fireEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(screen.queryByRole('dialog')).toBeNull();
    first.unmount();

    renderWithProviders(<Host shown={lapsed} />);
    await settle();
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('does not open a warning again that went while open and came back', async () => {
    const fallback = warning('fallback:model-a');
    const view = renderWithProviders(<Host shown={fallback} />);
    await settle();
    expect(screen.getByRole('dialog')).toBeInTheDocument();

    view.rerender(<Host shown={null} />);
    await settle();
    view.rerender(<Host shown={fallback} />);
    await settle();
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  // A cached view waits for its anchor to show. By then another view may
  // have shown the warning and had it answered.
  it('does not open a warning a hidden view waited on once another view has shown it', async () => {
    const io = watchIntersections();
    try {
      const lapsed = warning('lapsed:model-a');
      const cached = renderWithProviders(<Host shown={lapsed} hidden />);
      await settle();
      expect(io.count()).toBe(1);

      const current = renderWithProviders(<Host shown={lapsed} />);
      await settle();
      fireEvent.click(screen.getByRole('button', { name: 'Not now' }));
      current.unmount();

      cached.rerender(<Host shown={lapsed} />);
      io.fire(true);
      await settle();
      expect(screen.queryByRole('dialog')).toBeNull();
    } finally {
      io.restore();
    }
  });

  it('closes once its anchor is hidden with its view, rather than float over the next one', async () => {
    const io = watchIntersections();
    try {
      const onExpire = vi.fn();
      const view = renderWithProviders(<Host shown={notice({ onExpire })} />);
      await settle();
      expect(screen.getByRole('dialog')).toBeInTheDocument();

      view.rerender(<Host shown={notice({ onExpire })} hidden />);
      io.fire(false);
      expect(screen.queryByRole('dialog')).toBeNull();
      expect(onExpire).toHaveBeenCalledOnce();
    } finally {
      io.restore();
    }
  });

  it('does not count a warning as seen in a hidden view whose picker is open', async () => {
    const io = watchIntersections();
    try {
      const lapsed = warning('lapsed:model-a');
      const cached = renderWithProviders(<Host shown={lapsed} blocked hidden />);
      await settle();
      cached.unmount();

      renderWithProviders(<Host shown={lapsed} />);
      await settle();
      expect(screen.getByRole('dialog')).toHaveTextContent('Your sign-in has expired.');
    } finally {
      io.restore();
    }
  });

  it('does not open a warning over the picker that already heads with it', async () => {
    const lapsed = warning('lapsed:model-a');
    const first = renderWithProviders(<Host shown={lapsed} blocked />);
    await settle();
    expect(screen.queryByRole('dialog')).toBeNull();
    first.unmount();

    renderWithProviders(<Host shown={lapsed} />);
    await settle();
    expect(screen.queryByRole('dialog')).toBeNull();
  });
});

describe("an offer's clock", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const tick = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

  it('runs out on its own, through the host re-rendering', async () => {
    const onExpire = vi.fn();
    const { rerender } = renderWithProviders(<Host shown={notice({ onExpire: () => onExpire() })} />);
    await settle();
    tick(OFFER_EXPIRES_MS - 1);
    rerender(<Host shown={notice({ onExpire: () => onExpire() })} />);
    tick(1);
    expect(onExpire).toHaveBeenCalledOnce();
  });

  it('waits while the pointer is on it, and starts over when it leaves', async () => {
    const onExpire = vi.fn();
    renderWithProviders(<Host shown={notice({ onExpire })} />);
    await settle();
    const popover = screen.getByRole('dialog');

    fireEvent.pointerEnter(popover);
    tick(OFFER_EXPIRES_MS * 2);
    expect(onExpire).not.toHaveBeenCalled();

    fireEvent.pointerLeave(popover);
    tick(OFFER_EXPIRES_MS - 1);
    expect(onExpire).not.toHaveBeenCalled();
    tick(1);
    expect(onExpire).toHaveBeenCalledOnce();
  });

  it('waits while focus is on it, so its action stays in reach', async () => {
    const onExpire = vi.fn();
    renderWithProviders(<Host shown={notice({ onExpire })} />);
    await settle();

    act(() => screen.getByRole('button', { name: 'Act' }).focus());
    tick(OFFER_EXPIRES_MS * 2);
    expect(onExpire).not.toHaveBeenCalled();

    act(() => (document.activeElement as HTMLElement).blur());
    tick(OFFER_EXPIRES_MS);
    expect(onExpire).toHaveBeenCalledOnce();
  });
});
