/**
 * The row at the top of a paged transcript asks for the next older page as the
 * reader nears it, and says so quietly while one loads or after one failed.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { OlderHistoryRow } from '../OlderHistoryRow';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

interface Observed {
  callback: IntersectionObserverCallback;
  options: IntersectionObserverInit | undefined;
  targets: Element[];
  disconnected: boolean;
}

let observed: Observed[] = [];

/** Reports the row in or out of range, as the browser would. */
function intersect(isIntersecting: boolean) {
  const current = observed.at(-1)!;
  act(() => {
    current.callback(
      current.targets.map((target) => ({ isIntersecting, target }) as IntersectionObserverEntry),
      {} as IntersectionObserver,
    );
  });
}

describe('OlderHistoryRow', () => {
  const OriginalIO = window.IntersectionObserver;
  const root = document.createElement('div');

  beforeEach(() => {
    observed = [];
    window.IntersectionObserver = class {
      private record: Observed;
      constructor(callback: IntersectionObserverCallback, options?: IntersectionObserverInit) {
        this.record = { callback, options, targets: [], disconnected: false };
        observed.push(this.record);
      }
      observe(target: Element) {
        this.record.targets.push(target);
      }
      unobserve() {}
      disconnect() {
        this.record.disconnected = true;
      }
      takeRecords() {
        return [];
      }
    } as unknown as typeof IntersectionObserver;
  });
  afterEach(() => {
    window.IntersectionObserver = OriginalIO;
  });

  const props = (overrides: Partial<Parameters<typeof OlderHistoryRow>[0]> = {}) => ({
    getRoot: () => root,
    status: 'idle' as const,
    canLoad: true,
    onLoad: vi.fn(),
    ...overrides,
  });

  it('asks for the older page once the top comes within range', () => {
    const p = props();
    render(<OlderHistoryRow {...p} />);

    intersect(false);
    expect(p.onLoad).not.toHaveBeenCalled();
    intersect(true);
    expect(p.onLoad).toHaveBeenCalledTimes(1);
  });

  it('measures the range in the transcript viewport, reaching above its top only', () => {
    render(<OlderHistoryRow {...props()} />);

    const { options } = observed[0];
    expect(options?.root).toBe(root);
    const [top, ...rest] = (options?.rootMargin ?? '').split(' ');
    expect(parseInt(top, 10)).toBeGreaterThan(0);
    expect(rest).toEqual(['0px', '0px', '0px']);
  });

  it('does not watch while no page may be asked for', () => {
    render(<OlderHistoryRow {...props({ canLoad: false })} />);
    expect(observed).toHaveLength(0);
  });

  it('does not watch without a viewport to measure in', () => {
    render(<OlderHistoryRow {...props({ getRoot: () => null })} />);
    expect(observed).toHaveLength(0);
  });

  it('watches afresh once a page has landed, so a short page asks for the next', () => {
    const p = props();
    const { rerender } = render(<OlderHistoryRow {...p} />);
    rerender(<OlderHistoryRow {...p} canLoad={false} status="loading" />);
    expect(observed[0].disconnected).toBe(true);

    rerender(<OlderHistoryRow {...p} />);
    expect(observed).toHaveLength(2);
    intersect(true);
    expect(p.onLoad).toHaveBeenCalledTimes(1);
  });

  it('shows nothing while idle', () => {
    render(<OlderHistoryRow {...props()} />);
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('names the load while a page is on its way', () => {
    render(<OlderHistoryRow {...props({ canLoad: false, status: 'loading' })} />);
    expect(screen.getByRole('status', { name: 'chat.loadingOlderHistory' })).toBeInTheDocument();
  });

  it('offers a retry after a page failed', () => {
    const p = props({ canLoad: false, status: 'failed' });
    render(<OlderHistoryRow {...p} />);

    expect(screen.getByRole('alert')).toHaveTextContent('chat.olderHistoryFailed');
    fireEvent.click(screen.getByRole('button', { name: 'common.retry' }));
    expect(p.onLoad).toHaveBeenCalledTimes(1);
  });

  it('keeps one box in every state, so nothing under it moves', () => {
    const p = props();
    const { container, rerender } = render(<OlderHistoryRow {...p} />);
    const row = container.firstElementChild!;
    const idle = row.className;

    for (const status of ['loading', 'failed', 'held'] as const) {
      rerender(<OlderHistoryRow {...p} canLoad={false} status={status} />);
      expect(container.firstElementChild).toBe(row);
      expect(row.className).toBe(idle);
    }
  });

  it('is absent at the start of the thread and while the transcript is rebuilt', () => {
    for (const status of ['end', 'reloading'] as const) {
      const { container, unmount } = render(<OlderHistoryRow {...props({ canLoad: false, status })} />);
      expect(container.firstElementChild).toBeNull();
      unmount();
    }
  });
});
