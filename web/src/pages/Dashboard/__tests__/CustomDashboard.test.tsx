/**
 * A chosen dashboard with nothing to show never blanks the page or loses its
 * place: the built-in one stands in with the reason and the way back, and
 * which of the two is on screen follows the read, not a remembered failure.
 */
import type { ReactNode, Ref } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { SavedDashboard, SavedDashboardsApi } from '@/lib/dashboards';
import CustomDashboard from '../CustomDashboard';
import type { DashboardSource } from '../hooks/useDashboardSource';

const mocks = vi.hoisted(() => ({ source: null as unknown }));

vi.mock('../hooks/useDashboardSource', () => ({ useDashboardSource: () => mocks.source }));
vi.mock('../hooks/useDashboardAgent', () => ({ useDashboardAgent: () => ({ build: vi.fn(), edit: vi.fn() }) }));
vi.mock('../components/DashboardHeader', () => ({
  default: ({ actions }: { actions?: ReactNode }) => <header>{actions}</header>,
}));
// A ref of the frame's own, so the page bridge's messages are told by their window.
vi.mock('@/lib/appBridge/useAppFrame', async () => {
  const { useRef } = await import('react');
  return {
    useAppFrame: (
      src: string,
      bridge: { grantKey: string | null; consent?: { dismissed: boolean; onDismiss: () => void } },
    ) => ({
      frame: { src, sandbox: '', relay: false, ref: useRef<HTMLIFrameElement>(null) },
      bridge: (
        <span
          data-testid="bridge"
          data-src={src}
          data-grant={bridge.grantKey ?? ''}
          data-dismissed={String(bridge.consent?.dismissed)}
          onClick={bridge.consent?.onDismiss}
        />
      ),
    }),
  };
});
vi.mock('../../ChatAgent/components/viewers/HostedFrame', () => ({
  HostedFrame: ({ src, title, onLoad, ref }: {
    src: string;
    title: string;
    onLoad?: () => void;
    ref?: Ref<HTMLIFrameElement>;
  }) => (
    <iframe ref={ref} src={src} title={title} onLoad={onLoad} />
  ),
}));

const app: SavedDashboard = { kind: 'app', code: 'c1', title: 'Markets', path: null, workspaceId: 'ws', threadId: null };

const source = (over: Partial<DashboardSource>): DashboardSource => ({
  url: null,
  sandbox: '',
  title: null,
  grantKey: null,
  away: null,
  refresh: vi.fn(),
  refreshing: false,
  ...over,
});

const dashboards = (): SavedDashboardsApi => ({
  list: [app],
  current: app,
  select: vi.fn(),
  add: vi.fn(),
  remove: vi.fn(),
  canAdd: vi.fn(),
});

function show(api = dashboards(), dashboard: SavedDashboard = app) {
  const ui = (shown: SavedDashboard) => (
    <CustomDashboard
      dashboard={shown}
      dashboards={api}
      builtIn={({ notice }) => <main data-testid="built-in">{notice}</main>}
    />
  );
  const view = renderWithProviders(ui(dashboard));
  return { api, again: (next = dashboard) => view.rerender(ui(next)) };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('CustomDashboard', () => {
  it('frames the dashboard once it has an address', () => {
    mocks.source = source({ url: 'https://app.test/' });
    show();
    expect(screen.getByTitle('Markets')).toHaveAttribute('src', 'https://app.test/');
    expect(screen.queryByTestId('built-in')).toBeNull();
  });

  it('shows the built-in dashboard with the reason while the read has failed, and asks again on request', () => {
    const refresh = vi.fn();
    mocks.source = source({ away: { kind: 'failed', detail: 'The machine is not answering' }, refresh });
    const { again } = show();
    expect(screen.getByTestId('built-in')).toHaveTextContent('The machine is not answering');
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(refresh).toHaveBeenCalled();
    // The read lands: the dashboard takes the page back with nothing to dismiss.
    mocks.source = source({ url: 'https://app.test/' });
    again();
    expect(screen.queryByTestId('built-in')).toBeNull();
    expect(screen.getByTitle('Markets')).toBeInTheDocument();
  });

  it('keeps the frame on screen under its own grants while a new address loads behind it', () => {
    // The app's command changed: the new address is another program's, with
    // grants of its own, and the one still on screen must not read under them.
    mocks.source = source({ url: 'https://app.test/?sig=1', grantKey: 'c1#old' });
    const { again } = show();
    fireEvent.load(screen.getByTitle('Markets'));
    mocks.source = source({ url: 'https://app.test/?sig=2', grantKey: 'c1#new' });
    again();
    const grants = Object.fromEntries(
      screen.getAllByTestId('bridge').map((b) => [b.getAttribute('data-src'), b.getAttribute('data-grant')]),
    );
    expect(grants).toEqual({ 'https://app.test/?sig=1': 'c1#old', 'https://app.test/?sig=2': 'c1#new' });
  });

  it('keeps "Not now" for the program it was said to, through a renewed address but not a new command', () => {
    mocks.source = source({ url: 'https://app.test/?sig=1', grantKey: 'c1#old' });
    const { again } = show();
    const bridge = () => screen.getAllByTestId('bridge').at(-1)!;
    fireEvent.click(bridge());
    expect(bridge()).toHaveAttribute('data-dismissed', 'true');
    // The signature renewed: the same program, still not asked again.
    fireEvent.load(screen.getByTitle('Markets'));
    mocks.source = source({ url: 'https://app.test/?sig=2', grantKey: 'c1#old' });
    again();
    expect(bridge()).toHaveAttribute('data-dismissed', 'true');
    // Its command changed: another program, which asks for itself.
    mocks.source = source({ url: 'https://app.test/?sig=3', grantKey: 'c1#new' });
    again();
    expect(bridge()).toHaveAttribute('data-grant', 'c1#new');
    expect(bridge()).toHaveAttribute('data-dismissed', 'false');
  });

  it('keeps the frame up when a later read fails behind it', () => {
    mocks.source = source({ url: 'https://app.test/', away: { kind: 'failed', detail: null } });
    show();
    expect(screen.getByTitle('Markets')).toBeInTheDocument();
    expect(screen.queryByTestId('built-in')).toBeNull();
  });

  it('offers to remove one that is gone', () => {
    mocks.source = source({ away: { kind: 'gone' } });
    const { api } = show();
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }));
    expect(api.remove).toHaveBeenCalledWith('c1');
  });

  it('lets the user not wait for a cold start, and says when it is up', () => {
    mocks.source = source({});
    const { again } = show();
    expect(screen.queryByText('Show the built-in dashboard meanwhile')).toBeNull();
    act(() => {
      vi.advanceTimersByTime(6_000);
    });
    fireEvent.click(screen.getByText('Show the built-in dashboard meanwhile'));
    expect(screen.getByTestId('built-in')).toHaveTextContent('Showing the built-in dashboard while Markets starts');
    mocks.source = source({ url: 'https://app.test/' });
    again();
    // Their choice stands until they ask for it.
    expect(screen.getByTestId('built-in')).toHaveTextContent('Markets is ready');
    fireEvent.click(screen.getByRole('button', { name: 'Open Markets' }));
    expect(screen.getByTitle('Markets')).toBeInTheDocument();
  });

  describe("keeps the reader's place on a page", () => {
    const desk: SavedDashboard = {
      kind: 'page', code: 'p1', title: 'Desk', path: 'index.html', workspaceId: 'ws', threadId: null, artifactId: 'a1',
    };
    const version = (seq: number) => `https://pages.test/g/tok/v/${seq}/index.html`;
    const framed = (src: string) =>
      screen.getAllByTitle('Desk').find((f) => f.getAttribute('src') === src) as HTMLIFrameElement;
    // The frames load nothing here, so each holds a window of its own to post from.
    const fromPage = (frame: HTMLIFrameElement | Window, data: unknown) =>
      act(() => {
        const source = frame instanceof HTMLIFrameElement ? frame.contentWindow! : frame;
        window.dispatchEvent(new MessageEvent('message', { data, source, origin: window.location.origin }));
      });
    const hello = { type: 'page:ready', v: 1, path: 'index.html' };
    const at = (y: number) => ({ type: 'page:scroll', v: 1, x: 0, y, moving: false });
    const restores = (frame: HTMLIFrameElement) => {
      const post = vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(() => {});
      return () => post.mock.calls.map(([message]) => message);
    };
    const restore = (y: number) => ({ type: 'page:restore-scroll', x: 0, y });

    it('opens a new version where the reader was on the one shown, and again at the latest place as it swaps in', () => {
      mocks.source = source({ url: version(1), grantKey: 'p1' });
      const { again } = show(dashboards(), desk);
      const first = framed(version(1));
      const toFirst = restores(first);
      fromPage(first, hello);
      // A dashboard opened afresh starts at the top, and the answer turns the reports on.
      expect(toFirst()).toEqual([restore(0)]);
      fireEvent.load(first);
      fromPage(first, at(640));
      // Not the frame's window: not the page's report.
      fromPage(window, at(9));

      mocks.source = source({ url: version(2), grantKey: 'p1' });
      again();
      const next = framed(version(2));
      const toNext = restores(next);
      fromPage(next, hello);
      expect(toNext()).toEqual([restore(640)]);

      // The reader keeps scrolling the one shown while the next loads behind
      // it, whose own jump into place is not the reader.
      fromPage(first, at(700));
      fromPage(next, at(12));
      fireEvent.load(next);
      act(() => {
        vi.advanceTimersByTime(800);
      });
      expect(toNext()).toEqual([restore(640), restore(700)]);
    });

    it('opens another dashboard the user switched to at its top', () => {
      mocks.source = source({ url: version(1), grantKey: 'p1' });
      const { again } = show(dashboards(), desk);
      const first = framed(version(1));
      fromPage(first, hello);
      fireEvent.load(first);
      fromPage(first, at(640));

      const other: SavedDashboard = { ...desk, code: 'p2', artifactId: 'a2' };
      mocks.source = source({ url: 'https://pages.test/g/tok2/v/5/index.html', grantKey: 'p2' });
      again(other);
      const next = framed('https://pages.test/g/tok2/v/5/index.html');
      const toNext = restores(next);
      fromPage(next, hello);
      expect(toNext()).toEqual([restore(0)]);
    });
  });
});
