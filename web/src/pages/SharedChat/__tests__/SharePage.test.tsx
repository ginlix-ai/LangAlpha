/**
 * `/s/` and `/a/` are one page: a metadata read and a dispatch on its `kind`.
 * The views are stubbed: what is under test is which one the page hands off
 * to, that the read waits for the session (the owner of a private link is
 * only recognised by the bearer), and that a 404 ends on the branded page
 * whose sign-in comes back to the link it was opened from.
 *
 * `@/config/hostMode` is mocked rather than inherited: `web/.env` sets
 * VITE_HOST_MODE, and the sign-in affordance is a platform-mode feature.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Route, Routes, useLocation } from 'react-router';

import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const auth = vi.hoisted(() => ({ isInitialized: true, isLoggedIn: false, userId: null as string | null }));

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));
vi.mock('../../../contexts/AuthContext', () => ({
  useAuth: () => ({ isInitialized: auth.isInitialized, isLoggedIn: auth.isLoggedIn, userId: auth.userId }),
}));
vi.mock('../../../contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));

vi.mock('../api', () => ({ getSharedMetadata: vi.fn() }));
vi.mock('../SharedChatView', () => ({
  default: ({ shareToken }: { shareToken: string }) => <div data-testid="chat-view">{shareToken}</div>,
}));
vi.mock('../SharedFileView', () => ({
  default: ({ code, metadata }: { code: string; metadata: { name: string } }) => (
    <div data-testid="file-view">{code}:{metadata.name}</div>
  ),
}));
vi.mock('../SharedAppView', () => ({
  default: ({ metadata }: { metadata: { title: string } }) => <div data-testid="app-view">{metadata.title}</div>,
}));

import { getSharedMetadata } from '../api';
import SharePage from '../SharePage';

const mockMetadata = getSharedMetadata as Mock;
const CODE = 'k7f2m9q1x4z8';

function EntryProbe() {
  const { pathname, search } = useLocation();
  return <div data-testid="entry">{pathname}{search}</div>;
}

function renderAt(route = `/s/${CODE}`) {
  return renderWithProviders(
    <Routes>
      <Route path="/s/:shareToken" element={<SharePage />} />
      <Route path="/a/:shareToken" element={<SharePage />} />
      <Route path="/app" element={<EntryProbe />} />
    </Routes>,
    { route },
  );
}

function notFound() {
  return Object.assign(new Error('Shared link not found (404)'), { response: { status: 404 } });
}

describe('SharePage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    auth.isInitialized = true;
    auth.isLoggedIn = false;
    auth.userId = null;
  });

  it('a thread opens the chat view', async () => {
    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat' });
    renderAt();
    expect(await screen.findByTestId('chat-view')).toHaveTextContent(CODE);
    expect(mockMetadata).toHaveBeenCalledWith(CODE, { asVisitor: false, path: null });
  });

  it('a file opens the file view', async () => {
    mockMetadata.mockResolvedValue({
      kind: 'file', name: 'report.html', path: 'results/report.html', access: 'public', frame_base: '/api/v1/wsfiles/s/abc/',
    });
    renderAt(`/a/${CODE}`);
    expect(await screen.findByTestId('file-view')).toHaveTextContent(`${CODE}:report.html`);
    expect(screen.queryByTestId('chat-view')).not.toBeInTheDocument();
  });

  it('an app link opens the app view', async () => {
    mockMetadata.mockResolvedValue({
      kind: 'app', title: 'Dashboard', url: 'https://8080-sandbox.example/?token=x', expires_in: 60 * 60,
    });
    renderAt(`/a/${CODE}`);
    expect(await screen.findByTestId('app-view')).toHaveTextContent('Dashboard');
  });

  it('titles the document after what the link opens, and gives the title back on leaving', async () => {
    document.title = 'LangAlpha';
    mockMetadata.mockResolvedValue({
      kind: 'file', name: 'report.html', path: 'results/report.html', access: 'public', frame_base: '/api/v1/wsfiles/s/abc/',
    });
    const { unmount } = renderAt(`/a/${CODE}`);
    await screen.findByTestId('file-view');
    expect(document.title).toBe('report.html');
    unmount();
    expect(document.title).toBe('LangAlpha');
  });

  it('a chat is titled too', async () => {
    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat', workspace_name: 'Research' });
    renderAt();
    await screen.findByTestId('chat-view');
    expect(document.title).toBe('Shared chat');
  });

  it('a failed refetch keeps the page it already drew', async () => {
    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat' });
    const { queryClient } = renderAt();
    await screen.findByTestId('chat-view');

    mockMetadata.mockRejectedValue(notFound());
    await act(() => queryClient.refetchQueries({ queryKey: queryKeys.share.all }));

    expect(mockMetadata).toHaveBeenCalledTimes(2);
    expect(screen.getByTestId('chat-view')).toBeInTheDocument();
    expect(screen.queryByText("You don't have access, or this link is unavailable")).not.toBeInTheDocument();
  });

  it('?as=visitor is passed through to the read', async () => {
    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat' });
    renderAt(`/s/${CODE}?as=visitor`);
    await screen.findByTestId('chat-view');
    expect(mockMetadata).toHaveBeenCalledWith(CODE, { asVisitor: true, path: null });
  });

  it('waits for the session before reading', async () => {
    auth.isInitialized = false;
    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat' });
    const { rerender } = renderAt();

    await new Promise((r) => setTimeout(r, 20));
    expect(mockMetadata).not.toHaveBeenCalled();

    auth.isInitialized = true;
    rerender(
      <Routes>
        <Route path="/s/:shareToken" element={<SharePage />} />
      </Routes>,
    );
    expect(await screen.findByTestId('chat-view')).toBeInTheDocument();
    expect(mockMetadata).toHaveBeenCalledTimes(1);
  });

  it('a 404 is the unavailable page, whose sign-in returns to the same link', async () => {
    mockMetadata.mockRejectedValue(notFound());
    const user = userEvent.setup();
    renderAt(`/a/${CODE}?path=reports%2Fq3.html`);

    expect(await screen.findByText("You don't have access, or this link is unavailable")).toBeInTheDocument();
    expect(screen.getByText('If this is your link, sign in to open it.')).toBeInTheDocument();
    // A 404 is final: no retry, and no second read.
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument();
    expect(mockMetadata).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    // The page an app was opened at survives the round trip through sign-in.
    const back = new URLSearchParams({ redirect: `/a/${CODE}?path=reports%2Fq3.html` });
    expect(await screen.findByTestId('entry')).toHaveTextContent(`/app?${back.toString()}`);
  });

  it('a sign-in that lands while the 404 is showing reads the link again as that viewer', async () => {
    mockMetadata.mockRejectedValue(notFound());
    const { rerender } = renderAt(`/a/${CODE}`);
    expect(await screen.findByText('If this is your link, sign in to open it.')).toBeInTheDocument();

    // Another tab (or the OAuth popup) signs the owner in; this page stays mounted.
    auth.isLoggedIn = true;
    auth.userId = 'owner-1';
    mockMetadata.mockResolvedValue({
      kind: 'file', name: 'report.html', path: 'report.html', access: 'owner', frame_base: '/g/', expires_in: 12 * 60 * 60,
    });
    rerender(
      <Routes>
        <Route path="/a/:shareToken" element={<SharePage />} />
      </Routes>,
    );
    expect(await screen.findByTestId('file-view')).toHaveTextContent(`${CODE}:report.html`);
    expect(mockMetadata).toHaveBeenCalledTimes(2);
  });

  it('a signed-in visitor to a 404 is not offered sign-in', async () => {
    auth.isLoggedIn = true;
    mockMetadata.mockRejectedValue(notFound());
    renderAt();

    expect(await screen.findByText("You don't have access, or this link is unavailable")).toBeInTheDocument();
    expect(screen.getByText('It may be private to its owner, turned off, or no longer exist.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Sign in' })).not.toBeInTheDocument();
  });

  it('any other failure is retried, then the failed page offers a retry that reads again', async () => {
    mockMetadata.mockRejectedValue(Object.assign(new Error('boom'), { response: { status: 500 } }));
    const user = userEvent.setup();
    renderAt();

    // Unlike a 404, a 500 is tried twice more (on React Query's backoff) first.
    expect(await screen.findByText("Couldn't load this link", {}, { timeout: 8_000 })).toBeInTheDocument();
    expect(mockMetadata).toHaveBeenCalledTimes(3);
    expect(screen.queryByRole('button', { name: 'Sign in' })).not.toBeInTheDocument();

    mockMetadata.mockResolvedValue({ kind: 'thread', thread_id: 't1', title: 'Shared chat' });
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByTestId('chat-view')).toBeInTheDocument();
    await waitFor(() => expect(mockMetadata).toHaveBeenCalledTimes(4));
  }, 15_000);

  describe('renews a signed credential on its own expiry', () => {
    beforeEach(() => {
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] });
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    async function readsAfter(minutes: number): Promise<number> {
      await act(() => vi.advanceTimersByTimeAsync(minutes * 60_000));
      return mockMetadata.mock.calls.length;
    }

    it("an app's hour-long URL a few minutes before it lapses, not every ten", async () => {
      // Each read mints a new URL, and a new URL reloads the running app.
      mockMetadata.mockImplementation(async () => ({
        kind: 'app', title: 'Dashboard', url: 'https://8080-sandbox.example/?token=x', expires_in: 60 * 60,
      }));
      renderAt(`/a/${CODE}`);
      await act(() => vi.advanceTimersByTimeAsync(0));
      expect(screen.getByTestId('app-view')).toBeInTheDocument();

      expect(await readsAfter(50)).toBe(1);
      expect(await readsAfter(6)).toBe(2);
    });

    it("an owner's 12h grant an hour early", async () => {
      mockMetadata.mockImplementation(async () => ({
        kind: 'file', name: 'r.html', path: 'r.html', access: 'owner', frame_base: '/api/v1/wsfiles/g/x/', expires_in: 12 * 60 * 60,
      }));
      renderAt(`/a/${CODE}`);
      await act(() => vi.advanceTimersByTimeAsync(0));

      expect(await readsAfter(10 * 60)).toBe(1);
      expect(await readsAfter(61)).toBe(2);
    });

    it('a public file carries no credential and is read once', async () => {
      mockMetadata.mockResolvedValue({
        kind: 'file', name: 'r.html', path: 'r.html', access: 'public', frame_base: '/api/v1/wsfiles/s/abc/',
      });
      renderAt(`/a/${CODE}`);
      await act(() => vi.advanceTimersByTimeAsync(0));

      expect(await readsAfter(24 * 60)).toBe(1);
    });

    it('an expiry already inside the margin waits a minute rather than looping', async () => {
      mockMetadata.mockImplementation(async () => ({
        kind: 'app', title: 'Dashboard', url: 'https://8080-sandbox.example/?token=x', expires_in: 60,
      }));
      renderAt(`/a/${CODE}`);
      await act(() => vi.advanceTimersByTimeAsync(0));

      expect(await readsAfter(0.5)).toBe(1);
      expect(await readsAfter(0.6)).toBe(2);
    });
  });
});
