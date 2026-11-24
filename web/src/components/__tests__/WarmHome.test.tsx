import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';

const api = vi.hoisted(() => ({
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1' })),
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: api.getHomeWorkspace,
}));

const warm = vi.hoisted(() => ({ warmWorkspace: vi.fn(async () => {}) }));
vi.mock('@/pages/ChatAgent/utils/warmWorkspace', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  warmWorkspace: warm.warmWorkspace,
}));

const setupStatus = vi.hoisted(() => ({
  readSetupStatusForWarm: vi.fn<() => Promise<{ needs_setup: boolean } | null>>(),
}));
vi.mock('@/hooks/useServerSetup', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  readSetupStatusForWarm: setupStatus.readSetupStatusForWarm,
}));

const route = vi.hoisted(() => ({ kind: 'done' as string }));
vi.mock('@/hooks/useOnboardingRoute', () => ({ useOnboardingRoute: () => ({ kind: route.kind }) }));

import { skipSetup } from '@/hooks/useSetupGate';
import { REWARM_AFTER_MS, WarmHome } from '../WarmHome';

/** WarmHome on an account whose live route still runs setup, or is done. */
function warmHome(setupRunning: boolean) {
  route.kind = setupRunning ? 'answers' : 'done';
  return <WarmHome />;
}

let now = 0;

beforeEach(() => {
  now = 1_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => now);
  setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: false });
});

afterEach(() => {
  vi.restoreAllMocks();
  api.getHomeWorkspace.mockClear();
  warm.warmWorkspace.mockClear();
  setupStatus.readSetupStatusForWarm.mockReset();
  sessionStorage.clear();
  document.body.replaceChildren();
});

/** A client that already knows Home's id, so the listeners attach on mount. */
function clientWithHome(): QueryClient {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.workspaces.home(), { workspace_id: 'home-1' });
  return queryClient;
}

/** Dispatches, then lets the Home lookup the warm waits on settle. */
async function tap(target: EventTarget = document.body): Promise<void> {
  await act(async () => {
    target.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true }));
  });
}

async function press(key: string): Promise<void> {
  await act(async () => {
    document.body.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
  });
}

describe('WarmHome while the account\'s setup runs on its answers', () => {
  it('starts nothing until a status read has answered', async () => {
    let answer: (status: { needs_setup: boolean }) => void = () => {};
    setupStatus.readSetupStatusForWarm.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const queryClient = clientWithHome();
    renderWithProviders(warmHome(true), { queryClient });

    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await act(async () => answer({ needs_setup: false }));
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', queryClient, 'input');
  });

  it('starts nothing when the setup step is still owed, and asks again next time', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: true });
    renderWithProviders(warmHome(true), { queryClient: clientWithHome() });

    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
    await tap();
    expect(setupStatus.readSetupStatusForWarm).toHaveBeenCalledTimes(2);
  });

  it('goes on once the user has exited setup', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: true });
    skipSetup();
    renderWithProviders(warmHome(true), { queryClient: clientWithHome() });
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('goes on when the reads ran out, so the app is not stuck', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue(null);
    renderWithProviders(warmHome(true), { queryClient: clientWithHome() });
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });
});

describe('WarmHome for an account past setup on a server not set up', () => {
  it('warms without asking, since the gate never sends the account to the step', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: true });
    const queryClient = clientWithHome();
    renderWithProviders(warmHome(false), { queryClient });
    await tap();
    expect(setupStatus.readSetupStatusForWarm).not.toHaveBeenCalled();
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', queryClient, 'input');
  });

  it('asks while the live route is still read, since a remembered "done" can be stale', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: true });
    route.kind = 'loading';
    renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });
    await tap();
    expect(setupStatus.readSetupStatusForWarm).toHaveBeenCalledTimes(1);
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('holds once the account is in setup again', async () => {
    setupStatus.readSetupStatusForWarm.mockResolvedValue({ needs_setup: true });
    const { rerender } = renderWithProviders(warmHome(false), { queryClient: clientWithHome() });
    rerender(warmHome(true));
    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });
});

describe('WarmHome', () => {
  it('waits for a click before warming Home', async () => {
    const queryClient = clientWithHome();
    renderWithProviders(warmHome(false), { queryClient });
    await act(async () => {});
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', queryClient, 'input');
  });

  it('looks Home up on the first click, not before', async () => {
    let resolveHome: (home: { workspace_id: string }) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((resolve) => { resolveHome = resolve; }),
    );
    renderWithProviders(warmHome(false));
    await act(async () => {});
    expect(api.getHomeWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(api.getHomeWorkspace).toHaveBeenCalledTimes(1);
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await act(async () => resolveHome({ workspace_id: 'home-1' }));
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', expect.anything(), 'input');
  });

  it('retries a failed lookup on the next interaction', async () => {
    api.getHomeWorkspace.mockRejectedValueOnce(new Error('offline'));
    renderWithProviders(warmHome(false));

    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(api.getHomeWorkspace).toHaveBeenCalledTimes(2);
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', expect.anything(), 'input');
  });

  it('does not warm Home found after unmount', async () => {
    let resolveHome: (home: { workspace_id: string }) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((resolve) => { resolveHome = resolve; }),
    );
    const { unmount } = renderWithProviders(warmHome(false));
    await tap();

    unmount();
    await act(async () => resolveHome({ workspace_id: 'home-1' }));
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('counts a key press, but not a modifier on its own', async () => {
    renderWithProviders(warmHome(false), { queryClient: clientWithHome() });

    await press('Meta');
    await press('Shift');
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await press('k');
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('counts a click that a component stops from bubbling', async () => {
    renderWithProviders(warmHome(false), { queryClient: clientWithHome() });
    const button = document.body.appendChild(document.createElement('button'));
    button.addEventListener('pointerdown', (event) => event.stopPropagation());

    await tap(button);
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('warms again only once the last warm is old enough', async () => {
    renderWithProviders(warmHome(false), { queryClient: clientWithHome() });
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);

    now += REWARM_AFTER_MS - 1;
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);

    now += 1;
    await press('k');
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(2);

    // The clock restarts at that warm.
    now += REWARM_AFTER_MS - 1;
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(2);
  });

  it('warms nothing on a power control, and waits as long after one as after a warm', async () => {
    renderWithProviders(warmHome(false), { queryClient: clientWithHome() });
    const stop = document.body.appendChild(document.createElement('button'));
    stop.setAttribute('data-computer-power', '');
    const icon = stop.appendChild(document.createElement('span'));

    await tap(icon);
    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    now += REWARM_AFTER_MS;
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);

    // One inside the window restarts the wait from there.
    now += REWARM_AFTER_MS - 1;
    await tap(stop);
    now += 1;
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('drops a pending warm once a power control is used', async () => {
    let resolveHome: (home: { workspace_id: string }) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((resolve) => { resolveHome = resolve; }),
    );
    renderWithProviders(warmHome(false));
    const stop = document.body.appendChild(document.createElement('button'));
    stop.setAttribute('data-computer-power', '');

    await tap();
    await tap(stop);
    await act(async () => resolveHome({ workspace_id: 'home-1' }));
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('keeps the wait a power control began when an older lookup fails', async () => {
    let failHome: (error: Error) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((_resolve, reject) => { failHome = reject; }),
    );
    renderWithProviders(warmHome(false));
    const stop = document.body.appendChild(document.createElement('button'));
    stop.setAttribute('data-computer-power', '');

    await tap();
    await tap(stop);
    await act(async () => failHome(new Error('offline')));
    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('stops listening once unmounted', async () => {
    const { unmount } = renderWithProviders(warmHome(false), { queryClient: clientWithHome() });

    unmount();
    await tap();
    await press('k');
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });
});
