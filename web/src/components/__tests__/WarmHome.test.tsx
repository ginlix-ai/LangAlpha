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

import { REWARM_AFTER_MS, WarmHome } from '../WarmHome';

let now = 0;

beforeEach(() => {
  now = 1_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => now);
});

afterEach(() => {
  vi.restoreAllMocks();
  api.getHomeWorkspace.mockClear();
  warm.warmWorkspace.mockClear();
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

describe('WarmHome', () => {
  it('waits for a click before warming Home', async () => {
    const queryClient = clientWithHome();
    renderWithProviders(<WarmHome />, { queryClient });
    await act(async () => {});
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', queryClient);
  });

  it('looks Home up on the first click, not before', async () => {
    let resolveHome: (home: { workspace_id: string }) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((resolve) => { resolveHome = resolve; }),
    );
    renderWithProviders(<WarmHome />);
    await act(async () => {});
    expect(api.getHomeWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(api.getHomeWorkspace).toHaveBeenCalledTimes(1);
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await act(async () => resolveHome({ workspace_id: 'home-1' }));
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', expect.anything());
  });

  it('retries a failed lookup on the next interaction', async () => {
    api.getHomeWorkspace.mockRejectedValueOnce(new Error('offline'));
    renderWithProviders(<WarmHome />);

    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await tap();
    expect(api.getHomeWorkspace).toHaveBeenCalledTimes(2);
    expect(warm.warmWorkspace).toHaveBeenCalledWith('home-1', expect.anything());
  });

  it('does not warm Home found after unmount', async () => {
    let resolveHome: (home: { workspace_id: string }) => void = () => {};
    api.getHomeWorkspace.mockImplementationOnce(
      () => new Promise((resolve) => { resolveHome = resolve; }),
    );
    const { unmount } = renderWithProviders(<WarmHome />);
    await tap();

    unmount();
    await act(async () => resolveHome({ workspace_id: 'home-1' }));
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('counts a key press, but not a modifier on its own', async () => {
    renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });

    await press('Meta');
    await press('Shift');
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await press('k');
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('counts a click that a component stops from bubbling', async () => {
    renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });
    const button = document.body.appendChild(document.createElement('button'));
    button.addEventListener('pointerdown', (event) => event.stopPropagation());

    await tap(button);
    expect(warm.warmWorkspace).toHaveBeenCalledTimes(1);
  });

  it('warms again only once the last warm is old enough', async () => {
    renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });
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
    renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });
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
    renderWithProviders(<WarmHome />);
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
    renderWithProviders(<WarmHome />);
    const stop = document.body.appendChild(document.createElement('button'));
    stop.setAttribute('data-computer-power', '');

    await tap();
    await tap(stop);
    await act(async () => failHome(new Error('offline')));
    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });

  it('stops listening once unmounted', async () => {
    const { unmount } = renderWithProviders(<WarmHome />, { queryClient: clientWithHome() });

    unmount();
    await tap();
    await press('k');
    expect(warm.warmWorkspace).not.toHaveBeenCalled();
  });
});
