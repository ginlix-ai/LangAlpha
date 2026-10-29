/**
 * When this Mac is offered, which row is this Mac, and when remote control is
 * turned off on the way out: only once no computer logs in to the helper.
 * Turning off a Mac still in use is the failure that costs, so every rule that
 * keeps it on is pinned here, a failed list read among them.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { MutationObserver, QueryClient } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';

import type { RemoteControlStatus } from '@/lib/desktop';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputerProviderOption, ComputerProvidersResponse } from '@/types/api';

// The shell's preload runs before the bundle, so the bridge is in place before
// any module reads it.
const bridge = vi.hoisted(() => {
  const b: Record<string, unknown> = { version: '0.4.0', platform: 'darwin' };
  (window as unknown as { langalphaDesktop: unknown }).langalphaDesktop = b;
  return b;
});

vi.mock('../../utils/api', () => ({ getComputers: vi.fn() }));

import { getComputers } from '../../utils/api';
import { CREATE_COMPUTER_MUTATION_KEY } from '../useComputers';
import {
  isThisMac,
  releaseRemoteControl,
  remoteControlOffered,
  useReleaseRefusedRemoteControl,
} from '../useRemoteControl';

const mockGetComputers = getComputers as Mock;

const MAC_KEY = 'SHA256:helperHostKey';

const STATUS: RemoteControlStatus = {
  supported: true,
  service: 'on',
  helper: 'connected',
  paired: true,
  fingerprint: MAC_KEY,
  account: 'alex',
};

const UNSUPPORTED: RemoteControlStatus = {
  supported: false,
  reason: 'os_version',
  service: 'off',
  helper: null,
  paired: false,
  fingerprint: null,
  account: 'alex',
};

const SSH: ComputerProviderOption = { kind: 'ssh', is_default: false, allowed: null };
const DAYTONA: ComputerProviderOption = { kind: 'daytona', is_default: true, allowed: null };
const offering = (
  providers: ComputerProviderOption[],
  tunnels = true,
): ComputerProvidersResponse => ({ providers, tunnels });

function computer(id: string, fingerprint: string | null, over: Partial<Computer> = {}): Computer {
  return {
    computer_id: id,
    user_id: 'u1',
    kind: 'ssh',
    name: id,
    status: 'running',
    resource_tier: 'standard',
    is_always_on: false,
    is_primary: false,
    root_dir: '/Users/alex/langalpha',
    ssh: fingerprint === null
      ? null
      : { host: 'alex-mac', port: 22, username: 'alex', fingerprint, tunnel: true, tunnel_connected: true, helper: true },
    ...over,
  };
}

let getStatus: Mock;
let disable: Mock;

beforeEach(() => {
  vi.clearAllMocks();
  getStatus = vi.fn(async () => STATUS);
  disable = vi.fn(async () => ({ ...STATUS, service: 'off', helper: null, paired: false, fingerprint: null }));
  bridge.getRemoteControl = getStatus;
  bridge.enableRemoteControl = vi.fn();
  bridge.disableRemoteControl = disable;
  mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
});

const client = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });

/** Let a queued release run to its end. */
const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

/** A create in flight on `queryClient`, as the Computers dialog runs one, ended by the test. */
function createInFlight(queryClient: QueryClient) {
  let end: (failure?: Error) => void = () => {};
  const observer = new MutationObserver(queryClient, {
    mutationKey: CREATE_COMPUTER_MUTATION_KEY,
    mutationFn: () => new Promise<void>((resolve, reject) => {
      end = (failure) => (failure ? reject(failure) : resolve());
    }),
  });
  void observer.mutate().catch(() => {});
  return (failure?: Error) => end(failure);
}

describe('remoteControlOffered', () => {
  it('needs a supported shell and an SSH option this server can tunnel to', () => {
    expect(remoteControlOffered(STATUS, offering([DAYTONA, SSH]))).toBe(true);
  });

  it.each<[string, RemoteControlStatus | null, ComputerProvidersResponse | undefined]>([
    ['no answer from the shell', null, offering([SSH])],
    ['a shell that cannot run the helper here', UNSUPPORTED, offering([SSH])],
    ['no SSH option', STATUS, offering([DAYTONA])],
    ['SSH not on the plan', STATUS, offering([{ ...SSH, allowed: false }])],
    ['no tunnel gateway', STATUS, offering([SSH], false)],
    ['a server too old to say', STATUS, { providers: [SSH] } as ComputerProvidersResponse],
    ['providers still loading', STATUS, undefined],
  ])('is not offered with %s', (_label, status, providers) => {
    expect(remoteControlOffered(status, providers)).toBe(false);
  });
});

describe('isThisMac', () => {
  it('matches the row pinned to the helper host key', () => {
    expect(isThisMac(computer('a', MAC_KEY), MAC_KEY)).toBe(true);
    expect(isThisMac(computer('b', 'SHA256:other'), MAC_KEY)).toBe(false);
  });

  // Before pairing the helper has no key, and a sandbox row has no ssh at
  // all: neither may read as a match of null with null.
  it('matches nothing without a fingerprint', () => {
    expect(isThisMac(computer('a', MAC_KEY), null)).toBe(false);
    expect(isThisMac(computer('c', null), null)).toBe(false);
  });
});

describe('releaseRemoteControl', () => {
  it('turns remote control off when no computer is this Mac', async () => {
    mockGetComputers.mockResolvedValue({ computers: [computer('a', 'SHA256:other')], total: 1 });
    await releaseRemoteControl(client());
    expect(disable).toHaveBeenCalledTimes(1);
  });

  it('keeps it on while a computer is this Mac', async () => {
    mockGetComputers.mockResolvedValue({ computers: [computer('a', MAC_KEY)], total: 1 });
    await releaseRemoteControl(client());
    expect(disable).not.toHaveBeenCalled();
  });

  it('does not count a removed row on its way out of the list', async () => {
    mockGetComputers.mockResolvedValue({ computers: [computer('a', MAC_KEY, { status: 'deleted' })], total: 1 });
    await releaseRemoteControl(client(), { onlyFor: MAC_KEY });
    expect(disable).toHaveBeenCalledTimes(1);
  });

  it('after a removal, keeps it on while another computer shares the host key', async () => {
    mockGetComputers.mockResolvedValue({ computers: [computer('b', MAC_KEY)], total: 1 });
    await releaseRemoteControl(client(), { onlyFor: MAC_KEY });
    expect(disable).not.toHaveBeenCalled();
  });

  // The helper was paired again since the row was read: the removed computer
  // was a Mac it no longer is.
  it('after a removal, leaves a helper that has since become another Mac', async () => {
    getStatus.mockResolvedValue({ ...STATUS, fingerprint: 'SHA256:newKey' });
    await releaseRemoteControl(client(), { onlyFor: MAC_KEY });
    expect(disable).not.toHaveBeenCalled();
  });

  it('keeps it on when the list cannot be read, and does not throw', async () => {
    mockGetComputers.mockRejectedValue(new Error('Network Error'));
    await expect(releaseRemoteControl(client())).resolves.toBeUndefined();
    expect(disable).not.toHaveBeenCalled();
  });

  it('does nothing in a shell that cannot run the helper', async () => {
    getStatus.mockResolvedValue({ ...UNSUPPORTED, reason: 'not_packaged' });
    await releaseRemoteControl(client());
    expect(disable).not.toHaveBeenCalled();
    expect(mockGetComputers).not.toHaveBeenCalled();
  });

  // A setup that closes on the computer it just added would otherwise read
  // the list before that computer is on it.
  it('waits for a create in flight, and keeps it on for the computer it adds', async () => {
    const queryClient = client();
    const land = createInFlight(queryClient);
    const released = releaseRemoteControl(queryClient);
    await settle();
    expect(mockGetComputers).not.toHaveBeenCalled();

    mockGetComputers.mockResolvedValue({ computers: [computer('a', MAC_KEY)], total: 1 });
    land();
    await released;
    expect(mockGetComputers).toHaveBeenCalled();
    expect(disable).not.toHaveBeenCalled();
  });

  // Waiting forever would hold every later turn on or off behind it.
  it('decides once a create in flight fails', async () => {
    const queryClient = client();
    const land = createInFlight(queryClient);
    const released = releaseRemoteControl(queryClient);
    await settle();
    land(new Error('Request failed with status code 409'));
    await released;
    expect(disable).toHaveBeenCalledTimes(1);
  });
});

describe('useReleaseRefusedRemoteControl', () => {
  const REFUSED: RemoteControlStatus = { ...STATUS, helper: 'refused' };

  it('turns off a helper whose tunnel the deployment took away, once no computer is this Mac', async () => {
    getStatus.mockResolvedValue(REFUSED);
    renderHookWithProviders(() => useReleaseRefusedRemoteControl(REFUSED));
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  // Removed on another device: the list this app read a moment ago still has it.
  it('asks the server, not a list read before the removal', async () => {
    getStatus.mockResolvedValue(REFUSED);
    const queryClient = client();
    queryClient.setQueryData(queryKeys.computers.lists(), { computers: [computer('a', MAC_KEY)], total: 1 });
    renderHookWithProviders(() => useReleaseRefusedRemoteControl(REFUSED), { queryClient });
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  it('keeps a refused helper while a computer of this account is still this Mac', async () => {
    getStatus.mockResolvedValue(REFUSED);
    mockGetComputers.mockResolvedValue({ computers: [computer('a', MAC_KEY)], total: 1 });
    renderHookWithProviders(() => useReleaseRefusedRemoteControl(REFUSED));
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it.each([
    ['connected, perhaps for another account signed in here', STATUS],
    ['still connecting', { ...STATUS, helper: 'connecting' as const }],
    ['paused', { ...STATUS, service: 'off' as const, helper: null }],
  ])('leaves a helper that is %s', async (_, status) => {
    renderHookWithProviders(() => useReleaseRefusedRemoteControl(status));
    await settle();
    expect(getStatus).not.toHaveBeenCalled();
    expect(disable).not.toHaveBeenCalled();
  });
});
