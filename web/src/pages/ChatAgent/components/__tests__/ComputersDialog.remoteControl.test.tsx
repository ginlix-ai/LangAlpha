/**
 * This Mac among the computer rows, in the desktop app: the row whose host key
 * is the helper's says so, says when remote control is paused, and removing it
 * turns remote control off, unless another computer still logs in there. A
 * helper's row reads the same on any device. A helper the deployment refuses
 * is turned off once no row is this Mac, and a create or removal settles
 * remote control even after its dialog is gone.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { RemoteControlResult, RemoteControlStatus } from '@/lib/desktop';
import type { Computer, SshPairingStatus, SshProbeResult } from '@/types/api';

// The shell's preload runs before the bundle, so the bridge is in place before
// any module reads it.
const bridge = vi.hoisted(() => {
  const b: Record<string, unknown> = { version: '0.4.0', platform: 'darwin' };
  (window as unknown as { langalphaDesktop: unknown }).langalphaDesktop = b;
  return b;
});

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'oss',
  isPlatformMode: false,
  APP_ENTRY_PATH: '/',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async () => {
  const errors = await vi.importActual<typeof import('@/api/errors')>('@/api/errors');
  return {
    getComputers: vi.fn(),
    getComputerProviders: vi.fn(),
    createComputer: vi.fn(),
    deleteComputer: vi.fn(),
    getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
    createSshPairing: vi.fn(),
    reconnectComputer: vi.fn(),
    getSshPairing: vi.fn(),
    connectSshPairing: vi.fn(),
    probeSsh: vi.fn(),
    getSshKey: vi.fn(),
    streamComputerEvents: vi.fn(async () => {}),
    streamWorkspaceEvents: vi.fn(async () => {}),
    apiErrorObject: errors.apiErrorObject,
    apiErrorReason: errors.apiErrorReason,
    apiErrorStatus: errors.apiErrorStatus,
    apiErrorDetailMessage: errors.apiErrorDetailMessage,
  };
});

import {
  createComputer,
  createSshPairing,
  deleteComputer,
  getComputerProviders,
  getComputers,
  getSshPairing,
  reconnectComputer,
} from '../../utils/api';
import ComputersDialog from '../ComputersDialog';
import { openDetails } from './computersDialogHarness';

const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;
const mockDelete = deleteComputer as Mock;
const mockCreate = createComputer as Mock;
const mockMint = createSshPairing as Mock;
const mockPairing = getSshPairing as Mock;
const mockReconnect = reconnectComputer as Mock;

const MAC_KEY = 'SHA256:helperHostKey';

const STATUS: RemoteControlStatus = {
  supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: MAC_KEY, account: 'alex',
};

function row(id: string, name: string, over: Partial<Computer> = {}): Computer {
  return {
    computer_id: id, user_id: 'u1', kind: 'daytona', name, status: 'running', resource_tier: 'standard',
    is_always_on: false, is_primary: false, workspace_count: 0, root_dir: '/home/workspace', ssh: null, ...over,
  };
}

function sshRow(id: string, name: string, fingerprint: string, over: Partial<Computer> = {}): Computer {
  return row(id, name, {
    kind: 'ssh',
    root_dir: '/Users/alex/langalpha',
    ssh: { host: 'alex-mac', port: 22, username: 'alex', fingerprint, tunnel: true, tunnel_connected: true, helper: true },
    ...over,
  });
}

const PRIMARY = row('c-primary', 'Alpha Research', { is_primary: true, workspace_count: 2 });
const MAC = sshRow('c-mac', 'alex-mac', MAC_KEY);
const BOX = sshRow('c-box', 'build box', 'SHA256:otherMachine', {
  root_dir: '/home/dev/langalpha',
  ssh: {
    host: 'box.example.com', port: 22, username: 'dev', fingerprint: 'SHA256:otherMachine',
    tunnel: false, tunnel_connected: null, helper: false,
  },
});

let getStatus: Mock;
let enable: Mock;
let disable: Mock;

beforeEach(() => {
  vi.clearAllMocks();
  getStatus = vi.fn(async () => STATUS);
  disable = vi.fn(async () => ({ ...STATUS, service: 'off', helper: null, paired: false, fingerprint: null }));
  enable = vi.fn();
  bridge.getRemoteControl = getStatus;
  bridge.enableRemoteControl = enable;
  bridge.disableRemoteControl = disable;
  mockGetComputers.mockResolvedValue({ computers: [PRIMARY, MAC, BOX], total: 3 });
  mockProviders.mockResolvedValue({ providers: [{ kind: 'daytona', is_default: true, allowed: null }] });
});

function open() {
  return renderWithProviders(<ComputersDialog panel={{ view: 'list' }} onClose={() => {}} />);
}

async function rowNamed(name: string) {
  const rows = await screen.findAllByTestId('computer-card');
  const found = rows.find((r) => within(r).queryByText(name));
  if (!found) throw new Error(`No row named ${name}`);
  return found;
}

function deferred<T>() {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

/** Let a release queued after the removal run to its end. */
async function settle() {
  for (let i = 0; i < 10; i++) await act(async () => { await Promise.resolve(); });
}

async function removeRow(user: ReturnType<typeof userEvent.setup>, name: string) {
  await openDetails(user, name);
  await user.click(screen.getByRole('button', { name: 'Remove' }));
  return screen.findByRole('dialog', { name: `Remove ${name}?` });
}

describe('the rows', () => {
  it('mark the computer that is this Mac, and only that one', async () => {
    open();
    await waitFor(async () => expect(within(await rowNamed('alex-mac')).getByText('This Mac')).toBeInTheDocument());
    expect(within(await rowNamed('build box')).queryByText('This Mac')).not.toBeInTheDocument();
    expect(within(await rowNamed('Alpha Research')).queryByText('This Mac')).not.toBeInTheDocument();
    expect(screen.queryByText(/Remote control is paused/)).not.toBeInTheDocument();
  });

  it("describe this Mac by the account it works as, not by how it is reached", async () => {
    open();
    const mac = await rowNamed('alex-mac');
    await within(mac).findByText('This Mac');
    expect(within(mac).getByText('Remote control')).toBeInTheDocument();
    expect(within(mac).getByText('as alex')).toBeInTheDocument();
    expect(within(mac).queryByText('Your machine (SSH)')).not.toBeInTheDocument();
    expect(within(mac).queryByText('tunnel')).not.toBeInTheDocument();
    expect(within(mac).queryByText(/alex@/)).not.toBeInTheDocument();
    // Any other SSH computer is still described by its login and address.
    const box = await rowNamed('build box');
    expect(within(box).getByText('Your machine (SSH)')).toBeInTheDocument();
    expect(within(box).getByText(/dev@box\.example\.com/)).toBeInTheDocument();
  });

  it("say this Mac is not connected while its tunnel is down", async () => {
    const offline = sshRow('c-mac', 'alex-mac', MAC_KEY, {
      ssh: { host: 'alex-mac', port: 22, username: 'alex', fingerprint: MAC_KEY, tunnel: true, tunnel_connected: false, helper: true },
    });
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, offline, BOX], total: 3 });
    open();
    const mac = await rowNamed('alex-mac');
    expect(await within(mac).findByText('Not connected')).toBeInTheDocument();
    expect(within(mac).queryByText('tunnel offline')).not.toBeInTheDocument();
  });

  it("say on this Mac's row when remote control is paused", async () => {
    getStatus.mockResolvedValue({ ...STATUS, service: 'off', helper: null });
    open();
    const mac = await rowNamed('alex-mac');
    expect(await within(mac).findByText('Remote control is paused. Resume it from the LangAlpha menu.')).toBeInTheDocument();
    expect(within(await rowNamed('build box')).queryByText(/Remote control is paused/)).not.toBeInTheDocument();
  });

  // Being answered by a helper is the server's to say, so a device that
  // cannot ask the helper names the row the same and marks none as this Mac.
  it('describe a helper computer the same way where the helper cannot be asked, with no key to remove', async () => {
    delete bridge.getRemoteControl;
    delete bridge.enableRemoteControl;
    delete bridge.disableRemoteControl;
    mockDelete.mockResolvedValue({ key: null });
    const user = userEvent.setup();
    open();
    const mac = await rowNamed('alex-mac');
    expect(within(mac).getByText('Remote control')).toBeInTheDocument();
    expect(within(mac).getByText('as alex')).toBeInTheDocument();
    expect(within(mac).queryByText(/alex@/)).not.toBeInTheDocument();
    await settle();
    expect(screen.queryByText('This Mac')).not.toBeInTheDocument();

    const confirm = await removeRow(user, 'alex-mac');
    expect(confirm).not.toHaveTextContent('Remote control');
    expect(within(confirm).queryByRole('checkbox', { name: /authorized_keys/ })).not.toBeInTheDocument();
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(mockDelete).toHaveBeenCalledWith('c-mac', { removeKey: false, deleteWorkspaces: [], deleteFiles: false });
  });
});

describe('removing this Mac', () => {
  it('says remote control turns off too, and turns it off once the computer is gone', async () => {
    mockDelete.mockImplementation(async () => {
      mockGetComputers.mockResolvedValue({ computers: [PRIMARY, BOX], total: 2 });
      return { key: null };
    });
    const user = userEvent.setup();
    open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');

    const confirm = await removeRow(user, 'alex-mac');
    expect(confirm).toHaveTextContent('Remote control on this Mac turns off too.');
    // Its key is in the helper's own folder, which turning it off deletes.
    expect(within(confirm).queryByRole('checkbox', { name: /authorized_keys/ })).not.toBeInTheDocument();
    expect(disable).not.toHaveBeenCalled();

    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(mockDelete).toHaveBeenCalledWith('c-mac', { removeKey: false, deleteWorkspaces: [], deleteFiles: false });
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  it('keeps remote control on while another computer still logs in to this Mac', async () => {
    const second = sshRow('c-mac-2', 'alex-mac research', MAC_KEY, { root_dir: '/Users/alex/research' });
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, MAC, second], total: 3 });
    mockDelete.mockImplementation(async () => {
      mockGetComputers.mockResolvedValue({ computers: [PRIMARY, second], total: 2 });
      return { key: null };
    });
    const user = userEvent.setup();
    open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');

    const confirm = await removeRow(user, 'alex-mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Remove alex-mac?' })).not.toBeInTheDocument());
    await waitFor(() => expect(getStatus.mock.calls.length).toBeGreaterThan(1));
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('keeps remote control on when the removal is refused', async () => {
    mockDelete.mockRejectedValue(Object.assign(new Error('Request failed with status code 409'), {
      response: { status: 409, data: { detail: { code: 'busy', message: 'busy' } }, headers: {} },
    }));
    const user = userEvent.setup();
    open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');

    const confirm = await removeRow(user, 'alex-mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(await within(confirm).findByRole('alert')).toHaveTextContent('This computer is busy. Try again in a moment.');
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('leaves remote control alone when another machine is removed', async () => {
    mockDelete.mockResolvedValue({ key: null });
    const user = userEvent.setup();
    open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');

    const confirm = await removeRow(user, 'build box');
    expect(confirm).not.toHaveTextContent('Remote control');
    expect(within(confirm).getByRole('checkbox', { name: 'Remove our key from ~/.ssh/authorized_keys' })).toBeChecked();
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(mockDelete).toHaveBeenCalledWith('c-box', { removeKey: true, deleteWorkspaces: [], deleteFiles: false });
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Remove build box?' })).not.toBeInTheDocument());
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});

describe('a helper the deployment refuses', () => {
  it('is turned off once no computer of this account is this Mac', async () => {
    getStatus.mockResolvedValue({ ...STATUS, helper: 'refused' });
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, BOX], total: 2 });
    open();
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  it("is left while this Mac's row is still here", async () => {
    getStatus.mockResolvedValue({ ...STATUS, helper: 'refused' });
    open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});

describe('outliving the dialog', () => {
  it('turns remote control off when this Mac is removed, though the dialog closed first', async () => {
    const answer = deferred<{ key: null }>();
    mockDelete.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    const { unmount } = open();
    await within(await rowNamed('alex-mac')).findByText('This Mac');
    const confirm = await removeRow(user, 'alex-mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(mockDelete).toHaveBeenCalled();

    unmount();
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, BOX], total: 2 });
    await act(async () => answer.resolve({ key: null }));
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });
});

describe('adding a computer', () => {
  const UNPAIRED: RemoteControlStatus = {
    supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: 'alex',
  };

  const probe = (over: Partial<SshProbeResult>): SshProbeResult => ({
    host_key: 'ssh-ed25519 AAAAhelper', fingerprint: MAC_KEY, os: 'Darwin', arch: 'arm64', distro: 'macOS 15.1',
    home: '/Users/alex', shell: '/bin/zsh', is_root: false, has_sudo: true, disk_free_bytes: null, supported: true,
    problems: [], suggested_root_dir: '/Users/alex/langalpha', hostname: 'alex-mac', helper: true, ...over,
  });

  const ready = (target: NonNullable<SshPairingStatus['target']>, found: SshProbeResult): SshPairingStatus => ({
    status: 'ready', expires_at: '2026-10-23T12:15:00Z', error: null, target, probe: found,
    machine: { username: target.username, hostname: found.hostname ?? '', os: found.os, addresses: [] },
  });

  const MAC_READY = ready({ host: 'alex-mac', port: 22, username: 'alex', tunnel_id: 'tun-1' }, probe({}));
  const BOX_READY = ready(
    { host: 'box.example.com', port: 22, username: 'dev' },
    probe({
      host_key: 'ssh-ed25519 AAAAbox', fingerprint: 'SHA256:otherMachine', os: 'linux', arch: 'x86_64',
      distro: 'Ubuntu 24.04', home: '/home/dev', shell: '/bin/bash', suggested_root_dir: '/home/dev/langalpha',
      hostname: 'box', helper: false,
    }),
  );
  const WAITING: SshPairingStatus = {
    status: 'waiting', expires_at: '2026-10-23T12:15:00Z', machine: null, target: null, probe: null, error: null,
  };

  beforeEach(() => {
    getStatus.mockResolvedValue(UNPAIRED);
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY], total: 1 });
    mockProviders.mockResolvedValue({
      providers: [{ kind: 'daytona', is_default: true, allowed: null }, { kind: 'ssh', is_default: false, allowed: null }],
      tunnels: true,
    });
    mockMint.mockImplementation(async () => {
      const token = `tok${mockMint.mock.calls.length}`;
      const script_url = `https://api.example.com/api/v1/computers/ssh/pairings/${token}/script`;
      return { token, script_url, command: `curl ${script_url} | sh`, expires_at: '2026-10-23T12:15:00Z', local_only: false };
    });
    mockPairing.mockResolvedValue(WAITING);
  });

  async function turnOnThisMac(user: ReturnType<typeof userEvent.setup>) {
    const view = open();
    await user.click(await screen.findByRole('button', { name: 'Add a computer' }));
    await user.click(await screen.findByRole('radio', { name: 'This Mac' }));
    await user.click(screen.getByRole('checkbox', { name: /^I understand LangAlpha's agent/ }));
    await user.click(screen.getByRole('button', { name: 'Turn on remote control' }));
    await waitFor(() => expect(enable).toHaveBeenCalledTimes(1));
    return view;
  }

  // The form lays its provider cards out by this Mac's card, so one opened
  // ahead of the shell's answer would enter on three cards and reflow to four.
  it.each([
    ['opened onto', () => renderWithProviders(<ComputersDialog panel={{ view: 'create' }} onClose={() => {}} />)],
    ['added from the rail', async () => {
      const user = userEvent.setup();
      open();
      await user.click(await screen.findByRole('button', { name: 'Add a computer' }));
    }],
  ])('opens the form %s only once the desktop app has said whether this Mac is one', async (_, show) => {
    // The form's code is in hand, as the panel fetches it as it opens.
    await import('../CreateComputerDialog');
    const answer = deferred<RemoteControlStatus>();
    getStatus.mockReturnValue(answer.promise);
    await show();
    // Everything else the form waits on has answered, and React Query has told
    // its readers, which it does on a timer.
    await waitFor(() => expect(mockProviders).toHaveBeenCalled());
    await act(() => new Promise((r) => setTimeout(r, 20)));
    expect(screen.queryByRole('dialog', { name: 'New computer' })).toBeNull();

    await act(async () => answer.resolve(UNPAIRED));
    const form = await screen.findByRole('dialog', { name: 'New computer' });
    expect(within(form).getByRole('radio', { name: 'This Mac' })).toBeInTheDocument();
  });

  // A channel that threw on an earlier open left null behind, which is no answer.
  it('waits for the desktop app again when all it kept is a read that threw', async () => {
    await import('../CreateComputerDialog');
    const queryClient = createTestQueryClient();
    queryClient.setQueryData(queryKeys.desktop.remoteControl(), null);
    const answer = deferred<RemoteControlStatus>();
    getStatus.mockReturnValue(answer.promise);
    renderWithProviders(<ComputersDialog panel={{ view: 'create' }} onClose={() => {}} />, { queryClient });
    await waitFor(() => expect(getStatus).toHaveBeenCalled());
    await act(() => new Promise((r) => setTimeout(r, 20)));
    expect(screen.queryByRole('dialog', { name: 'New computer' })).toBeNull();

    await act(async () => answer.resolve(UNPAIRED));
    const form = await screen.findByRole('dialog', { name: 'New computer' });
    expect(within(form).getByRole('radio', { name: 'This Mac' })).toBeInTheDocument();
  });

  // The form lays out on the read the panel waited for, so an answer that
  // changes under it waits for the next poll rather than its entrance.
  it('opens the form on the answer the panel read, not a read of its own', async () => {
    getStatus.mockResolvedValueOnce(null);
    renderWithProviders(<ComputersDialog panel={{ view: 'create' }} onClose={() => {}} />);
    const form = await screen.findByRole('dialog', { name: 'New computer' });
    await act(() => new Promise((r) => setTimeout(r, 20)));
    expect(getStatus).toHaveBeenCalledTimes(1);
    expect(within(form).queryByRole('radio', { name: 'This Mac' })).toBeNull();
  });

  it('opens the form though the list that says whether this Mac is added was refused', async () => {
    mockGetComputers.mockRejectedValue(new Error('Request failed with status code 503'));
    renderWithProviders(<ComputersDialog panel={{ view: 'create' }} onClose={() => {}} />);
    const form = await screen.findByRole('dialog', { name: 'New computer' });
    expect(within(form).queryByRole('radio', { name: 'This Mac' })).toBeNull();
  });

  // Turning remote control on and off take turns on this Mac; adding any
  // other machine is no part of that.
  it('adds another machine over SSH while remote control is still turning on', async () => {
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    // The first pairing is the one handed to the helper.
    mockPairing.mockImplementation(async (token: string) => (token === 'tok2' ? BOX_READY : WAITING));
    mockCreate.mockResolvedValue(BOX);
    const user = userEvent.setup();
    await turnOnThisMac(user);

    await user.click(screen.getByRole('radio', { name: /^Your machine \(SSH\)/ }));
    await screen.findByTestId('ssh-ready');
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    await waitFor(() => expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'ssh', ssh: expect.objectContaining({ host: 'box.example.com' }) }),
    ));
    // Settled, since the bridge takes one call at a time across tests too.
    await act(async () => answer.resolve({ ok: false, error: 'cancelled', status: UNPAIRED }));
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('keeps remote control on when the dialog closes before the Mac it added is created', async () => {
    enable.mockImplementation(async (): Promise<RemoteControlResult> => {
      getStatus.mockResolvedValue(STATUS);
      return { ok: true, status: STATUS };
    });
    mockPairing.mockResolvedValue(MAC_READY);
    const created = deferred<Computer>();
    mockCreate.mockImplementation(() => created.promise);
    const user = userEvent.setup();
    const { unmount } = await turnOnThisMac(user);
    await screen.findByTestId('ssh-ready');
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    await waitFor(() => expect(mockCreate).toHaveBeenCalled());

    unmount();
    mockGetComputers.mockClear();
    await settle();
    // Not read until the new row can be on it.
    expect(mockGetComputers).not.toHaveBeenCalled();
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, MAC], total: 2 });
    await act(async () => created.resolve(MAC));
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});

describe('reconnecting this Mac', () => {
  const OLD_KEY = 'SHA256:helperKeyBeforeReset';
  const UNPAIRED: RemoteControlStatus = {
    supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: 'alex',
  };
  // The row before: its helper was reset, so its pinned key is no longer the helper's.
  const BROKEN = sshRow('c-mac', 'alex-mac', OLD_KEY, {
    status: 'stopped',
    provider_ref: 'alex-mac',
    start_failure: {
      code: 'not_connected', message: 'alex-mac is not connected.', owner: null, remedy: 'reconnect',
      at: '2026-10-25T16:40:00Z',
    },
  });
  // The row once the server took the new connection.
  const RECONNECTED = sshRow('c-mac', 'alex-mac', MAC_KEY, { status: 'starting', provider_ref: 'alex-mac' });
  const READY: SshPairingStatus = {
    status: 'ready', expires_at: '2026-10-25T17:00:00Z', error: null,
    target: { host: 'alex-mac', port: 22, username: 'alex', tunnel_id: 'tun-2' },
    probe: {
      host_key: 'ssh-ed25519 AAAAhelper', fingerprint: MAC_KEY, os: 'Darwin', arch: 'arm64', distro: 'macOS 15.1',
      home: '/Users/alex', shell: '/bin/zsh', is_root: false, has_sudo: true, disk_free_bytes: null, supported: true,
      problems: [], suggested_root_dir: '/Users/alex/langalpha', hostname: 'alex-mac', helper: true,
    },
    machine: { username: 'alex', hostname: 'alex-mac', os: 'Darwin', addresses: [] },
  };

  beforeEach(() => {
    getStatus.mockResolvedValue(UNPAIRED);
    enable.mockImplementation(async (): Promise<RemoteControlResult> => {
      getStatus.mockResolvedValue(STATUS);
      return { ok: true, status: STATUS };
    });
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, BROKEN], total: 2 });
    mockMint.mockResolvedValue({
      token: 'tok-mac', script_url: 'https://api.example.com/api/v1/computers/ssh/pairings/tok-mac/script',
      command: 'curl', expires_at: '2026-10-25T17:00:00Z', local_only: false,
    });
    mockPairing.mockResolvedValue(READY);
  });

  async function pairAgain(user: ReturnType<typeof userEvent.setup>) {
    const view = open();
    await user.click(within(await rowNamed('alex-mac')).getByRole('button', { name: 'Reconnect…' }));
    const dialog = await screen.findByRole('dialog', { name: 'Reconnect alex-mac' });
    await user.click(within(dialog).getByRole('checkbox', { name: /^I understand LangAlpha's agent/ }));
    await user.click(within(dialog).getByRole('button', { name: 'Turn on remote control' }));
    await within(dialog).findByTestId('ssh-ready');
    expect(mockMint).toHaveBeenCalledWith({ computerId: 'c-mac' });
    expect(enable).toHaveBeenCalledWith('https://api.example.com/api/v1/computers/ssh/pairings/tok-mac/script');
    return { view, dialog };
  }

  it("re-pairs the helper to this computer and keeps it on once the row names the helper's new key", async () => {
    mockReconnect.mockImplementation(async () => {
      mockGetComputers.mockResolvedValue({ computers: [PRIMARY, RECONNECTED], total: 2 });
      return { computer_id: 'c-mac', status: 'starting' };
    });
    const user = userEvent.setup();
    const { dialog } = await pairAgain(user);
    expect(dialog).toHaveTextContent('Workspaces stay in /Users/alex/langalpha.');

    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    expect(mockReconnect).toHaveBeenCalledWith('c-mac', {
      host: 'alex-mac', port: 22, username: 'alex', tunnel_id: 'tun-2', host_key: 'ssh-ed25519 AAAAhelper',
    });
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Reconnect alex-mac' })).not.toBeInTheDocument());
    // The row is this Mac again at once, and closing on it left remote control on.
    await within(await rowNamed('alex-mac')).findByText('This Mac');
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('waits for a reconnect still in flight before deciding the helper is unused', async () => {
    const answer = deferred<{ computer_id: string; status: string }>();
    mockReconnect.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    const { view, dialog } = await pairAgain(user);
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    expect(mockReconnect).toHaveBeenCalled();

    // The panel goes while the request runs; the server still has the old key.
    view.unmount();
    await settle();
    expect(disable).not.toHaveBeenCalled();
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, RECONNECTED], total: 2 });
    await act(async () => answer.resolve({ computer_id: 'c-mac', status: 'starting' }));
    await waitFor(() => expect(getStatus.mock.calls.length).toBeGreaterThan(2));
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('turns the helper off again when the dialog is left without reconnecting', async () => {
    const user = userEvent.setup();
    const { dialog } = await pairAgain(user);
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('is not offered on a Mac that is already another computer', async () => {
    getStatus.mockResolvedValue(STATUS);
    mockGetComputers.mockResolvedValue({ computers: [PRIMARY, BROKEN, sshRow('c-other', 'studio', MAC_KEY)], total: 3 });
    const user = userEvent.setup();
    open();
    await within(await rowNamed('studio')).findByText('This Mac');
    await user.click(within(await rowNamed('alex-mac')).getByRole('button', { name: 'Reconnect…' }));
    const dialog = await screen.findByRole('dialog', { name: 'Reconnect alex-mac' });
    expect(within(dialog).getByTestId('reconnect-other-mac')).toHaveTextContent('This Mac is already studio.');
    expect(within(dialog).queryByRole('button', { name: 'Turn on remote control' })).not.toBeInTheDocument();
    expect(mockMint).not.toHaveBeenCalled();
  });
});
