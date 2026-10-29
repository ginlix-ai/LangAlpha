/**
 * A start that failed, as the Computers dialog keeps it: the card and the
 * details say why next to Stopped, with the fix the server named beside
 * Start, and the two fixes an SSH computer has: Reinstall (take its folder
 * back, or move to a new one) and Reconnect (pair its machine again).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import type { Computer, ComputerStartFailure, SshPairingStatus, SshProbeResult } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'oss',
  isPlatformMode: false,
  APP_ENTRY_PATH: '/',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import {
  createSshPairing,
  getComputerProviders,
  getComputers,
  getComputerStorage,
  getSshPairing,
  reconnectComputer,
  reinstallComputer,
} from '../../utils/api';
import { openDetails, openDialog as open, type User } from './computersDialogHarness';

const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;
const mockStorage = getComputerStorage as Mock;
const mockReinstall = reinstallComputer as Mock;
const mockReconnect = reconnectComputer as Mock;
const mockMint = createSshPairing as Mock;
const mockPairing = getSshPairing as Mock;

const ID = 'c-box';
const ROOT = '/home/dev/langalpha';
const OLD_KEY = 'SHA256:oldHostKey';

const MARKED: ComputerStartFailure = {
  code: 'folder_marked',
  message: "/home/dev/langalpha on box is already another computer's folder.",
  owner: 'c0ffee00',
  remedy: 'reinstall',
  at: '2026-10-25T16:40:00Z',
};
const NOT_CONNECTED: ComputerStartFailure = {
  code: 'auth_failed',
  message: 'The machine did not accept our key for dev.',
  owner: null,
  remedy: 'reconnect',
  at: '2026-10-25T16:40:00Z',
};

function box(over: Partial<Computer> = {}): Computer {
  return {
    computer_id: ID, user_id: 'u1', kind: 'ssh', name: 'build box', status: 'stopped', resource_tier: 'standard',
    is_always_on: false, is_primary: false, workspace_count: 1, root_dir: ROOT, files_backup: true, provider_ref: 'box',
    ssh: {
      host: 'box.example.com', port: 22, username: 'dev', fingerprint: OLD_KEY,
      tunnel: false, tunnel_connected: null, helper: false,
    },
    ...over,
  };
}

const PRIMARY: Computer = {
  computer_id: 'c-primary', user_id: 'u1', kind: 'daytona', name: 'Alpha Research', status: 'running',
  resource_tier: 'standard', is_always_on: false, is_primary: true, workspace_count: 2,
  root_dir: '/home/workspace', ssh: null,
};

function listWith(row: Computer) {
  mockGetComputers.mockResolvedValue({ computers: [row, PRIMARY], total: 2 });
}

/** The server's answer to an accepted restart: the start is claimed, and the failure cleared. */
function acceptRestart() {
  listWith(box({ status: 'starting', start_failure: null }));
  return { computer_id: ID, status: 'starting' };
}

function refusal(status: number, detail: Record<string, unknown>) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail }, headers: {} },
  });
}

async function boxCard() {
  return screen.findByRole('article', { name: 'build box' });
}

beforeEach(() => {
  vi.clearAllMocks();
  mockProviders.mockResolvedValue({ providers: [{ kind: 'ssh', is_default: true, allowed: null }], tunnels: true });
  mockStorage.mockResolvedValue({ live: false, disk: null, workspaces: [], other_bytes: 0 });
});

describe('a failed start on the card', () => {
  it('reads Stopped with why, and leads with Reinstall beside Start', async () => {
    listWith(box({ start_failure: MARKED }));
    open();
    const card = await boxCard();
    expect(within(card).getByText('Stopped')).toBeInTheDocument();
    expect(within(card).queryByText('Error')).not.toBeInTheDocument();
    const note = within(card).getByTestId('computer-start-failure');
    expect(note).toHaveTextContent(`Couldn't start. ${MARKED.message}`);
    expect(within(card).getByRole('button', { name: 'Reinstall…' })).toBeInTheDocument();
    expect(within(card).getByRole('button', { name: 'Start' })).toBeEnabled();
    expect(within(card).queryByRole('button', { name: 'Reconnect…' })).not.toBeInTheDocument();
  });

  it('offers Reconnect for a machine it could not log in to', async () => {
    listWith(box({ start_failure: NOT_CONNECTED }));
    open();
    const card = await boxCard();
    expect(within(card).getByTestId('computer-start-failure')).toHaveTextContent(NOT_CONNECTED.message);
    expect(within(card).getByRole('button', { name: 'Reconnect…' })).toBeInTheDocument();
    expect(within(card).getByRole('button', { name: 'Start' })).toBeEnabled();
  });

  it('offers only Start where trying again is the fix', async () => {
    listWith(box({ start_failure: { ...NOT_CONNECTED, code: 'timeout', message: 'The connection timed out.', remedy: 'retry' } }));
    open();
    const card = await boxCard();
    expect(within(card).getByTestId('computer-start-failure')).toHaveTextContent('The connection timed out.');
    expect(within(card).queryByRole('button', { name: 'Reinstall…' })).not.toBeInTheDocument();
    expect(within(card).queryByRole('button', { name: 'Reconnect…' })).not.toBeInTheDocument();
    expect(within(card).getByRole('button', { name: 'Start' })).toBeEnabled();
  });

  it('says why a computer in error failed, but offers no repair the routes would refuse', async () => {
    listWith(box({ status: 'error', start_failure: MARKED }));
    open();
    const card = await boxCard();
    expect(within(card).getByTestId('computer-start-failure')).toHaveTextContent(MARKED.message);
    expect(within(card).queryByRole('button', { name: 'Reinstall…' })).not.toBeInTheDocument();
    expect(within(card).queryByRole('button', { name: 'Reconnect…' })).not.toBeInTheDocument();
  });

  it('drops the failure while a new start is under way', async () => {
    listWith(box({ status: 'starting', provider_ref: 'box', start_failure: MARKED }));
    open();
    const card = await boxCard();
    expect(within(card).queryByTestId('computer-start-failure')).not.toBeInTheDocument();
    expect(within(card).queryByRole('button', { name: 'Reinstall…' })).not.toBeInTheDocument();
  });

  it('says it in full in the details too, with the same fix', async () => {
    listWith(box({ start_failure: MARKED }));
    const user = userEvent.setup();
    open();
    await openDetails(user, 'build box');
    expect(await screen.findByTestId('computer-start-failure')).toHaveTextContent(MARKED.message);
    expect(screen.getByRole('button', { name: 'Reinstall…' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start' })).toBeInTheDocument();
  });
});

async function openReinstall(user: User) {
  const card = await boxCard();
  await user.click(within(card).getByRole('button', { name: 'Reinstall…' }));
  return screen.findByRole('dialog', { name: 'Reinstall build box' });
}

describe('Reinstall', () => {
  beforeEach(() => listWith(box({ start_failure: MARKED })));

  it('takes the folder back, saying another account may hold it, and starts', async () => {
    mockReinstall.mockImplementation(async () => acceptRestart());
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    const takeBack = within(dialog).getByRole('radio', { name: 'Take the folder back' });
    expect(takeBack).toBeChecked();
    expect(dialog).toHaveTextContent('possibly one from another LangAlpha account or server');
    expect(dialog).toHaveTextContent(`Keep using ${ROOT}.`);

    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    // Only from the computer it was shown holding the folder.
    expect(mockReinstall).toHaveBeenCalledWith(ID, { folder: 'take_back', take_over: 'c0ffee00' });
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Reinstall build box' })).not.toBeInTheDocument());
    // The 202 arms the status stream, and the failure it fixed is gone.
    const card = await boxCard();
    expect(within(card).getByText('Starting')).toBeInTheDocument();
    expect(within(card).queryByTestId('computer-start-failure')).not.toBeInTheDocument();
  });

  it('moves to a new folder beside the old one, saying the files come back from backup', async () => {
    mockReinstall.mockResolvedValue({ computer_id: ID, status: 'starting' });
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('radio', { name: 'Use a new folder' }));
    const folder = within(dialog).getByRole('textbox', { name: 'New folder' });
    expect(folder).toHaveValue(`${ROOT}-2`);
    expect(dialog).toHaveTextContent('Workspace files come back from their backup.');

    await user.clear(folder);
    await user.type(folder, '~/langalpha-work');
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    expect(mockReinstall).toHaveBeenCalledWith(ID, { folder: 'new', root_dir: '~/langalpha-work' });
  });

  it('says workspace files stay in the old folder when backup is off', async () => {
    listWith(box({ start_failure: MARKED, files_backup: false }));
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('radio', { name: 'Use a new folder' }));
    expect(dialog).toHaveTextContent(`Backup is off for this computer, so workspace files stay in ${ROOT}.`);
  });

  it("shows the server's sentence when the folder is another of the user's computers", async () => {
    const message = 'The folder is Research box, one of your computers. Remove it to free the folder.';
    mockReinstall.mockRejectedValue(refusal(409, { code: 'folder_owned', message }));
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    expect(await within(dialog).findByRole('alert')).toHaveTextContent(message);
    expect(within(dialog).queryByRole('button', { name: 'Take this folder' })).not.toBeInTheDocument();
  });

  it('offers to take a new folder another computer holds, or another path', async () => {
    mockReinstall
      .mockRejectedValueOnce(refusal(409, { code: 'folder_marked', message: 'marked', owner: 'feed0001' }))
      .mockResolvedValueOnce({ computer_id: ID, status: 'starting' });
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('radio', { name: 'Use a new folder' }));
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent("That folder is another computer's");
    expect(alert).toHaveTextContent('Or enter another folder.');

    await user.click(within(alert).getByRole('button', { name: 'Take this folder' }));
    expect(mockReinstall).toHaveBeenLastCalledWith(ID, { folder: 'new', root_dir: `${ROOT}-2`, take_over: 'feed0001' });
  });

  it('no longer offers to take the folder once another path is typed', async () => {
    mockReinstall.mockRejectedValue(refusal(409, { code: 'folder_marked', message: 'marked', owner: 'feed0001' }));
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('radio', { name: 'Use a new folder' }));
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    await within(dialog).findByRole('button', { name: 'Take this folder' });
    await user.type(within(dialog).getByRole('textbox', { name: 'New folder' }), 'x');
    expect(within(dialog).queryByRole('button', { name: 'Take this folder' })).not.toBeInTheDocument();
  });

  it('asks again when another computer marked the folder since, and takes it from that one', async () => {
    mockReinstall
      .mockRejectedValueOnce(refusal(409, { code: 'folder_marked', message: 'marked', owner: 'feed0001' }))
      .mockResolvedValueOnce({ computer_id: ID, status: 'starting' });
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent('Taking it back stops that computer working there.');

    await user.click(within(alert).getByRole('button', { name: 'Take it back from that computer' }));
    expect(mockReinstall).toHaveBeenLastCalledWith(ID, { folder: 'take_back', take_over: 'feed0001' });
  });

  it('offers only a new folder when the folder cannot be used at all', async () => {
    listWith(
      box({ start_failure: { ...MARKED, code: 'root_dir', owner: null, message: `${ROOT} holds files.` } }),
    );
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    expect(within(dialog).queryByRole('radio', { name: 'Take the folder back' })).not.toBeInTheDocument();
    expect(within(dialog).getByRole('radio', { name: 'Use a new folder' })).toBeChecked();
  });

  it("shows why a new folder can't be used, in the server's words", async () => {
    const message = '/home/dev/langalpha-2 holds files. Pick an empty folder.';
    mockReinstall.mockRejectedValue(refusal(422, { code: 'root_dir', message }));
    const user = userEvent.setup();
    open();
    const dialog = await openReinstall(user);
    await user.click(within(dialog).getByRole('radio', { name: 'Use a new folder' }));
    await user.click(within(dialog).getByRole('button', { name: 'Reinstall' }));
    expect(await within(dialog).findByRole('alert')).toHaveTextContent(message);
  });
});

describe('Reconnect over SSH', () => {
  const NEW_KEY = 'SHA256:newHostKey';
  const probe: SshProbeResult = {
    host_key: 'ssh-ed25519 AAAAnew', fingerprint: NEW_KEY, os: 'Linux', arch: 'x86_64', distro: 'Ubuntu 24.04',
    home: '/home/dev', shell: '/bin/bash', is_root: false, has_sudo: true, disk_free_bytes: null, supported: true,
    problems: [], suggested_root_dir: ROOT, hostname: 'box', helper: false,
  };
  const READY: SshPairingStatus = {
    status: 'ready', expires_at: '2026-10-25T17:00:00Z', error: null,
    target: { host: 'box', port: 22, username: 'dev', tunnel_id: 'tun-9' },
    probe,
    machine: { username: 'dev', hostname: 'box', os: 'Linux', addresses: [] },
  };

  beforeEach(() => {
    listWith(box({ start_failure: NOT_CONNECTED }));
    mockMint.mockResolvedValue({
      token: 'tok-re', script_url: 'https://api.example.com/s', command: 'curl https://api.example.com/s | sh',
      expires_at: '2026-10-25T17:00:00Z', local_only: false,
    });
    mockPairing.mockResolvedValue(READY);
  });

  async function openReconnect(user: User) {
    const card = await boxCard();
    await user.click(within(card).getByRole('button', { name: 'Reconnect…' }));
    return screen.findByRole('dialog', { name: 'Reconnect build box' });
  }

  it("pairs with a command minted for this computer and keeps its folder", async () => {
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    expect(mockMint).toHaveBeenCalledWith({ computerId: ID });
    expect(dialog).toHaveTextContent(`Workspaces stay in ${ROOT}.`);
    // What only a new machine chooses is not offered again.
    expect(within(dialog).queryByRole('button', { name: 'Use a different machine' })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole('checkbox', { name: 'Back up workspace files' })).not.toBeInTheDocument();
    expect(within(dialog).queryByTestId('ssh-preview-notice')).not.toBeInTheDocument();
  });

  it('warns before moving the computer to a machine that holds nothing of it', async () => {
    mockReconnect
      .mockRejectedValueOnce(refusal(409, { code: 'different_machine', message: 'box holds nothing of this computer.' }))
      .mockImplementationOnce(async () => acceptRestart());
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent("This isn't the machine build box was on");
    expect(alert).toHaveTextContent('anything never backed up stays on the old machine');
    expect(within(dialog).queryByRole('button', { name: 'Reconnect' })).not.toBeInTheDocument();

    await user.click(within(dialog).getByRole('button', { name: 'Move to this machine' }));
    expect(mockReconnect).toHaveBeenLastCalledWith(ID, {
      host: 'box', port: 22, username: 'dev', tunnel_id: 'tun-9', host_key: 'ssh-ed25519 AAAAnew', move: true,
    });
  });

  it('says the files stay on the old machine when backup is off', async () => {
    listWith(box({ start_failure: NOT_CONNECTED, files_backup: false }));
    mockReconnect.mockRejectedValueOnce(refusal(409, { code: 'different_machine', message: 'box holds nothing of this computer.' }));
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent('Backup is off for this computer, so workspace files stay on the old machine');
    expect(alert).toHaveTextContent('a workspace backed up before then offers to restore that backup');
    expect(alert).not.toHaveTextContent('Workspace files come back from their backup');
    expect(within(dialog).getByRole('button', { name: 'Move to this machine' })).toBeInTheDocument();
  });

  it('adds the machine as a computer of its own instead of moving this one there', async () => {
    mockReconnect.mockRejectedValueOnce(refusal(409, { code: 'different_machine', message: 'box holds nothing of this computer.' }));
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent('To keep both, add this machine as a computer of its own, and reconnect build box to the machine it was on.');

    await user.click(within(alert).getByRole('button', { name: 'Add this machine as a new computer' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Reconnect build box' })).not.toBeInTheDocument());
    const create = await screen.findByRole('dialog', { name: 'New computer' });
    expect(await within(create).findByTestId('ssh-setup')).toBeInTheDocument();
    expect(mockReconnect).toHaveBeenCalledTimes(1);
  });

  it('sends where the machine was reached, and starts it', async () => {
    mockReconnect.mockImplementation(async () => acceptRestart());
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    expect(mockReconnect).toHaveBeenCalledWith(ID, {
      host: 'box', port: 22, username: 'dev', tunnel_id: 'tun-9', host_key: 'ssh-ed25519 AAAAnew',
    });
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Reconnect build box' })).not.toBeInTheDocument());
    const card = await boxCard();
    expect(within(card).getByText('Starting')).toBeInTheDocument();
    expect(within(card).queryByTestId('computer-start-failure')).not.toBeInTheDocument();
  });

  it('offers to take the folder back when the machine answered but its folder is held', async () => {
    mockReconnect
      .mockRejectedValueOnce(refusal(409, { code: 'folder_marked', message: 'marked', owner: 'feed0002' }))
      .mockResolvedValueOnce({ computer_id: ID, status: 'starting' });
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    const alert = await within(dialog).findByRole('alert');
    expect(alert).toHaveTextContent("That folder is another computer's");
    await user.click(within(alert).getByRole('button', { name: 'Take the folder back' }));
    expect(mockReconnect).toHaveBeenLastCalledWith(ID, expect.objectContaining({ host: 'box', take_over: 'feed0002' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Reconnect build box' })).not.toBeInTheDocument());
  });

  it('keeps a confirmed move when the folder there is held and taken back', async () => {
    mockReconnect
      .mockRejectedValueOnce(refusal(409, { code: 'different_machine', message: 'box holds nothing of this computer.' }))
      .mockRejectedValueOnce(refusal(409, { code: 'folder_marked', message: 'marked', owner: 'feed0002' }))
      .mockResolvedValueOnce({ computer_id: ID, status: 'starting' });
    const user = userEvent.setup();
    open();
    const dialog = await openReconnect(user);
    await within(dialog).findByTestId('ssh-ready');
    await user.click(within(dialog).getByRole('button', { name: 'Reconnect' }));
    await user.click(await within(dialog).findByRole('button', { name: 'Move to this machine' }));
    const alert = await within(dialog).findByRole('alert');
    await user.click(await within(alert).findByRole('button', { name: 'Take the folder back' }));

    // The move was confirmed once: taking the folder there does not ask again.
    expect(mockReconnect).toHaveBeenLastCalledWith(ID, expect.objectContaining({ move: true, take_over: 'feed0002' }));
  });
});

describe('Reconnect of a Mac from a browser', () => {
  it('says to reconnect it from the app on that Mac', async () => {
    listWith(box({
      name: 'alex-mac',
      root_dir: '/Users/alex/langalpha',
      start_failure: { ...NOT_CONNECTED, code: 'not_connected', message: 'alex-mac is not connected.' },
      ssh: { host: 'alex-mac', port: 22, username: 'alex', fingerprint: OLD_KEY, tunnel: true, tunnel_connected: false, helper: true },
    }));
    const user = userEvent.setup();
    open();
    const card = await screen.findByRole('article', { name: 'alex-mac' });
    await user.click(within(card).getByRole('button', { name: 'Reconnect…' }));
    const dialog = await screen.findByRole('dialog', { name: 'Reconnect alex-mac' });
    expect(within(dialog).getByTestId('reconnect-elsewhere')).toHaveTextContent(
      'Open LangAlpha on that Mac and choose Reconnect there.',
    );
    expect(within(dialog).queryByRole('button', { name: 'Reconnect' })).not.toBeInTheDocument();
    expect(mockMint).not.toHaveBeenCalled();
  });
});
