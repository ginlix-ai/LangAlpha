/**
 * Adding a machine over SSH, through the New computer dialog: the pairing
 * command by default, the address by hand as the way around it, and the one
 * summary both end on.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { ComputerProviderOption, SshPairingStatus, SshProbeResult } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'oss',
  isPlatformMode: false,
  APP_ENTRY_PATH: '/',
}));

vi.mock('../../utils/api', () => ({
  getComputerProviders: vi.fn(),
  // Read only where this Mac is offered, which it never is here.
  getComputers: vi.fn(),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  createSshPairing: vi.fn(),
  getSshPairing: vi.fn(),
  connectSshPairing: vi.fn(),
  probeSsh: vi.fn(),
  getSshKey: vi.fn(),
}));

import {
  connectSshPairing,
  createSshPairing,
  getComputerProviders,
  getComputers,
  getSshKey,
  getSshPairing,
  probeSsh,
} from '../../utils/api';
import CreateComputerDialog from '../CreateComputerDialog';

const mockProviders = getComputerProviders as Mock;
const mockMint = createSshPairing as Mock;
const mockStatus = getSshPairing as Mock;
const mockConnect = connectSshPairing as Mock;
const mockProbe = probeSsh as Mock;
const mockKey = getSshKey as Mock;
const mockComputers = getComputers as Mock;

const DAYTONA_AND_SSH: ComputerProviderOption[] = [
  { kind: 'daytona', is_default: true, allowed: null, default_name: 'Cloud computer' },
  { kind: 'ssh', is_default: false, allowed: null, default_name: 'SSH computer' },
];

const pairing = (token: string) => ({
  token,
  command: `echo 'url = "https://app.example.com/api/v1/computers/ssh/pairings/${token}/script"' | curl -fsSL -K - | sh`,
  script_url: `https://app.example.com/api/v1/computers/ssh/pairings/${token}/script`,
  expires_at: '2026-10-18T12:15:00Z',
});

const probe = (over: Partial<SshProbeResult> = {}): SshProbeResult => ({
  host_key: 'ssh-ed25519 AAAAC3Nz', fingerprint: 'SHA256:abc123', os: 'linux', arch: 'x86_64',
  distro: 'Ubuntu 24.04', home: '/home/me', shell: '/bin/bash', is_root: false, has_sudo: true,
  disk_free_bytes: null, supported: true, problems: [], suggested_root_dir: '/home/me/langalpha',
  hostname: 'devbox', helper: false, ...over,
});

const MACHINE = { username: 'me', hostname: 'devbox', os: 'Linux', addresses: ['198.51.100.7', '10.0.0.4'] };

const status = (over: Partial<SshPairingStatus>): SshPairingStatus => ({
  status: 'waiting', expires_at: '2026-10-18T12:15:00Z', machine: null, target: null, probe: null, error: null, ...over,
});

const READY = status({
  status: 'ready',
  machine: MACHINE,
  target: { host: '198.51.100.7', port: 22, username: 'me' },
  probe: probe(),
});

/** Shaped as the API client rejects. */
function apiError(code: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${code}`), { response: { status: code, data: { detail } } });
}

function open(onSubmit = vi.fn()) {
  renderWithProviders(<CreateComputerDialog onClose={() => {}} onSubmit={onSubmit} busy={false} error={null} />);
  return onSubmit;
}

async function pickSsh(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('radio', { name: /^Your machine \(SSH\)/ }));
}

beforeEach(() => {
  vi.clearAllMocks();
  mockProviders.mockResolvedValue({ providers: DAYTONA_AND_SSH });
  mockComputers.mockResolvedValue({ computers: [] });
  mockMint.mockResolvedValue(pairing('tok1'));
  mockStatus.mockResolvedValue(status({}));
  mockKey.mockResolvedValue({
    public_key: 'ssh-ed25519 AAAAkey langalpha',
    fingerprint: 'SHA256:key',
    authorized_keys_command: "sh -c 'mkdir -p ~/.ssh && echo key >> ~/.ssh/authorized_keys'",
  });
});

describe('pairing', () => {
  it('mints one command once SSH is picked and waits for the machine', async () => {
    const user = userEvent.setup();
    open();
    // The name field sits at the top for every provider, SSH included.
    expect(await screen.findByRole('textbox', { name: /^Name/ })).toBeInTheDocument();
    expect(mockMint).not.toHaveBeenCalled();

    await pickSsh(user);

    // Until the machine answers, the field says it will be named after it.
    expect(screen.getByRole('textbox', { name: /^Name/ })).toHaveAttribute('placeholder', "Defaults to the machine's name");
    expect(await screen.findByText(pairing('tok1').command)).toBeInTheDocument();
    const script = screen.getByRole('link', { name: 'View script' });
    expect(script).toHaveAttribute('href', pairing('tok1').script_url);
    expect(script).toHaveAttribute('target', '_blank');
    expect(await screen.findByText('Waiting for your machine...')).toBeInTheDocument();
    expect(mockMint).toHaveBeenCalledTimes(1);
    expect(mockStatus).toHaveBeenCalledWith('tok1');
    expect(screen.getByRole('button', { name: 'Add computer' })).toBeDisabled();
  });

  it('says who checked in while the server connects', async () => {
    mockStatus.mockResolvedValue(status({ status: 'checking', machine: MACHINE }));
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    expect(await screen.findByText('me@devbox checked in. Connecting...')).toBeInTheDocument();
  });

  it('shows the machine once ready and adds it with the pinned key, the prefilled name and folder', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const onSubmit = open();
    await pickSsh(user);

    const summary = await screen.findByTestId('ssh-ready');
    expect(within(summary).getByText('me@198.51.100.7')).toBeInTheDocument();
    expect(within(summary).getByText('Ubuntu 24.04 · x86_64 · sudo available')).toBeInTheDocument();
    // The summary has no name of its own: the top field holds the hostname, the name it gets.
    expect(within(summary).queryByRole('textbox', { name: /^Name/ })).not.toBeInTheDocument();
    const top = screen.getByRole('textbox', { name: 'Name' });
    expect(top).toHaveValue('devbox');
    expect(within(summary).getByText('Workspaces go in /home/me/langalpha.')).toBeInTheDocument();
    expect(within(summary).getByText('Host key SHA256:abc123')).toBeInTheDocument();

    // The name the field holds is the server's own, so it goes unsent: the
    // server numbers past a computer that took it meanwhile, which a name sent
    // would be refused for.
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalledWith({
      resource_tier: 'standard',
      kind: 'ssh',
      ssh: {
        host: '198.51.100.7', port: 22, username: 'me',
        host_key: 'ssh-ed25519 AAAAC3Nz', root_dir: '/home/me/langalpha',
      },
      files_backup: true,
    });
  });

  it('backs up the workspace files unless the user unticks it', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const onSubmit = open();
    await pickSsh(user);

    const summary = await screen.findByTestId('ssh-ready');
    const backup = within(summary).getByRole('checkbox', { name: 'Back up workspace files' });
    expect(backup).toBeChecked();
    expect(backup).toHaveAccessibleDescription(/so a lost folder can be restored/);

    await user.click(backup);
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ kind: 'ssh', files_backup: false }));
  });

  it('sends the name and folder the user typed', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const onSubmit = open();
    await pickSsh(user);

    const summary = await screen.findByTestId('ssh-ready');
    await user.type(screen.getByRole('textbox', { name: /^Name/ }), 'Lab box');
    await user.click(within(summary).getByRole('button', { name: 'Change' }));
    const folder = within(summary).getByRole('textbox', { name: 'Folder' });
    expect(folder).toHaveFocus();
    expect(folder).toHaveValue('/home/me/langalpha');
    await user.clear(folder);
    await user.type(folder, '/srv/agent');

    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      name: 'Lab box',
      ssh: expect.objectContaining({ root_dir: '/srv/agent' }),
    }));
  });

  it('offers to take over a folder another computer marked, and sends the one it named', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    renderWithProviders(
      <CreateComputerDialog
        onClose={() => {}}
        onSubmit={onSubmit}
        busy={false}
        error="/home/me/langalpha on devbox is the folder of a LangAlpha computer this account does not have."
        takeOver="dd44d9d0ddb144d6b135d767eaecc5b7"
      />,
    );
    await pickSsh(user);
    await screen.findByTestId('ssh-ready');

    await user.click(screen.getByRole('button', { name: 'Take over this folder' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      ssh: expect.objectContaining({
        root_dir: '/home/me/langalpha',
        take_over: 'dd44d9d0ddb144d6b135d767eaecc5b7',
      }),
    }));
  });

  it('offers no take-over for any other refusal', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    renderWithProviders(
      <CreateComputerDialog onClose={() => {}} onSubmit={vi.fn()} busy={false} error="/home/me/langalpha on devbox already holds files." />,
    );
    await pickSsh(user);
    await screen.findByTestId('ssh-ready');

    expect(screen.getByRole('alert')).toHaveTextContent('already holds files');
    expect(screen.queryByRole('button', { name: 'Take over this folder' })).not.toBeInTheDocument();
  });

  it('starts a new pairing for a different machine', async () => {
    mockMint.mockResolvedValueOnce(pairing('tok1')).mockResolvedValueOnce(pairing('tok2'));
    mockStatus.mockImplementation(async (token: string) => (token === 'tok1' ? READY : status({})));
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await screen.findByTestId('ssh-ready');

    await user.click(screen.getByRole('button', { name: 'Use a different machine' }));

    expect(screen.getByTestId('ssh-pairing')).toHaveFocus();
    expect(await screen.findByText(pairing('tok2').command)).toBeInTheDocument();
    expect(screen.queryByText(pairing('tok1').command)).not.toBeInTheDocument();
    expect(await screen.findByText('Waiting for your machine...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Add computer' })).toBeDisabled();
  });

  it('offers the address the machine reported when the server could not reach it, and connects there', async () => {
    mockStatus.mockResolvedValue(status({
      status: 'failed',
      machine: MACHINE,
      error: { code: 'unreachable', message: 'No answer on 198.51.100.7:22.' },
    }));
    mockConnect.mockResolvedValue(status({
      status: 'ready',
      machine: MACHINE,
      target: { host: '198.51.100.7', port: 2222, username: 'me' },
      probe: probe(),
    }));
    const user = userEvent.setup();
    open();
    await pickSsh(user);

    expect(await screen.findByText('We could not reach your machine over SSH. It must accept SSH from the internet.')).toBeInTheDocument();
    const field = screen.getByRole('textbox', { name: 'Host or SSH command' });
    expect(field).toHaveValue('me@198.51.100.7');

    await user.type(field, ':2222');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(mockConnect).toHaveBeenCalledWith('tok1', { host: '198.51.100.7', port: 2222 });
    expect(await screen.findByText('me@198.51.100.7:2222')).toBeInTheDocument();
  });

  it('keeps the login the command ran as', async () => {
    mockStatus.mockResolvedValue(status({
      status: 'failed',
      machine: MACHINE,
      error: { code: 'timeout', message: 'Timed out.' },
    }));
    const user = userEvent.setup();
    open();
    await pickSsh(user);

    const field = await screen.findByRole('textbox', { name: 'Host or SSH command' });
    await user.clear(field);
    await user.type(field, 'root@198.51.100.7');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(await screen.findByText('The command ran as me. To use another login, run it as that user.')).toHaveAttribute('role', 'alert');
    expect(mockConnect).not.toHaveBeenCalled();
  });

  it('replaces an expired pairing without a word', async () => {
    mockMint.mockResolvedValueOnce(pairing('tok1')).mockResolvedValueOnce(pairing('tok2'));
    mockStatus.mockImplementation(async (token: string) => {
      if (token === 'tok1') throw apiError(404, 'This command has expired.');
      return status({});
    });
    const user = userEvent.setup();
    open();
    await pickSsh(user);

    expect(await screen.findByText(pairing('tok2').command)).toBeInTheDocument();
    expect(mockMint).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('falls back to setting it up by hand when no pairing can be had', async () => {
    mockMint.mockRejectedValue(apiError(503, 'SSH keys cannot be stored on this server.'));
    const user = userEvent.setup();
    open();
    await pickSsh(user);

    expect(await screen.findByText('The one-line command is not available on this server, so set it up by hand.')).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Host or SSH command' })).toBeInTheDocument();
  });
});

describe('the SSH computer cap', () => {
  const LIMIT = { code: 'ssh_computer_limit', message: 'server words', limit: 2 };

  it('says why on a refused pairing, in place of the generic note', async () => {
    mockMint.mockRejectedValue(apiError(409, LIMIT));
    const user = userEvent.setup();
    open();
    await pickSsh(user);

    expect(await screen.findByText('You can have up to 2 SSH computers. Remove one to add another.')).toBeInTheDocument();
    expect(screen.queryByText(/one-line command is not available/)).not.toBeInTheDocument();
  });

  it('says why on a refused probe', async () => {
    mockMint.mockRejectedValue(apiError(503, 'SSH keys cannot be stored on this server.'));
    mockProbe.mockRejectedValue(apiError(409, LIMIT));
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await user.type(await screen.findByRole('textbox', { name: 'Host or SSH command' }), 'me@198.51.100.7');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(await screen.findByText('You can have up to 2 SSH computers. Remove one to add another.')).toBeInTheDocument();
  });
});

describe('by hand', () => {
  it('asks for the user, shows the key step on a refusal, and adds the machine once it connects', async () => {
    mockProbe
      .mockRejectedValueOnce(apiError(422, { code: 'auth_failed', message: 'Permission denied (publickey).' }))
      .mockResolvedValueOnce(probe({ hostname: '', is_root: true }));
    const user = userEvent.setup();
    const onSubmit = open();
    await pickSsh(user);

    await user.click(await screen.findByRole('button', { name: 'Set it up by hand instead' }));
    const field = screen.getByRole('textbox', { name: 'Host or SSH command' });
    expect(field).toHaveFocus();
    // No key step before the first Connect.
    expect(screen.queryByTestId('ssh-key-step')).not.toBeInTheDocument();

    await user.type(field, '203.0.113.10{Enter}');
    expect(await screen.findByText('Include the user, like ubuntu@203.0.113.10.')).toBeInTheDocument();
    expect(mockProbe).not.toHaveBeenCalled();

    await user.clear(field);
    await user.type(field, 'ssh -p 2222 me@203.0.113.10');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(mockProbe).toHaveBeenCalledWith({ host: '203.0.113.10', port: 2222, username: 'me' }, { computerId: null });
    expect(await screen.findByText("Our key isn't on me@203.0.113.10 yet. Run this there as me, then Connect again:")).toBeInTheDocument();
    expect(await screen.findByText("sh -c 'mkdir -p ~/.ssh && echo key >> ~/.ssh/authorized_keys'")).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Connect' }));
    const summary = await screen.findByTestId('ssh-ready');
    expect(within(summary).getByText('me@203.0.113.10:2222')).toBeInTheDocument();
    expect(within(summary).getByText('Connecting as root.')).toBeInTheDocument();
    // No hostname: the name falls back to the host.
    expect(screen.getByRole('textbox', { name: /^Name/ })).toHaveValue('203.0.113.10');

    // The server falls back to the host the same way, so no name is sent.
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      ssh: expect.objectContaining({ host: '203.0.113.10', port: 2222, username: 'me' }),
    }));
    expect(onSubmit.mock.calls[0][0]).not.toHaveProperty('name');
  });

  it('goes back to the form with the address kept when the user picks a different machine', async () => {
    mockProbe.mockResolvedValue(probe());
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await user.click(await screen.findByRole('button', { name: 'Set it up by hand instead' }));
    await user.type(screen.getByRole('textbox', { name: 'Host or SSH command' }), 'me@box.example.com{Enter}');
    await screen.findByTestId('ssh-ready');

    await user.click(screen.getByRole('button', { name: 'Use a different machine' }));

    const field = screen.getByRole('textbox', { name: 'Host or SSH command' });
    expect(field).toHaveValue('me@box.example.com');
    expect(field).toHaveFocus();
    expect(screen.getByRole('button', { name: 'Add computer' })).toBeDisabled();
  });

  it('blocks an unsupported machine', async () => {
    mockProbe.mockResolvedValue(probe({ supported: false, problems: ['No /bin/bash on this machine.'] }));
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await user.click(await screen.findByRole('button', { name: 'Set it up by hand instead' }));
    await user.type(screen.getByRole('textbox', { name: 'Host or SSH command' }), 'me@box.example.com{Enter}');

    expect(await screen.findByText('This machine is not supported.')).toBeInTheDocument();
    expect(screen.getByText('No /bin/bash on this machine.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Add computer' })).toBeDisabled();
  });

  it('says why when the machine refused before it could be probed', async () => {
    mockProbe.mockRejectedValue(apiError(422, { code: 'unsupported', message: 'me@box.example.com could not run bash.' }));
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await user.click(await screen.findByRole('button', { name: 'Set it up by hand instead' }));
    await user.type(screen.getByRole('textbox', { name: 'Host or SSH command' }), 'me@box.example.com{Enter}');

    expect(await screen.findByText('me@box.example.com could not run bash.')).toBeInTheDocument();
  });

  it('returns to the one-line command', async () => {
    const user = userEvent.setup();
    open();
    await pickSsh(user);
    await user.click(await screen.findByRole('button', { name: 'Set it up by hand instead' }));
    await user.click(screen.getByRole('button', { name: 'Use the one-line command instead' }));

    expect(screen.getByTestId('ssh-pairing')).toHaveFocus();
    expect(screen.getByText(pairing('tok1').command)).toBeInTheDocument();
    // The pairing minted on the way in is kept, not replaced.
    await waitFor(() => expect(mockMint).toHaveBeenCalledTimes(1));
  });
});


describe('the name it gets', () => {
  it('numbers the hostname past a computer that has it, and follows the source until the user types', async () => {
    mockStatus.mockResolvedValue(READY);
    mockComputers.mockResolvedValue({ computers: [{ computer_id: 'c1', name: 'DevBox' }, { computer_id: 'c2', name: 'Cloud computer' }] });
    const user = userEvent.setup();
    open();
    const field = await screen.findByRole('textbox', { name: 'Name' });
    await waitFor(() => expect(field).toHaveValue('Cloud computer 2'));

    await pickSsh(user);
    await screen.findByTestId('ssh-ready');
    expect(screen.getByRole('textbox', { name: 'Name' })).toHaveValue('devbox 2');
  });

  it('sends a typed name, and none once the field is cleared', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const onSubmit = open();
    await pickSsh(user);
    await screen.findByTestId('ssh-ready');
    const field = screen.getByRole('textbox', { name: 'Name' });

    // Focusing selects the suggestion, so typing replaces it.
    await user.click(field);
    await user.keyboard('Lab box');
    expect(field).toHaveValue('Lab box');
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit.mock.calls[0][0]).toHaveProperty('name', 'Lab box');

    await user.clear(field);
    expect(field).toHaveAttribute('placeholder', 'devbox');
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit.mock.calls[1][0]).not.toHaveProperty('name');
  });
});
