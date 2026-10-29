/**
 * Adding this Mac by remote control, through the New computer dialog, against
 * a stubbed desktop bridge: when the option is offered, the disclosure that
 * comes before anything happens, the pairing and every way it can stop, and
 * turning the helper off again when the dialog closes on nothing.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { RemoteControlResult, RemoteControlStatus } from '@/lib/desktop';
import type { Computer, ComputerProviderOption, SshPairingStatus, SshProbeResult } from '@/types/api';

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

vi.mock('../../utils/api', () => ({
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  createSshPairing: vi.fn(),
  getSshPairing: vi.fn(),
  connectSshPairing: vi.fn(),
  probeSsh: vi.fn(),
  getSshKey: vi.fn(),
}));

import { createSshPairing, getComputerProviders, getComputers, getSshPairing } from '../../utils/api';
import { addComputerRow } from '../../hooks/useComputers';
import CreateComputerDialog from '../CreateComputerDialog';

const mockProviders = getComputerProviders as Mock;
const mockGetComputers = getComputers as Mock;
const mockMint = createSshPairing as Mock;
const mockStatus = getSshPairing as Mock;

const MAC_KEY = 'SHA256:helperHostKey';

const PROVIDERS: ComputerProviderOption[] = [
  { kind: 'daytona', is_default: true, allowed: null },
  { kind: 'ssh', is_default: false, allowed: null },
];

const UNPAIRED: RemoteControlStatus = {
  supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: 'alex',
};
const PAIRED: RemoteControlStatus = {
  supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: MAC_KEY, account: 'alex',
};

const pairing = (token: string) => ({
  token,
  command: `echo 'url = "https://api.example.com/api/v1/computers/ssh/pairings/${token}/script"' | curl -fsSL -K - | sh`,
  script_url: `https://api.example.com/api/v1/computers/ssh/pairings/${token}/script`,
  expires_at: '2026-10-23T12:15:00Z',
  local_only: false,
});

const probe: SshProbeResult = {
  host_key: 'ssh-ed25519 AAAAhelper', fingerprint: MAC_KEY, os: 'Darwin', arch: 'arm64',
  distro: 'macOS 15.1', home: '/Users/alex', shell: '/bin/zsh', is_root: false, has_sudo: true,
  disk_free_bytes: null, supported: true, problems: [], suggested_root_dir: '/Users/alex/langalpha',
  hostname: 'alex-mac', helper: true,
};

const status = (over: Partial<SshPairingStatus>): SshPairingStatus => ({
  status: 'waiting', expires_at: '2026-10-23T12:15:00Z', machine: null, target: null, probe: null, error: null, ...over,
});

const READY = status({
  status: 'ready',
  machine: { username: 'alex', hostname: 'alex-mac', os: 'Darwin', addresses: [] },
  target: { host: 'alex-mac', port: 22, username: 'alex', tunnel_id: 'tun-1' },
  probe,
});

function macRow(fingerprint: string): Computer {
  return {
    computer_id: 'c-mac', user_id: 'u1', kind: 'ssh', name: 'alex-mac', status: 'running',
    resource_tier: 'standard', is_always_on: false, is_primary: false, root_dir: '/Users/alex/langalpha',
    ssh: { host: 'alex-mac', port: 22, username: 'alex', fingerprint, tunnel: true, tunnel_connected: true, helper: true },
  };
}

function deferred<T>() {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

/** Shaped as the API client rejects. */
function apiError(code: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${code}`), { response: { status: code, data: { detail } } });
}

// What the helper reports, which an enable that pairs moves on.
let macStatus: RemoteControlStatus;
let getStatus: Mock;
let enable: Mock;
let disable: Mock;

beforeEach(() => {
  vi.clearAllMocks();
  macStatus = UNPAIRED;
  getStatus = vi.fn(async () => macStatus);
  enable = vi.fn(async (): Promise<RemoteControlResult> => {
    macStatus = PAIRED;
    return { ok: true, status: PAIRED };
  });
  disable = vi.fn(async () => {
    macStatus = UNPAIRED;
    return UNPAIRED;
  });
  bridge.getRemoteControl = getStatus;
  bridge.enableRemoteControl = enable;
  bridge.disableRemoteControl = disable;
  mockProviders.mockResolvedValue({ providers: PROVIDERS, tunnels: true });
  mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
  mockMint.mockImplementation(async () => pairing(`tok${mockMint.mock.calls.length}`));
  mockStatus.mockResolvedValue(status({}));
});

function open(onSubmit = vi.fn()) {
  const view = renderWithProviders(<CreateComputerDialog onClose={() => {}} onSubmit={onSubmit} busy={false} error={null} />);
  return { ...view, onSubmit };
}

async function pickThisMac(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('radio', { name: 'This Mac' }));
}

async function agreeAndTurnOn(user: ReturnType<typeof userEvent.setup>) {
  await pickThisMac(user);
  await user.click(screen.getByRole('checkbox', { name: /^I understand LangAlpha's agent/ }));
  await user.click(screen.getByRole('button', { name: 'Turn on remote control' }));
}

/** Let the release a closing dialog queued run to its end. */
async function settle() {
  for (let i = 0; i < 10; i++) await act(async () => { await Promise.resolve(); });
}

describe('the This Mac option', () => {
  it('sits after the hosted providers and before SSH in the desktop app, marked as a preview', async () => {
    open();
    const card = await screen.findByRole('radio', { name: 'This Mac' });
    expect(screen.getAllByRole('radio').slice(0, 3)).toEqual([
      screen.getByRole('radio', { name: 'Daytona' }),
      card,
      screen.getByRole('radio', { name: 'Your machine (SSH)' }),
    ]);
    expect(card).toHaveAccessibleDescription(/Preview.*Works even with LangAlpha closed/);
    expect(card).not.toBeChecked();
  });

  it.each([
    ['this server runs no tunnel gateway', { providers: PROVIDERS, tunnels: false }],
    ['SSH is not on the plan', { providers: [PROVIDERS[0], { ...PROVIDERS[1], allowed: false }], tunnels: true }],
  ])('is not offered when %s', async (_label, response) => {
    mockProviders.mockResolvedValue(response);
    open();
    await screen.findByRole('radio', { name: /^Daytona/ });
    await settle();
    expect(screen.queryByRole('radio', { name: 'This Mac' })).not.toBeInTheDocument();
  });

  it('is not offered where the helper cannot run', async () => {
    getStatus.mockResolvedValue({ ...UNPAIRED, supported: false, reason: 'os_version' });
    open();
    await screen.findByRole('radio', { name: /^Daytona/ });
    await settle();
    expect(screen.queryByRole('radio', { name: 'This Mac' })).not.toBeInTheDocument();
  });

  // The shell answers null to a page it does not count as the app's.
  it('is not offered by a shell that gives this page no answer', async () => {
    getStatus.mockResolvedValue(null);
    open();
    await screen.findByRole('radio', { name: /^Daytona/ });
    await settle();
    expect(screen.queryByRole('radio', { name: 'This Mac' })).not.toBeInTheDocument();
  });

  // A shell that could turn it on but not off again would strand the user.
  it('is not offered by a shell missing any of the three methods', async () => {
    delete bridge.disableRemoteControl;
    open();
    await screen.findByRole('radio', { name: /^Daytona/ });
    await settle();
    expect(screen.queryByRole('radio', { name: 'This Mac' })).not.toBeInTheDocument();
    expect(getStatus).not.toHaveBeenCalled();
  });

  it('says it is already added once a computer is this Mac, rather than pairing again', async () => {
    macStatus = PAIRED;
    mockGetComputers.mockResolvedValue({ computers: [macRow(MAC_KEY)], total: 1 });
    open();
    const card = await screen.findByRole('radio', { name: 'This Mac' });
    expect(card).toBeDisabled();
    expect(card).toHaveAccessibleDescription(/Already added/);
    expect(screen.getByTestId('this-mac-another')).toHaveTextContent(
      'To add another Mac, get the desktop app on it and open this form there.',
    );
  });

  it('offers a second Mac while another Mac is already a computer of this account', async () => {
    mockGetComputers.mockResolvedValue({ computers: [macRow('SHA256:anotherMac')], total: 1 });
    open();
    const card = await screen.findByRole('radio', { name: 'This Mac' });
    await waitFor(() => expect(card).toBeEnabled());
    expect(card).not.toHaveAccessibleDescription(/Already added/);
    expect(screen.queryByTestId('this-mac-another')).not.toBeInTheDocument();
  });
});

describe('turning it on', () => {
  it('says what it means first, naming the account, and touches nothing until the user agrees', async () => {
    const user = userEvent.setup();
    open();
    await pickThisMac(user);

    const disclosure = screen.getByTestId('remote-control-disclosure');
    expect(disclosure).toHaveTextContent("LangAlpha's agent will work on this Mac as alex, with everything that account can do.");
    expect(disclosure).toHaveTextContent('Anyone who signs in to your LangAlpha account can direct it.');
    expect(screen.getByTestId('remote-control-preview-notice')).toHaveTextContent('Remote control is in preview');
    // A Mac has no tier to pick, and the name field at the top says it is named after the machine.
    expect(screen.getByRole('textbox', { name: /^Name/ })).toHaveAttribute('placeholder', "Defaults to the machine's name");
    expect(screen.queryByRole('radio', { name: /^Standard/ })).not.toBeInTheDocument();

    const turnOn = screen.getByRole('button', { name: 'Turn on remote control' });
    expect(turnOn).toBeDisabled();
    await user.click(screen.getByRole('checkbox', { name: "I understand LangAlpha's agent can run commands and change files on this Mac as alex." }));
    expect(turnOn).toBeEnabled();
    expect(mockMint).not.toHaveBeenCalled();
    expect(enable).not.toHaveBeenCalled();
  });

  it('pairs this Mac, says what it is doing, and adds it as the SSH pairing would', async () => {
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    const { onSubmit, queryClient } = open();
    await agreeAndTurnOn(user);

    expect(await screen.findByText('Turning on remote control…')).toBeInTheDocument();
    expect(mockMint).toHaveBeenCalledTimes(1);
    expect(enable).toHaveBeenCalledWith(pairing('tok1').script_url);
    // Polled while the helper pairs, not after.
    await waitFor(() => expect(mockStatus).toHaveBeenCalledWith('tok1'));
    expect(screen.getByRole('button', { name: 'Add computer' })).toBeDisabled();

    await act(async () => answer.resolve({ ok: true, status: PAIRED }));
    expect(await screen.findByText('Connecting this Mac…')).toBeInTheDocument();

    mockStatus.mockResolvedValue(READY);
    await act(() => queryClient.invalidateQueries({ queryKey: ['ssh-pairings'] }));
    const summary = await screen.findByTestId('ssh-ready');
    // Named as its row will be, by the account it works as.
    expect(summary).toHaveTextContent('as alex');
    expect(summary).not.toHaveTextContent('alex@alex-mac');
    expect(summary).not.toHaveTextContent('tunnel');
    // This Mac is the only machine this way adds.
    expect(screen.queryByRole('button', { name: 'Use a different machine' })).not.toBeInTheDocument();
    // Named as the machine unless the user types over it: the top field shows it as its default.
    expect(screen.getByRole('textbox', { name: /^Name/ })).toHaveAttribute('placeholder', 'alex-mac');

    // The server gives it that name, numbered past the user's other computers.
    await user.click(screen.getByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalledWith({
      ssh: {
        host: 'alex-mac', port: 22, username: 'alex', tunnel_id: 'tun-1',
        host_key: 'ssh-ed25519 AAAAhelper', root_dir: '/Users/alex/langalpha',
      },
      files_backup: true,
      resource_tier: 'standard',
      kind: 'ssh',
    });
  });

  it("goes back to the disclosure quietly when the user cancels the app's own confirmation", async () => {
    enable.mockResolvedValue({ ok: false, error: 'cancelled', status: UNPAIRED });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);

    const turnOn = await screen.findByRole('button', { name: 'Turn on remote control' });
    expect(turnOn).toBeEnabled();
    expect(screen.getByRole('checkbox', { name: /^I understand/ })).toBeChecked();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('waits for approval in System Settings, and Continue pairs again with a new pairing', async () => {
    enable.mockResolvedValueOnce({ ok: false, error: 'needs_approval', status: { ...UNPAIRED, service: 'needs_approval' } });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);

    expect(await screen.findByTestId('remote-control-approval')).toHaveTextContent(
      'macOS needs your OK. In System Settings, under General, Login Items, allow LangAlpha in the background, then continue.',
    );
    await user.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(enable).toHaveBeenCalledTimes(2));
    expect(mockMint).toHaveBeenCalledTimes(2);
    expect(enable).toHaveBeenLastCalledWith(pairing('tok2').script_url);
    expect(await screen.findByText('Connecting this Mac…')).toBeInTheDocument();
  });

  // Allowing it starts the helper on the pairing it was already given.
  it('goes on by itself once the user allows it, without Continue', async () => {
    enable.mockResolvedValue({ ok: false, error: 'needs_approval', status: { ...UNPAIRED, service: 'needs_approval' } });
    const user = userEvent.setup();
    const { queryClient } = open();
    await agreeAndTurnOn(user);
    await screen.findByTestId('remote-control-approval');

    mockStatus.mockResolvedValue(READY);
    await act(() => queryClient.invalidateQueries({ queryKey: ['ssh-pairings'] }));
    expect(await screen.findByTestId('ssh-ready')).toBeInTheDocument();
    expect(mockMint).toHaveBeenCalledTimes(1);
  });

  it("shows the deployment's sentence when pairing failed, and tries again", async () => {
    enable.mockResolvedValueOnce({ ok: false, error: 'failed', message: 'This pairing has expired.', status: UNPAIRED });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);

    expect(await screen.findByRole('alert')).toHaveTextContent('This pairing has expired.');
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(enable).toHaveBeenCalledTimes(2));
    expect(enable).toHaveBeenLastCalledWith(pairing('tok2').script_url);
  });

  it.each(['timeout', 'not_started'] as const)('says it plainly on %s', async (error) => {
    enable.mockResolvedValue({ ok: false, error, status: UNPAIRED });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't turn on remote control. Try again.");
    expect(screen.getByRole('button', { name: 'Try again' })).toBeEnabled();
  });

  it.each([
    ['gives no answer', () => Promise.resolve(null)],
    ['throws', () => Promise.reject(new Error("No handler registered for 'remote-control:enable'"))],
  ])('says it plainly when the shell %s, and tries again', async (_label, answer) => {
    enable.mockImplementation(answer);
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't turn on remote control. Try again.");
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(enable).toHaveBeenCalledTimes(2));
  });

  it("shows the server's refusal while the helper is still answering", async () => {
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    mockStatus.mockResolvedValue(status({ status: 'failed', error: { code: 'not_connected', message: 'The tunnel did not come up.' } }));
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    expect(await screen.findByRole('alert')).toHaveTextContent('The tunnel did not come up.');
    // Settled, since the bridge takes one call at a time across tests too.
    await act(async () => answer.resolve({ ok: false, error: 'failed', message: 'The tunnel did not come up.', status: UNPAIRED }));
  });

  it('says why when the account already has all the SSH computers it may', async () => {
    mockMint.mockRejectedValue(apiError(409, { code: 'ssh_computer_limit', limit: 3 }));
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    expect(await screen.findByRole('alert')).toHaveTextContent('You can have up to 3 SSH computers. Remove one to add another.');
    expect(enable).not.toHaveBeenCalled();
  });

  it('says why, with nothing to retry, when the app refuses a pairing address that is not https', async () => {
    enable.mockResolvedValue({ ok: false, error: 'bad_url', status: UNPAIRED });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    expect(await screen.findByRole('alert')).toHaveTextContent("needs this server's address to start with https");
    expect(screen.queryByRole('button', { name: 'Try again' })).not.toBeInTheDocument();
  });

  it("leaves a helper paired elsewhere for the app to replace after its own confirmation", async () => {
    // Another account signed in on this Mac, whose computer is not on this list.
    macStatus = { ...PAIRED, fingerprint: 'SHA256:anotherAccount' };
    enable.mockResolvedValue({ ok: false, error: 'cancelled', status: macStatus });
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    await waitFor(() => expect(enable).toHaveBeenCalledTimes(1));
    await screen.findByRole('button', { name: 'Turn on remote control' });
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});

describe('moving to another provider', () => {
  const pickSsh = (user: ReturnType<typeof userEvent.setup>) =>
    user.click(screen.getByRole('radio', { name: /^Your machine \(SSH\)/ }));

  it('turns remote control off, once, and coming back starts from the disclosure', async () => {
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await screen.findByText('Connecting this Mac…');

    await pickSsh(user);
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
    await pickThisMac(user);
    expect(screen.getByRole('button', { name: 'Turn on remote control' })).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: /^I understand LangAlpha's agent/ })).toBeChecked();

    unmount();
    await settle();
    expect(disable).toHaveBeenCalledTimes(1);
  });

  it('waits for an enable still in flight, and its answer moves nothing on screen', async () => {
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    open();
    await agreeAndTurnOn(user);
    await waitFor(() => expect(enable).toHaveBeenCalled());

    await pickSsh(user);
    await settle();
    expect(disable).not.toHaveBeenCalled();
    macStatus = PAIRED;
    await act(async () => answer.resolve({ ok: true, status: PAIRED }));
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
    await pickThisMac(user);
    expect(screen.getByRole('button', { name: 'Turn on remote control' })).toBeInTheDocument();
  });

  it('touches nothing when the user moves away before turning it on', async () => {
    const user = userEvent.setup();
    open();
    await pickThisMac(user);
    await pickSsh(user);
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});

describe('closing the dialog', () => {
  it('turns remote control off when closed after pairing, with no computer added', async () => {
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await screen.findByText('Connecting this Mac…');

    unmount();
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  it('waits for an enable still in flight before turning it off', async () => {
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await waitFor(() => expect(enable).toHaveBeenCalled());

    unmount();
    await settle();
    expect(disable).not.toHaveBeenCalled();
    macStatus = PAIRED;
    answer.resolve({ ok: true, status: PAIRED });
    await waitFor(() => expect(disable).toHaveBeenCalledTimes(1));
  });

  it('touches nothing when closed before turning it on', async () => {
    const user = userEvent.setup();
    const { unmount } = open();
    await pickThisMac(user);
    await user.click(screen.getByRole('checkbox', { name: /^I understand/ }));

    unmount();
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('touches nothing when closed after the user cancelled', async () => {
    enable.mockResolvedValue({ ok: false, error: 'cancelled', status: UNPAIRED });
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await screen.findByRole('button', { name: 'Turn on remote control' });

    unmount();
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  // Each is the shell saying no having changed nothing, so the helper,
  // paired here to another account signed in on this Mac, is not this
  // dialog's to turn off.
  it.each([
    ['refused the address', { ok: false, error: 'bad_url' }],
    ['could not run the helper', { ok: false, error: 'unsupported' }],
    ['could not take the old pairing down', { ok: false, error: 'not_replaced' }],
    ['gave no answer', null],
  ] as const)('touches nothing when closed after the shell %s', async (_label, answer) => {
    macStatus = { ...PAIRED, fingerprint: 'SHA256:anotherAccount' };
    enable.mockResolvedValue(answer && { ...answer, status: macStatus });
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await screen.findByRole('alert');

    unmount();
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('touches nothing when closed during an enable the shell then refuses', async () => {
    macStatus = { ...PAIRED, fingerprint: 'SHA256:anotherAccount' };
    const answer = deferred<RemoteControlResult>();
    enable.mockImplementation(() => answer.promise);
    const user = userEvent.setup();
    const { unmount } = open();
    await agreeAndTurnOn(user);
    await waitFor(() => expect(enable).toHaveBeenCalled());

    unmount();
    await act(async () => answer.resolve({ ok: false, error: 'bad_url', status: macStatus }));
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });

  it('keeps it on when the dialog closes on the computer it added', async () => {
    mockStatus.mockResolvedValue(READY);
    const user = userEvent.setup();
    const { unmount, onSubmit, queryClient } = open();
    await agreeAndTurnOn(user);
    await user.click(await screen.findByRole('button', { name: 'Add computer' }));
    expect(onSubmit).toHaveBeenCalled();

    // The Computers dialog's create puts the new row on the list, as the
    // server will, and closes this one.
    mockGetComputers.mockResolvedValue({ computers: [macRow(MAC_KEY)], total: 1 });
    addComputerRow(queryClient, macRow(MAC_KEY));
    getStatus.mockClear();
    unmount();
    await waitFor(() => expect(getStatus).toHaveBeenCalled());
    await settle();
    expect(disable).not.toHaveBeenCalled();
  });
});
