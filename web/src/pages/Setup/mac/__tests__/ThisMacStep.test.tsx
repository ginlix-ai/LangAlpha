/**
 * The pair prompt in the Mac app, against a stubbed shell: each stage the
 * remote-control flow passes through (the shell's confirmation, Login Items,
 * pairing, its deadline, paired), what the record is told, and where the
 * prompt goes once answered. Where pairing cannot apply it is skipped.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Route, Routes, useLocation } from 'react-router';

import { renderWithProviders } from '@/test/utils';
import type { RemoteControlResult, RemoteControlStatus } from '@/lib/desktop';
import type { Computer, SshPairingStatus } from '@/types/api';

vi.mock('@/config/hostMode', () => ({ HOST_MODE: 'platform', isPlatformMode: true, APP_ENTRY_PATH: '/app' }));

const shell = vi.hoisted(() => ({
  inShell: true,
  getRemoteControl: vi.fn(),
  enableRemoteControl: vi.fn(),
  disableRemoteControl: vi.fn(),
  openConsolePage: vi.fn(),
}));
vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/desktop')>()),
  isDesktopShell: () => shell.inShell,
  canRemoteControl: () => shell.inShell,
  canAddThisMac: () => shell.inShell,
  getRemoteControl: () => shell.getRemoteControl(),
  enableRemoteControl: (url: string) => shell.enableRemoteControl(url),
  disableRemoteControl: () => shell.disableRemoteControl(),
  openConsolePage: (url: string, options?: object) => shell.openConsolePage(url, options),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(),
  createSshPairing: vi.fn(),
  getSshPairing: vi.fn(),
  createComputer: vi.fn(),
  getHomeWorkspace: vi.fn(),
  startWorkspace: vi.fn(),
}));
vi.mock('@/pages/Dashboard/utils/api', () => ({ updateCurrentUser: vi.fn() }));
vi.mock('@/pages/ThisMac/computerChoiceGate', () => ({ markAskedOnThisMac: vi.fn() }));
const account = vi.hoisted(() => ({ userId: 'u1' }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { user_id: account.userId }, isLoading: false, isError: false }) }));

const record = vi.hoisted(() => ({ update: vi.fn(), finish: vi.fn(), facts: {} as Record<string, unknown> }));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({ progress: null, isLoading: false, isError: false, update: record.update, isUpdating: false }),
}));
vi.mock('@/pages/Setup/answers/flow', () => ({
  useAnswerFlow: () => ({ ...record.facts, finish: record.finish }),
}));

import {
  createComputer, createSshPairing, getComputerProviders, getComputers, getHomeWorkspace, getSshPairing, startWorkspace,
} from '@/pages/ChatAgent/utils/api';
import { updateCurrentUser } from '@/pages/Dashboard/utils/api';
import { markAskedOnThisMac } from '@/pages/ThisMac/computerChoiceGate';
import ThisMacStep from '../ThisMacStep';

const MAC_KEY = 'SHA256:helperHostKey';
const UNPAIRED: RemoteControlStatus = { supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: 'nadia' };
const PAIRED: RemoteControlStatus = { supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: MAC_KEY, account: 'nadia' };

const READY: SshPairingStatus = {
  status: 'ready',
  expires_at: '2026-11-15T12:15:00Z',
  machine: { username: 'nadia', hostname: 'nadia-mbp', os: 'Darwin', addresses: [] },
  target: { host: 'nadia-mbp', port: 22, username: 'nadia', tunnel_id: 'tun-1' },
  probe: {
    host_key: 'ssh-ed25519 AAAA', fingerprint: MAC_KEY, os: 'Darwin', arch: 'arm64', distro: 'macOS 26', home: '/Users/nadia',
    shell: '/bin/zsh', is_root: false, has_sudo: true, disk_free_bytes: null, supported: true, problems: [],
    suggested_root_dir: '/Users/nadia/langalpha', hostname: 'nadia-mbp', helper: true,
  },
  error: null,
};
const WAITING: SshPairingStatus = { ...READY, status: 'waiting', machine: null, target: null, probe: null };

const MAC_ROW: Computer = {
  computer_id: 'c-mac', user_id: 'u1', kind: 'ssh', name: 'MacBook Pro', status: 'running', resource_tier: 'standard',
  is_always_on: false, is_primary: false, root_dir: '/Users/nadia/langalpha',
  ssh: { host: 'nadia-mbp', port: 22, username: 'nadia', fingerprint: MAC_KEY, tunnel: true, tunnel_connected: true, helper: true, distro: 'macOS 26' },
};

function facts(over: Record<string, unknown> = {}) {
  return {
    isLoading: false,
    progress: { models: 'claude_code', runs_on: null, plan_step_done_at: 'x', this_mac: null, setup_on: 'mac_pending', setup_step: null, setup_completed_at: null },
    answers: { models: 'claude_code', runsOn: 'own_computer' },
    ownComputers: [],
    ...over,
  };
}

function Where() {
  const location = useLocation();
  return <div data-testid="where">{location.pathname}</div>;
}

const routes = () => (
  <Routes>
    <Route path="/setup/this-mac" element={<ThisMacStep />} />
    <Route path="*" element={<Where />} />
  </Routes>
);

function open(route = '/setup/this-mac') {
  return renderWithProviders(routes(), { route });
}

const pairButton = () => screen.findByRole('button', { name: 'Pair this Mac' });

beforeEach(() => {
  vi.clearAllMocks();
  account.userId = 'u1';
  shell.inShell = true;
  shell.getRemoteControl.mockResolvedValue(UNPAIRED);
  shell.disableRemoteControl.mockResolvedValue(UNPAIRED);
  record.update.mockImplementation(async (patch: object) => patch);
  record.finish.mockResolvedValue(true);
  record.facts = facts();
  (getComputerProviders as Mock).mockResolvedValue({ providers: [{ kind: 'daytona', is_default: true, allowed: null }, { kind: 'ssh', is_default: false, allowed: null }], tunnels: true });
  (getComputers as Mock).mockResolvedValue({ computers: [], total: 0 });
  (createSshPairing as Mock).mockImplementation(async () => ({
    token: 'tok', command: '', script_url: 'https://api.example/api/v1/computers/ssh/pairings/tok/script', expires_at: '', local_only: false,
  }));
  (getSshPairing as Mock).mockResolvedValue(WAITING);
  (createComputer as Mock).mockResolvedValue(MAC_ROW);
  (getHomeWorkspace as Mock).mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'stopped', is_home: true });
  (startWorkspace as Mock).mockResolvedValue({ workspace_id: 'home-1', status: 'starting' });
  (updateCurrentUser as Mock).mockResolvedValue({});
});

const CLOUD_PRIMARY: Computer = { ...MAC_ROW, computer_id: 'c-cloud', kind: 'daytona', name: 'Cloud', is_primary: true, ssh: null };

function pairsOnClick() {
  shell.enableRemoteControl.mockImplementation(async () => {
    (getSshPairing as Mock).mockResolvedValue(READY);
    return { ok: true, status: PAIRED };
  });
}

describe('the pair prompt', () => {
  it('asks first, naming the macOS account the agent acts as', async () => {
    open();
    expect(await screen.findByRole('heading', { name: 'Use this Mac with LangAlpha?' })).toBeInTheDocument();
    expect(screen.getByText(/as nadia, the macOS account you're using now/)).toBeInTheDocument();
    expect(shell.enableRemoteControl).not.toHaveBeenCalled();
  });

  it('waits on the shell\'s confirmation, and a cancel there asks again', async () => {
    let answer: (r: RemoteControlResult) => void = () => {};
    shell.enableRemoteControl.mockImplementation(() => new Promise((r) => { answer = r; }));
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(shell.enableRemoteControl).toHaveBeenCalledWith('https://api.example/api/v1/computers/ssh/pairings/tok/script');
    expect(screen.getByRole('button', { name: 'Pair this Mac' })).toBeDisabled();
    await act(async () => answer({ ok: false, error: 'cancelled', status: UNPAIRED }));
    expect(await pairButton()).toBeEnabled();
    expect(record.update).not.toHaveBeenCalled();
  });

  it('shows Login Items when macOS holds the helper back, and Open there tries again', async () => {
    shell.enableRemoteControl.mockResolvedValue({ ok: false, error: 'needs_approval', status: { ...UNPAIRED, service: 'needs_approval' } });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByRole('heading', { name: 'Pairing this Mac' })).toBeInTheDocument();
    expect(screen.getByText(/macOS held back the part of LangAlpha/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Open Login Items' }));
    await waitFor(() => expect(shell.enableRemoteControl).toHaveBeenCalledTimes(2));
  });

  it('says so when pairing runs past the helper\'s deadline, and tries again', async () => {
    shell.enableRemoteControl.mockResolvedValue({ ok: false, error: 'timeout', status: UNPAIRED });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByRole('alert')).toHaveTextContent("Pairing didn't finish in 80 seconds. Try again.");
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(shell.enableRemoteControl).toHaveBeenCalledTimes(2));
  });

  it('says so where the helper reached a Mac that cannot run as a computer, with Try again and Not now', async () => {
    const unsupported = { ...READY, probe: { ...READY.probe!, supported: false, problems: ['macOS 12 is too old.'] } };
    shell.enableRemoteControl.mockImplementation(async () => {
      (getSshPairing as Mock).mockResolvedValue(unsupported);
      return { ok: true, status: PAIRED };
    });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByRole('alert')).toHaveTextContent('This machine is not supported. macOS 12 is too old.');
    expect(screen.getByRole('button', { name: 'Try again' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Not now' })).toBeEnabled();
    expect(createComputer).not.toHaveBeenCalled();
    // Fixed meanwhile, the Mac is tried again with a new pairing.
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(shell.enableRemoteControl).toHaveBeenCalledTimes(2));
  });

  it('says so where setup could not go on past the computer step', async () => {
    record.facts = facts({ finishFailed: true });
    open();
    await pairButton();
    expect(screen.getByRole('alert')).toHaveTextContent("This step couldn't be saved. Try again.");
  });

  it('makes this Mac a computer once paired, records it, and hands setup a browser started to the continue prompt', async () => {
    // The list names the new computer once it is made, as the server's does.
    (getComputers as Mock).mockImplementation(async () => {
      const made = (createComputer as Mock).mock.calls.length > 0;
      return { computers: made ? [MAC_ROW] : [], total: made ? 1 : 0 };
    });
    shell.enableRemoteControl.mockImplementation(async () => {
      shell.getRemoteControl.mockResolvedValue(PAIRED);
      (getSshPairing as Mock).mockResolvedValue(READY);
      return { ok: true, status: PAIRED };
    });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByRole('heading', { name: 'This Mac is paired' }, { timeout: 5000 })).toBeInTheDocument();
    expect(createComputer).toHaveBeenCalledWith(expect.objectContaining({ kind: 'ssh', resource_tier: 'standard' }));
    expect(screen.getByTestId('this-mac-computer')).toHaveTextContent('MacBook Pro');
    expect(record.update).toHaveBeenCalledWith({ this_mac: 'paired', runs_on: 'own_computer' });
    // The user's first computer, so Home is on it, and no cloud computer is built for them.
    expect(screen.getByText('Home and new workspaces go on this Mac.')).toBeInTheDocument();
    expect(createComputer).toHaveBeenCalledWith(expect.not.objectContaining({ make_primary: true }));
    expect(updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    // This Mac being a computer now is the prompt's own doing, not a reason to skip it.
    await act(async () => { await new Promise((r) => setTimeout(r, 300)); });
    expect(screen.getByRole('heading', { name: 'This Mac is paired' })).toBeInTheDocument();
    expect(record.update).not.toHaveBeenCalledWith({ this_mac: 'unavailable' });
    await user.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/continue');
    expect(markAskedOnThisMac).toHaveBeenCalled();
  });

  it('moves Home here from a cloud computer for an own-computer answer', async () => {
    record.facts = facts({ progress: { ...facts().progress as object, runs_on: 'own_computer' } });
    (getComputers as Mock).mockResolvedValue({ computers: [CLOUD_PRIMARY], total: 1 });
    pairsOnClick();
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByText(/Home is moving to this Mac/, {}, { timeout: 5000 })).toBeInTheDocument();
    expect(createComputer).toHaveBeenCalledWith(expect.objectContaining({ kind: 'ssh', make_primary: true }));
    expect(updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    expect(startWorkspace).not.toHaveBeenCalled();
  });

  it("moves Home here from another machine of the user's own", async () => {
    record.facts = facts({ progress: { ...facts().progress as object, runs_on: 'own_computer' } });
    (getComputers as Mock).mockResolvedValue({ computers: [{ ...MAC_ROW, computer_id: 'c-studio', name: 'Studio', is_primary: true }], total: 1 });
    pairsOnClick();
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    expect(await screen.findByText('Home is moving to this Mac from Studio.', {}, { timeout: 5000 })).toBeInTheDocument();
    expect(createComputer).toHaveBeenCalledWith(expect.objectContaining({ make_primary: true }));
  });

  it('binds Home on the cloud before adding this Mac for a cloud answer', async () => {
    record.facts = facts({ answers: { models: 'claude_code', runsOn: 'cloud' }, progress: { ...facts().progress as object, runs_on: 'cloud' } });
    pairsOnClick();
    const user = userEvent.setup();
    open();
    expect(await screen.findByText('Pair it, and LangAlpha can run your tasks here.')).toBeInTheDocument();
    await user.click(await pairButton());
    expect(await screen.findByRole('heading', { name: 'This Mac is paired' }, { timeout: 5000 })).toBeInTheDocument();
    expect(updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'cloud' });
    expect(startWorkspace).toHaveBeenCalledWith('home-1', { lazy: true, reason: 'onboarding' });
    const order = (fn: unknown) => (fn as Mock).mock.invocationCallOrder[0];
    expect(order(updateCurrentUser)).toBeLessThan(order(startWorkspace));
    expect(order(startWorkspace)).toBeLessThan(order(createComputer));
    expect(createComputer).toHaveBeenCalledWith(expect.not.objectContaining({ make_primary: true }));
    expect(screen.queryByText(/Home and new workspaces/)).not.toBeInTheDocument();
  });

  it('records Not now, and leaves pairing alone', async () => {
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('button', { name: 'Not now' }));
    expect(record.update).toHaveBeenCalledWith({ this_mac: 'declined' });
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/continue');
    expect(shell.enableRemoteControl).not.toHaveBeenCalled();
    // The desktop app's first run asks nothing more on this Mac.
    expect(markAskedOnThisMac).toHaveBeenCalled();
  });

  it('makes no computer where pairing finishes after Not now, even where saving that was refused', async () => {
    let answer: (r: RemoteControlResult) => void = () => {};
    shell.enableRemoteControl.mockImplementation(() => new Promise((r) => { answer = r; }));
    record.update.mockRejectedValue(new Error('refused'));
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    await user.click(screen.getByRole('button', { name: 'Not now' }));
    expect(record.update).toHaveBeenCalledWith({ this_mac: 'declined' });
    (getSshPairing as Mock).mockResolvedValue(READY);
    await act(async () => answer({ ok: true, status: PAIRED }));
    await act(async () => { await new Promise((r) => setTimeout(r, 3000)); });
    expect(createComputer).not.toHaveBeenCalled();
    expect(record.update).not.toHaveBeenCalledWith(expect.objectContaining({ this_mac: 'paired' }));
  }, 10_000);

  it('starts over for another account signing in here, clearing the last one\'s refused Not now', async () => {
    record.update.mockRejectedValue(new Error('refused'));
    const user = userEvent.setup();
    const view = open();
    await user.click(await screen.findByRole('button', { name: 'Not now' }));
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    account.userId = 'u2';
    view.rerender(routes());
    expect(await pairButton()).toBeInTheDocument();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('carries on the computer step where setup already runs in the app', async () => {
    record.facts = facts({ progress: { ...facts().progress as object, setup_on: 'app', runs_on: 'own_computer' } });
    shell.enableRemoteControl.mockImplementation(async () => {
      (getSshPairing as Mock).mockResolvedValue(READY);
      return { ok: true, status: PAIRED };
    });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    await user.click(await screen.findByRole('button', { name: 'Continue' }, { timeout: 5000 }).then(async (b) => {
      await waitFor(() => expect(b).toBeEnabled());
      return b;
    }));
    await waitFor(() => expect(record.finish).toHaveBeenCalled());
    expect(record.update).toHaveBeenCalledWith({ this_mac: 'paired' });
  });
});

describe('where pairing cannot apply', () => {
  it('records unavailable in a browser and goes on to the platform questions with no answers', async () => {
    shell.inShell = false;
    record.facts = facts({ answers: null, progress: { ...facts().progress as object, models: null, setup_on: null } });
    open('/setup/this-mac?then=https%3A%2F%2Fevil.example%2Fx');
    await waitFor(() => expect(shell.openConsolePage).toHaveBeenCalledWith('/account/onboarding/use', { inPlace: true }));
    expect(record.update).toHaveBeenCalledWith({ this_mac: 'unavailable' });
    // Nothing was asked here, so the desktop app's first run may still ask.
    expect(markAskedOnThisMac).not.toHaveBeenCalled();
  });

  it('follows a then on the platform for a user with no answers', async () => {
    shell.inShell = false;
    record.facts = facts({ answers: null, progress: { ...facts().progress as object, models: null, setup_on: null, this_mac: 'declined' } });
    const then = `${window.location.origin}/account/onboarding/use?from=app`;
    open(`/setup/this-mac?then=${encodeURIComponent(then)}`);
    await waitFor(() => expect(shell.openConsolePage).toHaveBeenCalledWith(then, { inPlace: true }));
    expect(record.update).not.toHaveBeenCalled();
  });

  it('skips a Mac that already is a computer, keeping an answer already given', async () => {
    shell.getRemoteControl.mockResolvedValue(PAIRED);
    (getComputers as Mock).mockResolvedValue({ computers: [MAC_ROW], total: 1 });
    record.facts = facts({ progress: { ...facts().progress as object, this_mac: 'paired' } });
    open();
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/continue');
    expect(record.update).not.toHaveBeenCalled();
  });

  it('moves on without an answer where the shell could not say whether this Mac can pair', async () => {
    shell.getRemoteControl.mockResolvedValue(null);
    open();
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/continue');
    expect(record.update).not.toHaveBeenCalled();
  });

  it('reads the computers list again where it failed, recording nothing', async () => {
    (getComputers as Mock).mockRejectedValueOnce(new Error('down'));
    const user = userEvent.setup();
    open();
    expect(await screen.findByTestId('setup-load-failed')).toHaveTextContent("This step couldn't load.");
    expect(record.update).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(await pairButton()).toBeInTheDocument();
  });

  it('reads the providers again where they failed, recording nothing', async () => {
    (getComputerProviders as Mock).mockRejectedValueOnce(new Error('down'));
    const user = userEvent.setup();
    open();
    expect(await screen.findByTestId('setup-load-failed')).toHaveTextContent("This step couldn't load.");
    expect(record.update).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(await pairButton()).toBeInTheDocument();
  });

  it('reads the record again where it failed', async () => {
    const retryProgress = vi.fn();
    record.facts = facts({ progress: null, progressFailed: true, retryProgress });
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('button', { name: 'Try again' }));
    expect(retryProgress).toHaveBeenCalled();
  });

  it('offers the skip again where recording it failed', async () => {
    shell.inShell = false;
    record.update.mockRejectedValueOnce(new Error('down'));
    const user = userEvent.setup();
    open();
    expect(await screen.findByTestId('setup-load-failed')).toHaveTextContent("This step couldn't be saved. Try again.");
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/continue');
    expect(record.update).toHaveBeenCalledTimes(2);
  });
});

describe('a folder another computer marks', () => {
  it('pairs once the user takes the folder over', async () => {
    (createComputer as Mock).mockRejectedValueOnce(Object.assign(new Error('Request failed with status code 409'), {
      response: { status: 409, data: { detail: { code: 'folder_marked', message: 'marked', owner: 'feed0001' } }, headers: {} },
    }));
    shell.enableRemoteControl.mockImplementation(async () => {
      (getSshPairing as Mock).mockResolvedValue(READY);
      return { ok: true, status: PAIRED };
    });
    const user = userEvent.setup();
    open();
    await user.click(await pairButton());
    await user.click(await screen.findByRole('button', { name: 'Take over this folder' }, { timeout: 5000 }));
    await waitFor(() => expect(createComputer).toHaveBeenCalledTimes(2));
    expect((createComputer as Mock).mock.calls[1][0]).toEqual(expect.objectContaining({ ssh: expect.objectContaining({ take_over: 'feed0001' }) }));
    expect(await screen.findByRole('heading', { name: 'This Mac is paired' }, { timeout: 5000 })).toBeInTheDocument();
  }, 15000);
});
