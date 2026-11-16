/**
 * The desktop app's first run: asked only in the desktop app on a Mac that
 * can pair itself, once per account and Mac, while no computer is this Mac,
 * and nothing under it (Home's warm-up among it) runs until it has its
 * answer. Whether this Mac was asked is all the gate reads, so a launch after
 * that holds on nothing. "Work on this Mac" adds the Mac, says plainly that refusing backup
 * keeps it from being primary, and asks the server with the create to move
 * Home to it from a cloud computer or another machine once the Mac's first
 * start is through. The answer is saved on the user (`PUT /users/me`).
 */
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

import { WarmHome } from '@/components/WarmHome';
import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

const shell = vi.hoisted(() => ({
  mac: true,
  remote: true,
  status: {
    supported: true, service: 'off', helper: null, paired: false, fingerprint: null as string | null, account: 'alex',
  },
}));
vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  canAddThisMac: () => shell.mac && shell.remote,
  canRemoteControl: () => shell.remote,
  getRemoteControl: async () => shell.status,
}));

const user = vi.hoisted(() => ({
  updateCurrentUser: vi.fn(async (fields: Record<string, unknown>) => fields),
}));
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  updateCurrentUser: user.updateCurrentUser,
}));

const api = vi.hoisted(() => ({
  computers: [] as Partial<Computer>[],
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(async () => ({ providers: [{ kind: 'daytona' }, { kind: 'ssh', allowed: true }], tunnels: true })),
  createComputer: vi.fn(),
  makeComputerPrimary: vi.fn(),
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1' })),
}));
api.getComputers.mockImplementation(async () => ({ computers: api.computers }));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getComputers: api.getComputers,
  getComputerProviders: api.getComputerProviders,
  createComputer: api.createComputer,
  makeComputerPrimary: api.makeComputerPrimary,
  getHomeWorkspace: api.getHomeWorkspace,
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

// The pairing itself is the Computers dialog's, tested there: here it is
// already done, and the user's backup pick is all that varies.
const pairing = vi.hoisted(() => ({ filesBackup: true }));
vi.mock('@/pages/ChatAgent/components/remoteControlSetupState', () => ({
  useRemoteControlSetup: () => ({
    acknowledged: true,
    setAcknowledged: () => {},
    phase: { kind: 'ready' },
    action: null,
    start: () => {},
    readyStep: { filesBackup: pairing.filesBackup },
    ready: { probe: {}, target: {} },
    create: {
      ssh: { host: 'localhost', port: 22, username: 'alex', host_key: 'ssh-ed25519 AAAA', tunnel_id: 'tun-1' },
      files_backup: pairing.filesBackup,
    },
    defaultName: 'alex-mac',
  }),
}));
vi.mock('@/pages/ChatAgent/components/remoteControlSetup', () => ({
  RemoteControlSetup: () => <div data-testid="pairing" />,
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', preference: 'light', setPreference: () => {}, toggleTheme: () => {} }),
}));

import { ThisMacFirstRunGate } from '../ThisMacFirstRunGate';
import { askedOnThisMac, markAskedOnThisMac } from '../computerChoiceGate';

/** The page loads as a chunk of its own, which a busy run can take a while to hand over. */
const WAIT = { timeout: 5000 };

const CLOUD_HOME: Partial<Computer> = {
  computer_id: 'c-cloud', name: 'Cloud One', kind: 'daytona', status: 'running', is_primary: true,
};

const OFFICE_HOME: Partial<Computer> = {
  computer_id: 'c-office', name: 'Office Mac', kind: 'ssh', status: 'running', is_primary: true,
  provider_ref: 'ssh-1', ssh: { fingerprint: 'SHA256:office' } as Computer['ssh'],
};

function macRow(filesBackup: boolean): Partial<Computer> {
  return {
    computer_id: 'c-mac', name: 'Alex Mac', kind: 'ssh', status: 'stopped', is_primary: false,
    root_dir: '/Users/alex/langalpha', files_backup: filesBackup, ssh: { fingerprint: 'SHA256:mac' } as Computer['ssh'],
  };
}

function draw() {
  return renderWithProviders(
    <ThisMacFirstRunGate>
      <WarmHome />
      <div data-testid="app" />
    </ThisMacFirstRunGate>,
  );
}

async function tap() {
  await act(async () => {
    document.body.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true }));
  });
}

// The first run is its own chunk, with framer beside it: loaded once here,
// so no test's first wait is spent on the import.
beforeAll(async () => {
  await Promise.all([import('../ThisMacFirstRun'), import('@/lib/framer')]);
});

beforeEach(() => {
  shell.mac = true;
  shell.remote = true;
  shell.status.fingerprint = null;
  pairing.filesBackup = true;
  api.computers = [];
  api.createComputer.mockImplementation(async () => {
    const row = macRow(pairing.filesBackup);
    api.computers = [...api.computers, row];
    return row;
  });
});

afterEach(() => {
  vi.clearAllMocks();
  document.body.replaceChildren();
  localStorage.clear();
});

describe('the first-run gate', () => {
  it('lets a browser straight through and asks nothing', async () => {
    shell.mac = false;
    draw();
    expect(screen.getByTestId('app')).toBeInTheDocument();
    expect(api.getComputers).not.toHaveBeenCalled();
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalled();
  });

  it('asks in the desktop app on a Mac not yet asked, and holds Home until it answers', async () => {
    draw();
    expect(await screen.findByText('Where should your agents work?', {}, WAIT)).toBeInTheDocument();
    expect(screen.queryByTestId('app')).toBeNull();
    await tap();
    expect(warm.warmWorkspace).not.toHaveBeenCalled();

    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Use the cloud' }));
    });
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'cloud' });
    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
    await tap();
    expect(warm.warmWorkspace).toHaveBeenCalled();
  });

  it('does not hold a Mac already asked, nor read anything first', () => {
    markAskedOnThisMac();
    draw();
    expect(screen.getByTestId('app')).toBeInTheDocument();
    expect(api.getComputers).not.toHaveBeenCalled();
  });

  it('asks on another Mac although the account answered on one', async () => {
    draw();
    expect(await screen.findByText('Where should your agents work?', {}, WAIT)).toBeInTheDocument();
    expect(screen.queryByTestId('app')).toBeNull();
  });

  it('does not ask when this Mac is already one of the computers, nor again', async () => {
    shell.status.fingerprint = 'SHA256:mac';
    api.computers = [macRow(true)];
    draw();
    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
    expect(screen.queryByText('Where should your agents work?')).toBeNull();
    expect(askedOnThisMac('local-dev-user')).toBe(true);
  });

  // Let through unasked, Home's warm-up would build a cloud computer for a
  // user who never chose one.
  it('holds on a server read that failed, and asks once it reads', async () => {
    api.getComputerProviders.mockRejectedValueOnce(new Error('offline'));
    draw();
    expect(
      await screen.findByText(
        "Couldn't check this Mac with the server. Try again, or continue without setting it up.",
        {},
        WAIT,
      ),
    ).toBeInTheDocument();
    expect(screen.queryByTestId('app')).toBeNull();

    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    });

    expect(await screen.findByRole('button', { name: 'Work on this Mac' }, WAIT)).toBeInTheDocument();
  });

  it('opens the app on Continue anyway, and asks again next launch', async () => {
    api.getComputerProviders.mockRejectedValueOnce(new Error('offline'));
    draw();
    await act(async () => {
      fireEvent.click(await screen.findByRole('button', { name: 'Continue anyway' }, WAIT));
    });

    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
    expect(askedOnThisMac('local-dev-user')).toBe(false);
  });

  // An account from before the first run, whose Mac is already a computer,
  // never saved a choice: the next launch must not hold on that.
  it('does not hold the next launch of a user with no saved choice whose Mac is already added', async () => {
    shell.status.fingerprint = 'SHA256:mac';
    api.computers = [macRow(true)];
    const first = draw();
    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
    expect(user.updateCurrentUser).not.toHaveBeenCalled();
    first.unmount();
    api.getComputers.mockClear();

    draw();
    expect(screen.getByTestId('app')).toBeInTheDocument();
    expect(api.getComputers).not.toHaveBeenCalled();
  });

  it('offers to keep Home on the machine it is on, which saves no cloud answer', async () => {
    api.computers = [OFFICE_HOME];
    draw();
    expect(
      await screen.findByText(
        'Work on this Mac moves Home here from Office Mac and makes this Mac your primary computer. Office Mac stays one of your computers.',
        {},
        WAIT,
      ),
    ).toBeInTheDocument();
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Keep Home on Office Mac' }));
    });
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
  });

  it('says to a user with a cloud Home that working here moves Home, and offers to keep the cloud', async () => {
    api.computers = [CLOUD_HOME];
    draw();
    expect(await screen.findByText(/moves Home and makes this Mac your primary computer/, {}, WAIT)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Keep using the cloud' })).toBeInTheDocument();
  });
});

describe('working on this Mac', () => {
  async function addThisMac() {
    draw();
    const work = await screen.findByRole('button', { name: 'Work on this Mac' }, WAIT);
    await act(async () => {
      fireEvent.click(work);
    });
    expect(screen.getByTestId('pairing')).toBeInTheDocument();
  }

  // The server moves Home once the Mac's first start is through, so the
  // first run is over as soon as the Mac is added and the answer saved.
  it('asks with the create to make the Mac primary for a user with a cloud Home, which moves Home', async () => {
    api.computers = [CLOUD_HOME];
    api.createComputer.mockImplementation(async () => {
      const row = { ...macRow(true), status: 'starting', provider_ref: null };
      api.computers = [CLOUD_HOME, row];
      return row;
    });
    await addThisMac();
    expect(screen.queryByTestId('this-mac-backup-off')).toBeNull();
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Add this Mac' }));
    });
    expect(await screen.findByText(/Home is moving to this Mac/, {}, WAIT)).toBeInTheDocument();
    expect(api.createComputer).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'ssh', files_backup: true, make_primary: true }),
    );
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    expect(api.makeComputerPrimary).not.toHaveBeenCalled();
    expect(screen.queryByTestId('app')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Get started' }));
    expect(await screen.findByTestId('app', {}, WAIT)).toBeInTheDocument();
  });

  it('says Home moves from the machine it is on, not from the cloud', async () => {
    api.computers = [OFFICE_HOME];
    await addThisMac();
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Add this Mac' }));
    });
    expect(await screen.findByText('Home is moving to this Mac from Office Mac.', {}, WAIT)).toBeInTheDocument();
    expect(screen.queryByText(/cloud/)).toBeNull();
    expect(api.createComputer).toHaveBeenCalledWith(expect.objectContaining({ make_primary: true }));
  });

  it('says a Mac without backup leaves Home on the machine it is on', async () => {
    api.computers = [OFFICE_HOME];
    pairing.filesBackup = false;
    await addThisMac();
    expect(screen.getByTestId('this-mac-backup-off')).toHaveTextContent(
      "Without backup, this Mac can't be your primary computer, so Home stays on Office Mac.",
    );
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Add this Mac' }));
    });
    expect(await screen.findByText(/so Home stays on Office Mac\./, {}, WAIT)).toBeInTheDocument();
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    expect(api.createComputer.mock.calls[0][0]).not.toHaveProperty('make_primary');
  });

  it('says plainly that a Mac without backup cannot be primary, and leaves Home on the cloud', async () => {
    api.computers = [CLOUD_HOME];
    pairing.filesBackup = false;
    await addThisMac();
    expect(screen.getByTestId('this-mac-backup-off')).toHaveTextContent(
      "Without backup, this Mac can't be your primary computer, so Home stays on the cloud.",
    );
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Add this Mac' }));
    });
    expect(await screen.findByText(/Without backup it can't be your primary computer, so Home stays on the cloud/, {}, WAIT)).toBeInTheDocument();
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'cloud' });
    expect(api.createComputer.mock.calls[0][0]).not.toHaveProperty('make_primary');
  });

  it('makes nothing primary by hand for a new user: the Mac is where Home and workspaces go', async () => {
    await addThisMac();
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Add this Mac' }));
    });
    expect(await screen.findByText('Home and new workspaces go on this Mac.', {}, WAIT)).toBeInTheDocument();
    expect(user.updateCurrentUser).toHaveBeenCalledWith({ computer_choice: 'own_machine' });
    expect(api.createComputer.mock.calls[0][0]).not.toHaveProperty('make_primary');
    await waitFor(() => expect(screen.getByText('/Users/alex/langalpha')).toBeInTheDocument());
  });
});
