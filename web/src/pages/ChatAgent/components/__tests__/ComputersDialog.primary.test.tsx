/**
 * Make primary on any computer, a machine the user brings included. What the
 * confirmation says depends on where Home stands (`GET /workspaces/home/placement`):
 * which cloud computer stands in while the machine can't be reached, none, or
 * Home's own computer being unreachable, when Home is restored from its backup.
 * A machine with its file backup off is asked to turn it on first, which
 * Make primary does in the same request.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { runAuthResets } from '@/lib/authResets';
import type { HomePlacement } from '../../utils/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import {
  getComputerProviders,
  getComputers,
  getComputerStorage,
  getHomePlacement,
  getHomeWorkspace,
  getWorkspace,
  getWorkspaceMove,
  getWorkspaces,
  makeComputerPrimary,
  setComputerFilesBackup,
} from '../../utils/api';
import { DAYTONA_ONLY, PRIMARY_ID, SECOND_ID, openDetails, openDialog as open, withSecond } from './computersDialogHarness';

const HOME_ID = '9e2a7c41-5b3d-4f8e-a1c6-2d7b8e4f0a93';

const mockGetComputers = getComputers as Mock;
const mockPlacement = getHomePlacement as Mock;
const mockMakePrimary = makeComputerPrimary as Mock;
const mockSetFilesBackup = setComputerFilesBackup as Mock;

/** The second machine as one its owner brought, reached through its tunnel. */
function ownMachine(patch: Record<string, unknown> = {}) {
  return withSecond({
    kind: 'ssh',
    name: 'Office Mac',
    status: 'running',
    provider_ref: 'ssh-1',
    files_backup: true,
    ssh: {
      host: 'office', port: 22, username: 'dev', fingerprint: 'SHA256:x',
      tunnel: true, tunnel_connected: true, helper: true,
    },
    ...patch,
  });
}

function placement(patch: Partial<HomePlacement> = {}): HomePlacement {
  return {
    computer_id: PRIMARY_ID,
    computer_name: 'Alpha Research',
    computer_kind: 'daytona',
    unreachable: false,
    stand_in: { computer_id: PRIMARY_ID, name: 'Alpha Research', status: 'running' },
    handback_owed: false,
    handback: null,
    left_on_stand_in: null,
    move: null,
    ...patch,
  };
}

function conflict(code: string, message: string) {
  return Object.assign(new Error('Request failed with status code 409'), {
    status: 409,
    response: { status: 409, data: { detail: { code, message } }, headers: {} },
  });
}

async function openMakePrimary(name = 'Office Mac') {
  const user = userEvent.setup();
  open();
  await openDetails(user, name);
  await user.click(screen.getByRole('button', { name: 'Make primary' }));
  return { user, dialog: await screen.findByRole('dialog', { name: /primary\?$|^Turn on backup/ }) };
}

beforeEach(() => {
  vi.clearAllMocks();
  runAuthResets();
  mockGetComputers.mockResolvedValue(ownMachine());
  (getComputerProviders as Mock).mockResolvedValue({ providers: DAYTONA_ONLY });
  (getWorkspaces as Mock).mockResolvedValue({ workspaces: [], total: 0 });
  (getComputerStorage as Mock).mockResolvedValue({ live: true, disk: null, workspaces: [], other_bytes: 0 });
  (getHomeWorkspace as Mock).mockResolvedValue({ workspace_id: HOME_ID, name: 'Home', computer_id: PRIMARY_ID, status: 'running' });
  (getWorkspace as Mock).mockResolvedValue({ workspace_id: HOME_ID, name: 'Home', computer_id: PRIMARY_ID, status: 'running' });
  (getWorkspaceMove as Mock).mockResolvedValue(null);
  mockPlacement.mockResolvedValue(placement());
  mockMakePrimary.mockResolvedValue({ computer_id: SECOND_ID, is_primary: true, move: null });
  mockSetFilesBackup.mockImplementation(async (computerId: string, enabled: boolean) => ({
    ...ownMachine().computers[1], computer_id: computerId, files_backup: enabled,
  }));
});

describe('ComputersDialog: making a machine the user brings primary', () => {
  it("holds Make primary on a machine that can't be reached, and says why", async () => {
    mockGetComputers.mockResolvedValue(ownMachine({
      ssh: { host: 'office', port: 22, username: 'dev', fingerprint: 'SHA256:x', tunnel: true, tunnel_connected: false, helper: true },
    }));
    const user = userEvent.setup();
    open();
    await openDetails(user, 'Office Mac');
    const button = screen.getByRole('button', { name: 'Make primary' });
    expect(button).toBeDisabled();
    expect(button).toHaveAccessibleDescription("You can make it primary once it's connected.");
  });

  it('says why a make primary asked with the create was dropped, beside Make primary', async () => {
    mockGetComputers.mockResolvedValue(ownMachine({
      make_primary: { state: 'dropped', error: { code: 'turn_active', message: 'A turn is running in Home.' } },
    }));
    const user = userEvent.setup();
    open();
    await openDetails(user, 'Office Mac');
    expect(screen.getByTestId('computer-primary-dropped')).toHaveTextContent(
      "Office Mac wasn't made primary. A turn is running in Home.",
    );
    expect(screen.getByRole('button', { name: 'Make primary' })).toBeEnabled();
  });

  it('names the cloud computer that stands in while the machine is away', async () => {
    const { user, dialog } = await openMakePrimary();
    expect(await within(dialog).findByText(/the Chief of Staff works on Alpha Research, starting it if it's stopped/)).toBeInTheDocument();

    await user.click(within(dialog).getByRole('button', { name: 'Make primary' }));
    await waitFor(() => expect(mockMakePrimary).toHaveBeenCalledWith(SECOND_ID, { fromBackup: false, turnOnBackup: false }));
    expect(mockSetFilesBackup).not.toHaveBeenCalled();
  });

  it('says the Chief of Staff waits where the user has no cloud computer', async () => {
    mockPlacement.mockResolvedValue(placement({ stand_in: null }));
    const { dialog } = await openMakePrimary();
    expect(
      await within(dialog).findByText("New workspaces will be created on Office Mac, and Home moves there too. While Office Mac can't be reached, the Chief of Staff waits for it."),
    ).toBeInTheDocument();
  });

  it("restores Home from its backup when Home's computer can't be reached", async () => {
    // Home is on Old Mac, the primary, which is gone.
    const [alpha, office] = ownMachine().computers;
    mockGetComputers.mockResolvedValue({
      computers: [
        { ...alpha, is_primary: false },
        office,
        {
          ...office,
          computer_id: 'old-mac',
          name: 'Old Mac',
          is_primary: true,
          ssh: { host: 'old', port: 22, username: 'dev', fingerprint: 'SHA256:y', tunnel: true, tunnel_connected: false, helper: true },
        },
      ],
      total: 3,
    });
    (getWorkspace as Mock).mockResolvedValue({ workspace_id: HOME_ID, name: 'Home', computer_id: 'old-mac', status: 'stopped' });
    mockPlacement.mockResolvedValue(placement({ computer_id: 'old-mac', computer_name: 'Old Mac', computer_kind: 'ssh', unreachable: true }));
    const { user, dialog } = await openMakePrimary('Alpha Research');
    expect(
      await within(dialog).findByText("Old Mac can't be reached, so Home is restored on Alpha Research from its last backup."),
    ).toBeInTheDocument();

    await user.click(within(dialog).getByRole('button', { name: 'Make primary' }));
    await waitFor(() => expect(mockMakePrimary).toHaveBeenCalledWith(PRIMARY_ID, { fromBackup: true, turnOnBackup: false }));
  });

  it('asks to turn backup on first, then turns it on and makes it primary in one request', async () => {
    mockGetComputers.mockResolvedValue(ownMachine({ files_backup: false }));
    const { user, dialog } = await openMakePrimary();
    expect(dialog).toHaveAccessibleName('Turn on backup for Office Mac?');
    expect(within(dialog).getByText("Home will live on this computer. With backup on, Home can be restored if it's lost.")).toBeInTheDocument();

    await user.click(within(dialog).getByRole('button', { name: 'Turn on and make primary' }));
    await waitFor(() => expect(mockMakePrimary).toHaveBeenCalledWith(SECOND_ID, { fromBackup: false, turnOnBackup: true }));
    expect(mockSetFilesBackup).not.toHaveBeenCalled();
  });

  it('asks to turn backup on when the server refuses for it', async () => {
    mockMakePrimary.mockRejectedValueOnce(conflict('not_backed_up', 'File backup is off on this computer.'));
    const { user, dialog } = await openMakePrimary();
    await user.click(within(dialog).getByRole('button', { name: 'Make primary' }));

    expect(await within(dialog).findByRole('button', { name: 'Turn on and make primary' })).toBeInTheDocument();
    expect(dialog).toHaveAccessibleName('Turn on backup for Office Mac?');
    expect(within(dialog).queryByRole('alert')).not.toBeInTheDocument();
  });
});
