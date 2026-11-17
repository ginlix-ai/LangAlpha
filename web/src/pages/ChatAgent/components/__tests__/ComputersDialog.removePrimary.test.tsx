/**
 * Removing the primary when it is a machine the user brings: the confirmation
 * asks where Home moves and sends one DELETE naming it (`home_to`). The
 * server moves Home and removes the computer once Home is there; the dialog
 * follows Home's move, then reads how the removal ended: gone, with what it
 * left behind, or why it stays. The user's only computer can't be removed,
 * and the refusals the route names are worded here.
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
import { toast } from '@/components/ui/use-toast';

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import {
  deleteComputer,
  getComputerProviders,
  getComputerRemoval,
  getComputers,
  getComputerStorage,
  getHomePlacement,
  getHomeWorkspace,
  getWorkspace,
  getWorkspaceMove,
  getWorkspaces,
  makeComputerPrimary,
} from '../../utils/api';
import { DAYTONA_ONLY, LIVE_LIST, PRIMARY_ID, SECOND_ID, openDetails, openDialog as open } from './computersDialogHarness';

const HOME_ID = '9e2a7c41-5b3d-4f8e-a1c6-2d7b8e4f0a93';
const MOVE_ID = '6f0b2d8a-1c4e-4a7b-9e3d-5b8c2a1f7e64';

const mockGetComputers = getComputers as Mock;
const mockDelete = deleteComputer as Mock;
const mockRemoval = getComputerRemoval as Mock;
const mockMakePrimary = makeComputerPrimary as Mock;
const mockGetMove = getWorkspaceMove as Mock;
const mockPlacement = getHomePlacement as Mock;

const [HOSTED, SECOND] = LIVE_LIST.computers;

/** The list with the first machine a Mac the user brought, holding Home as the primary. */
function macPrimary(tunnelConnected: boolean | null = true, others = [{ ...SECOND, status: 'running', provider_ref: 'sbx-2' }]) {
  const mac = {
    ...HOSTED,
    kind: 'ssh',
    name: 'Office Mac',
    workspace_count: 0,
    root_dir: '~/langalpha',
    files_backup: true,
    ssh: {
      host: 'office', port: 22, username: 'dev', fingerprint: 'SHA256:x',
      tunnel: true, tunnel_connected: tunnelConnected, helper: true,
    },
  };
  return { computers: [mac, ...others], total: 1 + others.length };
}

/** The list once the server removed the Mac: Home is on the second machine, now primary. */
function macGone() {
  return { computers: [{ ...SECOND, status: 'running', provider_ref: 'sbx-2', is_primary: true }], total: 1 };
}

/** The list with the Mac still on it, its removal ended as `state` says. */
function macKept(state: string, error: { code: string; message: string } | null, homeMoved: boolean) {
  const list = macPrimary();
  const [mac, second] = list.computers;
  return {
    ...list,
    computers: [
      { ...mac, is_primary: !homeMoved, removal: { state, move_id: MOVE_ID, error } },
      { ...second, is_primary: homeMoved },
    ],
  };
}

type List = ReturnType<typeof macPrimary>;

/** The removal as the server reads it off `list`: the Mac's own, or removed once it is gone. */
function removalOn(list: List, removed: unknown = null) {
  const mac = list.computers.find((c) => c.computer_id === PRIMARY_ID) as { removal?: unknown } | undefined;
  return mac ? mac.removal : { state: 'removed', move_id: MOVE_ID, error: null, removed };
}

/** DELETE accepted with a 202: Home moves first, and `after` is what the server reads once it has. */
function acceptRemoval(after: { move: ReturnType<typeof homeMove>; list: List; removed?: unknown }) {
  mockDelete.mockImplementation(async () => {
    mockGetMove.mockResolvedValue(after.move);
    mockGetComputers.mockResolvedValue(after.list);
    mockRemoval.mockResolvedValue(removalOn(after.list, after.removed));
    return { key: null, files: null, token: null, workspaces_deleted: null, move_id: MOVE_ID };
  });
}

function homeMove(phase: string) {
  return {
    move_id: MOVE_ID,
    workspace_id: HOME_ID,
    source_computer_id: PRIMARY_ID,
    target_computer_id: SECOND_ID,
    phase,
    make_primary: true,
    from_backup: false,
    error: null as { code: string; message: string } | null,
    created_at: '2026-11-15T09:00:00Z',
  };
}

function placement(patch: Partial<HomePlacement> = {}): HomePlacement {
  return {
    computer_id: PRIMARY_ID,
    computer_name: 'Office Mac',
    computer_kind: 'ssh',
    unreachable: false,
    stand_in: { computer_id: SECOND_ID, name: 'Second machine', status: 'running' },
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

async function openRemove(name: string) {
  const user = userEvent.setup();
  open();
  await openDetails(user, name);
  await user.click(screen.getByRole('button', { name: 'Remove' }));
  return { user, confirm: await screen.findByRole('dialog', { name: `Remove ${name}?` }) };
}

beforeEach(() => {
  vi.clearAllMocks();
  runAuthResets();
  mockGetComputers.mockResolvedValue(macPrimary());
  (getComputerProviders as Mock).mockResolvedValue({ providers: DAYTONA_ONLY });
  (getWorkspaces as Mock).mockResolvedValue({ workspaces: [], total: 0 });
  (getComputerStorage as Mock).mockResolvedValue({ live: true, disk: null, workspaces: [], other_bytes: 0 });
  (getHomeWorkspace as Mock).mockResolvedValue({ workspace_id: HOME_ID, name: 'Home', computer_id: PRIMARY_ID, status: 'running' });
  (getWorkspace as Mock).mockResolvedValue({ workspace_id: HOME_ID, name: 'Home', computer_id: PRIMARY_ID, status: 'running' });
  mockGetMove.mockResolvedValue(null);
  mockPlacement.mockResolvedValue(placement());
  mockDelete.mockResolvedValue({ computer_id: PRIMARY_ID, workspaces_deleted: [], key: null, files: null, token: null });
});

describe('ComputersDialog: removing a primary the user brought', () => {
  it('sends one DELETE naming where Home goes, follows the move, and is done once the computer is gone', async () => {
    acceptRemoval({ move: homeMove('done'), list: macGone() });
    const { user, confirm } = await openRemove('Office Mac');
    const picker = within(confirm).getByRole('radiogroup', { name: 'Home moves to' });
    expect(within(picker).getByRole('radio', { name: /^Second machine/ })).toBeChecked();

    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Remove Office Mac?' })).toBeNull());
    expect(mockDelete).toHaveBeenCalledTimes(1);
    expect(mockDelete).toHaveBeenCalledWith(PRIMARY_ID, {
      removeKey: false, deleteWorkspaces: [], deleteFiles: false, homeTo: SECOND_ID,
    });
    expect(mockGetMove).toHaveBeenCalledWith(HOME_ID);
    expect(mockMakePrimary).not.toHaveBeenCalled();
    expect(toast).toHaveBeenCalledWith(expect.objectContaining({ title: 'Office Mac removed' }));
  });

  it('says what a removal after Home\'s move left on the machine', async () => {
    acceptRemoval({
      move: homeMove('done'),
      list: macGone(),
      removed: { key: { outcome: 'unreached', used_by: null, fingerprint: null, comment: null }, files: null, token: null, workspaces_deleted: [] },
    });
    const { user, confirm } = await openRemove('Office Mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));

    await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({
      title: 'Office Mac removed',
      description: 'We could not reach the machine, so our key is still there.',
      duration: Infinity,
    })));
    expect(mockRemoval).toHaveBeenCalledWith(PRIMARY_ID);
  });

  it('removes at once when Home had nothing to move', async () => {
    const { user, confirm } = await openRemove('Office Mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({ title: 'Office Mac removed' })));
    expect(mockDelete).toHaveBeenCalledTimes(1);
  });

  it("says Home is restored from its backup when Home's placement says the machine can't be reached", async () => {
    // The row's tunnel flag is as old as the list; the placement is Home's own reading.
    mockGetComputers.mockResolvedValue(macPrimary(true));
    mockPlacement.mockResolvedValue(placement({ unreachable: true }));
    const { user, confirm } = await openRemove('Office Mac');
    expect(
      await within(confirm).findByText("Office Mac can't be reached, so Home is restored on Second machine from its last backup."),
    ).toBeInTheDocument();

    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith(PRIMARY_ID, expect.objectContaining({ homeTo: SECOND_ID })));
  });

  it("holds a machine of the user's own that can't be reached, and picks the next", async () => {
    const asleep = {
      ...SECOND,
      computer_id: 'c-asleep',
      name: 'Laptop',
      kind: 'ssh',
      status: 'running',
      provider_ref: 'ssh-1',
      files_backup: true,
      ssh: { host: 'laptop', port: 22, username: 'dev', fingerprint: 'SHA256:y', tunnel: true, tunnel_connected: false, helper: true },
    };
    mockGetComputers.mockResolvedValue(macPrimary(true, [asleep, { ...SECOND, status: 'running', provider_ref: 'sbx-2' }]));
    const { confirm } = await openRemove('Office Mac');
    const picker = within(confirm).getByRole('radiogroup', { name: 'Home moves to' });
    const laptop = within(picker).getByRole('radio', { name: /^Laptop/ });
    expect(laptop).toBeDisabled();
    expect(laptop.closest('label') ?? laptop.parentElement).toHaveTextContent("Can't be reached");
    expect(within(picker).getByRole('radio', { name: /^Second machine/ })).toBeChecked();
  });

  it('says how the removal ended once Home stopped moving after the dialog closed', async () => {
    acceptRemoval({ move: homeMove('preparing'), list: macPrimary() });
    const user = userEvent.setup();
    const view = open();
    await openDetails(user, 'Office Mac');
    await user.click(screen.getByRole('button', { name: 'Remove' }));
    const confirm = await screen.findByRole('dialog', { name: 'Remove Office Mac?' });
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));
    expect(await within(confirm).findByRole('button', { name: 'Moving Home…' })).toBeDisabled();

    view.unmount();
    const kept = macKept('home_did_not_move', { code: 'target_unavailable', message: 'Second machine stopped answering.' }, false);
    mockGetMove.mockResolvedValue({ ...homeMove('failed'), error: { code: 'target_unavailable', message: 'Second machine stopped answering.' } });
    mockGetComputers.mockResolvedValue(kept);
    mockRemoval.mockResolvedValue(removalOn(kept));
    await waitFor(
      () => expect(toast).toHaveBeenCalledWith(expect.objectContaining({
        variant: 'destructive',
        title: "Couldn't remove Office Mac",
        description: 'Second machine stopped answering.',
      })),
      { timeout: 5000 },
    );
    expect(mockDelete).toHaveBeenCalledTimes(1);
  });

  it("says Home didn't move, and the computer stays", async () => {
    acceptRemoval({
      move: { ...homeMove('failed'), error: { code: 'target_unavailable', message: 'Second machine stopped answering.' } },
      list: macKept('home_did_not_move', { code: 'target_unavailable', message: 'Second machine stopped answering.' }, false),
    });
    const { user, confirm } = await openRemove('Office Mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));

    expect(await within(confirm).findByRole('alert')).toHaveTextContent('Second machine stopped answering.');
    expect(mockDelete).toHaveBeenCalledTimes(1);
    // The dialog says it, so no toast does.
    expect(toast).not.toHaveBeenCalled();
  });

  it("says Home moved but the computer wasn't removed, and why", async () => {
    acceptRemoval({
      move: homeMove('done'),
      list: macKept('not_removed', { code: 'has_workspaces', message: 'A workspace is on it.' }, true),
    });
    const { user, confirm } = await openRemove('Office Mac');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));

    expect(await within(confirm).findByRole('alert')).toHaveTextContent(
      "Home moved to Second machine, but Office Mac wasn't removed. A workspace not listed here is on it now. Check the list and try again.",
    );
  });

  it('holds the confirmation while a removal from before it opened still waits on Home', async () => {
    mockGetComputers.mockResolvedValue(macKept('waiting_for_home', null, false));
    mockGetMove.mockResolvedValue(homeMove('backing_up'));
    const { confirm } = await openRemove('Office Mac');
    expect(await within(confirm).findByRole('button', { name: 'Moving Home…' })).toBeDisabled();
  });

  it("says nothing of a removal whose move is not Home's latest", async () => {
    mockGetComputers.mockResolvedValue(macKept('home_did_not_move', { code: 'cancelled', message: 'Home stayed.' }, false));
    mockGetMove.mockResolvedValue({ ...homeMove('done'), move_id: 'another-move' });
    const { confirm } = await openRemove('Office Mac');
    expect(within(confirm).queryByRole('alert')).toBeNull();
  });

  it('offers Add a computer, not Remove, on the only computer', async () => {
    mockGetComputers.mockResolvedValue(macPrimary(true, []));
    const { user, confirm } = await openRemove('Office Mac');
    expect(within(confirm).getByText("This is your only computer, so it can't be removed. Add another computer first.")).toBeInTheDocument();
    expect(within(confirm).getByRole('button', { name: 'Remove computer' })).toBeDisabled();

    await user.click(within(confirm).getByRole('button', { name: 'Add a computer' }));
    expect(await screen.findByRole('dialog', { name: 'New computer' })).toBeInTheDocument();
    expect(mockMakePrimary).not.toHaveBeenCalled();
    expect(mockDelete).not.toHaveBeenCalled();
  });
});

describe('ComputersDialog: refusals to remove', () => {
  beforeEach(() => {
    mockGetComputers.mockResolvedValue(LIVE_LIST);
  });

  it('offers Add a computer when the server says it is the only computer', async () => {
    mockDelete.mockRejectedValue(conflict('only_computer', 'This is your only computer.'));
    const { user, confirm } = await openRemove('Second machine');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));

    const alert = await within(confirm).findByRole('alert');
    expect(alert).toHaveTextContent("This is your only computer, so it can't be removed. Add another computer first.");
    expect(within(alert).getByRole('button', { name: 'Add a computer' })).toBeInTheDocument();
  });

  it('names the machine Home is waiting on when a hand-back is owed', async () => {
    mockDelete.mockRejectedValue(conflict('handback_owed', 'Files are still to be copied back.'));
    const { user, confirm } = await openRemove('Second machine');
    await user.click(within(confirm).getByRole('button', { name: 'Remove computer' }));

    expect(await within(confirm).findByRole('alert')).toHaveTextContent(
      "Files written on this computer while Office Mac was away haven't been copied back into Home yet. Try again once Office Mac is back.",
    );
  });

  it('says which of Home\'s files a removal deletes once a hand-back left them there', async () => {
    mockPlacement.mockResolvedValue(placement({ left_on_stand_in: { failed: false, paths: ['huge.bin'], computer_id: SECOND_ID } }));
    const { confirm } = await openRemove('Second machine');

    expect(await within(confirm).findByTestId('remove-computer-home-files')).toHaveTextContent(
      'Left here and not in Home: huge.bin. Removing this computer deletes them.',
    );
  });

  it('says a given-up hand-back\'s files go with the computer', async () => {
    mockPlacement.mockResolvedValue(placement({ left_on_stand_in: { failed: true, paths: [], computer_id: SECOND_ID } }));
    const { confirm } = await openRemove('Second machine');

    expect(await within(confirm).findByTestId('remove-computer-home-files')).toHaveTextContent(
      "Files written here while Office Mac was away couldn't be copied into Home. Removing this computer deletes them.",
    );
  });
});
