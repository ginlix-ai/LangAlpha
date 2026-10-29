/**
 * A computer's details, in the rail's place inside the Computers dialog: what
 * the card leaves out, storage by workspace, and the way back to the rail.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import { toast } from '@/components/ui/use-toast';
import { getComputerProviders, getComputers, getComputerStorage, renameComputer } from '../../utils/api';
import { DAYTONA_ONLY, LIVE_LIST, PRIMARY_ID, SECOND_ID, openDetails, openDialog as open } from './computersDialogHarness';

const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;
const mockStorage = getComputerStorage as Mock;
const mockRename = renameComputer as Mock;
const mockToast = toast as unknown as Mock;

beforeEach(() => {
  vi.clearAllMocks();
  mockGetComputers.mockResolvedValue(LIVE_LIST);
  mockProviders.mockResolvedValue({ providers: DAYTONA_ONLY });
  mockStorage.mockResolvedValue({ live: true, disk: null, workspaces: [], other_bytes: 0 });
});

describe('ComputersDialog: details', () => {
  const STORAGE = {
    live: true,
    disk: null,
    workspaces: [
      { workspace_id: 'ws-small', name: 'Small one', dir_name: 'small-one', bytes: 2_000 },
      { workspace_id: 'ws-big', name: 'Big one', dir_name: 'big-one', bytes: 5_000_000 },
    ],
    other_bytes: 1_000_000,
  };

  it('shows a card with Details only, and the rest of the machine in its details', async () => {
    const user = userEvent.setup();
    open();
    const [card] = await screen.findAllByTestId('computer-card');
    expect(within(card).getByRole('button', { name: 'Details' })).toBeInTheDocument();
    for (const name of ['Remove', 'Rename computer', 'Change spec']) {
      expect(within(card).queryByRole('button', { name })).not.toBeInTheDocument();
    }

    await openDetails(user, 'Alpha Research');
    expect(screen.getByRole('button', { name: 'Rename computer' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Change spec' })).toBeInTheDocument();
    // The rail gave way: no cards are left to press.
    await waitFor(() => expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument());
  });

  // The server's refusal of a name the second computer already has.
  const nameTaken = () => Object.assign(new Error('Request failed with status code 409'), {
    status: 409,
    response: {
      status: 409,
      data: {
        detail: {
          code: 'name_taken',
          message: 'You already have a computer named "Second machine".',
          name: 'Second machine',
          computer_id: SECOND_ID,
        },
      },
      headers: {},
    },
  });

  it("brings a rename onto another computer's name back to the field, saying so", async () => {
    const user = userEvent.setup();
    mockRename.mockRejectedValue(nameTaken());
    open();
    await openDetails(user, 'Alpha Research');

    await user.click(screen.getByRole('button', { name: 'Rename computer' }));
    const field = screen.getByRole('textbox', { name: 'Computer name' });
    await user.clear(field);
    await user.type(field, 'second machine{Enter}');

    const said = await screen.findByRole('alert');
    expect(said).toHaveTextContent('You already have a computer named "Second machine". Choose another name.');
    expect(mockRename).toHaveBeenCalledWith(PRIMARY_ID, 'second machine');
    const back = screen.getByRole('textbox', { name: 'Computer name' });
    expect(back).toHaveValue('second machine');
    expect(back).toHaveAccessibleDescription(said.textContent ?? '');
    // Said in the field, not again in a toast.
    expect(mockToast).not.toHaveBeenCalled();

    // Left as it was refused, the field backs out to the name it had.
    await user.tab();
    expect(screen.queryByRole('textbox', { name: 'Computer name' })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(mockRename).toHaveBeenCalledTimes(1);
  });

  it('keeps a name typed after a rename was sent when that rename is refused', async () => {
    const user = userEvent.setup();
    let refuse: (err: unknown) => void = () => {};
    mockRename.mockImplementationOnce(() => new Promise((_, reject) => { refuse = reject; }));
    open();
    await openDetails(user, 'Alpha Research');

    await user.click(screen.getByRole('button', { name: 'Rename computer' }));
    await user.clear(screen.getByRole('textbox', { name: 'Computer name' }));
    await user.type(screen.getByRole('textbox', { name: 'Computer name' }), 'second machine{Enter}');
    // Reopened before the answer, and another name begun.
    await user.click(screen.getByRole('button', { name: 'Rename computer' }));
    const field = screen.getByRole('textbox', { name: 'Computer name' });
    await user.clear(field);
    await user.type(field, 'Lab box');

    const reads = mockGetComputers.mock.calls.length;
    refuse(nameTaken());
    // The rename has settled once the list is read again.
    await waitFor(() => expect(mockGetComputers.mock.calls.length).toBeGreaterThan(reads));
    expect(screen.getByRole('textbox', { name: 'Computer name' })).toHaveValue('Lab box');
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('tells a refusal that lands after the details were left in a toast', async () => {
    const user = userEvent.setup();
    let refuse: (err: unknown) => void = () => {};
    mockRename.mockImplementationOnce(() => new Promise((_, reject) => { refuse = reject; }));
    open();
    const back = await openDetails(user, 'Alpha Research');
    await user.click(screen.getByRole('button', { name: 'Rename computer' }));
    const field = screen.getByRole('textbox', { name: 'Computer name' });
    await user.clear(field);
    await user.type(field, 'second machine');
    // Leaving blurs the field, which sends the name, and the field goes with
    // the details before the answer comes.
    await user.click(back);
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Rename computer' })).not.toBeInTheDocument());
    expect(mockRename).toHaveBeenCalledWith(PRIMARY_ID, 'second machine');

    refuse(nameTaken());
    await waitFor(() => expect(mockToast).toHaveBeenCalledWith(expect.objectContaining({
      description: 'You already have a computer named "Second machine". Choose another name.',
    })));
  });

  it('returns to the rail on Computers, with that card in the middle and its Details focused', async () => {
    const user = userEvent.setup();
    open();
    const back = await openDetails(user, 'Second machine');
    // The view replaced the button that opened it, so focus starts on the way back.
    await waitFor(() => expect(back).toHaveFocus());

    await user.click(back);
    // By role, so the rail left before, still fading out, is not among them.
    const second = await screen.findByRole('article', { name: 'Second machine' });
    expect(second).toHaveClass('is-active');
    expect(screen.getByRole('article', { name: 'Alpha Research' })).not.toHaveClass('is-active');
    await waitFor(() => expect(within(second).getByRole('button', { name: 'Details' })).toHaveFocus());
  });

  it('reads storage by workspace once the details open, and not before', async () => {
    const user = userEvent.setup();
    mockStorage.mockResolvedValue(STORAGE);
    open();
    await screen.findAllByTestId('computer-card');
    expect(mockStorage).not.toHaveBeenCalled();

    await openDetails(user, 'Alpha Research');
    await waitFor(() => expect(mockStorage).toHaveBeenCalledWith(PRIMARY_ID));
    // No toggle to ask for it.
    expect(screen.queryByRole('button', { name: 'By workspace' })).not.toBeInTheDocument();
    const list = await screen.findByRole('list', { name: 'By workspace' });
    const rows = within(list).getAllByRole('listitem');
    // Largest first, with what no workspace owns last.
    expect(rows.map((r) => r.textContent)).toEqual([
      expect.stringContaining('Big one'),
      expect.stringContaining('Small one'),
      expect.stringContaining('System, caches and shared files'),
    ]);
  });

  it('says to start a stopped machine to see its storage by workspace', async () => {
    const user = userEvent.setup();
    mockStorage.mockResolvedValue({ live: false, disk: null, workspaces: [], other_bytes: 0 });
    open();
    await openDetails(user, 'Second machine');
    expect(await screen.findByText('Start the computer to see what each workspace uses.')).toBeInTheDocument();
    expect(screen.queryByRole('list', { name: 'By workspace' })).not.toBeInTheDocument();
  });

  it('opens straight onto a computer\'s details when asked to', async () => {
    mockStorage.mockResolvedValue(STORAGE);
    open({ view: 'details', computerId: SECOND_ID });

    expect(await screen.findByRole('button', { name: 'Computers' })).toBeInTheDocument();
    expect(await screen.findByRole('list', { name: 'By workspace' })).toBeInTheDocument();
    expect(mockStorage).toHaveBeenCalledWith(SECOND_ID);
    expect(mockStorage).not.toHaveBeenCalledWith(PRIMARY_ID);
    expect(screen.getByRole('button', { name: 'Remove' })).toBeInTheDocument();
    expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument();
  });

  it('opens onto the rail with a computer\'s card in the middle when asked to', async () => {
    open({ view: 'list', computerId: SECOND_ID });

    const cards = await screen.findAllByTestId('computer-card');
    await waitFor(() => expect(cards[1]).toHaveClass('is-active'));
    expect(screen.queryByRole('button', { name: 'Computers' })).not.toBeInTheDocument();
    expect(mockStorage).not.toHaveBeenCalled();
  });
});
