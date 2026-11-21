/**
 * The Computers dialog opened from a place that lists the machines itself
 * (Settings' Computers section, Claude Code's view): that place is the list,
 * so the panel holds one machine or the New computer form, and every way out
 * of either lands back there rather than on the panel's rail.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null, isLoading: false, isError: false }) }));

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import { queryKeys } from '@/lib/queryKeys';
import { createComputer, getComputerProviders, getComputers, getWorkspaces } from '../../utils/api';
import {
  closeComputersPanel,
  openComputersPanel,
  useCloseComputersOnLeave,
  useComputersPanel,
} from '../../hooks/computerPanelStore';
import ComputersDialog from '../ComputersDialog';
import { CREATED, DAYTONA_ONLY, LIVE_LIST, SECOND_ID } from './computersDialogHarness';

const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;
const mockWorkspaces = getWorkspaces as Mock;
const mockCreate = createComputer as Mock;

beforeEach(() => {
  vi.clearAllMocks();
  act(() => closeComputersPanel());
  mockGetComputers.mockResolvedValue(LIVE_LIST);
  mockProviders.mockResolvedValue({ providers: DAYTONA_ONLY });
  mockWorkspaces.mockResolvedValue({ workspaces: [], total: 0 });
});

function StoreDriven() {
  const { panel, opening } = useComputersPanel();
  return <ComputersDialog panel={panel} opening={opening} onClose={closeComputersPanel} />;
}

async function openFromSettings() {
  const rendered = renderWithProviders(<StoreDriven />);
  act(() => openComputersPanel({ view: 'details', computerId: SECOND_ID, returnTo: 'settings' }));
  const back = await screen.findByRole('button', { name: 'Settings' });
  return { ...rendered, back };
}

describe('ComputersDialog: opened from a place that lists the machines', () => {
  it('goes back to Settings from the details, with no place on a rail to count', async () => {
    const user = userEvent.setup();
    const { back } = await openFromSettings();

    expect(screen.queryByText('2 / 2')).not.toBeInTheDocument();
    await user.click(back);

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Computers' })).not.toBeInTheDocument());
    expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument();
  });

  it('names Claude Code as the way back when opened from its view', async () => {
    const user = userEvent.setup();
    renderWithProviders(<StoreDriven />);
    act(() => openComputersPanel({ view: 'details', computerId: SECOND_ID, returnTo: 'claudeCode' }));

    await user.click(await screen.findByRole('button', { name: 'Claude Code' }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Computers' })).not.toBeInTheDocument());
    expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument();
  });

  it('closes rather than falling back to the rail once its machine leaves the list', async () => {
    const { queryClient } = await openFromSettings();

    mockGetComputers.mockResolvedValue({
      computers: LIVE_LIST.computers.filter((c) => c.computer_id !== SECOND_ID),
      total: 1,
    });
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.computers.all }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Computers' })).not.toBeInTheDocument());
    expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument();
  });

  it('re-reads a list that is behind the page before giving up on the machine', async () => {
    const user = userEvent.setup();
    // The list was cached before the machine arrived; the page that opened it already shows it.
    mockGetComputers
      .mockResolvedValueOnce({ computers: LIVE_LIST.computers.filter((c) => c.computer_id !== SECOND_ID), total: 1 })
      .mockResolvedValue(LIVE_LIST);
    renderWithProviders(<StoreDriven />);
    act(() => openComputersPanel({ view: 'details', computerId: SECOND_ID, returnTo: 'claudeCode' }));

    await user.click(await screen.findByRole('button', { name: 'Claude Code' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Computers' })).not.toBeInTheDocument());
  });

  it('closes once a computer is created, the page listing it from then on', async () => {
    const user = userEvent.setup();
    mockCreate.mockImplementation(async () => {
      mockGetComputers.mockResolvedValue({ computers: [...LIVE_LIST.computers, CREATED], total: 3 });
      return CREATED;
    });
    renderWithProviders(<StoreDriven />);
    act(() => openComputersPanel({ view: 'create', returnTo: 'settings' }));
    const form = await screen.findByRole('dialog', { name: 'New computer' });

    await user.type(within(form).getByLabelText(/^Name/), 'Research box');
    await user.click(within(form).getByRole('button', { name: 'Create computer' }));

    // Both leave: the form, and the panel stepped back beneath it, which
    // would otherwise come forward onto the rail with the new card.
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument());
    await waitFor(() => expect(screen.queryByText('Computers', { selector: 'h3' })).not.toBeInTheDocument());
    expect(screen.queryByTestId('computer-card')).not.toBeInTheDocument();
  });
});

describe('useCloseComputersOnLeave', () => {
  function Page({ page }: { page?: 'settings' }) {
    useCloseComputersOnLeave(page);
    return null;
  }
  const opened = () => {
    let panel: ReturnType<typeof useComputersPanel>['panel'] = null;
    function Read() {
      panel = useComputersPanel().panel;
      return null;
    }
    render(<Read />).unmount();
    return panel;
  };

  it('closes the panel as its page goes, except one opened for Settings, which Settings then shows', () => {
    const chat = render(<Page />);
    act(() => openComputersPanel({ view: 'create', pick: 'thisMac', returnTo: 'settings' }));
    chat.unmount();
    expect(opened()).toEqual({ view: 'create', pick: 'thisMac', returnTo: 'settings' });

    const settings = render(<Page page="settings" />);
    settings.unmount();
    expect(opened()).toBeNull();

    const other = render(<Page />);
    act(() => openComputersPanel({ view: 'details', computerId: SECOND_ID, returnTo: 'claudeCode' }));
    other.unmount();
    expect(opened()).toBeNull();
  });
});
