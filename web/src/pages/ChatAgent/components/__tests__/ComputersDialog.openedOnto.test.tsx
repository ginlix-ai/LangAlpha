/**
 * The Computers dialog opened onto New computer from outside it (the
 * gallery's New computer), where the form is the destination: leaving it
 * leaves the panel, and only a computer made in it lands on the rail.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()));

import { createComputer, getComputerProviders, getComputers, getWorkspaceQuota, getWorkspaces } from '../../utils/api';
import { closeComputersPanel, openComputersPanel, useComputersPanel } from '../../hooks/computerPanelStore';
import ComputersDialog from '../ComputersDialog';
import { backdropOf, computersDialog, CREATED, DAYTONA_AND_E2B, DAYTONA_ONLY, LIVE_LIST } from './computersDialogHarness';

const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;
const mockQuota = getWorkspaceQuota as Mock;
const mockWorkspaces = getWorkspaces as Mock;
const mockCreate = createComputer as Mock;

beforeEach(() => {
  vi.clearAllMocks();
  mockGetComputers.mockResolvedValue(LIVE_LIST);
  mockProviders.mockResolvedValue({ providers: DAYTONA_ONLY });
  mockWorkspaces.mockResolvedValue({ workspaces: [], total: 0 });
});

describe('ComputersDialog: opened onto New computer', () => {
  // Driven through the store, as the host drives it, so a close is the panel's.
  function StoreDriven() {
    const { panel, opening } = useComputersPanel();
    return <ComputersDialog panel={panel} opening={opening} onClose={closeComputersPanel} />;
  }

  async function openOntoCreate() {
    const rendered = renderWithProviders(<StoreDriven />);
    act(() => openComputersPanel({ view: 'create' }));
    const form = await screen.findByRole('dialog', { name: 'New computer' });
    return { ...rendered, form };
  }

  it('opens the form over the page with the list stepped back beneath it', async () => {
    const { form } = await openOntoCreate();

    expect(computersDialog()).toHaveAttribute('inert');
    expect(backdropOf(form).style.backgroundColor).toBe('transparent');
    expect(backdropOf(computersDialog()).style.backgroundColor).toBe('var(--color-bg-overlay-strong)');
  });

  it('closes everything on Cancel, leaving no Computers list behind, and reopens onto the list', async () => {
    const user = userEvent.setup();
    const { form } = await openOntoCreate();

    await user.click(within(form).getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument());
    await waitFor(() => expect(screen.queryByText('Computers', { selector: 'h3' })).not.toBeInTheDocument());

    // A later open without the form is the list, as ever.
    act(() => openComputersPanel());
    expect(await screen.findByRole('dialog', { name: 'Computers' })).not.toHaveAttribute('inert');
    expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument();
  });

  it('stays on the new computer when the list comes back in the server\'s order', async () => {
    // The server lists the primary first, then the most recently active: the
    // new computer second, ahead of the ones that were there before it.
    // The refetch lands after the rail has gone to the row the create put on
    // the list, as it does over a network.
    const user = userEvent.setup();
    const [primary, ...rest] = LIVE_LIST.computers;
    let refetched: (value: unknown) => void = () => {};
    mockCreate.mockImplementation(async () => {
      mockGetComputers.mockReturnValue(new Promise((resolve) => { refetched = resolve; }));
      return CREATED;
    });
    const { form } = await openOntoCreate();

    await user.type(within(form).getByLabelText(/^Name/), 'Research box');
    await user.click(within(form).getByRole('button', { name: 'Create computer' }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument());
    const arrived = (await screen.findAllByTestId('computer-card')).find((c) => c.textContent?.includes('Research box'));
    await waitFor(() => expect(arrived).toHaveClass('is-active'));

    act(() => refetched({ computers: [primary, CREATED, ...rest], total: 3 }));
    await waitFor(() => expect(screen.getAllByTestId('computer-card')[1]).toHaveTextContent('Research box'));
    const cards = screen.getAllByTestId('computer-card');
    await waitFor(() => expect(cards[1]).toHaveClass('is-active'));
    expect(cards.filter((c) => c.classList.contains('is-active'))).toHaveLength(1);
  });

  it('lands on the new computer in the list once one is created', async () => {
    const user = userEvent.setup();
    mockCreate.mockImplementation(async () => {
      mockGetComputers.mockResolvedValue({ computers: [...LIVE_LIST.computers, CREATED], total: 3 });
      return CREATED;
    });
    const { form } = await openOntoCreate();

    await user.type(within(form).getByLabelText(/^Name/), 'Research box');
    await user.click(within(form).getByRole('button', { name: 'Create computer' }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument());
    expect(screen.getByRole('dialog', { name: 'Computers' })).not.toHaveAttribute('inert');
    const row = (await screen.findAllByTestId('computer-card')).find((r) => r.textContent?.includes('Research box'));
    expect(row).toBeDefined();
    // Brought to the middle of the rail, where a card takes input.
    expect(row).toHaveClass('is-active');

    // From here the panel's own New computer is the in-list one: cancel comes back.
    await user.click(screen.getByRole('button', { name: 'Add a computer' }));
    const again = await screen.findByRole('dialog', { name: 'New computer' });
    await user.click(within(again).getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument());
    expect(screen.getByRole('dialog', { name: 'Computers' })).toBeInTheDocument();
  });

  it('opens the list instead when the plan has no room for another computer', async () => {
    mockProviders.mockResolvedValue({ providers: DAYTONA_AND_E2B });
    mockQuota.mockResolvedValue({ performance: null, max: null, always_on: null, computers: { used: 2, limit: 2 } });
    renderWithProviders(<StoreDriven />);
    act(() => openComputersPanel({ view: 'create' }));

    // Held is known once the providers and the count land; the list steps forward then.
    await waitFor(() => expect(computersDialog()).not.toHaveAttribute('inert'));
    expect(within(computersDialog()).getByRole('button', { name: 'Add a computer' })).toBeDisabled();
    expect(screen.queryByRole('dialog', { name: 'New computer' })).not.toBeInTheDocument();
  });
});
