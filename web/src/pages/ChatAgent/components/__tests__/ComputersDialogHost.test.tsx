/**
 * Spec-change outcomes are announced off the list itself, so whichever read
 * sees a change settle (the poll, window focus, a status reconcile, an
 * invalidation) is the one that reports it.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputerSpecChange, ComputersResponse } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'oss',
  isPlatformMode: false,
  APP_ENTRY_PATH: '/',
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(async () => ({ providers: [] })),
  getComputerStorage: vi.fn(),
  setComputerSpec: vi.fn(),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
}));

import { toast } from '@/components/ui/use-toast';
import { getComputerProviders, getComputers, setComputerSpec } from '../../utils/api';
import {
  closeComputerSpec,
  closeComputersPanel,
  openComputerSpec,
  openComputersPanel,
} from '../../hooks/computerPanelStore';
import { recordPrepareFailure } from '../../hooks/computerPreparation';
import ComputersDialogHost from '../ComputersDialogHost';

const mockToast = toast as unknown as Mock;
const mockGetComputers = getComputers as Mock;

const ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const STARTED = '2026-09-24T18:00:00Z';

function machine(spec_change: ComputerSpecChange | null, status = 'running'): Computer {
  return {
    computer_id: ID,
    user_id: 'u1',
    kind: 'daytona',
    name: 'Alpha Research',
    status,
    resource_tier: 'standard',
    is_always_on: false,
    is_primary: true,
    workspace_count: 1,
    root_dir: '/home/workspace',
    // A machine with a sandbox, so a start here is a restart, not a preparation.
    provider_ref: '91885905-79ed-48dc-81ac-0a3d2d608458',
    spec_change,
  } as Computer;
}

function change(overrides: Partial<ComputerSpecChange> = {}): ComputerSpecChange {
  return {
    target_tier: 'performance',
    from_tier: 'standard',
    state: 'in_progress',
    started_at: STARTED,
    ...overrides,
  };
}

function list(computer: Computer): ComputersResponse {
  return { computers: [computer], total: 1 };
}

/** Any read of the list: here an invalidation, as window focus or a reconcile would do. */
async function refetchReturning(queryClient: ReturnType<typeof renderWithProviders>['queryClient'], computer: Computer) {
  mockGetComputers.mockResolvedValue(list(computer));
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() });
  });
}

/** Render with the first list read landed, so the host has seen what it holds. */
async function mount() {
  const rendered = renderWithProviders(<ComputersDialogHost />);
  await waitFor(() =>
    expect(rendered.queryClient.getQueryData(queryKeys.computers.lists())).toBeDefined(),
  );
  await act(async () => {});
  return rendered;
}

beforeEach(() => {
  vi.clearAllMocks();
  runAuthResets();
});

afterEach(() => {
  act(() => {
    closeComputerSpec();
    closeComputersPanel();
  });
});

describe('ComputersDialogHost, spec-change outcomes', () => {
  it('stays quiet about an outcome that settled before the page loaded', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change({ state: 'succeeded' }))));
    await mount();
    expect(mockToast).not.toHaveBeenCalled();
  });

  it('announces a settled row that arrives through any refetch, not only its own poll', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change())));
    const { queryClient } = await mount();

    await refetchReturning(queryClient, machine(change({ state: 'succeeded' })));

    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({ description: 'Alpha Research now runs at Medium.' }),
      ),
    );
    expect(mockToast).toHaveBeenCalledTimes(1);
  });

  it('announces once, however many reads follow', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change())));
    const { queryClient } = await mount();

    await refetchReturning(queryClient, machine(change({ state: 'succeeded' })));
    await waitFor(() => expect(mockToast).toHaveBeenCalledTimes(1));
    await refetchReturning(queryClient, machine(change({ state: 'succeeded', target_tier: 'performance' }), 'stopped'));
    expect(mockToast).toHaveBeenCalledTimes(1);
  });

  it('keeps watching a change across a remount, and announces it once', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change())));
    const first = await mount();
    first.unmount();

    // The page mounts the host in separate mobile and desktop branches.
    mockGetComputers.mockResolvedValue(list(machine(change({ state: 'succeeded' }))));
    await mount();

    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({ description: 'Alpha Research now runs at Medium.' }),
      ),
    );
    expect(mockToast).toHaveBeenCalledTimes(1);
  });

  it('waits past a stale row that predates the change instead of reading it as the outcome', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change())));
    const { queryClient } = await mount();

    await refetchReturning(
      queryClient,
      machine(change({ state: 'failed', started_at: '2026-09-01T00:00:00Z', error: { code: 'busy', message: 'x', files: [] } })),
    );
    expect(mockToast).not.toHaveBeenCalled();

    await refetchReturning(queryClient, machine(change({ state: 'succeeded' })));
    await waitFor(() => expect(mockToast).toHaveBeenCalledTimes(1));
  });

  it("relays the server's sentence for a downgrade that does not fit, as a toast while the dialog is closed", async () => {
    const message =
      'Cannot downgrade: files on this computer (2.5 GiB) exceed what the Standard spec holds. Free up space first.';
    mockGetComputers.mockResolvedValue(list(machine(change({ target_tier: 'standard', from_tier: 'performance' }))));
    const { queryClient } = await mount();

    await refetchReturning(
      queryClient,
      machine(change({ state: 'failed', error: { code: 'disk_too_small', message, files: [] } })),
    );

    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not change the spec of Alpha Research',
          description: message,
        }),
      ),
    );
  });

  it('shows progress in the open dialog, then the failure inline', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change(), 'starting')));
    const { queryClient } = renderWithProviders(<ComputersDialogHost />);
    act(() => openComputerSpec(ID));

    expect(await screen.findByTestId('spec-change-progress')).toHaveTextContent('Upgrading to Medium…');

    await refetchReturning(
      queryClient,
      machine(change({ state: 'failed', error: { code: 'interrupted', message: 'server wording', files: [] } })),
    );

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(
      "The spec change was interrupted before it finished. Your files are safe. Check the computer's current spec and try again.",
    );
    expect(screen.queryByTestId('spec-change-progress')).not.toBeInTheDocument();
    expect(mockToast).not.toHaveBeenCalled();
  });

  it('closes the open dialog on success and says so', async () => {
    mockGetComputers.mockResolvedValue(list(machine(change(), 'starting')));
    const { queryClient } = renderWithProviders(<ComputersDialogHost />);
    act(() => openComputerSpec(ID));
    await screen.findByTestId('spec-change-progress');

    await refetchReturning(queryClient, machine(change({ state: 'succeeded' })));

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(mockToast).toHaveBeenCalledWith(
      expect.objectContaining({ description: 'Alpha Research now runs at Medium.' }),
    );
  });

  it('closes the dialog when the server answers 200, already at that tier', async () => {
    mockGetComputers.mockResolvedValue(list(machine(null)));
    (setComputerSpec as Mock).mockResolvedValue({ ...machine(null), resource_tier: 'performance' });
    await mount();
    act(() => openComputerSpec(ID));

    await userEvent.click(await screen.findByRole('radio', { name: /^Medium/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Back up and change spec' }));

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(mockToast).toHaveBeenCalledWith(expect.objectContaining({ description: 'Medium' }));
  });
});

describe("ComputersDialogHost, the computer's sizes", () => {
  it("offers the sizes the computer's provider runs", async () => {
    (getComputerProviders as Mock).mockResolvedValue({
      providers: [
        {
          kind: 'e2b',
          is_default: true,
          allowed: true,
          tiers: [
            { tier: 'standard', cpu: 1, memory_gib: 2, disk_gib: 5 },
            { tier: 'performance', cpu: 2, memory_gib: 4, disk_gib: 10 },
            { tier: 'max', cpu: 2, memory_gib: 8, disk_gib: 10 },
            { tier: 'xlarge', cpu: 4, memory_gib: 8, disk_gib: 20 },
          ],
        },
      ],
    });
    mockGetComputers.mockResolvedValue(list({ ...machine(null), kind: 'e2b' }));
    await mount();
    act(() => openComputerSpec(ID));

    expect(await screen.findByRole('radio', { name: /^Extra Large/ })).toHaveAccessibleDescription(
      /^4 vCPU\s*·\s*8 GB RAM\s*·\s*20 GB disk$/,
    );
  });

  it('re-reads the tier counts once a change is accepted', async () => {
    mockGetComputers.mockResolvedValue(list(machine(null)));
    (setComputerSpec as Mock).mockResolvedValue(machine(change()));
    const { queryClient } = await mount();
    queryClient.setQueryDefaults(queryKeys.workspaces.quota(), { gcTime: Infinity });
    queryClient.setQueryData(queryKeys.workspaces.quota(), { performance: null, max: null, always_on: null });
    act(() => openComputerSpec(ID));

    await userEvent.click(await screen.findByRole('radio', { name: /^Medium/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Back up and change spec' }));

    await waitFor(() => expect(queryClient.getQueryState(queryKeys.workspaces.quota())?.isInvalidated).toBe(true));
  });
});

describe('ComputersDialogHost, preparation outcomes', () => {
  // A new computer: starting, and no sandbox yet.
  const preparing = (): Computer => ({ ...machine(null, 'starting'), provider_ref: null });

  it('says a computer it saw being prepared is ready, once, and re-reads it', async () => {
    mockGetComputers.mockResolvedValue(list(preparing()));
    const { queryClient } = await mount();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    // What the fan-out does with the stream's last frame.
    act(() => {
      queryClient.setQueryData(queryKeys.computers.lists(), list({ ...preparing(), status: 'running' }));
    });
    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(expect.objectContaining({ title: 'Alpha Research is ready' })),
    );
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.computers.lists() });

    // The re-read, here answered by a read that left before the stream's
    // frame, then by one that brings the sandbox; a second host or a remount
    // reads the same list.
    await refetchReturning(queryClient, machine(null, 'running'));
    renderWithProviders(<ComputersDialogHost />);
    await act(async () => {});
    expect(mockToast).toHaveBeenCalledTimes(1);
  });

  it("names a failed preparation's reason and that Start tries again, then re-reads the row", async () => {
    mockGetComputers.mockResolvedValue(list(preparing()));
    const { queryClient } = await mount();

    act(() => {
      recordPrepareFailure(ID, 'The sandbox image could not be built.');
      queryClient.setQueryData(queryKeys.computers.lists(), list({ ...preparing(), status: 'error' }));
    });
    await waitFor(() =>
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({ variant: 'destructive', title: "Couldn't prepare Alpha Research" }),
      ),
    );
    const { container } = render(<>{mockToast.mock.calls[0][0].description}</>);
    expect(container).toHaveTextContent('The sandbox image could not be built.');
    expect(container).toHaveTextContent('Start it to try again.');

    // The re-read finds it stopped, and that is not a second failure.
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalledTimes(2));
    await refetchReturning(queryClient, { ...preparing(), status: 'stopped' });
    expect(mockToast).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['leaves the list', (): ComputersResponse => ({ computers: [], total: 0 })],
    ['comes back removed', (): ComputersResponse => list({ ...preparing(), status: 'deleted' })],
  ])('stays quiet about a computer being prepared that %s', async (_case, after) => {
    mockGetComputers.mockResolvedValue(list(preparing()));
    const { queryClient } = await mount();

    act(() => {
      queryClient.setQueryData(queryKeys.computers.lists(), after());
    });
    await act(async () => {});
    expect(mockToast).not.toHaveBeenCalled();
  });

  it('stays quiet about an ordinary restart and a computer already up at load', async () => {
    mockGetComputers.mockResolvedValue({
      computers: [machine(null, 'starting'), { ...machine(null, 'running'), computer_id: 'other', provider_ref: null }],
      total: 2,
    });
    const { queryClient } = await mount();
    await refetchReturning(queryClient, machine(null, 'running'));
    expect(mockToast).not.toHaveBeenCalled();
  });
});

describe('ComputersDialogHost, one panel at a time', () => {
  // Found by its title: covered, it is hidden from the tree, and a role query
  // cannot name a dialog whose label sits inside the hidden part.
  const computersDialog = () =>
    screen.getByText('Computers', { selector: 'h3' }).closest<HTMLElement>('[role="dialog"]') as HTMLElement;
  const backdropOf = (dialog: HTMLElement) => dialog.parentElement as HTMLElement;

  it('has Change spec take the Computers panel\'s place, and give it back on Escape', async () => {
    mockGetComputers.mockResolvedValue(list(machine(null)));
    await mount();
    act(() => openComputersPanel());
    await screen.findByRole('dialog', { name: 'Computers' });

    act(() => openComputerSpec(ID));
    const spec = await screen.findByRole('dialog', { name: 'Change spec for Alpha Research' });
    expect(computersDialog()).toHaveAttribute('inert');
    // The Computers backdrop stays as the only dimming.
    expect(backdropOf(spec).style.backgroundColor).toBe('transparent');
    expect(backdropOf(computersDialog()).style.backgroundColor).toBe('var(--color-bg-overlay-strong)');

    await userEvent.keyboard('{Escape}');
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Change spec for Alpha Research' })).not.toBeInTheDocument(),
    );
    expect(screen.getByRole('dialog', { name: 'Computers' })).not.toHaveAttribute('inert');
  });

  it('dims the page itself when opened on its own, from a disk warning', async () => {
    mockGetComputers.mockResolvedValue(list(machine(null)));
    await mount();

    act(() => openComputerSpec(ID));
    const spec = await screen.findByRole('dialog', { name: 'Change spec for Alpha Research' });
    expect(screen.queryByText('Computers', { selector: 'h3' })).not.toBeInTheDocument();
    expect(backdropOf(spec).style.backgroundColor).toBe('var(--color-bg-overlay-strong)');
  });
});
