/**
 * The offer to restore a workspace whose folder is gone from the machine the
 * user brought. It shows from the workspace row, answers through one request
 * either surface can make, and a 409 (answered elsewhere) is not an error.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient } from '@tanstack/react-query';

import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getWorkspace: vi.fn(),
  getComputers: vi.fn(),
  chooseFilesRestore: vi.fn(),
}));

import { chooseFilesRestore, getComputers, getWorkspace } from '../../utils/api';
import { RestoreOffer } from '../RestoreOffer';

const mockGetWorkspace = getWorkspace as Mock;
const mockGetComputers = getComputers as Mock;
const mockChoose = chooseFilesRestore as Mock;

const WS = '0d6f1f9e-2c8b-4f3e-9a51-7c1e2b9d4a10';
const BOX = 'a3c1e8f2-4b6d-4e0a-9c7b-1d2e3f4a5b6c';

let row: Workspace;

function offered(overrides: Partial<Workspace> = {}): Workspace {
  return {
    workspace_id: WS,
    name: 'Research',
    status: 'running',
    computer_id: BOX,
    files_restore_offered_at: '2026-10-21T08:00:00Z',
    files_backed_up_at: null,
    ...overrides,
  } as Workspace;
}

function client() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function conflict(status: number, detail: string) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    status,
    response: { status, data: { detail }, headers: {} },
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  row = offered();
  mockGetWorkspace.mockImplementation(async () => row);
  mockGetComputers.mockResolvedValue({
    computers: [{ computer_id: BOX, kind: 'ssh', name: 'devbox', status: 'running', provider_ref: 'me@devbox' }],
  });
});

describe('RestoreOffer', () => {
  it('shows nothing while no offer stands', async () => {
    row = offered({ files_restore_offered_at: null });
    const { queryClient } = renderWithProviders(<RestoreOffer workspaceId={WS} variant="banner" />, { queryClient: client() });
    await waitFor(() => expect(queryClient.getQueryState(queryKeys.workspaces.detail(WS))?.status).toBe('success'));
    expect(screen.queryByTestId('restore-offer')).not.toBeInTheDocument();
  });

  it('names the machine and restores on the user\'s word, then drops the offer', async () => {
    const user = userEvent.setup();
    mockChoose.mockImplementation(async () => {
      row = offered({ files_restore_offered_at: null });
      return row;
    });
    renderWithProviders(<RestoreOffer workspaceId={WS} variant="banner" />, { queryClient: client() });

    const offer = await screen.findByTestId('restore-offer');
    expect(await within(offer).findByText("This workspace's folder is missing on devbox.")).toBeInTheDocument();
    // No backup time known, so the question goes undated.
    expect(within(offer).getByText('Restore it from its backup?')).toBeInTheDocument();

    await user.click(within(offer).getByRole('button', { name: 'Restore files' }));
    expect(mockChoose).toHaveBeenCalledWith(WS, true);
    await waitFor(() => expect(screen.queryByTestId('restore-offer')).not.toBeInTheDocument());
  });

  it('asks before keeping the folder empty', async () => {
    const user = userEvent.setup();
    mockChoose.mockImplementation(async () => {
      row = offered({ files_restore_offered_at: null });
      return row;
    });
    renderWithProviders(<RestoreOffer workspaceId={WS} variant="notice" />, { queryClient: client() });

    await user.click(await screen.findByRole('button', { name: 'Keep it empty' }));
    const confirm = await screen.findByRole('dialog', { name: 'Keep the folder empty?' });
    expect(mockChoose).not.toHaveBeenCalled();

    await user.click(within(confirm).getByRole('button', { name: 'Keep it empty' }));
    await waitFor(() => expect(mockChoose).toHaveBeenCalledWith(WS, false));
  });

  it('says why a restore failed, in the server\'s words', async () => {
    const user = userEvent.setup();
    mockChoose.mockRejectedValue(conflict(503, "Couldn't reach devbox over SSH."));
    renderWithProviders(<RestoreOffer workspaceId={WS} variant="banner" />, { queryClient: client() });

    await user.click(await screen.findByRole('button', { name: 'Restore files' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't reach devbox over SSH.");
    // Still offered, so the user can try again.
    expect(screen.getByRole('button', { name: 'Restore files' })).toBeInTheDocument();
  });

  it('takes a 409 as the offer having been answered elsewhere, with nothing to report', async () => {
    const user = userEvent.setup();
    mockChoose.mockImplementation(async () => {
      row = offered({ files_restore_offered_at: null });
      throw conflict(409, 'No restore is waiting on a choice.');
    });
    renderWithProviders(<RestoreOffer workspaceId={WS} variant="banner" />, { queryClient: client() });

    await user.click(await screen.findByRole('button', { name: 'Restore files' }));
    await waitFor(() => expect(screen.queryByTestId('restore-offer')).not.toBeInTheDocument());
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('shows one answer running in every place the offer is shown', async () => {
    const user = userEvent.setup();
    let land: (value: Workspace) => void = () => {};
    mockChoose.mockImplementation(() => new Promise<Workspace>((resolve) => { land = resolve; }));
    renderWithProviders(
      <>
        <RestoreOffer workspaceId={WS} variant="banner" />
        <RestoreOffer workspaceId={WS} variant="notice" />
      </>,
      { queryClient: client() },
    );

    const [first] = await screen.findAllByRole('button', { name: 'Restore files' });
    await user.click(first);
    await waitFor(() => expect(screen.getAllByText('Restoring files…')).toHaveLength(2));
    expect(screen.queryByRole('button', { name: 'Restore files' })).not.toBeInTheDocument();

    await act(async () => {
      row = offered({ files_restore_offered_at: null });
      land(row);
    });
    await waitFor(() => expect(screen.queryAllByTestId('restore-offer')).toHaveLength(0));
    expect(mockChoose).toHaveBeenCalledTimes(1);
  });
});
