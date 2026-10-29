/**
 * Deleting a workspace on a machine the user brought leaves its folder there
 * unless the user ticks the box; every other computer's folder goes as before.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  deleteWorkspace: vi.fn(async () => {}),
  getComputers: vi.fn(),
}));

vi.mock('../WorkspaceMoveOutcomes', () => ({ WorkspaceMoveOutcomes: () => null }));

import { deleteWorkspace, getComputers } from '../../utils/api';
import DeleteConfirmModal from '../DeleteConfirmModal';
import { useWorkspaceActions, type MenuWorkspace } from '../workspaceActions';

const mockDelete = deleteWorkspace as Mock;
const mockGetComputers = getComputers as Mock;

function computer(computer_id: string, kind: string, name: string, root_dir: string): Computer {
  return {
    computer_id,
    user_id: 'user-1',
    kind,
    name,
    status: 'running',
    resource_tier: 'standard',
    is_always_on: false,
    is_primary: kind !== 'ssh',
    root_dir,
  } as Computer;
}

const LAPTOP = computer('c-ssh', 'ssh', 'My laptop', '~/langalpha/');
const CLOUD = computer('c-cloud', 'daytona', 'Cloud', '/home/workspace');

function Harness({ workspace }: { workspace: MenuWorkspace }) {
  const actions = useWorkspaceActions({});
  return (
    <>
      <button type="button" onClick={() => actions.openDelete(workspace)}>open</button>
      {actions.dialogs}
    </>
  );
}

const onLaptop: MenuWorkspace = { workspace_id: 'ws-1', name: 'Research', computer_id: 'c-ssh', dir_name: 'Research' };
const onCloud: MenuWorkspace = { workspace_id: 'ws-2', name: 'Macro', computer_id: 'c-cloud', dir_name: 'Macro' };

beforeEach(() => {
  vi.clearAllMocks();
  mockGetComputers.mockResolvedValue({ computers: [LAPTOP, CLOUD], total: 2 });
});

describe('deleting a workspace on the owner\'s machine', () => {
  it('says where the files are, keeps them by default, and deletes them only when asked', async () => {
    renderWithProviders(<Harness workspace={onLaptop} />);
    fireEvent.click(screen.getByText('open'));

    expect(await screen.findByText('Its files are in ~/langalpha/Research on My laptop, and they stay there.')).toBeInTheDocument();
    const box = screen.getByRole('checkbox', { name: 'Also delete ~/langalpha/Research from this machine' });
    expect(box).not.toBeChecked();

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith('ws-1', { deleteFiles: false }));

    fireEvent.click(screen.getByText('open'));
    fireEvent.click(await screen.findByRole('checkbox', { name: 'Also delete ~/langalpha/Research from this machine' }));
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(mockDelete).toHaveBeenLastCalledWith('ws-1', { deleteFiles: true }));
  });

  it('opens unticked again after a cancel', async () => {
    renderWithProviders(<Harness workspace={onLaptop} />);
    fireEvent.click(screen.getByText('open'));
    fireEvent.click(await screen.findByRole('checkbox'));
    expect(screen.getByRole('checkbox')).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    fireEvent.click(screen.getByText('open'));
    expect(await screen.findByRole('checkbox')).not.toBeChecked();
  });

  it('offers no choice on a hosted computer, where the folder always goes', async () => {
    renderWithProviders(<Harness workspace={onCloud} />);
    fireEvent.click(screen.getByText('open'));

    expect(await screen.findByText(/will be permanently deleted/)).toBeInTheDocument();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(screen.queryByRole('checkbox')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith('ws-2', { deleteFiles: false }));
  });

  it('opens once it knows the computer, so it never words the owner\'s machine as a hosted one', async () => {
    let answer!: (list: { computers: Computer[]; total: number }) => void;
    mockGetComputers.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    renderWithProviders(<Harness workspace={onLaptop} />);
    fireEvent.click(screen.getByText('open'));

    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(screen.queryByRole('dialog')).toBeNull();

    answer({ computers: [LAPTOP, CLOUD], total: 2 });
    expect(await screen.findByRole('checkbox', { name: 'Also delete ~/langalpha/Research from this machine' }))
      .not.toBeChecked();
    expect(screen.queryByText(/will be permanently deleted/)).toBeNull();
  });

  it('gives the general warning when the computers cannot be read', async () => {
    mockGetComputers.mockRejectedValue(new Error('offline'));
    renderWithProviders(<Harness workspace={onLaptop} />);
    fireEvent.click(screen.getByText('open'));

    expect(await screen.findByText(/will be permanently deleted/)).toBeInTheDocument();
  });

  it('takes the deleted row off every cached list before the refetch lands', async () => {
    // Kept while unobserved, as a list another page cached would be.
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderWithProviders(<Harness workspace={onCloud} />, { queryClient });
    const gallery = queryKeys.workspaces.list({ view: 'all', sortBy: 'activity', includeHome: true });
    const sidebar = queryKeys.workspaces.list({ limit: 20, includeHome: true, sortBy: 'custom', offset: 0 });
    for (const key of [gallery, sidebar]) {
      queryClient.setQueryData(key, { workspaces: [onLaptop, onCloud], total: 2 });
    }
    fireEvent.click(screen.getByText('open'));

    fireEvent.click(await screen.findByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith('ws-2', { deleteFiles: false }));
    await waitFor(() => expect(queryClient.getQueryData(gallery)).toEqual({ workspaces: [onLaptop], total: 1 }));
    expect(queryClient.getQueryData(sidebar)).toEqual({ workspaces: [onLaptop], total: 1 });
    queryClient.clear();
  });

  it('never offers it for a thread', () => {
    renderWithProviders(
      <DeleteConfirmModal
        isOpen
        workspaceName="Q3 notes"
        itemType="thread"
        ownMachine={{ folder: '~/langalpha/Research', computer: 'My laptop' }}
        onConfirm={vi.fn()}
        onCancel={vi.fn()}
        isDeleting={false}
      />,
    );
    expect(screen.queryByRole('checkbox')).toBeNull();
    expect(screen.getByText(/will be permanently deleted/)).toBeInTheDocument();
  });
});
