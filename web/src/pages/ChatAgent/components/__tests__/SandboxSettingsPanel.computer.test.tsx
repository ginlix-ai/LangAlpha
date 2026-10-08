/**
 * Stop and start belong to the machine.
 *
 * A workspace is a folder; the sandbox that runs is the computer's. So the
 * panel has to act on the computer and say so before the button is pressed,
 * because the other projects on that machine go down with it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';

const COMPUTER_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const WORKSPACE_ID = 'aecc944a-61b4-4e68-8a1d-28e1b1823001';

const mockGetSandboxStats = vi.fn();
const mockGetWorkspace = vi.fn();
const mockGetComputers = vi.fn();
const mockStartComputer = vi.fn();
const mockStopComputer = vi.fn();

vi.mock('../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    getSandboxStats: (...a: unknown[]) => mockGetSandboxStats(...a),
    getWorkspace: (...a: unknown[]) => mockGetWorkspace(...a),
    getComputers: (...a: unknown[]) => mockGetComputers(...a),
    startComputer: (...a: unknown[]) => mockStartComputer(...a),
    stopComputer: (...a: unknown[]) => mockStopComputer(...a),
    installSandboxPackages: vi.fn(),
    refreshWorkspace: vi.fn(),
    streamComputerEvents: vi.fn(async () => {}),
    streamWorkspaceEvents: vi.fn(async () => {}),
  };
});

vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
    defaults: { baseURL: '' },
  },
}));

import { api } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';
import { SandboxSettingsContent } from '../SandboxSettingsPanel';

const mockPost = api.post as unknown as Mock;

function stats(state: string) {
  return {
    state,
    sandbox_id: '91885905-79ed-48dc-81ac-0a3d2d608458',
    resources: {},
    packages: [],
    skills: [],
    mcp_servers: [],
  };
}

function computerList(status: string) {
  return {
    computers: [{
      computer_id: COMPUTER_ID,
      user_id: 'wp17-live-user',
      kind: 'daytona',
      name: 'Alpha Research',
      status,
      resource_tier: 'standard',
      is_always_on: false,
      is_primary: true,
      root_dir: '/home/workspace',
    }],
    total: 1,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetSandboxStats.mockResolvedValue(stats('running'));
  mockGetComputers.mockResolvedValue(computerList('running'));
  mockGetWorkspace.mockResolvedValue({
    workspace_id: WORKSPACE_ID,
    name: 'Live Alpha',
    status: 'running',
    computer_id: COMPUTER_ID,
    dir_name: 'live-alpha-16ad',
  });
});

describe('SandboxSettingsPanel start/stop', () => {
  it('stops the computer, not the workspace, when the workspace names one', async () => {
    const user = userEvent.setup();
    mockStopComputer.mockResolvedValue({ computer_id: COMPUTER_ID, status: 'stopped', message: 'ok' });
    const { queryClient } = renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);
    const detail = queryKeys.workspaces.detail('cached-sibling');
    queryClient.setQueryDefaults(detail, { gcTime: Infinity });
    queryClient.setQueryData(detail, { status: 'running', computer_id: COMPUTER_ID });

    await user.click(await screen.findByRole('button', { name: /stop/i }));
    await waitFor(() => expect(mockStopComputer).toHaveBeenCalledWith(COMPUTER_ID));
    await waitFor(() => expect(queryClient.getQueryState(detail)?.isInvalidated).toBe(true));
    expect(queryClient.getQueryData(detail)).toMatchObject({ status: 'stopped' });
    // The workspace alias is not used when the machine is known.
    expect(mockPost).not.toHaveBeenCalled();
  });

  it.each(['stopped', 'creating'])('starts a %s computer lazily', async (status) => {
    const user = userEvent.setup();
    mockGetSandboxStats.mockResolvedValue(stats(status));
    mockGetComputers.mockResolvedValue(computerList(status));
    mockStartComputer.mockResolvedValue({ computer_id: COMPUTER_ID, status: 'starting', message: 'ok' });
    renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);

    await user.click(await screen.findByRole('button', { name: /^start$/i }));
    await waitFor(() => expect(mockStartComputer).toHaveBeenCalledWith(COMPUTER_ID, { lazy: true }));
  });

  it('offers no Stop when the row names no machine', async () => {
    mockGetWorkspace.mockResolvedValue({
      workspace_id: WORKSPACE_ID, name: 'Legacy', status: 'running', computer_id: null,
    });
    renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);

    expect(await screen.findByRole('button', { name: /refresh/i })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /stop/i })).not.toBeInTheDocument();
  });

  it('starts through the workspace route when the row names no machine', async () => {
    const user = userEvent.setup();
    mockGetSandboxStats.mockResolvedValue(stats('stopped'));
    mockGetWorkspace.mockResolvedValue({
      workspace_id: WORKSPACE_ID, name: 'Legacy', status: 'stopped', computer_id: null,
    });
    mockPost.mockResolvedValue({ data: {} });
    renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);

    await user.click(await screen.findByRole('button', { name: /^start$/i }));
    await waitFor(() =>
      expect(mockPost).toHaveBeenCalledWith(`/api/v1/workspaces/${WORKSPACE_ID}/start`),
    );
    expect(mockStartComputer).not.toHaveBeenCalled();
  });

  it('names the machine and the folder, and warns that the siblings go with it', async () => {
    renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);

    expect(await screen.findByText('On Alpha Research')).toBeInTheDocument();
    expect(screen.getByText('live-alpha-16ad')).toBeInTheDocument();
    expect(
      screen.getByText('Starting or stopping this computer affects every workspace on it.'),
    ).toBeInTheDocument();
  });

  it('says nothing about a machine when the workspace has none', async () => {
    mockGetWorkspace.mockResolvedValue({
      workspace_id: WORKSPACE_ID, name: 'Legacy', status: 'running', computer_id: null,
    });
    renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);

    await screen.findByRole('button', { name: /refresh/i });
    expect(screen.queryByText(/^On /)).not.toBeInTheDocument();
    expect(screen.queryByText(/affects every workspace on it/)).not.toBeInTheDocument();
  });
});


it('uses the settled computer stream status even when stats still reports starting', async () => {
  mockGetSandboxStats.mockResolvedValue(stats('starting'));
  mockGetComputers.mockResolvedValue(computerList('starting'));
  const { queryClient } = renderWithProviders(<SandboxSettingsContent workspaceId={WORKSPACE_ID} />);
  await screen.findByText('On Alpha Research');
  await act(async () => {
    queryClient.setQueryData(queryKeys.computers.lists(), computerList('running'));
  });
  expect(await screen.findByRole('button', { name: /stop/i })).toBeEnabled();
  expect(screen.queryByRole('button', { name: /^start$/i })).not.toBeInTheDocument();
});
