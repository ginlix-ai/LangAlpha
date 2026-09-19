/**
 * The computer surface, rendered against the live `GET /api/v1/computers`
 * payload from the wt3 stack.
 *
 * `@/config/hostMode` is mocked rather than inherited: `web/.env` sets
 * VITE_HOST_MODE and Vite loads it for the Vitest run too, so a test that let
 * the mode default would be asserting whichever mode the developer's env
 * happened to carry.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('../../utils/api', () => ({
  getComputers: vi.fn(),
  createComputer: vi.fn(),
  startComputer: vi.fn(),
  stopComputer: vi.fn(),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
  formatApiErrorDetail: (err: unknown) =>
    (err as { message?: string })?.message ?? 'Request failed',
}));

import {
  createComputer,
  getComputers,
  startComputer,
  stopComputer,
} from '../../utils/api';
import ComputersDialog from '../ComputersDialog';

const mockGetComputers = getComputers as Mock;
const mockCreate = createComputer as Mock;
const mockStart = startComputer as Mock;
const mockStop = stopComputer as Mock;

const PRIMARY_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const SECOND_ID = '3b788b47-352e-4cfb-94f0-920b599ea227';

// Verbatim from curl against :8060. `workspace_count` is the server's own
// count of the folders on each machine, which is the whole point: the dialog is
// handed no workspace list to count for itself.
const LIVE_LIST = {
  computers: [
    {
      computer_id: PRIMARY_ID,
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Alpha Research',
      status: 'running',
      resource_tier: 'standard',
      is_always_on: false,
      is_primary: true,
      workspace_count: 3,
      root_dir: '/home/workspace',
      provider_ref: '91885905-79ed-48dc-81ac-0a3d2d608458',
      config: {},
    },
    {
      computer_id: SECOND_ID,
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Second machine',
      status: 'stopped',
      resource_tier: 'performance',
      is_always_on: false,
      is_primary: false,
      workspace_count: 0,
      root_dir: '/home/workspace',
      provider_ref: null,
      config: {},
    },
  ],
  total: 2,
};

function open() {
  return renderWithProviders(<ComputersDialog open onOpenChange={() => {}} />);
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetComputers.mockResolvedValue(LIVE_LIST);
});

describe('ComputersDialog', () => {
  it('lists each machine with its status, tier and primary badge', async () => {
    open();
    expect(await screen.findByText('Alpha Research')).toBeInTheDocument();
    expect(screen.getByText('Second machine')).toBeInTheDocument();

    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(screen.getByText('Stopped')).toBeInTheDocument();

    expect(screen.getByText('Standard')).toBeInTheDocument();
    expect(screen.getByText('Performance')).toBeInTheDocument();

    // Only the primary is badged.
    expect(screen.getAllByText('Primary')).toHaveLength(1);
  });

  it("says how many workspaces a machine holds, from the machine's own count", async () => {
    open();
    expect(await screen.findByText('3 workspaces')).toBeInTheDocument();
    expect(screen.getByText('0 workspaces')).toBeInTheDocument();
  });

  it('stops the running machine', async () => {
    const user = userEvent.setup();
    mockStop.mockResolvedValue({ computer_id: PRIMARY_ID, status: 'stopped', message: 'ok' });
    open();

    await user.click(await screen.findByRole('button', { name: /stop/i }));
    expect(mockStop).toHaveBeenCalledWith(PRIMARY_ID);
  });

  it('starts the stopped machine lazily, so the stream reports the boot', async () => {
    const user = userEvent.setup();
    mockStart.mockResolvedValue({ computer_id: SECOND_ID, status: 'starting', message: 'ok' });
    open();

    // Two rows, one Start button: the running machine offers Stop instead.
    await user.click(await screen.findByRole('button', { name: /start/i }));
    expect(mockStart).toHaveBeenCalledWith(SECOND_ID, { lazy: true });
  });

  it('creates a computer at the tier chosen in the picker', async () => {
    const user = userEvent.setup();
    mockCreate.mockResolvedValue({ computer_id: 'new', status: 'stopped', resource_tier: 'max' });
    open();

    await user.click(await screen.findByRole('button', { name: /new computer/i }));
    await user.type(screen.getByLabelText(/name/i), 'Research box');
    await user.click(screen.getByRole('radio', { name: /max/i }));
    await user.click(screen.getByRole('button', { name: /create computer/i }));

    await waitFor(() =>
      expect(mockCreate).toHaveBeenCalledWith({ name: 'Research box', resource_tier: 'max' }),
    );
  });

  it('shows the quota service\'s own sentence on a 429, not copy written here', async () => {
    const user = userEvent.setup();
    const platformMessage =
      'You are using 3 of 3 computers on the Starter plan. Upgrade or stop one to add another.';
    mockCreate.mockRejectedValue(
      Object.assign(new Error('Request failed with status code 429'), {
        status: 429,
        rateLimitInfo: {
          message: platformMessage,
          type: 'workspace_limit',
          current: 3,
          limit: 3,
          remaining: 0,
        },
      }),
    );
    open();

    await user.click(await screen.findByRole('button', { name: /new computer/i }));
    await user.click(screen.getByRole('button', { name: /create computer/i }));

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(platformMessage);
    // Not axios's status line, and nothing this client composed.
    expect(alert).not.toHaveTextContent('status code 429');
  });

  it('does not fetch while closed', () => {
    renderWithProviders(<ComputersDialog open={false} onOpenChange={() => {}} />);
    expect(mockGetComputers).not.toHaveBeenCalled();
  });
});
