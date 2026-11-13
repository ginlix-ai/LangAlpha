/**
 * A page that mounts the host takes `?resize=<computer>` off its address and
 * opens Change spec on that computer at Medium, the size Claude Code runs at:
 * how the plan page's way back, and a refusal kept in a thread, ask for it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { useLocation } from 'react-router';
import { useState } from 'react';

import { renderWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import type { Computer } from '@/types/api';

const host = vi.hoisted(() => ({ platform: false }));
vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'oss',
  get isPlatformMode() {
    return host.platform;
  },
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

import { getComputers, getWorkspaceQuota, setComputerSpec } from '../../utils/api';
import { closeComputerSpec, closeComputersPanel, useComputersPanel } from '../../hooks/computerPanelStore';
import ComputersDialogHost from '../ComputersDialogHost';

const SMALL = {
  computer_id: 'c-1',
  user_id: 'u1',
  kind: 'daytona',
  name: 'Cloud computer',
  status: 'running',
  resource_tier: 'standard',
  is_always_on: false,
  is_primary: true,
  workspace_count: 1,
  root_dir: '/home/workspace',
  provider_ref: 'sandbox-1',
  spec_change: null,
} as Computer;

function Probe() {
  const { pathname, search } = useLocation();
  const { specComputerId, specRequest } = useComputersPanel();
  return <span data-testid="probe">{`at:${pathname}${search} spec:${specComputerId}/${specRequest?.tier ?? null}`}</span>;
}

function mount(route: string) {
  return renderWithProviders(
    <>
      <ComputersDialogHost />
      <Probe />
    </>,
    { route },
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  host.platform = false;
  runAuthResets();
  vi.mocked(getComputers).mockResolvedValue({ computers: [SMALL], total: 1 });
});

afterEach(() => {
  act(() => {
    closeComputerSpec();
    closeComputersPanel();
  });
});

describe('ComputersDialogHost, the resize query', () => {
  it('opens Change spec on that computer with Medium wanted, and takes only the resize param off the address', async () => {
    mount('/settings?tab=model&resize=c-1');
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=model spec:c-1/performance'));
    expect(await screen.findByRole('radio', { name: /^Medium/ })).toBeChecked();
  });

  it('closes Change spec when its page leaves, so the next page with a host does not open it unasked', async () => {
    function Page() {
      const [hosted, setHosted] = useState(true);
      return (
        <>
          {hosted && <ComputersDialogHost />}
          <button type="button" onClick={() => setHosted(false)}>leave</button>
          <Probe />
        </>
      );
    }
    renderWithProviders(<Page />, { route: '/settings?tab=model&resize=c-1' });
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('spec:c-1/performance'));
    fireEvent.click(screen.getByRole('button', { name: 'leave' }));
    expect(screen.getByTestId('probe')).toHaveTextContent('spec:null/null');
  });

  it('back from the plan page, reads the quota and sends the change asking the server to read the plan again', async () => {
    host.platform = true;
    vi.mocked(setComputerSpec).mockRejectedValue(new Error('refused'));
    mount('/settings?tab=model&resize=c-1&plan_changed=1');
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=model spec:c-1/performance'));
    await waitFor(() => expect(getWorkspaceQuota).toHaveBeenCalled());
    expect(vi.mocked(getWorkspaceQuota).mock.calls).toEqual([[true]]);
    expect(await screen.findByRole('radio', { name: /^Medium/ })).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Back up and change spec' }));
    await waitFor(() => expect(setComputerSpec).toHaveBeenCalledWith('c-1', 'performance', true));
  });

  it('asks for no read again on a resize link that does not come from the plan page', async () => {
    host.platform = true;
    vi.mocked(setComputerSpec).mockRejectedValue(new Error('refused'));
    mount('/settings?tab=model&resize=c-1');
    expect(await screen.findByRole('radio', { name: /^Medium/ })).toBeChecked();
    await waitFor(() => expect(getWorkspaceQuota).toHaveBeenCalled());
    expect(vi.mocked(getWorkspaceQuota).mock.calls).toEqual([[false]]);
    fireEvent.click(screen.getByRole('button', { name: 'Back up and change spec' }));
    await waitFor(() => expect(setComputerSpec).toHaveBeenCalledWith('c-1', 'performance', false));
  });

  it('leaves the address and the dialog alone without it', async () => {
    mount('/settings?tab=model');
    await act(async () => {});
    expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=model spec:null/null');
    expect(screen.queryByRole('radio')).toBeNull();
  });
});
