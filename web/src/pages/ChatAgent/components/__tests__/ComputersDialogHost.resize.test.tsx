/**
 * A page that mounts the host takes `?resize=<computer>` off its address and
 * opens Change spec on that computer at Medium, the size Claude Code runs at:
 * how the plan page's way back, and a refusal kept in a thread, ask for it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { useLocation } from 'react-router';
import { useState } from 'react';

import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
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

function mount(route: string, queryClient = createTestQueryClient()) {
  return renderWithProviders(
    <>
      <ComputersDialogHost />
      <Probe />
    </>,
    { route, queryClient },
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

describe('ComputersDialogHost, the resize query for Home', () => {
  const OWN_MAC = { ...SMALL, computer_id: 'c-mac', kind: 'ssh', name: 'Mac', is_primary: true } as Computer;
  const OTHER = { ...SMALL, computer_id: 'c-2', name: 'Other cloud', is_primary: false } as Computer;

  it("opens Change spec on Home's computer at the size the plan now opens, once, asking the server to read the plan again", async () => {
    host.platform = true;
    vi.mocked(getComputers).mockResolvedValue({ computers: [OTHER, SMALL], total: 2 });
    vi.mocked(getWorkspaceQuota).mockResolvedValue({ performance: { used: 0, limit: 1 }, max: { used: 0, limit: 1 }, always_on: null });
    vi.mocked(setComputerSpec).mockRejectedValue(new Error('refused'));
    mount('/settings?tab=plan&resize=home&tier=max');
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=plan spec:c-1/max'));
    expect(await screen.findByRole('radio', { name: /^Large/ })).toBeChecked();
    expect(vi.mocked(getWorkspaceQuota).mock.calls[0]).toEqual([true]);
    fireEvent.click(screen.getByRole('button', { name: 'Back up and change spec' }));
    await waitFor(() => expect(setComputerSpec).toHaveBeenCalledWith('c-1', 'max', true));
    act(() => closeComputerSpec());
    await act(async () => {});
    expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=plan spec:null/null');
  });

  it('opens with nothing picked for a size the provider does not offer, or one this build does not know', async () => {
    for (const tier of ['xlarge', 'enormous']) {
      const { unmount } = mount(`/settings?resize=home&tier=${tier}`);
      await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent(/^at:\/settings spec:c-1\//));
      expect(await screen.findByRole('radio', { name: /^Small/ })).toBeChecked();
      expect(screen.getByRole('button', { name: 'Back up and change spec' })).toBeDisabled();
      act(() => closeComputerSpec());
      unmount();
    }
  });

  it('never picks a size smaller than Home is now', async () => {
    vi.mocked(getComputers).mockResolvedValue({ computers: [{ ...SMALL, resource_tier: 'max' }], total: 1 });
    mount('/settings?resize=home&tier=performance');
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings spec:c-1/max'));
    expect(await screen.findByRole('radio', { name: /^Large/ })).toBeChecked();
  });

  it("opens nothing where Home is on the user's own machine, or on no computer, and takes the query off", async () => {
    for (const computers of [[OWN_MAC, OTHER], [OTHER], []]) {
      vi.mocked(getComputers).mockResolvedValue({ computers, total: computers.length });
      const { unmount } = mount('/settings?tab=plan&resize=home&tier=max');
      await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=plan spec:null/null'));
      expect(screen.queryByRole('radio')).toBeNull();
      unmount();
    }
  });

  it('reads Home off a fresh list, where the cached one names a primary another device has since replaced', async () => {
    const FORMER = { ...SMALL, computer_id: 'c-old', name: 'Former primary' } as Computer;
    for (const cached of [FORMER, { ...OWN_MAC, computer_id: 'c-old' } as Computer]) {
      const queryClient = createTestQueryClient();
      queryClient.setQueryData(queryKeys.computers.lists(), { computers: [cached, OTHER], total: 2 });
      vi.mocked(getComputers).mockResolvedValue({ computers: [{ ...cached, is_primary: false }, SMALL], total: 2 });
      const { unmount } = mount('/settings?tab=plan&resize=home&tier=performance', queryClient);
      await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=plan spec:c-1/performance'));
      act(() => closeComputerSpec());
      unmount();
    }
  });

  it('opens nothing where the list cannot be read', async () => {
    vi.mocked(getComputers).mockRejectedValue(new Error('down'));
    mount('/settings?tab=plan&resize=home&tier=max');
    await waitFor(() => expect(screen.getByTestId('probe')).toHaveTextContent('at:/settings?tab=plan spec:null/null'), { timeout: 5000 });
  });
});
