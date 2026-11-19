/**
 * The browser's side of the Mac route: getting the app, waiting on it, the
 * app taking setup over, and following it to the first chat or back.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Route, Routes, useLocation } from 'react-router';

import { renderWithProviders } from '@/test/utils';
import type { Computer } from '@/types/api';

vi.mock('@/config/hostMode', () => ({ HOST_MODE: 'platform', isPlatformMode: true, APP_ENTRY_PATH: '/app' }));

const state = vi.hoisted(() => ({ update: vi.fn(), facts: {} as Record<string, unknown>, refetch: [] as unknown[], userId: 'u1', updating: false }));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({ progress: null, isLoading: false, isError: false, update: state.update, isUpdating: state.updating }),
}));
vi.mock('@/pages/Setup/answers/useSetupFacts', () => ({
  useSetupFacts: (options: unknown) => {
    state.refetch.push(options);
    return state.facts;
  },
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { user_id: state.userId, email: 'nadia@example.com' }, isLoading: false }) }));

import MacHandoffStep from '../MacHandoffStep';

const MAC: Computer = {
  computer_id: 'c-mac', user_id: 'u1', kind: 'ssh', name: 'MacBook Pro', status: 'running', resource_tier: 'standard',
  is_always_on: false, is_primary: false, root_dir: '/Users/nadia/langalpha', created_at: '2026-11-15T10:00:00Z',
  ssh: { host: 'mbp', port: 22, username: 'nadia', fingerprint: 'k', tunnel: true, tunnel_connected: true, helper: true },
};

const record = (over: object = {}) => ({
  models: 'claude_code', runs_on: 'own_computer', plan_step_done_at: 'x', this_mac: null, setup_on: null,
  setup_step: null, setup_completed_at: null, ...over,
});

function facts(progress: object, over: object = {}) {
  return { progress, answers: { models: 'claude_code', runsOn: 'own_computer' }, ownComputers: [], nextPath: '/setup/computer', ...over };
}

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function open() {
  return renderWithProviders(
    <Routes>
      <Route path="/setup/mac" element={<MacHandoffStep />} />
      <Route path="*" element={<Where />} />
    </Routes>,
    { route: '/setup/mac' },
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  state.refetch = [];
  state.userId = 'u1';
  state.updating = false;
  state.update.mockImplementation(async (patch: object) => patch);
  state.facts = facts(record());
});

describe('continuing on the Mac from a browser', () => {
  it('offers the app, then waits on it once the user goes to get it', async () => {
    const opened = vi.spyOn(window, 'open').mockReturnValue(null);
    const user = userEvent.setup();
    open();
    expect(screen.getByRole('heading', { name: 'Continue on your Mac' })).toBeInTheDocument();
    expect(screen.getByText('Open it and sign in as nadia@example.com')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Get the Mac app/ }));
    expect(opened).toHaveBeenCalledWith('https://langalpha.ai/download', '_blank', 'noopener,noreferrer');
    expect(state.update).toHaveBeenCalledWith({ setup_on: 'mac_pending' });
    expect(screen.getByRole('heading', { name: 'Finish setup in the LangAlpha app on your Mac' })).toBeInTheDocument();
    expect(state.refetch[0]).toEqual({ refetchInterval: 3000 });
  });

  it('starts over for another account signing in here, which never went to get the app', async () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    const user = userEvent.setup();
    const view = open();
    await user.click(screen.getByRole('button', { name: /Get the Mac app/ }));
    expect(screen.getByRole('heading', { name: 'Finish setup in the LangAlpha app on your Mac' })).toBeInTheDocument();
    state.userId = 'u2';
    view.rerender(
      <Routes>
        <Route path="/setup/mac" element={<MacHandoffStep />} />
        <Route path="*" element={<Where />} />
      </Routes>,
    );
    expect(screen.getByRole('heading', { name: 'Continue on your Mac' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Get the Mac app/ })).toBeInTheDocument();
  });

  it('holds every way off the page while a write moving setup is in flight', () => {
    state.updating = true;
    open();
    expect(screen.getByRole('button', { name: /Get the Mac app/ })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Back' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Continue here instead' })).toBeDisabled();
  });

  it('keeps the way to get the app where the write saying so is refused', async () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    state.update.mockRejectedValue(new Error('refused'));
    const user = userEvent.setup();
    open();
    await user.click(screen.getByRole('button', { name: /Get the Mac app/ }));
    expect(await screen.findByRole('alert')).toHaveTextContent("This step couldn't be saved. Try again.");
    expect(screen.getByRole('heading', { name: 'Continue on your Mac' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Get the Mac app/ })).toBeEnabled();
  });

  it('shows where setup went once the app took it, naming the paired Mac', () => {
    state.facts = facts(record({ setup_on: 'app', this_mac: 'paired' }), { ownComputers: [MAC] });
    open();
    expect(screen.getByRole('heading', { name: 'Setup moved to your Mac' })).toBeInTheDocument();
    expect(screen.getByText("You're finishing setup in the LangAlpha app on MacBook Pro.")).toBeInTheDocument();
    expect(screen.getByTestId('mac-handoff-mac')).toHaveTextContent('Paired');
  });

  it('follows setup to the first chat when the app finishes it', async () => {
    state.facts = facts(record({ setup_on: 'app', setup_completed_at: '2026-11-15T10:05:00Z' }));
    open();
    expect(await screen.findByTestId('where')).toHaveTextContent('/chat');
  });

  it('resumes here when the app hands setup back', async () => {
    state.facts = facts(record({ setup_on: 'app' }));
    const view = open();
    state.facts = facts(record({ setup_on: 'browser', setup_step: 'computer' }), { nextPath: '/setup/model' });
    view.rerender(
      <Routes>
        <Route path="/setup/mac" element={<MacHandoffStep />} />
        <Route path="*" element={<Where />} />
      </Routes>,
    );
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/model');
  });

  it('resumes here when the app hands back straight from the tab\'s own Get the Mac app', async () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    const user = userEvent.setup();
    const view = open();
    await user.click(screen.getByRole('button', { name: /Get the Mac app/ }));
    state.facts = facts(record({ setup_on: 'mac_pending' }));
    const routes = () => (
      <Routes>
        <Route path="/setup/mac" element={<MacHandoffStep />} />
        <Route path="*" element={<Where />} />
      </Routes>
    );
    view.rerender(routes());
    state.facts = facts(record({ setup_on: 'browser' }), { nextPath: '/setup/computer' });
    view.rerender(routes());
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/computer');
  });

  it('still resumes on the app\'s hand-back after Continue here instead was refused', async () => {
    state.facts = facts(record({ setup_on: 'mac_pending' }));
    state.update.mockRejectedValueOnce(new Error('refused'));
    const user = userEvent.setup();
    const view = open();
    await user.click(screen.getByRole('button', { name: 'Continue here instead' }));
    expect(screen.queryByTestId('where')).toBeNull();
    state.facts = facts(record({ setup_on: 'browser' }), { nextPath: '/setup/model' });
    view.rerender(
      <Routes>
        <Route path="/setup/mac" element={<MacHandoffStep />} />
        <Route path="*" element={<Where />} />
      </Routes>,
    );
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/model');
  });

  it.each(['mac_pending', 'app'])('takes setup back from the app (%s) before going back to the computer step', async (setupOn) => {
    state.facts = facts(record({ setup_on: setupOn }));
    const user = userEvent.setup();
    open();
    await user.click(screen.getByRole('button', { name: 'Back' }));
    expect(state.update).toHaveBeenCalledWith({ setup_on: 'browser' });
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/computer'));
  });

  it('stays and says so where taking setup back on Back is refused', async () => {
    state.facts = facts(record({ setup_on: 'app' }));
    state.update.mockRejectedValueOnce(new Error('refused'));
    const user = userEvent.setup();
    open();
    await user.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("This step couldn't be saved. Try again.");
    expect(screen.queryByTestId('where')).toBeNull();
  });

  it('goes back without a write while setup is still this tab\'s', async () => {
    const user = userEvent.setup();
    open();
    await user.click(screen.getByRole('button', { name: 'Back' }));
    expect(state.update).not.toHaveBeenCalled();
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/computer');
  });

  it('takes setup back on Continue here instead', async () => {
    state.facts = facts(record({ setup_on: 'mac_pending' }));
    const user = userEvent.setup();
    open();
    await user.click(screen.getByRole('button', { name: 'Continue here instead' }));
    expect(state.update).toHaveBeenCalledWith({ setup_on: 'browser' });
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/computer'));
  });
});
