/**
 * "Continue setup on this Mac?": what is left, and either answer moving the
 * record and the app. A Mac never asked to pair is asked first.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Route, Routes, useLocation } from 'react-router';

import { renderWithProviders } from '@/test/utils';

vi.mock('@/config/hostMode', () => ({ HOST_MODE: 'platform', isPlatformMode: true, APP_ENTRY_PATH: '/app' }));
vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/desktop')>()),
  isDesktopShell: () => state.desktop,
  canRemoteControl: () => true,
  canAddThisMac: () => true,
  getRemoteControl: async () => ({ supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: 'nadia' }),
}));
vi.mock('@/pages/ChatAgent/utils/api', () => ({ getComputers: vi.fn(), getComputerProviders: vi.fn() }));

const state = vi.hoisted(() => ({ update: vi.fn(), facts: {} as Record<string, unknown>, desktop: true, updating: false }));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({ progress: null, isLoading: false, isError: false, update: state.update, isUpdating: state.updating }),
}));
vi.mock('@/pages/Setup/answers/useSetupFacts', () => ({ useSetupFacts: () => state.facts }));

import { getComputerProviders, getComputers } from '@/pages/ChatAgent/utils/api';
import ContinueStep from '../ContinueStep';

function facts(thisMac: string | null, over: object = {}) {
  return {
    isLoading: false,
    progress: { models: 'claude_code', runs_on: 'own_computer', this_mac: thisMac, setup_on: 'mac_pending', setup_step: null, setup_completed_at: null },
    answers: { models: 'claude_code', runsOn: 'own_computer' },
    facts: {},
    rail: ['computer', 'model', 'default'],
    next: 'model',
    nextPath: '/setup/model',
    ...over,
  };
}

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function open() {
  return renderWithProviders(
    <Routes>
      <Route path="/setup/continue" element={<ContinueStep />} />
      <Route path="*" element={<Where />} />
    </Routes>,
    { route: '/setup/continue' },
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  state.desktop = true;
  state.updating = false;
  state.update.mockImplementation(async (patch: object) => patch);
  state.facts = facts('paired');
  (getComputerProviders as Mock).mockResolvedValue({ providers: [{ kind: 'ssh', is_default: false, allowed: null }], tunnels: true });
  (getComputers as Mock).mockResolvedValue({ computers: [], total: 0 });
});

describe('the continue prompt', () => {
  it('lists what is left for a paired Mac, and Continue here takes setup over', async () => {
    const user = userEvent.setup();
    open();
    expect(await screen.findByRole('heading', { name: 'Continue setup on this Mac?' })).toBeInTheDocument();
    expect(screen.getByText('This Mac is paired')).toBeInTheDocument();
    expect([...screen.getByTestId('continue-left').children].map((li) => li.textContent?.replace(/^\d+/, ''))).toEqual([
      'Your Claude setup token for this Mac', 'Your default model', 'A few questions about how you invest', 'Your first chat',
    ]);
    await user.click(screen.getByRole('button', { name: 'Continue here' }));
    expect(state.update).toHaveBeenCalledWith({ setup_on: 'app' });
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/model'));
  });

  it('starts with a computer where the Mac was not paired, and can hand setup back to the browser', async () => {
    state.facts = facts('declined', { next: 'computer', nextPath: '/setup/computer' });
    const user = userEvent.setup();
    open();
    expect(await screen.findByText("This Mac isn't paired")).toBeInTheDocument();
    expect(screen.getByText(/You can pair it later from Computers/)).toBeInTheDocument();
    expect(screen.getByTestId('continue-left').firstChild).toHaveTextContent('A computer for your tasks');
    await user.click(screen.getByRole('button', { name: "I'll finish in the browser" }));
    expect(state.update).toHaveBeenCalledWith({ setup_on: 'browser' });
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/$/));
  });

  it('asks to pair first where this Mac could be paired and was never asked', async () => {
    state.facts = facts(null);
    open();
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/this-mac');
  });

  it('reads the computers again where they failed, rather than guessing whether this Mac was asked', async () => {
    (getComputers as Mock).mockRejectedValueOnce(new Error('down'));
    state.facts = facts(null);
    const user = userEvent.setup();
    open();
    expect(await screen.findByTestId('setup-load-failed')).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Continue setup on this Mac?' })).toBeNull();
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(await screen.findByTestId('where')).toHaveTextContent('/setup/this-mac');
  });

  it('never lets a browser take setup over, sending it where setup resumes', () => {
    state.desktop = false;
    state.facts = facts('declined', { progress: { ...facts('declined').progress, setup_on: 'browser' } });
    open();
    expect(screen.queryByRole('button', { name: /Continue here/ })).toBeNull();
    expect(screen.getByTestId('where')).toHaveTextContent('/setup');
    expect(state.update).not.toHaveBeenCalled();
  });

  it('reads the facts again where one could not be read, instead of loading for good', async () => {
    const retryFacts = vi.fn();
    state.facts = facts('paired', { factsFailed: true, next: null, nextPath: null, retryFacts });
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('button', { name: 'Try again' }));
    expect(retryFacts).toHaveBeenCalled();
  });

  it('holds both choices while one is being saved', async () => {
    state.updating = true;
    open();
    expect(await screen.findByRole('button', { name: "I'll finish in the browser" })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Continue here' })).toBeDisabled();
  });

  it('reads the record again where it failed', async () => {
    const retryProgress = vi.fn();
    state.facts = facts(null, { progress: null, answers: null, facts: null, next: null, progressFailed: true, retryProgress });
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('button', { name: 'Try again' }));
    expect(retryProgress).toHaveBeenCalled();
  });
});
