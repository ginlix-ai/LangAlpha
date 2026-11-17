/**
 * Self-hosted's two questions offer only what the server serves: Claude Code
 * where it is offered, its own models where it serves them, and the user's
 * own computer where SSH computers are offered (the server's computer is
 * then the only answer, so it starts chosen).
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

const h = vi.hoisted(() => ({
  claudeCode: null as null | object,
  claudeCodeUnread: false,
  retryClaudeCode: vi.fn(),
  serves: false,
  providers: [] as { kind: string; allowed?: boolean | null }[],
  progress: null as null | object,
  update: vi.fn(),
  userLoading: false,
  userFailed: false,
  refetchUser: vi.fn(),
  claudeCodeLoading: false,
  computers: [] as object[],
  choice: null as null | string,
  userId: 'a',
  providersFailed: false,
  refetchProviders: vi.fn(),
}));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss', APP_ENTRY_PATH: '/' }));
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({
    user: h.userLoading || h.userFailed ? null : { user_id: h.userId, deployment_serves: h.serves, computer_choice: h.choice },
    isLoading: h.userLoading,
    isError: h.userFailed,
    refetch: h.refetchUser,
  }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: h.claudeCode, isLoading: h.claudeCodeLoading, unread: h.claudeCodeUnread, retry: h.retryClaudeCode }),
}));
vi.mock('@/pages/ChatAgent/hooks/useComputers', () => ({
  useComputerProviders: () => (h.providersFailed
    ? { data: undefined, isError: true, refetch: h.refetchProviders }
    : { data: { providers: h.providers }, isError: false, refetch: h.refetchProviders }),
  useComputers: () => ({ data: { computers: h.computers }, isLoading: false }),
}));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({ progress: h.progress, isLoading: false, update: h.update, isUpdating: false }),
}));

import UseStep from '../UseStep';

function renderStep() {
  render(
    <MemoryRouter>
      <UseStep />
    </MemoryRouter>,
  );
}

describe('UseStep', () => {
  beforeEach(() => {
    h.claudeCode = null;
    h.claudeCodeUnread = false;
    h.serves = false;
    h.providers = [{ kind: 'docker' }];
    h.progress = null;
    h.update = vi.fn(() => Promise.resolve({}));
    h.userLoading = false;
    h.userFailed = false;
    h.refetchUser = vi.fn();
    h.claudeCodeLoading = false;
    h.computers = [];
    h.choice = null;
    h.userId = 'a';
    h.providersFailed = false;
    h.refetchProviders = vi.fn();
  });

  const HOME_STAYS = 'Home stays on this Mac. Move it in Settings.';
  const MAC = { computer_id: 'mac', name: 'MacBook Pro', kind: 'ssh', status: 'running', is_primary: true, ssh: { helper: true } };

  it('says Home stays on this Mac under the cloud answer while the Mac holds it', () => {
    h.providers = [{ kind: 'docker' }, { kind: 'ssh' }];
    h.computers = [MAC];
    h.progress = { models: 'subscription_or_key', runs_on: 'cloud' };
    renderStep();
    expect(screen.getByText(HOME_STAYS)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('radio', { name: /On your own computer/ }));
    expect(screen.queryByText(HOME_STAYS)).toBeNull();
  });

  it('says nothing where Home is on an SSH host rather than a Mac paired through the app', () => {
    h.computers = [{ ...MAC, ssh: { helper: false } }];
    renderStep();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
    expect(screen.queryByText(HOME_STAYS)).toBeNull();
  });

  it('says nothing where Home moved to a cloud computer since the own-machine choice', () => {
    h.choice = 'own_machine';
    h.computers = [{ ...MAC, is_primary: false }, { computer_id: 'cl', kind: 'docker', status: 'running', is_primary: true }];
    renderStep();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
    expect(screen.queryByText(HOME_STAYS)).toBeNull();
  });

  it('says nothing where Home is on a cloud computer or none', () => {
    h.computers = [{ ...MAC, is_primary: false }, { computer_id: 'cl', kind: 'docker', status: 'running', is_primary: true }];
    renderStep();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
    expect(screen.queryByText(HOME_STAYS)).toBeNull();
  });

  it.each(['userLoading', 'claudeCodeLoading'] as const)('offers no answer to save while a read that defines them is pending (%s)', (read) => {
    h[read] = true;
    renderStep();
    expect(screen.queryByRole('button', { name: 'Continue' })).toBeNull();
    expect(screen.queryByRole('radio')).toBeNull();
  });

  it('offers to read the profile again where it could not be read, instead of the answers without it', () => {
    h.userFailed = true;
    renderStep();
    expect(screen.queryByRole('radio')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.refetchUser).toHaveBeenCalled();
  });

  it('offers to read the computer providers again where they could not be read, instead of an own computer it never read', () => {
    h.providersFailed = true;
    h.progress = { models: 'subscription_or_key', runs_on: 'own_computer' };
    renderStep();
    expect(screen.queryByRole('radio')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Continue' })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.refetchProviders).toHaveBeenCalled();
  });

  it("drops one account's picks when another signs in, for that account's saved answers", () => {
    h.providers = [{ kind: 'docker' }, { kind: 'ssh' }];
    h.progress = { models: 'subscription_or_key', runs_on: 'cloud' };
    const { rerender } = render(<MemoryRouter><UseStep /></MemoryRouter>);
    fireEvent.click(screen.getByRole('radio', { name: /On your own computer/ }));
    expect(screen.getByRole('radio', { name: /On your own computer/ })).toBeChecked();
    h.userId = 'b';
    rerender(<MemoryRouter><UseStep /></MemoryRouter>);
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
  });

  it('offers only what the server serves', () => {
    renderStep();
    expect(screen.queryByRole('radio', { name: /Claude Code/ })).toBeNull();
    expect(screen.queryByRole('radio', { name: /Models this server provides/ })).toBeNull();
    expect(screen.queryByRole('radio', { name: /On your own computer/ })).toBeNull();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
  });

  it('offers Claude Code, its models and an own computer where the server does', () => {
    h.claudeCode = { on: false };
    h.serves = true;
    h.providers = [{ kind: 'docker' }, { kind: 'ssh' }];
    renderStep();
    expect(screen.getByRole('radio', { name: /Claude Code/ })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Models this server provides/ })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /On your own computer/ })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).not.toBeChecked();
  });

  it('offers no own computer where the plan leaves SSH computers out', () => {
    h.providers = [{ kind: 'docker' }, { kind: 'ssh', allowed: false }];
    renderStep();
    expect(screen.queryByRole('radio', { name: /On your own computer/ })).toBeNull();
    expect(screen.getByRole('radio', { name: /On a computer this server runs/ })).toBeChecked();
  });

  it('offers to read Claude Code\'s setup again where it could not be read, instead of offering it unread', () => {
    h.claudeCodeUnread = true;
    h.retryClaudeCode = vi.fn();
    renderStep();
    expect(screen.queryByRole('radio')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retryClaudeCode).toHaveBeenCalled();
  });

  it('starts finished setup over when the answers change, and keeps its place when they do not', async () => {
    h.serves = true;
    h.progress = { models: 'subscription_or_key', runs_on: 'cloud', setup_step: 'default', setup_completed_at: '2026-11-01T00:00:00Z' };
    renderStep();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(h.update).toHaveBeenCalledWith({ models: 'subscription_or_key', runs_on: 'cloud' }));
    fireEvent.click(screen.getByRole('radio', { name: /Models this server provides/ }));
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(h.update).toHaveBeenLastCalledWith({
      models: 'langalpha_models', runs_on: 'cloud', setup_step: null, setup_completed: null, plan_step_done: null,
    }));
  });
});
