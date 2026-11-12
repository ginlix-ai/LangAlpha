/**
 * The Claude sign-in is listed once connected, or once its status is read and
 * the server does not say the user may not start one (`can_sign_in` false). A
 * server that does not say (null) offers it to everyone. While the status
 * loads, it is not offered.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { OAuthStatus } from '@/api/oauth';

const status = vi.hoisted(() => ({ data: undefined as unknown, provider: vi.fn() }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useOAuthStatus: (provider: string) => {
    status.provider(provider);
    return { data: status.data };
  },
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { SubscriptionChoices } from '../SubscriptionChoices';

const claude = (canSignIn: boolean | null) => ({
  connected: false, state: 'disconnected', can_sign_in: canSignIn,
}) as OAuthStatus;
const names = () => screen.getAllByRole('button').map((b) => b.querySelector('.truncate')?.textContent);
const render = (configured: string[] = [], claudeCode = false) => renderWithProviders(
  <SubscriptionChoices
    planGroups={[]}
    configured={new Set(configured)}
    onPickSignIn={vi.fn()}
    onPickPlan={vi.fn()}
    claudeCode={claudeCode ? { status: 'not_connected', onPick: vi.fn() } : undefined}
  />,
);

describe('SubscriptionChoices: Claude sign-in', () => {
  beforeEach(() => {
    status.data = undefined;
    status.provider.mockClear();
  });

  it('reads the status of the Claude sign-in', () => {
    status.data = claude(true);
    render();
    expect(status.provider).toHaveBeenCalledWith('claude-oauth');
  });

  it('lists it where the user may start a sign-in', () => {
    status.data = claude(true);
    render();
    expect(names()).toEqual(['ChatGPT', 'Claude', 'Grok']);
  });

  it('hides it where the user may not start one and it is not connected', () => {
    status.data = claude(false);
    render();
    expect(names()).toEqual(['ChatGPT', 'Grok']);
  });

  it('lists it when the server does not say, as an older one answers', () => {
    status.data = claude(null);
    render();
    expect(names()).toEqual(['ChatGPT', 'Claude', 'Grok']);
  });

  it('hides it while the status loads', () => {
    status.data = undefined;
    render();
    expect(names()).toEqual(['ChatGPT', 'Grok']);
  });

  it('keeps it where the user may not start one but it is connected', () => {
    status.data = claude(false);
    render(['claude-oauth']);
    expect(names()).toEqual(['ChatGPT', 'Claude', 'Grok']);
  });

  it('keeps it while the status loads when it is connected', () => {
    status.data = undefined;
    render(['claude-oauth']);
    expect(names()).toEqual(['ChatGPT', 'Claude', 'Grok']);
  });

  it('never hides the other sign-ins on can_sign_in', () => {
    status.data = claude(false);
    render();
    expect(screen.getByText('ChatGPT')).toBeInTheDocument();
    expect(screen.getByText('Grok')).toBeInTheDocument();
  });

  it('still lists the Claude Code row where the Claude sign-in is hidden', () => {
    status.data = claude(false);
    render([], true);
    expect(names()).toEqual(['ChatGPT', 'Claude Code', 'Grok']);
  });

  it('lists the Claude Code row while the status loads', () => {
    status.data = undefined;
    render([], true);
    expect(names()).toEqual(['ChatGPT', 'Claude Code', 'Grok']);
  });

  it('lists Claude Code right under the Claude sign-in where both show', () => {
    status.data = claude(true);
    render([], true);
    expect(names()).toEqual(['ChatGPT', 'Claude', 'Claude Code', 'Grok']);
  });
});
