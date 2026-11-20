/**
 * The cloud steps apply only once what they rest on is read: Home's provider,
 * whose sizes the steps offer, and the account's rules on /users/me, which can
 * keep Claude Code off. A read still going keeps them loading; one that failed
 * offers none.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';

const h = vi.hoisted(() => ({
  providers: undefined as unknown,
  providersLoading: false,
  user: null as unknown,
  userLoading: false,
  rulesUnknown: false,
  allowed: true,
  setup: null as unknown,
  custody: null as unknown,
  home: null as unknown,
  feature: null as unknown,
}));

vi.mock('@/hooks/useAccount', () => ({
  useRule: () => ({ allowed: h.allowed, who: { start: '', mid: '' } }),
  useAccount: () => ({ unknown: h.rulesUnknown }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: h.setup, isLoading: false }),
  useClaudeCodeStatus: () => ({ saved: false, isLoading: false, email: null }),
}));
vi.mock('@/hooks/useClaudeCodeCustody', () => ({
  useClaudeCodeCustody: () => ({ custody: h.custody, isLoading: false }),
}));
vi.mock('@/hooks/useFeatures', () => ({
  useFeatures: () => ({ data: [h.feature] }),
}));
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: h.user, isLoading: h.userLoading, isError: false }),
}));
vi.mock('@/pages/ChatAgent/hooks/useTierQuota', async (orig) => ({
  ...(await orig<object>()),
  useTierQuota: () => ({ data: null, isLoading: false }),
}));
vi.mock('@/pages/ChatAgent/hooks/useComputers', async (orig) => ({
  ...(await orig<object>()),
  useComputers: () => ({ data: { computers: [h.home] }, isLoading: false }),
  useComputerProviders: () => ({ data: h.providers, isLoading: h.providersLoading }),
}));

import i18n from '@/i18n';
import { stepsMissing, useClaudeCodeSteps } from '../useClaudeCodeSteps';

const HOME = { computer_id: 'home', kind: 'docker', is_primary: true, status: 'running', resource_tier: 'standard', spec_change: null };
const DOCKER = { providers: [{ kind: 'docker', tiers: [{ tier: 'standard' }, { tier: 'performance' }, { tier: 'max' }] }] };
const status = () => renderHook(() => useClaudeCodeSteps()).result.current.status;

describe('useClaudeCodeSteps: what the steps wait on', () => {
  beforeEach(() => {
    h.providers = DOCKER;
    h.providersLoading = false;
    h.user = { user_id: 'u1', policy: undefined };
    h.userLoading = false;
    h.rulesUnknown = false;
    h.allowed = true;
    h.setup = { on: true, cloud: true, computers: [] };
    h.custody = { mode: 'local', offered: ['stored'] };
    h.home = HOME;
    h.feature = { key: 'claude_code_runtime', gate: 'opt_in', enabled: false, set_by_organization: false };
  });

  it('applies once Home\'s provider and the account are read', () => {
    expect(status()).toBe('pending');
  });

  it('offers no step where the providers could not be read, or name no entry for Home', () => {
    h.providers = undefined;
    expect(status()).toBe('none');
    h.providers = { providers: [{ kind: 'e2b', tiers: [{ tier: 'performance' }] }] };
    expect(status()).toBe('none');
  });

  it('stays loading while the account is read, and offers no step once that read failed', () => {
    h.user = null;
    h.userLoading = true;
    expect(status()).toBe('loading');
    h.userLoading = false;
    expect(status()).toBe('none');
  });

  it('offers no step while the account\'s rules cannot be read', () => {
    h.rulesUnknown = true;
    expect(status()).toBe('none');
  });

  it('applies while Claude Code is off, whose choice is not read, where the server would store the token once it is on', () => {
    h.custody = null;
    h.setup = { on: false, cloud: true, computers: [] };
    expect(status()).toBe('pending');
    h.setup = { on: false, cloud: false, computers: [] };
    expect(status()).toBe('none');
  });

  it('holds the resize while a change to a smaller size runs, which no other change can start beside, and says so in the picker', () => {
    h.home = { ...HOME, resource_tier: 'performance', spec_change: { target_tier: 'standard', from_tier: 'performance', state: 'in_progress', started_at: '2026-11-19T00:00:00Z' } };
    const steps = renderHook(() => useClaudeCodeSteps()).result.current;
    expect(steps.medium.state).toBe('locked');
    expect(steps.next).toBe('medium');
    expect(stepsMissing(i18n.getFixedT('en-US'), steps)).toBe('Its computer is changing to Small.');
  });

  it('offers no step while Claude Code is off where the organization turned it off, which refuses the turn-on', () => {
    h.custody = null;
    h.setup = { on: false, cloud: true, computers: [] };
    h.feature = { key: 'claude_code_runtime', gate: 'opt_in', enabled: false, set_by_organization: true };
    expect(status()).toBe('none');
  });

  it('offers no step while Claude Code is on and its choice could not be read', () => {
    h.custody = null;
    expect(status()).toBe('none');
  });
});
