/**
 * Where Claude Code's token goes for each answer: on the platform, onto the
 * user's own computer for an own-computer answer and saved for the cloud
 * otherwise; on a self-hosted server, saved by the server whatever the
 * answer, since Home's turns run on that token.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ReactNode } from 'react';
import { renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const h = vi.hoisted(() => ({
  platform: true,
  runsOn: 'own_computer',
  status: { saved: false, state: 'no_token' } as { saved: boolean; state: string },
  ccComputers: [] as object[],
  computers: [{ computer_id: 'c1', name: 'lab-server', kind: 'ssh', status: 'running', created_at: '2026-11-01' }] as object[],
  custody: null as null | object,
  models: 'claude_code',
  providers: [] as object[],
  byok: false,
  computersFailed: false,
  providersUnread: false,
  ccUnread: false,
  statusUnread: false,
  progressFailed: false,
  policy: null as null | object,
}));
vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return h.platform; },
  HOST_MODE: 'platform',
  APP_ENTRY_PATH: '/app',
}));
vi.mock('@/hooks/useOnboardingProgress', () => ({
  useOnboardingProgress: () => ({
    progress: h.progressFailed ? null : { models: h.models, runs_on: h.runsOn, this_mac: null, setup_on: null, setup_step: null, setup_completed_at: null, plan_step_done_at: null },
    isLoading: false,
    isError: h.progressFailed,
    refetch: vi.fn(),
  }),
}));
vi.mock('@/pages/ChatAgent/hooks/useComputers', () => ({
  useComputers: () => ({
    data: h.computersFailed ? undefined : { computers: h.computers },
    isError: h.computersFailed,
    isLoading: false,
  }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeComputers: () => ({ computers: h.ccComputers, isLoading: false, unread: h.ccUnread, readAt: Date.parse('2026-11-18T12:00:00Z') }),
  useClaudeCodeStatus: () => ({ ...h.status, isLoading: false, unread: h.statusUnread }),
}));
vi.mock('@/hooks/useClaudeCodeCustody', () => ({
  useClaudeCodeCustody: () => ({ custody: h.custody, isLoading: false }),
}));
vi.mock('@/hooks/useAccount', () => ({ useAccount: () => ({ policy: h.policy }) }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  SERVER_KEY: '(server)',
  useConfiguredProviders: () => ({ providers: h.providers, byokEnabled: h.byok, isLoading: false, unread: h.providersUnread }),
}));

import { useSetupFacts } from '../useSetupFacts';

const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={new QueryClient()}>{children}</QueryClientProvider>
);
const readFacts = () => renderHook(() => useSetupFacts(), { wrapper }).result.current;

beforeEach(() => {
  h.computers = [{ computer_id: 'c1', name: 'lab-server', kind: 'ssh', status: 'running', created_at: '2026-11-01' }];
  h.models = 'claude_code';
  h.providers = [];
  h.computersFailed = false;
  h.providersUnread = false;
  h.ccUnread = false;
  h.statusUnread = false;
  h.progressFailed = false;
  h.policy = null;
});

describe('useSetupFacts record', () => {
  it('resumes on no step while the record itself could not be read', () => {
    h.progressFailed = true;
    const facts = readFacts();
    expect(facts.factsFailed).toBe(true);
    expect(facts.next).toBeNull();
  });
});

describe('useSetupFacts token target', () => {
  beforeEach(() => {
    h.platform = true;
    h.runsOn = 'own_computer';
    h.status = { saved: false, state: 'no_token' };
    h.ccComputers = [];
  });

  it('puts the token on the own computer on the platform', () => {
    const facts = readFacts();
    expect(facts.tokenTarget).toEqual({ kind: 'computer', computerId: 'c1', name: 'lab-server' });
  });

  it('saves it for the cloud on the platform for a cloud answer', () => {
    h.runsOn = 'cloud';
    const facts = readFacts();
    expect(facts.tokenTarget).toEqual({ kind: 'saved' });
  });

  it('has the server save it on self-hosted, own computer answered too', () => {
    h.platform = false;
    const facts = readFacts();
    expect(facts.tokenTarget).toEqual({ kind: 'saved' });
    expect(facts.rail).toContain('computer');
  });
});

describe('useSetupFacts with Home on this Mac and a cloud answer', () => {
  const MAC = { computer_id: 'mac', name: 'MacBook Pro', kind: 'ssh', status: 'running', is_primary: true, created_at: '2026-11-01', ssh: { helper: true } };

  beforeEach(() => {
    h.platform = true;
    h.runsOn = 'cloud';
    h.computers = [MAC];
    h.status = { saved: true, state: 'signed_in' };
    h.ccComputers = [];
  });

  it('puts the token on the Mac Home runs on, and counts only that one', () => {
    expect(readFacts().tokenTarget).toEqual({ kind: 'computer', computerId: 'mac', name: 'MacBook Pro' });
    expect(readFacts().next).toBe('model');
    h.ccComputers = [{ computer_id: 'mac', source: 'machine', state: 'signed_in', expires_at: null }];
    expect(readFacts().next).toBe('default');
    expect(readFacts().rail).not.toContain('computer');
  });

  it('targets the Mac over another machine of the user\'s own whose token works', () => {
    h.computers = [MAC, { computer_id: 'box', name: 'lab-server', kind: 'ssh', status: 'running', is_primary: false, created_at: '2026-11-10' }];
    h.ccComputers = [{ computer_id: 'box', source: 'machine', state: 'signed_in', expires_at: null }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'computer', computerId: 'mac', name: 'MacBook Pro' });
    expect(readFacts().next).toBe('model');
  });

  it('puts it on the Mac Home runs on for an own-computer answer too, over another whose token works', () => {
    h.runsOn = 'own_computer';
    h.computers = [MAC, { computer_id: 'box', name: 'lab-server', kind: 'ssh', status: 'running', is_primary: false, created_at: '2026-11-10' }];
    h.ccComputers = [{ computer_id: 'box', source: 'machine', state: 'signed_in', expires_at: null }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'computer', computerId: 'mac', name: 'MacBook Pro' });
    expect(readFacts().next).toBe('model');
  });

  it('on self-hosted, puts it on the Mac once it holds a token of its own, which a turn signs in with over the saved one', () => {
    h.platform = false;
    h.ccComputers = [{ computer_id: 'mac', source: 'machine', state: 'signed_in', expires_at: '2026-11-18T11:00:00Z' }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'computer', computerId: 'mac', name: 'MacBook Pro' });
    expect(readFacts().next).toBe('model');
    h.ccComputers = [{ computer_id: 'mac', source: 'saved', state: 'signed_in', expires_at: null }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'saved' });
    expect(readFacts().next).toBe('default');
  });

  it('on self-hosted, offers no saved token while whether the Mac holds one of its own is unknown', () => {
    h.platform = false;
    h.ccComputers = [{ computer_id: 'mac', source: null, state: 'checking', expires_at: null }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'computer', computerId: 'mac', name: 'MacBook Pro' });
    expect(readFacts().next).toBe('model');
    h.ccComputers = [];
    h.ccUnread = true;
    expect(readFacts().factsFailed).toBe(true);
    expect(readFacts().next).toBeNull();
  });

  it('on self-hosted, resumes on no step while the saved token\'s status could not be read', () => {
    h.platform = false;
    h.computers = [];
    h.statusUnread = true;
    expect(readFacts().tokenTarget).toEqual({ kind: 'saved' });
    expect(readFacts().factsFailed).toBe(true);
    expect(readFacts().next).toBeNull();
  });

  it('saves it for the cloud where Home is on a cloud computer, and on self-hosted', () => {
    h.computers = [{ ...MAC, is_primary: false }, { computer_id: 'cl', name: 'cloud', kind: 'daytona', status: 'running', is_primary: true }];
    expect(readFacts().tokenTarget).toEqual({ kind: 'saved' });
    h.computers = [MAC];
    h.platform = false;
    expect(readFacts().tokenTarget).toEqual({ kind: 'saved' });
  });
});

describe('useSetupFacts token in place', () => {
  beforeEach(() => {
    h.platform = true;
    h.status = { saved: false, state: 'no_token' };
    h.ccComputers = [];
    h.custody = null;
    h.computers = [{ computer_id: 'c1', name: 'lab-server', kind: 'ssh', status: 'running', created_at: '2026-11-01' }];
  });

  it('counts no cloud token where this server runs Claude Code on no cloud computer', () => {
    h.runsOn = 'cloud';
    h.status = { saved: true, state: 'signed_in' };
    h.custody = { mode: 'stored', offered: ['stored'], refusal: 'needs_own_computer' };
    expect(readFacts().next).toBe('model');
  });

  it('counts no own computer\'s token past its expiry, whatever its last check said', () => {
    h.runsOn = 'own_computer';
    h.ccComputers = [{ computer_id: 'c1', source: 'machine', state: 'signed_in', expires_at: '2026-11-18T11:00:00Z' }];
    expect(readFacts().next).toBe('model');
  });

  it('counts a working token on any own computer, and puts the step on that one', () => {
    h.runsOn = 'own_computer';
    h.computers = [
      { computer_id: 'c1', name: 'lab-server', kind: 'ssh', status: 'running', created_at: '2026-11-02' },
      { computer_id: 'c2', name: 'studio', kind: 'ssh', status: 'running', created_at: '2026-11-01' },
    ];
    h.ccComputers = [
      { computer_id: 'c1', source: 'machine', state: 'rejected' },
      { computer_id: 'c2', source: 'machine', state: 'signed_in', expires_at: null },
    ];
    const facts = readFacts();
    expect(facts.next).toBe('default');
    expect(facts.tokenTarget).toEqual({ kind: 'computer', computerId: 'c2', name: 'studio' });
  });

  it('counts a saved token for the cloud only while Claude takes it', () => {
    h.runsOn = 'cloud';
    h.status = { saved: true, state: 'signed_in' };
    expect(readFacts().next).toBe('default');
    h.status = { saved: true, state: 'rejected' };
    expect(readFacts().next).toBe('model');
  });

  it('counts the own computer\'s token only while Claude takes it', () => {
    h.runsOn = 'own_computer';
    h.ccComputers = [{ computer_id: 'c1', source: 'machine', state: 'signed_in' }];
    expect(readFacts().next).toBe('default');
    h.ccComputers = [{ computer_id: 'c1', source: 'machine', state: 'rejected' }];
    expect(readFacts().next).toBe('model');
  });

  it.each(['policy_blocked', 'needs_perl', 'offline'])('counts no own computer\'s token Claude Code cannot run on (%s)', (state) => {
    h.runsOn = 'own_computer';
    h.ccComputers = [{ computer_id: 'c1', source: 'machine', state }];
    expect(readFacts().next).toBe('model');
  });

  it('counts no saved token an organization policy blocks', () => {
    h.runsOn = 'cloud';
    h.status = { saved: true, state: 'policy_blocked' };
    expect(readFacts().next).toBe('model');
  });
});

describe('useSetupFacts provider connected', () => {
  beforeEach(() => {
    h.platform = true;
    h.runsOn = 'cloud';
    h.models = 'subscription_or_key';
    h.byok = false;
  });

  it('counts a key only while BYOK is on, as a turn runs on it', () => {
    h.providers = [{ provider: 'openai', access_type: 'api_key', state: 'connected', masked_key: 'sk-...abcd' }];
    expect(readFacts().next).toBe('model');
    h.byok = true;
    expect(readFacts().next).toBe('default');
  });

  it('counts no key or subscription the account\'s rules keep off', () => {
    h.byok = true;
    h.providers = [
      { provider: 'openai', access_type: 'api_key', state: 'connected', masked_key: 'sk-...abcd' },
      { provider: 'codex', access_type: 'oauth', state: 'connected' },
    ];
    h.policy = { own_model_access: { api_keys: false, subscriptions: false } };
    expect(readFacts().next).toBe('model');
    h.policy = { own_model_access: { api_keys: true, subscriptions: false } };
    expect(readFacts().next).toBe('default');
  });

  it('counts no key the rules keep off on a self-hosted server either', () => {
    h.platform = false;
    h.providers = [{ provider: 'openai', access_type: 'api_key', state: 'connected', masked_key: 'sk-...abcd' }];
    h.policy = { own_model_access: { api_keys: false, subscriptions: true } };
    expect(readFacts().next).toBe('model');
  });

  it('counts a subscription only while it is signed in, BYOK off', () => {
    h.providers = [{ provider: 'codex', access_type: 'oauth', state: 'connected' }];
    expect(readFacts().next).toBe('default');
    h.providers = [{ provider: 'codex', access_type: 'oauth', state: 'needs_reauth' }];
    expect(readFacts().next).toBe('model');
  });
});

describe('useSetupFacts reads that failed', () => {
  beforeEach(() => {
    h.platform = true;
    h.byok = false;
  });

  it('resumes on no step while the own computers could not be read', () => {
    h.runsOn = 'own_computer';
    h.models = 'langalpha_models';
    h.computersFailed = true;
    const facts = readFacts();
    expect(facts.factsFailed).toBe(true);
    expect(facts.next).toBeNull();
  });

  it('resumes on no step while what is connected could not be read, unless a connection was', () => {
    h.runsOn = 'cloud';
    h.models = 'subscription_or_key';
    h.providersUnread = true;
    expect(readFacts().next).toBeNull();
    h.providers = [{ provider: 'codex', access_type: 'oauth', state: 'connected' }];
    expect(readFacts().next).toBe('default');
  });
});
