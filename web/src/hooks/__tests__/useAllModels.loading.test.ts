/**
 * `useAllModels().isLoading` gates the composer's model picker, so it has to
 * cover every input to "which models can this account reach". The platform
 * access answer is one of them: while it is pending, a locked model reads as
 * reachable and a pick would save it as the account default.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  listsLoading: false,
  platformLoading: false,
  prefsLoaded: true,
  preferences: {} as Record<string, unknown>,
  features: [] as Array<{ key: string; enabled: boolean }>,
  claudeCode: vi.fn<() => Promise<ClaudeCodeStatus>>(),
}));

// The providers' own lists are read through React Query; none is read here.
vi.mock('../useProviderModels', () => ({ useMissingModels: () => ({ missingByProvider: new Map(), isLoading: h.listsLoading }) }));
vi.mock('../useModels', () => ({
  useModels: () => ({
    models: {
      models: {
        'prov-x': { models: ['model-x'], display_name: 'X' },
        'claude-code': { models: ['claude-sonnet-5-5-code'], display_name: 'Claude Code' },
      },
      model_metadata: {
        'model-x': { provider: 'prov-x' },
        'claude-sonnet-5-5-code': { provider: 'claude-code', sdk: 'claude-code', access_type: 'oauth' },
      },
    },
    isLoading: false,
  }),
}));
vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: h.prefsLoaded }),
}));
vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], isLoading: false }),
}));
// Flags are read through React Query; settled here, so only the hook's own
// reads are in flight.
vi.mock('../useFeatures', async (importActual) => ({
  ...(await importActual<typeof import('../useFeatures')>()),
  useFeatures: () => ({ data: h.features, isLoading: false }),
}));
vi.mock('@/api/model', () => ({ getClaudeCodeStatus: () => h.claudeCode() }));
// The status route's own client, for the one read below that goes through it.
vi.mock('@/api/client', () => ({ api: { get: vi.fn() } }));
vi.mock('../usePlatformModels', () => ({
  usePlatformModels: () => ({ platform: null, isLoading: h.platformLoading }),
  useModelAccessMap: () => ({}),
  getModelAccess: () => 'platform',
}));

import type { ClaudeCodeStatus } from '@/api/model';
import { useAllModels } from '../useAllModels';
import { resolveComposerModel } from '@/components/ui/chat-input.models';
import { api } from '@/api/client';

const status = (state: ClaudeCodeStatus['state']): ClaudeCodeStatus => ({ state, email: null, expires_at: null });

describe('useAllModels — isLoading', () => {
  beforeEach(() => { h.platformLoading = false; h.prefsLoaded = true; h.listsLoading = false; });

  it('stays loading while a connection\'s own model list is on its first read', () => {
    // Until it answers, the manifest models the credential cannot run read as
    // reachable, and a pick would be saved on the thread.
    h.listsLoading = true;
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.isLoading).toBe(true);
  });

  it('stays loading while only the platform access answer is pending', () => {
    h.platformLoading = true;
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.isLoading).toBe(true);
  });

  it('settles once every input has answered', () => {
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.isLoading).toBe(false);
  });
});

describe('useAllModels — catalogModelNames', () => {
  beforeEach(() => { h.platformLoading = false; h.prefsLoaded = true; h.preferences = {}; });

  it('carries a custom provider\'s own name, which the server runs as a model', () => {
    h.preferences = {
      model_preference: { custom_providers: [{ name: 'my-gateway', parent_provider: 'prov-x' }] },
    };
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.catalogModelNames.has('my-gateway')).toBe(true);
  });

  it('carries a model the user cannot reach, since only the catalog decides it is gone', () => {
    // No provider is configured, so the model is out of reach but still exists.
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.validModelNames.has('model-x')).toBe(false);
    expect(result.current.catalogModelNames.has('model-x')).toBe(true);
  });

  it('stays empty until the custom models are read, so no name reads as gone early', () => {
    h.prefsLoaded = false;
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.catalogModelNames.size).toBe(0);
  });
});

describe('useAllModels — Claude Code', () => {
  const flagged = [{ key: 'claude_code_runtime', enabled: true }];
  const has = (groups: Record<string, { models?: string[] }>) =>
    Object.values(groups).some((g) => g.models?.includes('claude-sonnet-5-5-code'));

  beforeEach(() => {
    h.platformLoading = false;
    h.prefsLoaded = true;
    h.preferences = {};
    h.features = [];
    h.claudeCode.mockReset();
  });

  it('lists none and reads no status without the flag', () => {
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(has(result.current.models)).toBe(false);
    expect(h.claudeCode).not.toHaveBeenCalled();
  });

  it('lists none when the server does not run it for the user, or its status could not be read', async () => {
    h.features = flagged;
    const answers = [async () => status('off'), async () => { throw new Error('404'); }];
    for (const answer of answers) {
      h.claudeCode.mockImplementationOnce(answer);
      const { result } = renderHookWithProviders(() => useAllModels());
      await waitFor(() => expect(result.current.isLoading).toBe(false));
      expect(has(result.current.models)).toBe(false);
    }
  });

  // Whether the user can run it today does not decide that it exists: the
  // composer keeps the thread on it, and a send gets the server's refusal.
  it('keeps a thread on one in the composer, without the flag or after a failed status read', async () => {
    const useThreadComposer = () => {
      const all = useAllModels();
      return { all, composer: resolveComposerModel('claude-sonnet-5-5-code', 'model-x', all.catalogModelNames) };
    };
    const off = renderHookWithProviders(useThreadComposer);
    expect(off.result.current.composer).toEqual({ seed: 'claude-sonnet-5-5-code', retired: null });

    h.features = flagged;
    h.claudeCode.mockRejectedValueOnce(new Error('status unavailable'));
    const failed = renderHookWithProviders(useThreadComposer);
    await waitFor(() => expect(failed.result.current.all.isLoading).toBe(false));
    expect(failed.result.current.composer).toEqual({ seed: 'claude-sonnet-5-5-code', retired: null });
  });

  // A new web on a server from before the status route: the route answers
  // 404, read through the real client call. Nothing is offered, and a thread
  // holding a Claude Code model keeps it rather than reading as retired.
  it('offers none against an older server whose status route answers 404, and keeps a thread on one', async () => {
    const { getClaudeCodeStatus } = await vi.importActual<typeof import('@/api/model')>('@/api/model');
    vi.mocked(api.get).mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 404'), { response: { status: 404, data: { detail: 'Not Found' } } }),
    );
    h.features = flagged;
    h.claudeCode.mockImplementationOnce(getClaudeCodeStatus);
    const { result } = renderHookWithProviders(() => {
      const all = useAllModels();
      return { all, composer: resolveComposerModel('claude-sonnet-5-5-code', 'model-x', all.catalogModelNames) };
    });
    await waitFor(() => expect(result.current.all.isLoading).toBe(false));
    expect(api.get).toHaveBeenCalledWith('/api/v1/claude-code/status');
    expect(has(result.current.all.models)).toBe(false);
    expect(result.current.all.claudeCodeState).toBe('off');
    expect(result.current.composer).toEqual({ seed: 'claude-sonnet-5-5-code', retired: null });
  });

  it('stays loading while the status is read', () => {
    h.features = flagged;
    h.claudeCode.mockReturnValue(new Promise(() => {}));
    const { result } = renderHookWithProviders(() => useAllModels());
    expect(result.current.isLoading).toBe(true);
  });

  it('lists them on their own payer, as a conversation model only, while the token works', async () => {
    h.features = flagged;
    h.claudeCode.mockResolvedValue(status('signed_in'));
    const { result } = renderHookWithProviders(() => useAllModels());
    await waitFor(() => expect(has(result.current.models)).toBe(true));
    expect(result.current.payers['claude-sonnet-5-5-code']).toBe('subscription:claude-code');
    expect(has(result.current.slotModels)).toBe(false);
    expect(result.current.claudeCodeState).toBe('signed_in');
  });

  // Every state but off lists them, so the picker can say why one cannot run.
  it.each(['no_token', 'checking', 'unverified', 'rejected'] as const)('lists them and says the state while it is %s', async (state) => {
    h.features = flagged;
    h.claudeCode.mockResolvedValue(status(state));
    const { result } = renderHookWithProviders(() => useAllModels());
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(has(result.current.models)).toBe(true);
    expect(result.current.claudeCodeState).toBe(state);
    expect(has(result.current.slotModels)).toBe(false);
  });

  // A status cached while opted in says nothing once the flag is off.
  it('reads as off once the user opts out', async () => {
    h.features = flagged;
    h.claudeCode.mockResolvedValue(status('signed_in'));
    const { result, rerender } = renderHookWithProviders(() => useAllModels());
    await waitFor(() => expect(result.current.claudeCodeState).toBe('signed_in'));
    h.features = [];
    rerender();
    expect(result.current.claudeCodeState).toBe('off');
    expect(has(result.current.models)).toBe(false);
  });
});
