/**
 * An `la` step's names come only from what the app already read, and follow
 * the cache as it changes.
 */
import React from 'react';
import { describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useLaNames } from '../useLaNames';

const THREAD = '346bee3b-8f31-43d3-b8a3-f6489e03d7aa';
const WS = '0f1e2d3c-4b5a-4968-8776-655443322110';
const HOME = '9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d';

function setup(calls: Array<{ toolName: string; args: Record<string, unknown> }>) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  const hook = renderHook(() => useLaNames(calls), { wrapper });
  return { qc, hook };
}

describe('useLaNames', () => {
  it('names a thread and a workspace from cached lists, and Home as the app does', () => {
    const { qc, hook } = setup([
      { toolName: 'Bash', args: { command: `la threads output ${THREAD}` } },
      { toolName: 'Bash', args: { command: `la workspaces delete ${WS} -y` } },
      { toolName: 'Bash', args: { command: `la threads list -w ${HOME}` } },
    ]);
    expect(hook.result.current.get(`thread:${THREAD}`)).toBeUndefined();
    act(() => {
      qc.setQueryData(queryKeys.threads.page(WS, 20), { threads: [{ thread_id: THREAD, workspace_id: WS, title: 'NVDA fair value' }] });
      qc.setQueryData(queryKeys.workspaces.list({ limit: 100 }), { workspaces: [{ workspace_id: WS, name: 'Semis' }, { workspace_id: HOME, name: 'Home', is_home: true }] });
    });
    expect(hook.result.current.get(`thread:${THREAD}`)).toBe('NVDA fair value');
    expect(hook.result.current.get(`workspace:${WS}`)).toBe('Semis');
    expect(hook.result.current.get(`workspace:${HOME}`)).toBe('Chief of Staff');
  });

  it('takes no title a search hit stands in with the first message', () => {
    const { qc, hook } = setup([{ toolName: 'Bash', args: { command: `la threads delete ${THREAD} -y` } }]);
    act(() => {
      qc.setQueryData(queryKeys.threads.search({ q: 'nvda', limit: 5 }), {
        threads: [{ thread_id: THREAD, title: 'what is nvda worth', has_title: false }], turns: [], files: [],
      });
    });
    expect(hook.result.current.get(`thread:${THREAD}`)).toBeUndefined();
  });

  it('keeps the same names while a cache event leaves them alone', () => {
    const { qc, hook } = setup([
      { toolName: 'Bash', args: { command: `la threads output ${THREAD}` } },
      { toolName: 'Bash', args: { command: `la workspaces delete ${WS} -y` } },
    ]);
    const before = hook.result.current;
    expect(before.size).toBe(0);
    act(() => {
      qc.setQueryData(queryKeys.workspaces.list({ limit: 100 }), { workspaces: [{ workspace_id: HOME, name: 'Home', is_home: true }] });
    });
    expect(hook.result.current).toBe(before);
  });

  it('looks up nothing for calls that are not la steps by id', () => {
    const { hook } = setup([
      { toolName: 'Bash', args: { command: 'ls -la' } },
      { toolName: 'Bash', args: { command: `la workspaces create Semis` } },
    ]);
    expect(hook.result.current.size).toBe(0);
  });
});
