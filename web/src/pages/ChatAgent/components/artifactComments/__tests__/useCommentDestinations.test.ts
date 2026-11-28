import { describe, expect, it, vi } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';

const mocks = vi.hoisted(() => ({
  record: {} as Record<string, unknown>,
  read: {} as Record<string, unknown>,
  check: {} as Record<string, unknown>,
}));
vi.mock('@/hooks/useArtifact', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useArtifact')>()),
  useArtifactRecord: () => mocks.record,
  useArtifactDestinations: () => mocks.read,
  useDestinationThread: () => mocks.check,
}));
vi.mock('../sendContext', () => ({
  useArtifactCommentSender: () => ({ beside: null, running: false, send: vi.fn() }),
}));

import type { ArtifactDestinations } from '@/types/api';
import { useCommentDestinations, useDraftDestination } from '../useCommentDestinations';

describe('trying the "To:" line again', () => {
  it('asks for the destinations again when they failed, though a later read of the record failed too', () => {
    const recordRefetch = vi.fn();
    const readRefetch = vi.fn();
    mocks.record = { data: { artifact_id: 'a-1' }, isError: true, refetch: recordRefetch };
    mocks.read = { data: undefined, isError: true, refetch: readRefetch };
    const { result } = renderHookWithProviders(() => useCommentDestinations('ws', { kind: 'drive', path: 'e-1' }));
    result.current.retry?.();
    expect(readRefetch).toHaveBeenCalledTimes(1);
    expect(recordRefetch).not.toHaveBeenCalled();
  });

  it('asks for the record again where it was never read, which leaves the destinations unasked', () => {
    const recordRefetch = vi.fn();
    const readRefetch = vi.fn();
    mocks.record = { data: undefined, isError: true, refetch: recordRefetch };
    mocks.read = { data: undefined, isError: false, refetch: readRefetch };
    const { result } = renderHookWithProviders(() => useCommentDestinations('ws', { kind: 'drive', path: 'e-1' }));
    result.current.retry?.();
    expect(recordRefetch).toHaveBeenCalledTimes(1);
    expect(readRefetch).not.toHaveBeenCalled();
  });
});

describe('a chat picked outside the menu, kept with the draft', () => {
  const destinations: ArtifactDestinations = {
    default: { rule: 'new', thread: null, workspace_id: 'ws' },
    bound: null, beside: null, creator: null,
  };
  const picked = { threadId: 't-picked' };
  const thread = { thread_id: 't-picked', title: 'Picked chat', workspace_id: 'ws', usable: true };

  function drafted(check: Record<string, unknown>) {
    mocks.record = { data: { artifact_id: 'a-1' }, isError: false, refetch: vi.fn() };
    mocks.read = { data: destinations, isError: false, refetch: vi.fn() };
    mocks.check = check;
    return renderHookWithProviders(() =>
      useDraftDestination('ws', { kind: 'drive', path: 'e-1' }, { to: picked, onTo: vi.fn(), searchable: true }));
  }

  it('says the check failed, with a retry that asks it again, where the server could not be asked', () => {
    const refetch = vi.fn();
    const { result } = drafted({ data: undefined, isError: true, refetch });
    expect(result.current.status.kind).toBe('failed');
    const status = result.current.status as { kind: 'failed'; retry: () => void };
    status.retry();
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it('sends or queues nowhere while the check is failed, rather than to the default chat', () => {
    const { result } = drafted({ data: undefined, isError: true, refetch: vi.fn() });
    expect(result.current.draftRoute('send')).toBeNull();
    expect(result.current.draftRoute('queue')).toBeNull();
  });

  it('still keeps a note while the check is failed', () => {
    const { result } = drafted({ data: undefined, isError: true, refetch: vi.fn() });
    expect(result.current.draftRoute('note')).toEqual({ kind: 'note' });
  });

  it('goes to the picked chat again once a retry answers', () => {
    const { result, rerender } = drafted({ data: undefined, isError: true, refetch: vi.fn() });
    mocks.check = { data: thread, isError: false, refetch: vi.fn() };
    rerender();
    expect(result.current.status).toEqual({ kind: 'ready', selected: 'thread:t-picked' });
    expect(result.current.draftRoute('send')).toEqual({ kind: 'send', to: picked });
  });

  it('goes to the default where the server says the chat takes none', () => {
    const { result } = drafted({ data: null, isError: false, refetch: vi.fn() });
    expect(result.current.status.kind).toBe('ready');
    expect(result.current.draftRoute('send')).toEqual({ kind: 'send', to: { newThreadIn: 'ws' } });
  });
});
