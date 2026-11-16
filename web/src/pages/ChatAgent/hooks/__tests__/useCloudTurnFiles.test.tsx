/**
 * A Home turn that ran on the cloud names files that may be on the cloud or
 * already in Home; until the hand-backs say which, a card must not open
 * Home's file of the same name.
 */
import { describe, expect, it, vi } from 'vitest';
import { waitFor } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';

import type { MessageRecord } from '../../components/messageList/types';
import type { TurnFile } from '../../utils/turnFiles';
import { useCloudTurnFiles } from '../useCloudTurnFiles';

const getHomeHandbacks = vi.fn();
vi.mock('../../utils/api/workspaces', () => ({ getHomeHandbacks: (since: string) => getHomeHandbacks(since) }));

const FALLBACK = {
  computer_id: 'mac-1',
  computer_name: 'MacBook Pro',
  cloud_computer_id: 'cloud-1',
  cloud_name: 'Cloud One',
  at: '2026-11-16T10:00:00Z',
  reason: 'unreachable',
};
const turned = [{ message: { id: 'u1', role: 'user', homeFallback: FALLBACK } as unknown as MessageRecord, turnIndex: 0 }];
const files = new Map<number, TurnFile[]>([
  [0, [{ path: 'report.md' }, { path: 'notes.md', workspaceId: 'ws-research' }]],
  [1, [{ path: 'plain.md' }]],
]);

describe('useCloudTurnFiles', () => {
  it('shows a cloud turn only the files that name their workspace until the hand-backs are read', () => {
    getHomeHandbacks.mockReturnValue(new Promise(() => {}));

    const { result } = renderHookWithProviders(() => useCloudTurnFiles(turned, files, { enabled: true }));

    expect(result.current.get(0)).toEqual([{ path: 'notes.md', workspaceId: 'ws-research' }]);
    expect(result.current.get(1)).toEqual([{ path: 'plain.md' }]);
  });

  it('keeps them back while the read fails', async () => {
    getHomeHandbacks.mockRejectedValue(new Error('offline'));

    const { result } = renderHookWithProviders(() => useCloudTurnFiles(turned, files, { enabled: true }));

    await waitFor(() => expect(getHomeHandbacks).toHaveBeenCalledWith('2026-11-16T10:00:00Z'));
    expect(result.current.get(0)).toEqual([{ path: 'notes.md', workspaceId: 'ws-research' }]);
  });
});
