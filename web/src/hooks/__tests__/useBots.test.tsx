/**
 * The Bots hooks: one list entry carrying every workspace's bots, a row read
 * by id, and three writes whose answers land in the single-row cache while the
 * grouped list is read again. The query keys are the contract the settings
 * section invalidates by prefix, so they are asserted directly.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { BotInfo } from '@/api/bots';

vi.mock('@/api/bots', () => ({
  getBots: vi.fn(),
  getBot: vi.fn(),
  createBot: vi.fn(),
  updateBot: vi.fn(),
  deleteBot: vi.fn(),
}));

import { createBot, deleteBot, getBot, getBots, updateBot } from '@/api/bots';
import { useBot, useBots, useCreateBot, useDeleteBot, useUpdateBot } from '../useBots';

const mockGetBots = getBots as unknown as ReturnType<typeof vi.fn>;
const mockGetBot = getBot as unknown as ReturnType<typeof vi.fn>;
const mockCreateBot = createBot as unknown as ReturnType<typeof vi.fn>;
const mockUpdateBot = updateBot as unknown as ReturnType<typeof vi.fn>;
const mockDeleteBot = deleteBot as unknown as ReturnType<typeof vi.fn>;

const BOT: BotInfo = {
  bot_id: 'bot-1',
  user_id: 'user-1',
  workspace_id: 'ws-1',
  name: 'Macro analyst',
  role: 'analyst',
  persona: '',
  avatar: {},
  provider: null,
  model: null,
  skills: [],
  tools_allow: [],
  tools_deny: [],
  approval: 'ask',
  memory_scopes: ['workspace'],
  status: 'active',
  primary_thread_id: null,
  created_at: null,
  updated_at: null,
};

beforeEach(() => {
  mockGetBots.mockReset();
  mockGetBot.mockReset();
  mockCreateBot.mockReset();
  mockUpdateBot.mockReset();
  mockDeleteBot.mockReset();
});

describe('useBots', () => {
  it('reads every workspace under the shared "all" key', async () => {
    mockGetBots.mockResolvedValue({ bots: [BOT], max_bots: 20 });
    const { result, queryClient } = renderHookWithProviders(() => useBots());

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetBots).toHaveBeenCalledWith(null);
    expect(result.current.data?.bots).toEqual([BOT]);
    expect(queryClient.getQueryData(queryKeys.bots.list(null))).toEqual({ bots: [BOT], max_bots: 20 });
    // The one answer is what the settings section groups, so "all" is a child
    // of the same list prefix a write invalidates.
    expect(queryKeys.bots.list(null)).toEqual(['bots', 'list', 'all']);
  });

  it('keys a workspace-scoped read apart from the group-all read', async () => {
    mockGetBots.mockResolvedValue({ bots: [], max_bots: 20 });
    const { result } = renderHookWithProviders(() => useBots('ws-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetBots).toHaveBeenCalledWith('ws-1');
    expect(queryKeys.bots.list('ws-1')).toEqual(['bots', 'list', 'ws-1']);
  });
});

describe('useBot', () => {
  it('reads one bot by id', async () => {
    mockGetBot.mockResolvedValue(BOT);
    const { result } = renderHookWithProviders(() => useBot('bot-1'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(BOT);
    expect(mockGetBot).toHaveBeenCalledWith('bot-1');
  });

  it('does not fetch until an id is in hand', () => {
    const { result } = renderHookWithProviders(() => useBot(null));

    expect(result.current.isFetching).toBe(false);
    expect(mockGetBot).not.toHaveBeenCalled();
  });
});

describe('useCreateBot', () => {
  it('seeds the row and reads the grouped list again', async () => {
    mockCreateBot.mockResolvedValue(BOT);
    const { result, queryClient } = renderHookWithProviders(() => useCreateBot());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await result.current.mutateAsync({ workspace_id: 'ws-1', name: 'Macro analyst' });

    expect(mockCreateBot).toHaveBeenCalledWith({ workspace_id: 'ws-1', name: 'Macro analyst' });
    expect(queryClient.getQueryData(queryKeys.bots.detail('bot-1'))).toEqual(BOT);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.bots.lists() });
    // A renamed/paused/deleted bot is also a member chip in an open room.
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.details() });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
  });
});

describe('useUpdateBot', () => {
  it('sends the patch and lands the server answer in both caches', async () => {
    const paused = { ...BOT, status: 'paused' as const };
    mockUpdateBot.mockResolvedValue(paused);
    const { result, queryClient } = renderHookWithProviders(() => useUpdateBot());
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await result.current.mutateAsync({ botId: 'bot-1', patch: { status: 'paused' } });

    expect(mockUpdateBot).toHaveBeenCalledWith('bot-1', { status: 'paused' });
    expect(queryClient.getQueryData(queryKeys.bots.detail('bot-1'))).toEqual(paused);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.bots.lists() });
    // A renamed/paused/deleted bot is also a member chip in an open room.
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.details() });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
  });
});

describe('useDeleteBot', () => {
  it('drops the row cache and reads the list again', async () => {
    mockDeleteBot.mockResolvedValue({ ok: true, bot_id: 'bot-1' });
    const { result, queryClient } = renderHookWithProviders(() => useDeleteBot());
    queryClient.setQueryData(queryKeys.bots.detail('bot-1'), BOT);
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');

    await result.current.mutateAsync('bot-1');

    expect(mockDeleteBot).toHaveBeenCalledWith('bot-1');
    expect(queryClient.getQueryData(queryKeys.bots.detail('bot-1'))).toBeUndefined();
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.bots.lists() });
    // A deleted bot disappears from any room's chips too.
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.details() });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.rooms.lists() });
  });
});
