/**
 * The Bots API module's wire contract: the one list call every workspace
 * shares, the calls that address one bot, and the body an edit sends. The
 * server's bot rows are the identity layer over a thread, so a wrong verb or
 * a dropped workspace id here would show up as bots in the wrong group.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@/api/client', () => ({
  api: { get: vi.fn(), post: vi.fn(), patch: vi.fn(), delete: vi.fn() },
}));

import { api } from '@/api/client';
import { createBot, deleteBot, getBot, getBots, updateBot, type BotInfo } from '../bots';

const mockGet = api.get as unknown as ReturnType<typeof vi.fn>;
const mockPost = api.post as unknown as ReturnType<typeof vi.fn>;
const mockPatch = api.patch as unknown as ReturnType<typeof vi.fn>;
const mockDelete = api.delete as unknown as ReturnType<typeof vi.fn>;

const BOT: BotInfo = {
  bot_id: 'bot-1',
  user_id: 'user-1',
  workspace_id: 'ws-1',
  name: 'Macro analyst',
  role: 'analyst',
  persona: 'Reads the macro calendar.',
  avatar: {},
  provider: 'anthropic',
  model: 'claude-sonnet-4-5',
  skills: [],
  tools_allow: [],
  tools_deny: [],
  approval: 'ask',
  memory_scopes: ['workspace'],
  status: 'active',
  primary_thread_id: null,
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
};

beforeEach(() => {
  mockGet.mockReset();
  mockPost.mockReset();
  mockPatch.mockReset();
  mockDelete.mockReset();
});

describe('getBots', () => {
  it('asks for every workspace when none is named', async () => {
    mockGet.mockResolvedValue({ data: { bots: [BOT], max_bots: 20 } });

    await expect(getBots()).resolves.toEqual({ bots: [BOT], max_bots: 20 });
    expect(mockGet).toHaveBeenCalledWith('/api/v1/bots', undefined);
  });

  it('narrows the list to one workspace when one is named', async () => {
    mockGet.mockResolvedValue({ data: { bots: [], max_bots: 20 } });

    await getBots('ws-1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/bots', { params: { workspace_id: 'ws-1' } });
  });
});

describe('getBot', () => {
  it('unwraps the one row', async () => {
    mockGet.mockResolvedValue({ data: BOT });

    await expect(getBot('bot-1')).resolves.toEqual(BOT);
    expect(mockGet).toHaveBeenCalledWith('/api/v1/bots/bot-1');
  });
});

describe('createBot', () => {
  it('posts the create body as given and unwraps the row', async () => {
    mockPost.mockResolvedValue({ data: BOT });
    const input = {
      workspace_id: 'ws-1',
      name: 'Macro analyst',
      persona: 'Reads the macro calendar.',
      approval: 'ask' as const,
    };

    await expect(createBot(input)).resolves.toEqual(BOT);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/bots', input);
  });
});

describe('updateBot', () => {
  it('patches the named bot with only the changed fields', async () => {
    mockPatch.mockResolvedValue({ data: { ...BOT, status: 'paused' } });

    await updateBot('bot-1', { status: 'paused' });
    expect(mockPatch).toHaveBeenCalledWith('/api/v1/bots/bot-1', { status: 'paused' });
  });
});

describe('deleteBot', () => {
  it('deletes the named bot and unwraps the answer', async () => {
    mockDelete.mockResolvedValue({ data: { ok: true, bot_id: 'bot-1' } });

    await expect(deleteBot('bot-1')).resolves.toEqual({ ok: true, bot_id: 'bot-1' });
    expect(mockDelete).toHaveBeenCalledWith('/api/v1/bots/bot-1');
  });
});
