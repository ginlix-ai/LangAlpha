/**
 * The chat header's bot identity: a thread row carries only `bot_id`, so the
 * chip reads the row (the detail query the composer keeps warm) and then the
 * bot itself. A thread with no bot draws nothing; the header keeps its generic
 * shape.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { BotInfo } from '@/api/bots';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  getThread: vi.fn(),
  useBot: vi.fn(),
  bot: null as unknown,
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getThread: h.getThread,
  getThreadShareStatus: vi.fn(),
  getWorkspaceThreads: vi.fn(),
}));

vi.mock('@/hooks/useBots', () => ({
  useBot: h.useBot,
}));

import { ThreadBotChip } from '../ThreadBotChip';

function bot(overrides: Partial<BotInfo> = {}): BotInfo {
  return {
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
    ...overrides,
  };
}

function thread(botId: string | null) {
  return { workspace_id: 'ws-1', thread_id: 't-1', title: null, bot_id: botId };
}

beforeEach(() => {
  h.getThread.mockReset();
  h.useBot.mockReset();
  h.bot = bot();
  h.getThread.mockResolvedValue(thread('bot-1'));
  // The real hook is disabled with no id, so a null id answers no bot.
  h.useBot.mockImplementation((botId: string | null) => ({ data: botId ? h.bot : null }));
});

describe('ThreadBotChip', () => {
  it('reads the thread row and then that bot, and draws its name', async () => {
    renderWithProviders(<ThreadBotChip threadId="t-1" />);

    expect(await screen.findByTestId('thread-bot')).toHaveTextContent('Macro analyst');
    expect(h.getThread).toHaveBeenCalledWith('t-1');
    await waitFor(() => expect(h.useBot).toHaveBeenCalledWith('bot-1'));
    // The header's face is the shared BotMark, not a local drawing.
    expect(screen.getByTestId('thread-bot').querySelector('[data-bot-mark]')).not.toBeNull();
  });

  it("draws the bot's avatar when it stores a URL", async () => {
    h.bot = bot({ avatar: { url: 'https://example.com/macro.png' } });
    renderWithProviders(<ThreadBotChip threadId="t-1" />);

    const chip = await screen.findByTestId('thread-bot');
    const image = chip.querySelector('img');
    expect(image).not.toBeNull();
    expect(image).toHaveAttribute('src', 'https://example.com/macro.png');
    // The name is beside it, so the picture is decorative.
    expect(image).toHaveAttribute('alt', '');
  });

  it('falls back to the name initial when no avatar is stored', async () => {
    h.bot = bot({ avatar: {} });
    renderWithProviders(<ThreadBotChip threadId="t-1" />);

    const chip = await screen.findByTestId('thread-bot');
    expect(chip.textContent).toContain('M');
  });

  it('draws nothing for a thread with no bot', async () => {
    h.getThread.mockResolvedValue(thread(null));
    renderWithProviders(<ThreadBotChip threadId="t-1" />);

    await waitFor(() => expect(h.getThread).toHaveBeenCalled());
    expect(screen.queryByTestId('thread-bot')).not.toBeInTheDocument();
  });

  it('reads no row for the not-yet-created default thread', () => {
    renderWithProviders(<ThreadBotChip threadId="__default__" />);

    expect(h.getThread).not.toHaveBeenCalled();
    expect(screen.queryByTestId('thread-bot')).not.toBeInTheDocument();
  });
});
