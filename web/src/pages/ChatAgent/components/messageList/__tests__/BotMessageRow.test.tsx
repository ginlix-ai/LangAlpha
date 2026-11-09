/**
 * A received bot-to-bot message in the transcript: the sender's line over the
 * sender's words, in the quiet shape ds-bot draws one. It is not the user's
 * message, so it offers none of a user bubble's actions, and a long body folds
 * to a press rather than running down the page.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { BotInfo } from '@/api/bots';
import { BotMessageRow, type BotMessageProvenance } from '../BotMessageRow';

const h = vi.hoisted(() => ({ bot: null as unknown, useBot: vi.fn() }));

vi.mock('@/hooks/useBots', () => ({
  useBot: h.useBot,
}));

function bot(overrides: Partial<BotInfo> = {}): BotInfo {
  return {
    bot_id: 'bot-2',
    user_id: 'user-1',
    workspace_id: 'ws-1',
    name: 'Scout',
    role: 'analyst',
    persona: '',
    avatar: {},
    provider: null,
    model: 'claude-sonnet-4-5',
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

const provenance = (overrides: Partial<BotMessageProvenance> = {}): BotMessageProvenance => ({
  kind: 'bot_message',
  fromBotId: 'bot-2',
  fromName: 'Scout',
  messageKind: 'request',
  messageId: 'msg-1',
  ...overrides,
});

function stubScrollHeight(px: number): () => void {
  const original = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'scrollHeight');
  Object.defineProperty(HTMLElement.prototype, 'scrollHeight', {
    configurable: true,
    get: () => px,
  });
  return () => {
    if (original) Object.defineProperty(HTMLElement.prototype, 'scrollHeight', original);
    else delete (HTMLElement.prototype as { scrollHeight?: unknown }).scrollHeight;
  };
}

beforeEach(() => {
  h.bot = null;
  h.useBot.mockReset();
  h.useBot.mockImplementation((botId: string | null) => ({ data: botId ? h.bot : null }));
});

describe('BotMessageRow', () => {
  it('draws the sender line and the sender\'s words under it', () => {
    renderWithProviders(
      <BotMessageRow id="row-1" provenance={provenance()} content="Deliver the filings by Friday." />,
    );

    expect(screen.getByText('Request from')).toBeInTheDocument();
    expect(screen.getByText('Scout')).toBeInTheDocument();
    expect(screen.getByText('Deliver the filings by Friday.')).toBeInTheDocument();
    // The sender line's face is the shared BotMark.
    expect(screen.getByText('Scout').closest('[data-bot-message]')?.querySelector('[data-bot-mark]')).not.toBeNull();
  });

  it.each([
    ['message', 'Message from'],
    ['reply', 'Reply from'],
    ['report', 'Report from'],
    ['question', 'Question from'],
    ['notice', 'Notice from'],
  ] as const)('names a %s by its own prefix', (messageKind, prefix) => {
    renderWithProviders(
      <BotMessageRow id="row-1" provenance={provenance({ messageKind })} content="Body." />,
    );

    expect(screen.getByText(prefix)).toBeInTheDocument();
  });

  it("resolves the sender's stored avatar when the bot is reachable", () => {
    h.bot = bot({ avatar: { url: 'https://example.com/scout.png' } });
    renderWithProviders(
      <BotMessageRow id="row-1" provenance={provenance()} content="Body." />,
    );

    const image = screen.getByText('Scout').closest('[data-bot-message]')?.querySelector('img');
    expect(image).toHaveAttribute('src', 'https://example.com/scout.png');
  });

  it('falls back to the sender name\'s initial when the bot cannot be read', () => {
    h.bot = null;
    renderWithProviders(
      <BotMessageRow id="row-1" provenance={provenance()} content="Body." />,
    );

    expect(screen.getByText('S')).toBeInTheDocument();
  });

  it('offers none of the user bubble\'s actions', () => {
    renderWithProviders(
      <BotMessageRow id="row-1" provenance={provenance()} content="Body." />,
    );

    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.queryByTitle(/copy/i)).toBeNull();
    expect(screen.queryByTitle(/edit/i)).toBeNull();
  });

  it('folds a long body behind a press', () => {
    const restore = stubScrollHeight(400);
    try {
      renderWithProviders(
        <BotMessageRow id="row-1" provenance={provenance()} content={'A long message. '.repeat(80)} />,
      );

      const toggle = screen.getByTestId('overflow-collapse-toggle');
      expect(toggle).toHaveTextContent('Show all');
      fireEvent.click(toggle);
      expect(toggle).toHaveTextContent('Show less');
    } finally {
      restore();
    }
  });

  it('draws the sender line alone while the live row has no body yet', () => {
    renderWithProviders(<BotMessageRow id="row-1" provenance={provenance()} content="" />);

    expect(screen.getByText('Request from')).toBeInTheDocument();
    expect(screen.queryByText('Deliver the filings by Friday.')).not.toBeInTheDocument();
  });
});
