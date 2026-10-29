/**
 * ChatView mounting harness (D2): mounts the REAL ChatView shell with the
 * data boundary mocked (useChatMessages + sibling data hooks + the api
 * barrel, shaped in chatViewHarness) so the presentation refactor of
 * ChatView's own body has a net.
 * Child components render real except ChatInput (heavy, separately owned);
 * assertions target ChatView-owned JSX: the transcript, the reconnecting
 * status, the model-resilience pill, and the error banner.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import { screen } from '@testing-library/react';

vi.mock('@/lib/framer', async () => (await import('./chatViewHarness')).framerMock());
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: 'en-US', resolvedLanguage: 'en-US' } }),
}));
vi.mock('@/lib/supabase', () => ({ supabase: null }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('../Markdown', async () => (await import('./chatViewHarness')).markdownMock());
// Heavy leaf owned by Phase 6; the harness targets ChatView's own body.
vi.mock('../../../../components/ui/chat-input', async () => (await import('./chatViewHarness')).chatInputMock());
vi.mock('../../hooks/useChatMessages', async (importOriginal) =>
  (await import('./chatViewHarness')).chatMessagesMock(await importOriginal()));
vi.mock('../../hooks/useWorkspaceFiles', async () => (await import('./chatViewHarness')).workspaceFilesMock());
vi.mock('../../hooks/useNavigationData', async (importOriginal) =>
  (await import('./chatViewHarness')).navigationDataMock(await importOriginal()));
vi.mock('../../utils/api', async (importOriginal) => (await import('./chatViewHarness')).apiMock(await importOriginal()));

import { assistant, chatState, mountChatView, resetChatState, userMsg } from './chatViewHarness';
import ChatView from '../ChatView';

beforeAll(() => {
  if (!Element.prototype.scrollTo) {
    Element.prototype.scrollTo = (() => {}) as typeof Element.prototype.scrollTo;
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = (() => {}) as typeof Element.prototype.scrollIntoView;
  }
});

beforeEach(resetChatState);

describe('ChatView mounting harness', () => {
  it('mounts idle with an empty transcript and the input area', () => {
    const { container } = mountChatView(ChatView);
    expect(screen.getByTestId('chat-input-stub')).toBeInTheDocument();
    expect(container.querySelector('[data-message-id]')).toBeNull();
  });

  it('renders transcript bubbles from the hook state', () => {
    chatState.messages = [
      userMsg('u1', 'hello mount'),
      assistant('a1', {
        content: 'assistant reply text',
        contentSegments: [{ type: 'text', content: 'assistant reply text', order: 0 }],
      }),
    ];
    const { container } = mountChatView(ChatView);
    expect(screen.getByText('hello mount')).toBeInTheDocument();
    expect(container.querySelector('[data-message-id="a1"]')).not.toBeNull();
  });

  it('floats the reconnecting chip over the composer, outside the transcript', () => {
    chatState.isReconnecting = true;
    mountChatView(ChatView);
    const chip = screen.getByText('chat.reconnecting');
    // In the transcript, a reconnect that catches up would take the line out
    // from under a reader at the end and drop the reply by its height.
    expect(chip.closest('[data-transcript-status]')).toBeNull();
  });

  it('shows the model-retry pill only while loading', () => {
    chatState.modelStatus = { kind: 'retrying', model: 'm1', attempt: 0, maxRetries: 2 };
    chatState.isLoading = true;
    mountChatView(ChatView);
    expect(screen.getByText('chat.modelRetrying')).toBeInTheDocument();
  });

  it('renders the error banner for a string error when idle', () => {
    chatState.messageError = 'Something went wrong on the server';
    mountChatView(ChatView);
    expect(screen.getByText('Something went wrong on the server')).toBeInTheDocument();
  });
});
