/**
 * An edit the session keeps opens in the box of the message it was typed over,
 * through the draft store ChatView hands its bubbles. Text whose message is
 * gone comes back from useChatMessages as `returnedEdit`, and goes into the
 * composer beside whatever is typed there, as steering the agent never read
 * does.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import React from 'react';
import { screen, waitFor } from '@testing-library/react';

const composer = vi.hoisted(() => ({ giveBack: vi.fn() }));

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
// The composer's handle is all this suite reads of it.
vi.mock('../../../../components/ui/chat-input', () => ({
  default: function ComposerStub({ ref }: { ref?: React.Ref<unknown> }) {
    React.useImperativeHandle(ref, () => ({ giveBack: composer.giveBack }));
    return <textarea data-testid="chat-input-stub" />;
  },
}));
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

beforeEach(() => {
  resetChatState();
  composer.giveBack.mockReset();
  chatState.messages = [
    { ...userMsg('history-user-0', 'question 0'), turnIndex: 0 },
    { ...assistant('history-assistant-0', { content: 'answer 0' }), turnIndex: 0 },
    { ...userMsg('history-user-1', 'question 1'), turnIndex: 1 },
    { ...assistant('history-assistant-1', { content: 'answer 1' }), turnIndex: 1 },
  ];
});

const editBoxes = () =>
  screen.queryAllByRole('textbox').filter((el) => el.getAttribute('data-testid') !== 'chat-input-stub') as HTMLTextAreaElement[];

describe('ChatView: an edit the session keeps', () => {
  it("opens the message's edit box on the text the session holds for it", async () => {
    chatState.editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'question 1 rewritten' });

    mountChatView(ChatView);

    await waitFor(() => expect(editBoxes().map((el) => el.value)).toEqual(['question 1 rewritten']));
    expect(composer.giveBack).not.toHaveBeenCalled();
  });

  it('gives the composer text whose message is gone', async () => {
    chatState.returnedEdit = 'question 1 rewritten';

    mountChatView(ChatView);

    await waitFor(() => expect(composer.giveBack).toHaveBeenCalledWith('question 1 rewritten'));
    expect(chatState.clearReturnedEdit).toHaveBeenCalled();
    expect(editBoxes()).toEqual([]);
  });

  it('gives the composer steering the agent finished without reading', async () => {
    chatState.returnedSteering = 'also check the margins';

    mountChatView(ChatView);

    await waitFor(() => expect(composer.giveBack).toHaveBeenCalledWith('also check the margins'));
    expect(chatState.clearReturnedSteering).toHaveBeenCalled();
  });
});
