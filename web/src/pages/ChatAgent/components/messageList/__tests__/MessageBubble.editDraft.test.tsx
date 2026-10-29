/**
 * An open edit lives in the host's draft store, not in the bubble: a reload
 * that remounts the bubble reopens the box with what was typed in it, an edit
 * the session hands back opens the same way, and a running turn keeps the
 * edit in its box rather than send it.
 */
import { describe, it, expect, vi } from 'vitest';
import { act, render, screen, fireEvent } from '@testing-library/react';
import { MessageBubble } from '../MessageBubble';
import { MessageActionsProvider, type MessageActions } from '../MessageActionsContext';
import { createEditDraftStore } from '../../../hooks/utils/editDrafts';
import type { MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: null }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

const message = {
  id: 'history-user-1',
  role: 'user',
  content: 'question 1',
  contentType: 'text',
  timestamp: new Date('2026-08-01T00:00:00Z'),
  isStreaming: false,
} as MessageRecord;

function bubble(actions: MessageActions, isLoading = false) {
  return (
    <MessageActionsProvider actions={actions}>
      <MessageBubble message={message} turnIndex={1} isTurnTail={false} isLoading={isLoading} />
    </MessageActionsProvider>
  );
}

const box = () => screen.queryByRole('textbox') as HTMLTextAreaElement | null;

describe('MessageBubble – an edit kept by the host', () => {
  it('reopens the box with what was typed after the bubble remounts', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    const actions = { onEditMessage: vi.fn(), editDrafts };
    const view = render(bubble(actions));
    fireEvent.click(screen.getByTitle('Edit message'));
    expect(box()!.value).toBe('question 1');
    fireEvent.change(box()!, { target: { value: 'question 1 half typed' } });

    // A reload drops the bubble and builds it again.
    view.unmount();
    render(bubble(actions));

    expect(box()!.value).toBe('question 1 half typed');
  });

  it('takes the focus back after a remount only when it held it', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    const actions = { onEditMessage: vi.fn(), editDrafts };
    const elsewhere = document.createElement('button');
    document.body.appendChild(elsewhere);

    let view = render(bubble(actions));
    fireEvent.click(screen.getByTitle('Edit message'));
    expect(document.activeElement).toBe(box());
    view.unmount();
    view = render(bubble(actions));
    expect(document.activeElement).toBe(box());

    // The user moved on before the reload; the box comes back without the focus.
    elsewhere.focus();
    view.unmount();
    elsewhere.blur();
    render(bubble(actions));
    expect(box()).not.toBeNull();
    expect(document.activeElement).toBe(document.body);
    elsewhere.remove();
  });

  it('opens on an edit the session hands back and sends it again', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    const onEditMessage = vi.fn();
    render(bubble({ onEditMessage, editDrafts }));
    expect(box()).toBeNull();

    act(() => editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'question 1 rewritten' }));
    fireEvent.keyDown(box()!, { key: 'Enter' });

    expect(onEditMessage).toHaveBeenCalledWith('history-user-1', 'question 1 rewritten');
    expect(box()).toBeNull();
    expect(editDrafts.of('thread-1')).toEqual([]);
  });

  it("keeps another thread's edit out of the box", () => {
    let shown = 'thread-1';
    const editDrafts = createEditDraftStore(() => shown);
    editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'written on thread 1' });
    shown = 'thread-2';
    render(bubble({ onEditMessage: vi.fn(), editDrafts }));
    expect(box()).toBeNull();
  });

  it('keeps the edit in its box while a turn runs', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    const onEditMessage = vi.fn();
    editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'question 1 rewritten' });
    render(bubble({ onEditMessage, editDrafts }, true));

    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
    fireEvent.keyDown(box()!, { key: 'Enter' });

    expect(onEditMessage).not.toHaveBeenCalled();
    expect(box()!.value).toBe('question 1 rewritten');
  });

  it('opens the box of the one unstored send edited when another repeats its text', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    const onEditMessage = vi.fn();
    // Neither reached the server, so neither has a turn.
    const unstored = (id: string) => ({ ...message, id, content: 'question 1', unadmitted: true }) as MessageRecord;
    render(
      <MessageActionsProvider actions={{ onEditMessage, editDrafts }}>
        <MessageBubble message={unstored('user-a')} turnIndex={null} isTurnTail={false} isLoading={false} />
        <MessageBubble message={unstored('user-b')} turnIndex={null} isTurnTail={false} isLoading={false} />
      </MessageActionsProvider>,
    );

    fireEvent.click(screen.getAllByTitle('Edit message')[1]);
    expect(screen.queryAllByRole('textbox')).toHaveLength(1);
    fireEvent.change(box()!, { target: { value: 'question 1 rewritten' } });
    fireEvent.keyDown(box()!, { key: 'Enter' });

    expect(onEditMessage).toHaveBeenCalledWith('user-b', 'question 1 rewritten');
    expect(screen.queryAllByRole('textbox')).toHaveLength(0);
  });

  it('closes on Cancel and forgets the text', () => {
    const editDrafts = createEditDraftStore(() => 'thread-1');
    editDrafts.put({ turnIndex: 1, original: 'question 1', text: 'never mind' });
    render(bubble({ onEditMessage: vi.fn(), editDrafts }));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(box()).toBeNull();
    expect(editDrafts.of('thread-1')).toEqual([]);
  });
});
