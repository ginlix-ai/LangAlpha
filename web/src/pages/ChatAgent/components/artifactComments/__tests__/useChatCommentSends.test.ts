/**
 * A file panel's send to the chat beside it (Send now, Send all, a reply's
 * send) settles when the chat's own send does: still on its way, and out of
 * the deck, for as long as the chat holds its comments, then sent once the
 * server takes them or refused with the chat's reason. The chat is played by
 * an account of its sends like the one useChatMessages gives
 * (`watchCommentSends`), which the hook writes to the real store.
 */
import { act } from '@testing-library/react';
import type { TFunction } from 'i18next';
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest';
import { fetchArtifactComments } from '@/hooks/useArtifact';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactCommentsShown, QueuedArtifactComment } from '@/types/api';
import type { CommentSendListener } from '../../../hooks/utils/requestKey';
import type { CommentInWorkspace } from '../artifactCommentSend';
import { refusalReason, sendBatches, type BatchOutcome } from '../commentSends';
import { TAKEN_MS } from '../commentSendStore';
import { useChatCommentSends } from '../useChatCommentSends';

const queued: { rows: QueuedArtifactComment[] } = { rows: [] };

vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: null }) }));
vi.mock('@/hooks/useArtifact', () => ({
  useQueuedArtifactComments: () => ({ data: queued.rows }),
  fetchArtifactComments: vi.fn(),
}));

const fetchRows = fetchArtifactComments as Mock;
const t = ((key: string) => key) as unknown as TFunction;

function comment(id: string, over: Partial<CommentInWorkspace> = {}): CommentInWorkspace {
  return {
    comment_id: id, artifact_id: 'a1', workspace_id: 'ws', number: 1, path: 'r.html', file_sha256: null, title: 'R',
    anchor: { kind: 'element' }, status: 'open', delivery: 'queued', thread_id: 't1', thread_title: 'T1',
    thread_archived: false, created_at: '', updated_at: 'v1', resolved_at: null,
    messages: [{ message_id: 'm', seq: 1, author: 'user', body: 'tighten', sent_at: null, request_key: null, thread_id: null, created_at: '' }],
    read_through: 1, unread: 0, ...over,
  } as CommentInWorkspace;
}

/**
 * `carries: false` is a chat that took nothing (no thread, a send it could
 * not start). `tell` speaks for the chat's request from then on.
 */
function mount({ carries = true }: { carries?: boolean } = {}) {
  const chat: ArtifactCommentsShown[] = [];
  const listeners = new Set<CommentSendListener>();
  const tell: CommentSendListener = (comments, outcome) => {
    for (const listener of [...listeners]) listener(comments, outcome);
  };
  const watch = (listener: CommentSendListener) => {
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  };
  const hook = renderHookWithProviders(() => useChatCommentSends({
    threadId: 't1',
    running: false,
    watch,
    chatCovered: () => false,
    // The chat's send takes its request at once and says so.
    sendToChat: (shown) => {
      chat.push(shown);
      if (carries) tell(shown, 'began');
    },
  }));
  return { ...hook, chat, tell };
}

/** Send all for one batch to the chat beside, as the toolbar does; `outcome()` is undefined until it settles. */
function sendAll(result: ReturnType<typeof mount>['result'], comments: CommentInWorkspace[]) {
  let outcomes: BatchOutcome[] | undefined;
  const done = sendBatches([{ to: { threadId: 't1' }, comments }], {
    sender: result.current.sender,
    sends: result.current.sends,
    reasonOf: (err) => refusalReason(t, err, true),
  }).then((o) => { outcomes = o; });
  return { done, outcome: () => outcomes?.map((o) => [o.threadId, o.reason]) };
}

describe('useChatCommentSends: a send to the chat beside', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    queued.rows = [comment('c1') as QueuedArtifactComment];
    fetchRows.mockReset();
  });
  afterEach(() => vi.useRealTimers());

  it('stays on its way past 20 s while the chat holds it, then counts as sent once taken', async () => {
    const { result, chat, tell } = mount();
    const send = sendAll(result, [comment('c1')]);
    await act(async () => { await vi.advanceTimersByTimeAsync(TAKEN_MS + 5_000); });

    expect(chat.map((s) => s.comments.map((c) => c.comment_id))).toEqual([['c1']]);
    expect(send.outcome()).toBeUndefined();
    // The chat knows the comments by id only, so the mark holds for any version of the row.
    expect(result.current.sends.get('c1')).toEqual({ phase: 'sending', at: null });
    expect(result.current.deck.ready).toEqual([]);

    await act(async () => {
      tell(chat[0], 'accepted');
      await send.done;
    });
    expect(send.outcome()).toEqual([['t1', null]]);
    expect(result.current.sends.get('c1')?.phase).toBe('taken');
    // The taken mark's bound runs from the server's answer.
    await act(async () => { await vi.advanceTimersByTimeAsync(TAKEN_MS); });
    expect(result.current.sends.get('c1')).toBeUndefined();
  });

  it('puts comments a stop handed back into the deck at once, reading the queue again', async () => {
    queued.rows = [comment('c1') as unknown as QueuedArtifactComment];
    const { result, chat, tell } = mount();
    const send = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[0], 'accepted');
      await send.done;
    });
    expect(result.current.deck.ready).toEqual([]);

    await act(async () => { tell(chat[0], 'returned'); });

    expect(result.current.sends.get('c1')).toBeUndefined();
    expect(result.current.deck.ready.map((c) => c.comment_id)).toEqual(['c1']);
  });

  it('takes what one send may carry, leaving the rest in the deck for the next', () => {
    queued.rows = Array.from({ length: 51 }, (_, i) => comment(`c${i}`) as unknown as QueuedArtifactComment);
    const { result } = mount();

    const shown = result.current.take('note');

    expect(shown?.comments).toHaveLength(50);
    expect(result.current.deck.ready).toHaveLength(51);
  });

  it('is refused with the chat\'s reason, back in the deck with it', async () => {
    const { result, chat, tell } = mount();
    const send = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[0], { refused: 'A reply was still running in this chat.' });
      await send.done;
    });
    expect(send.outcome()).toEqual([[null, 'A reply was still running in this chat.']]);
    expect(result.current.sends.get('c1')).toEqual({ phase: 'refused', reason: 'A reply was still running in this chat.' });
    expect(result.current.deck.ready.map((c) => c.comment_id)).toEqual(['c1']);
    expect(result.current.deck.error).toBe('A reply was still running in this chat.');
  });

  it('is refused with the chat\'s own words for a refusal it worded as a notice', async () => {
    const { result, chat, tell } = mount();
    const send = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[0], { refused: { message: 'A reply is still running.', kind: 'notice' } });
      await send.done;
    });
    expect(send.outcome()).toEqual([[null, 'A reply is still running.']]);
  });

  it('carries on through a retry that takes the comments on, until that one is answered', async () => {
    const { result, tell } = mount();
    const send = sendAll(result, [comment('c1')]);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
      // The resend knows its comments by id.
      tell({ ids: ['c1'], reading: 'chat' }, 'began');
      await vi.advanceTimersByTimeAsync(60_000);
    });
    expect(send.outcome()).toBeUndefined();
    expect(fetchRows).not.toHaveBeenCalled();
    expect(result.current.sends.get('c1')?.phase).toBe('sending');
    await act(async () => {
      tell({ ids: ['c1'], reading: 'chat' }, 'accepted');
      await send.done;
    });
    expect(send.outcome()).toEqual([['t1', null]]);
  });

  it('is not sent at once when the chat took nothing', async () => {
    const { result, chat } = mount({ carries: false });
    const send = sendAll(result, [comment('c1')]);
    await act(async () => { await send.done; });
    expect(chat).toHaveLength(1);
    expect(send.outcome()).toEqual([[null, 'pageViewer.comments.notSentQueued']]);
    expect(result.current.sends.get('c1')).toBeUndefined();
  });

  it('settles by the comments\' rows once the chat lets the request go unanswered', async () => {
    const { result, chat, tell } = mount();
    fetchRows.mockResolvedValueOnce([comment('c1')]);
    const queuedStill = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[0], 'dropped');
      await queuedStill.done;
    });
    expect(fetchRows).toHaveBeenCalledWith(expect.anything(), 'a1');
    expect(queuedStill.outcome()).toEqual([[null, 'pageViewer.comments.notSentQueued']]);
    expect(result.current.sends.get('c1')).toBeUndefined();

    fetchRows.mockResolvedValueOnce([comment('c1', { delivery: 'sent', thread_id: 't1' })]);
    const wentAfterAll = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[1], 'dropped');
      await wentAfterAll.done;
    });
    expect(wentAfterAll.outcome()).toEqual([['t1', null]]);
  });
});
