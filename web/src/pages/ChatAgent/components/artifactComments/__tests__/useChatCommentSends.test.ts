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
import type { CommentTarget } from '../../../utils/api/artifacts';
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
 * not start). `tell` speaks for the chat's request from then on. With
 * `threadId` null the chat is a new one, which takes its request once its
 * create has made the thread `t-new`, a few microtasks on. `move` is the chat
 * moving on (its first send naming its thread, a turn ending).
 */
function mount({ carries = true, threadId = 't1', running = false }: { carries?: boolean; threadId?: string | null; running?: boolean } = {}) {
  const at = { threadId, running };
  const chat: ArtifactCommentsShown[] = [];
  /** The thread the chat stood in as each send reached it. */
  const sentIn: (string | null)[] = [];
  const listeners = new Set<CommentSendListener>();
  const tell: CommentSendListener = (comments, outcome) => {
    for (const listener of [...listeners]) listener(comments, outcome);
  };
  const watch = (listener: CommentSendListener) => {
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  };
  const hook = renderHookWithProviders(() => useChatCommentSends({
    threadId: at.threadId ?? undefined,
    workspaceId: 'ws',
    running: at.running,
    watch,
    chatCovered: () => false,
    // The chat's send takes its request at once and says so.
    sendToChat: (shown) => {
      chat.push(shown);
      sentIn.push(at.threadId);
      if (!carries) return Promise.resolve(null);
      if (at.threadId) {
        tell(shown, 'began');
        return Promise.resolve(at.threadId);
      }
      return Promise.resolve().then(() => {
        tell(shown, 'began');
        return 't-new';
      });
    },
  }));
  const move = (next: Partial<typeof at>) => {
    Object.assign(at, next);
    hook.rerender();
  };
  return { ...hook, chat, sentIn, tell, move };
}

/** Send all for one batch to the chat beside, as the toolbar does; `outcome()` is undefined until it settles. */
function sendAll(result: ReturnType<typeof mount>['result'], comments: CommentInWorkspace[], to: CommentTarget = { threadId: 't1' }) {
  let outcomes: BatchOutcome[] | undefined;
  const done = sendBatches([{ to, comments }], {
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
    expect(send.outcome()).toEqual([[null, 'pageViewer:comments.notSentQueued']]);
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
    expect(queuedStill.outcome()).toEqual([[null, 'pageViewer:comments.notSentQueued']]);
    expect(result.current.sends.get('c1')).toBeUndefined();

    fetchRows.mockResolvedValueOnce([comment('c1', { delivery: 'sent', thread_id: 't1' })]);
    const wentAfterAll = sendAll(result, [comment('c1')]);
    await act(async () => {
      tell(chat[1], 'dropped');
      await wentAfterAll.done;
    });
    expect(wentAfterAll.outcome()).toEqual([['t1', null]]);
  });

  it('from a new chat, a send to a new thread in its workspace starts the chat\'s thread', async () => {
    const { result, chat, tell } = mount({ threadId: null });
    expect(result.current.sender).toMatchObject({ beside: { newThreadIn: 'ws' } });
    const send = sendAll(result, [comment('c1', { delivery: 'note', thread_id: null })], { newThreadIn: 'ws' });
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });

    expect(chat.map((s) => s.comments.map((c) => c.comment_id))).toEqual([['c1']]);
    expect(result.current.sends.get('c1')?.phase).toBe('sending');
    await act(async () => {
      tell(chat[0], 'accepted');
      await send.done;
    });
    // The thread the chat's create made, which its request was taken for.
    expect(send.outcome()).toEqual([['t-new', null]]);
    expect(fetchRows).not.toHaveBeenCalled();
  });

  it('from a new chat whose first send is making its thread, joins that thread once named', async () => {
    const { result, chat, sentIn, tell, move } = mount({ threadId: null, running: true });
    const send = sendAll(result, [comment('c1', { delivery: 'note', thread_id: null })], { newThreadIn: 'ws' });
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    // Nothing goes to the placeholder the chat stands in until then.
    expect(chat).toEqual([]);

    await act(async () => {
      move({ threadId: 't-made' });
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(sentIn).toEqual(['t-made']);
    await act(async () => {
      tell(chat[0], 'accepted');
      await send.done;
    });
    expect(send.outcome()).toEqual([['t-made', null]]);
  });

  it('from a new chat whose first send ends without a thread, is not sent', async () => {
    const { result, chat, move } = mount({ threadId: null, running: true });
    const send = sendAll(result, [comment('c1', { delivery: 'note', thread_id: null })], { newThreadIn: 'ws' });
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    // The chat's effects run once the move is committed.
    act(() => move({ running: false }));
    await act(async () => { await send.done; });
    expect(chat).toEqual([]);
    expect(send.outcome()).toEqual([[null, 'pageViewer:comments.notSentQueued']]);
  });

  it('settles by the comments\' rows once the chat goes before answering', async () => {
    const { result, chat, unmount } = mount();
    fetchRows.mockResolvedValueOnce([comment('c1', { delivery: 'sent', thread_id: 't1' })]);
    const send = sendAll(result, [comment('c1')]);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(chat).toHaveLength(1);

    await act(async () => {
      unmount();
      await send.done;
    });
    expect(send.outcome()).toEqual([['t1', null]]);
  });

  it('is not sent once the chat goes while its new thread is still being made', async () => {
    const { result, chat, unmount } = mount({ threadId: null, running: true });
    const send = sendAll(result, [comment('c1', { delivery: 'note', thread_id: null })], { newThreadIn: 'ws' });
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => {
      unmount();
      await send.done;
    });
    expect(chat).toEqual([]);
    expect(send.outcome()).toEqual([[null, 'pageViewer:comments.notSentQueued']]);
  });
});
