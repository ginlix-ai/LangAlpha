import { describe, expect, it, vi } from 'vitest';
import type { TFunction } from 'i18next';
import { CommentSendRefused } from '@/pages/ChatAgent/utils/api/artifacts';
import type { CommentInWorkspace } from '../artifactCommentSend';
import { nextMarks, type CommentSendWriter, type Marks } from '../commentSendStore';
import type { ArtifactCommentSender } from '../sendContext';
import { queuedBatches, refusalReason, sendBatches } from '../commentSends';
import { commentDestinations } from '../destinations';

function comment(id: string, over: Partial<CommentInWorkspace> = {}): CommentInWorkspace {
  return {
    comment_id: id, artifact_id: 'a', workspace_id: 'ws', number: 1, path: 'r.html', file_sha256: null, title: null,
    anchor: { kind: 'element' }, status: 'open', delivery: 'queued', thread_id: 't1', thread_title: 'T1',
    thread_archived: false, created_at: '', updated_at: `u-${id}`, resolved_at: null, messages: [],
    read_through: 0, unread: 0, ...over,
  } as CommentInWorkspace;
}

/** A writer over a plain map, with the store's own transitions. */
function memorySends(): CommentSendWriter & { marks: () => Marks } {
  let marks: Marks = new Map();
  return {
    marks: () => marks,
    sending: (comments) => { marks = nextMarks(marks, { type: 'sending', comments }); },
    taken: (comments) => { marks = nextMarks(marks, { type: 'taken', comments }); },
    refused: (ids, reason) => { marks = nextMarks(marks, { type: 'refused', ids, reason }); },
    clear: (ids) => { marks = nextMarks(marks, { type: 'clear', ids }); },
    release: (ids) => {
      const was = marks;
      marks = nextMarks(marks, { type: 'release', ids });
      return marks !== was;
    },
  };
}

const t = ((key: string) => key) as unknown as TFunction;
const pending = commentDestinations(t, undefined, null).targetFor;
const toNew = commentDestinations(t, {
  default: { rule: 'new', thread: null, workspace_id: 'ws' }, bound: null, beside: null, creator: null,
}, null).targetFor;

describe('queuedBatches', () => {
  it('starts another send each time one is as full as the server takes', () => {
    const many = Array.from({ length: 101 }, (_, i) => comment(String(i)));

    const batches = queuedBatches(many, pending);

    expect(batches.map((b) => b.comments.length)).toEqual([50, 50, 1]);
    expect(batches.map((b) => b.to)).toEqual([{ threadId: 't1' }, { threadId: 't1' }, { threadId: 't1' }]);
  });

  it('makes one send per thread, in pin order, of open queued comments only', () => {
    const batches = queuedBatches([
      comment('1'),
      comment('2', { thread_id: 't2' }),
      comment('3'),
      comment('4', { delivery: 'note', thread_id: null }),
      comment('5', { status: 'resolved' }),
    ], pending);
    expect(batches.map((b) => [b.to, b.comments.map((c) => c.comment_id)])).toEqual([
      [{ threadId: 't1' }, ['1', '3']],
      [{ threadId: 't2' }, ['2']],
    ]);
  });

  it('sends a comment whose thread is gone where new comments go, and holds it while that is unknown', () => {
    const orphan = comment('1', { thread_id: null });
    expect(queuedBatches([orphan], pending)).toEqual([]);
    expect(queuedBatches([orphan], toNew)).toEqual([{ to: { newThreadIn: 'ws' }, comments: [orphan] }]);
  });
});

describe('refusalReason', () => {
  it('says a gone comment in the reader\'s words, a conflict as the server put it, and anything else as where it still is', () => {
    expect(refusalReason(t, new CommentSendRefused('x', 'comments_unavailable', 409), true)).toBe('pageViewer.comments.notSentUnavailable');
    expect(refusalReason(t, new CommentSendRefused('The agent is stopping.', 'stopping', 409), true)).toBe('The agent is stopping.');
    expect(refusalReason(t, new CommentSendRefused('Failed to fetch', null, null), true)).toBe('pageViewer.comments.notSentQueued');
    expect(refusalReason(t, new Error('Failed to fetch'), false)).toBe('pageViewer.comments.notSentNote');
  });
});

describe('sendBatches', () => {
  it('sends one destination\'s batches in order and makes a new thread once', async () => {
    const answers: (() => void)[] = [];
    const send = vi.fn(() => new Promise<{ threadId: string }>((resolve) => {
      answers.push(() => resolve({ threadId: 'made' }));
    }));
    const sender = { besideThreadId: null, running: false, send } as unknown as ArtifactCommentSender;
    const to = { newThreadIn: 'ws' };
    const batches = [{ to, comments: [comment('1')] }, { to, comments: [comment('2')] }];

    const done = sendBatches(batches, { sender, sends: memorySends(), reasonOf: () => 'why' });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenLastCalledWith(batches[0].comments, to);

    answers[0]();
    await vi.waitFor(() => expect(send).toHaveBeenCalledTimes(2));
    expect(send).toHaveBeenLastCalledWith(batches[1].comments, { threadId: 'made' });

    answers[1]();
    expect((await done).map((o) => o.batch)).toEqual(batches);
  });

  it('marks each comment on its way, then taken or refused with the reason', async () => {
    const sends = memorySends();
    let release!: () => void;
    const send = vi.fn((_comments: readonly CommentInWorkspace[], to: { threadId: string } | { newThreadIn: string }) => {
      if ('threadId' in to && to.threadId === 't2') return Promise.reject(new Error('no'));
      return new Promise<{ threadId: string }>((resolve) => { release = () => resolve({ threadId: 't1' }); });
    });
    const sender: ArtifactCommentSender = { besideThreadId: null, running: false, send };
    const batches = queuedBatches([comment('1'), comment('2', { thread_id: 't2' }), comment('3')], pending);

    const done = sendBatches(batches, { sender, sends, reasonOf: () => 'why' });
    expect(sends.marks().get('1')).toEqual({ phase: 'sending', at: 'u-1' });
    expect(send).toHaveBeenCalledWith([expect.objectContaining({ comment_id: '1' }), expect.objectContaining({ comment_id: '3' })], { threadId: 't1' });
    expect(send).toHaveBeenCalledWith([expect.objectContaining({ comment_id: '2' })], { threadId: 't2' });
    release();
    const outcomes = await done;

    expect(sends.marks().get('1')).toEqual({ phase: 'taken', at: 'u-1' });
    expect(sends.marks().get('3')).toEqual({ phase: 'taken', at: 'u-3' });
    expect(sends.marks().get('2')).toEqual({ phase: 'refused', reason: 'why' });
    expect(outcomes.map((o) => [o.threadId, o.reason])).toEqual([['t1', null], [null, 'why']]);
  });

  it('leaves the marks of a batch the chat beside carries to the chat, and still reports how it went', async () => {
    const sends = memorySends();
    const send = vi.fn((_comments: readonly CommentInWorkspace[], to: { threadId: string } | { newThreadIn: string }) => (
      'threadId' in to && to.threadId === 't1'
        ? Promise.reject(new CommentSendRefused('A reply is still running.', null, 409))
        : Promise.resolve({ threadId: 't2' })
    ));
    const sender: ArtifactCommentSender = { besideThreadId: 't1', running: true, send };
    const batches = queuedBatches([comment('1'), comment('2', { thread_id: 't2' })], pending);

    const outcomes = await sendBatches(batches, { sender, sends, reasonOf: (err) => refusalReason(t, err, true) });

    expect(sends.marks().has('1')).toBe(false);
    expect(sends.marks().get('2')).toEqual({ phase: 'taken', at: 'u-2' });
    expect(outcomes.map((o) => [o.threadId, o.reason])).toEqual([[null, 'A reply is still running.'], ['t2', null]]);
  });
});
