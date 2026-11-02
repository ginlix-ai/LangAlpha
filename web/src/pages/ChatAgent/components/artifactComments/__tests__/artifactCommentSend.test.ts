// @vitest-environment node
import { describe, it, expect } from 'vitest';

import type { ArtifactCommentMessage, QueuedArtifactComment } from '@/types/api';
import {
  commentsByFile,
  openerMatchText,
  readArtifactCommentsShown,
  shownFromQueued,
} from '../artifactCommentSend';
import { groupCommentReplies, readArtifactCommentReply } from '../artifactCommentReply';

function message(author: 'user' | 'agent', body: string, sent: boolean): ArtifactCommentMessage {
  return {
    message_id: `${author}-${body}`,
    seq: 1,
    author,
    body,
    sent_at: sent ? '2026-10-29T00:00:00Z' : null,
    request_key: null,
    thread_id: null,
    created_at: '2026-10-29T00:00:00Z',
  };
}

function queued(overrides: Partial<QueuedArtifactComment> = {}): QueuedArtifactComment {
  return {
    comment_id: 'c-1',
    artifact_id: 'a-1',
    workspace_id: 'ws-1',
    number: 2,
    path: 'reports/q3/index.html',
    file_sha256: null,
    title: 'Q3 review',
    anchor: { kind: 'element', path: '#a', label: 'Revenue tile', quote: '$9.6B' },
    status: 'open',
    delivery: 'queued',
    thread_id: 't-1',
    thread_title: null,
    thread_archived: false,
    created_at: '2026-10-29T00:00:00Z',
    updated_at: '2026-10-29T00:00:00Z',
    resolved_at: null,
    messages: [message('user', 'Why so high?', false)],
    read_through: 1,
    unread: 0,
    ...overrides,
  };
}

describe('shownFromQueued', () => {
  it('shows the words a send delivers, marks a comment the agent heard before, and keeps where the person reads', () => {
    const shown = shownFromQueued('look at these', 'file', [
      queued(),
      queued({
        comment_id: 'c-2',
        number: 3,
        messages: [message('user', 'first', true), message('agent', 'answer', false), message('user', 'again', false), message('user', 'and more', false)],
      }),
    ]);
    expect(shown.note).toBe('look at these');
    expect(shown.reading).toBe('file');
    expect(shown.comments[0]).toMatchObject({
      comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws-1', path: 'reports/q3/index.html',
      number: 2, text: 'Why so high?', follow_up: false, title: 'Q3 review',
    });
    expect(shown.comments[1]).toMatchObject({ text: 'again\nand more', follow_up: true });
  });

  it('reads the anchor as the server shows it: whitespace folded, long text clipped', () => {
    const shown = shownFromQueued('', 'chat', [queued({ anchor: { kind: 'text', label: '  Revenue\n tile ', quote: 'x'.repeat(400) } })]);
    expect(shown.comments[0].label).toBe('Revenue tile');
    expect(shown.comments[0].quote).toHaveLength(200);
    expect(shown.comments[0].quote?.endsWith('…')).toBe(true);
    expect(shownFromQueued('', 'chat', [queued({ anchor: { kind: 'named' } })]).comments[0]).toMatchObject({ label: null, quote: null });
  });
});

describe('readArtifactCommentsShown', () => {
  it('reads a turn row, keeping the comments it can draw and dropping the rest', () => {
    const shown = readArtifactCommentsShown({
      note: 'n',
      reading: 'file',
      comments: [
        { comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws-1', path: 'r.html', title: 'T', number: 1, text: 'a', label: 'L', quote: null },
        { comment_id: 'c-2', artifact_id: 'a-1', number: 'two' },
        null,
        { comment_id: 'c-3', artifact_id: 'a-2', number: 4, text: 'b' },
      ],
    });
    expect(shown?.note).toBe('n');
    expect(shown?.reading).toBe('file');
    expect(shown?.comments.map((c) => c.comment_id)).toEqual(['c-1', 'c-3']);
    expect(shown?.comments[1]).toMatchObject({ title: null, path: '', label: null, quote: null, follow_up: false });
  });

  it('is null for anything that is not a comment send, and reads an unknown reading as the chat', () => {
    expect(readArtifactCommentsShown(undefined)).toBeNull();
    expect(readArtifactCommentsShown({ note: 'n' })).toBeNull();
    expect(readArtifactCommentsShown({ note: 'n', comments: [] })).toBeNull();
    expect(readArtifactCommentsShown({ comments: [{ comment_id: 'c' }] })).toBeNull();
    expect(readArtifactCommentsShown({ comments: [{ comment_id: 'c', artifact_id: 'a', number: 1 }] })?.reading).toBe('chat');
  });
});

describe('commentsByFile', () => {
  it('groups by file in the order shown, naming each by its title or else its file name', () => {
    const shown = shownFromQueued('', 'chat', [
      queued(),
      queued({ comment_id: 'c-2', artifact_id: 'a-2', title: null, path: 'notes/summary.html' }),
      queued({ comment_id: 'c-3' }),
    ]);
    expect(commentsByFile(shown.comments).map((g) => [g.title, g.comments.map((c) => c.comment_id)])).toEqual([
      ['Q3 review', ['c-1', 'c-3']],
      ['summary.html', ['c-2']],
    ]);
  });
});

describe('openerMatchText', () => {
  it('matches a comment send by its note, apart from a plain message of the same words', () => {
    const shown = shownFromQueued(' fix these ', 'chat', [queued()]);
    // The replay's query holds the block the agent read; the view's copy holds the note.
    expect(openerMatchText('fix these\n<file-comments>…</file-comments>', shown)).toBe(openerMatchText('fix these', shown));
    expect(openerMatchText('fix these', shown)).not.toBe(openerMatchText('fix these', null));
    expect(openerMatchText(' hello ', undefined)).toBe('hello');
    // Same note, different comments: two sends that must not read alike.
    const other = shownFromQueued(' fix these ', 'chat', [queued({ comment_id: 'other' })]);
    expect(openerMatchText('fix these', shown)).not.toBe(openerMatchText('fix these', other));
  });
});

describe('file comment replies', () => {
  const artifact = (number: number, extra: Record<string, unknown> = {}) => ({
    type: 'artifact_comment', artifact_id: 'a-1', workspace_id: 'ws-1', path: 'r.html',
    comment_id: `c-${number}`, number, resolved: false, text: `answer ${number}`, clipped: false, ...extra,
  });

  it('reads a reply off its artifact, and nothing else', () => {
    expect(readArtifactCommentReply(artifact(2, { resolved: true, clipped: true }))).toEqual({
      artifactId: 'a-1', workspaceId: 'ws-1', path: 'r.html', commentId: 'c-2', number: 2,
      resolved: true, text: 'answer 2', clipped: true,
    });
    // The tool's error result is an empty artifact.
    expect(readArtifactCommentReply({})).toBeNull();
    expect(readArtifactCommentReply(artifact(2, { number: '2' }))).toBeNull();
    // The page model's reply is gone with it.
    expect(readArtifactCommentReply(artifact(2, { type: 'page_comment' }))).toBeNull();
  });

  it('groups replies in a row to one file, resolved or not', () => {
    const members = [
      artifact(1), artifact(2), artifact(3, { resolved: true }), artifact(4, { artifact_id: 'a-2' }), artifact(5),
    ].map((a) => ({ reply: readArtifactCommentReply(a)! }));
    expect(groupCommentReplies(members).map((g) => g.map((m) => m.reply.number))).toEqual([[1, 2, 3], [4], [5]]);
  });

  it('lists a comment answered twice once, where it was first, as its latest reply left it', () => {
    const members = [artifact(1), artifact(2), artifact(1, { resolved: true, text: 'again' })]
      .map((a, i) => ({ key: `k-${i}`, reply: readArtifactCommentReply(a)! }));
    const groups = groupCommentReplies(members);
    expect(groups).toHaveLength(1);
    expect(groups[0].map((m) => [m.key, m.reply.number, m.reply.resolved, m.reply.text])).toEqual([
      ['k-0', 1, true, 'again'],
      ['k-1', 2, false, 'answer 2'],
    ]);
  });
});
