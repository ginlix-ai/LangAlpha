// @vitest-environment node
import { describe, expect, it } from 'vitest';

import type { ArtifactComment, ArtifactCommentMessage } from '@/types/api';
import { artifactUnread, latestSeq, readAll, readThrough, unreadComments, unreadPast } from '../artifactCommentReads';

function message(seq: number, author: ArtifactCommentMessage['author']): ArtifactCommentMessage {
  return {
    message_id: `m-${seq}`,
    seq,
    author,
    body: `${author} ${seq}`,
    sent_at: null,
    request_key: null,
    thread_id: null,
    created_at: '2026-10-30T00:00:00Z',
  };
}

// The owner asked, the agent answered twice, and the owner has read the first answer.
function comment(overrides: Partial<ArtifactComment> = {}): ArtifactComment {
  return {
    comment_id: 'c-1',
    artifact_id: 'a-1',
    number: 1,
    path: 'report.html',
    file_sha256: null,
    title: null,
    anchor: { kind: 'element', path: '#a' },
    status: 'resolved',
    delivery: 'sent',
    thread_id: 't-1',
    thread_title: null,
    thread_archived: false,
    created_at: '2026-10-30T00:00:00Z',
    updated_at: '2026-10-30T00:00:00Z',
    resolved_at: '2026-10-30T00:00:00Z',
    messages: [message(1, 'user'), message(2, 'agent'), message(3, 'agent')],
    read_through: 2,
    unread: 1,
    ...overrides,
  };
}

describe('unread', () => {
  it('counts only the agent past the cursor, whether or not the thread is resolved', () => {
    expect(unreadPast(comment(), 0)).toBe(2);
    expect(unreadPast(comment(), 2)).toBe(1);
    expect(unreadPast(comment({ messages: [message(1, 'user'), message(2, 'user')] }), 0)).toBe(0);
    expect(latestSeq(comment())).toBe(3);
    expect(latestSeq(comment(), 'agent')).toBe(3);
    expect(latestSeq(comment(), 'user')).toBe(1);
    expect(latestSeq(comment({ messages: [message(1, 'user')] }), 'agent')).toBe(0);
  });
});

describe('readThrough', () => {
  it('reads up to what the view displayed and leaves a later reply unread', () => {
    const later = comment({ messages: [...comment().messages, message(4, 'agent')], unread: 2 });
    expect(readThrough(later, 3)).toMatchObject({ read_through: 3, unread: 1 });
  });

  it('never moves back, nor past the latest message', () => {
    const read = comment({ read_through: 3, unread: 0 });
    expect(readThrough(read, 1)).toBe(read);
    expect(readThrough(comment(), 99)).toMatchObject({ read_through: 3, unread: 0 });
  });

  it('reads a whole file through each comment\'s latest message', () => {
    const other = comment({ comment_id: 'c-2', read_through: 0, unread: 2 });
    expect(readAll([comment(), other]).map((c) => [c.read_through, c.unread])).toEqual([[3, 0], [3, 0]]);
  });
});

describe('artifactUnread', () => {
  it('counts the comments holding something unread, not their messages, in the order given', () => {
    const comments = [
      comment({ comment_id: 'c-1', unread: 0 }),
      comment({ comment_id: 'c-2', unread: 2 }),
      comment({ comment_id: 'c-3', unread: 1 }),
    ];
    expect(artifactUnread(comments)).toEqual({ count: 2, commentIds: ['c-2', 'c-3'] });
    expect(unreadComments(comments)).toBe(2);
    expect(artifactUnread([])).toEqual({ count: 0, commentIds: [] });
  });
});
