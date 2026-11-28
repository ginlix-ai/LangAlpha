import React from 'react';
import { afterEach, describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { applyFeedEvent, applySnapshot, resetThreadLifecycle } from '@/lib/threadLifecycle/store';
import type { ArtifactComment } from '@/types/api';
import { CommentLayerContext, useCommentLook, type CommentLayerValue } from '../commentLayerContext';

function comment(over: Partial<ArtifactComment> = {}): ArtifactComment {
  return {
    comment_id: 'c-1', artifact_id: 'a-1', number: 1, path: 'notes.md', file_sha256: null, title: null,
    anchor: { kind: 'element' }, status: 'open', delivery: 'sent', thread_id: 't-1', thread_title: 'Chat',
    thread_archived: false, created_at: '', updated_at: 'u', resolved_at: null, read_through: 0, unread: 0, kind: 'drive',
    messages: [{ message_id: 'm-1', seq: 1, author: 'user', body: 'fix it', sent_at: '2026-11-27T00:00:00Z', request_key: 'k', thread_id: 't-1', created_at: '' }],
    ...over,
  } as ArtifactComment;
}

/** A layer with no chat beside it, as on the Drive. */
function look(c: ArtifactComment, { besideThreadId = null, besideRunning = false }: { besideThreadId?: string | null; besideRunning?: boolean } = {}) {
  const layer = {
    actions: { sendState: () => ({ sending: false, pending: false, refusal: null }) },
    besideThreadId,
    destinations: {},
    runningFor: (x: ArtifactComment) => besideRunning && (x.thread_id ?? besideThreadId) === besideThreadId,
  } as unknown as CommentLayerValue;
  const wrapper = ({ children }: { children: React.ReactNode }) => <CommentLayerContext value={layer}>{children}</CommentLayerContext>;
  return renderHook(() => useCommentLook(c), { wrapper });
}

const run = (status: string, seq = 1) => act(() => applyFeedEvent({
  type: status === 'running' ? 'run_started' : 'run_settled', thread_id: 't-1', run_id: `r-${seq}`, run_seq: seq, status,
}));

afterEach(() => resetThreadLifecycle());

describe('a comment sent to a chat no view beside it shows', () => {
  it('works while the user feed says its chat runs, and stops when it settles', () => {
    const { result } = look(comment());
    expect(result.current.working).toBe(false);

    run('running');
    expect(result.current.working).toBe(true);

    run('completed');
    expect(result.current).toMatchObject({ working: false, needsRetry: false });
  });

  it('asks for a retry when the run ended on an error, until a new run starts', () => {
    const { result } = look(comment());

    run('failed');
    expect(result.current.needsRetry).toBe(true);

    run('running', 2);
    expect(result.current).toMatchObject({ working: true, needsRetry: false });
  });

  it("knows a run in progress from the feed's snapshot, as after a reload", () => {
    act(() => applySnapshot({
      as_of_seq: 5, oldest_included_unseen_seq: 0, unseen: [],
      live: [{ thread_id: 't-1', workspace_id: 'home', run_id: 'r-5', run_seq: 5, status: 'running', last_seen_run_seq: 4 }],
    }));
    expect(look(comment()).result.current.working).toBe(true);
  });

});

describe('a comment sent to the chat beside the file', () => {
  it('reads its turn from the feed like any other, a failed run asking for a retry', () => {
    run('running');
    expect(look(comment(), { besideThreadId: 't-1' }).result.current.working).toBe(true);
    run('failed', 2);
    expect(look(comment(), { besideThreadId: 't-1' }).result.current.needsRetry).toBe(true);
  });

  it("works from the send, before the feed has heard of its run, rather than flicker", () => {
    run('failed');
    const { result } = look(comment(), { besideThreadId: 't-1', besideRunning: true });
    expect(result.current).toMatchObject({ working: true, needsRetry: false });
  });

  it('works on a new chat that has no thread yet, which only the chat knows of', () => {
    const fresh = comment({ thread_id: null });
    expect(look(fresh, { besideRunning: true }).result.current.working).toBe(true);
  });
});
