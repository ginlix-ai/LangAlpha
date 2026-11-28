import { describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ArtifactComment } from '@/types/api';
import type { CommentSendStore } from '../../../../artifactComments/commentSendStore';
import type { ArtifactCommentSender } from '../../../../artifactComments/sendContext';
import { useCommentActions } from '../useCommentActions';

const edit = vi.hoisted(() => vi.fn());
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  editArtifactCommentMessage: edit,
}));

const queued = (body: string) => ({
  comment_id: 'c-1', artifact_id: 'a-1', number: 1, delivery: 'queued', thread_id: 't-1', status: 'open',
  messages: [{ message_id: 'm-1', seq: 1, author: 'user', body, sent_at: null, request_key: null, thread_id: null, created_at: '' }],
}) as unknown as ArtifactComment;

describe("editing a comment's words", () => {
  it("shows the server's answer on the card at once and reads the chats' queues again", async () => {
    edit.mockResolvedValue(queued('new words'));
    const sender: ArtifactCommentSender = { beside: null, running: false, send: vi.fn() };
    const { result, queryClient } = renderHookWithProviders(() => useCommentActions({
      workspaceId: 'home', sender, sends: { get: () => undefined } as unknown as CommentSendStore,
    }));
    queryClient.setQueryData(queryKeys.artifacts.comments('a-1'), [queued('old words')]);
    queryClient.setQueryData(queryKeys.artifacts.queued('t-1'), []);

    await act(() => result.current.edit(queued('old words'), 'm-1', 'new words'));

    expect(edit).toHaveBeenCalledWith('a-1', 'c-1', 'm-1', 'new words');
    expect(queryClient.getQueryData<ArtifactComment[]>(queryKeys.artifacts.comments('a-1'))?.[0].messages[0].body).toBe('new words');
    expect(queryClient.getQueryState(queryKeys.artifacts.queued('t-1'))?.isInvalidated).toBe(true);
  });
});
