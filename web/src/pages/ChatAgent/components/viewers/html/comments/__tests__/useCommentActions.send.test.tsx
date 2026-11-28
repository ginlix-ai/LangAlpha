import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { addArtifactCommentMessage, CommentSendRefused } from '@/pages/ChatAgent/utils/api/artifacts';
import type { ArtifactComment } from '@/types/api';
import { nextMarks, type CommentSendStore, type Marks } from '../../../../artifactComments/commentSendStore';
import type { ArtifactCommentSender } from '../../../../artifactComments/sendContext';
import { useCommentActions } from '../useCommentActions';

const toast = vi.hoisted(() => vi.fn());
vi.mock('@/components/ui/use-toast', () => ({ toast }));
// A reply's write answers with the comment as it was given, so each test reads the send after it.
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  addArtifactCommentMessage: vi.fn(),
}));

function comment(id: string, over: Partial<ArtifactComment> = {}): ArtifactComment {
  return {
    comment_id: id, artifact_id: 'a-drive', number: 1, path: 'notes.md', file_sha256: null, title: null,
    anchor: { kind: 'element' }, status: 'open', delivery: 'note', thread_id: null, thread_title: null,
    thread_archived: false, created_at: '', updated_at: `u-${id}`, resolved_at: null, messages: [],
    read_through: 0, unread: 0, kind: 'drive', ...over,
  } as ArtifactComment;
}

function memorySends(): CommentSendStore {
  let marks: Marks = new Map();
  return {
    get: (id) => marks.get(id),
    sending: (comments) => { marks = nextMarks(marks, { type: 'sending', comments }); },
    taken: (comments) => { marks = nextMarks(marks, { type: 'taken', comments }); },
    refused: (ids, reason) => { marks = nextMarks(marks, { type: 'refused', ids, reason }); },
    clear: (ids) => { marks = nextMarks(marks, { type: 'clear', ids }); },
    release: () => false,
  };
}

function actionsWith(send: ArtifactCommentSender['send'], beside: ArtifactCommentSender['beside'] = null, entryId: string | null = null) {
  const sender: ArtifactCommentSender = { beside, running: false, send };
  return renderHookWithProviders(() => useCommentActions({ workspaceId: 'home', sender, sends: memorySends(), entryId })).result;
}

/** A send from the box: the words are written, then sent, and the box has closed. */
const fromBox = (actions: ReturnType<typeof actionsWith>, c: ArtifactComment) => {
  vi.mocked(addArtifactCommentMessage).mockResolvedValue(c);
  return act(() => actions.current.reply(c, 'words', { kind: 'send', to: { threadId: 't-1' } }));
};
const idle = () => new Promise((r) => setTimeout(r, 0));

beforeEach(() => toast.mockReset());

describe('a send from the box: where a refusal is said', () => {
  it("says a refusal where the person is, with the server's reason and that the words stay as a note", async () => {
    const send = vi.fn<ArtifactCommentSender['send']>()
      .mockRejectedValue(new CommentSendRefused('The chat is archived.', 'archived'));
    const actions = actionsWith(send);

    await fromBox(actions, comment('c-1'));

    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(toast.mock.calls[0][0]).toMatchObject({
      variant: 'destructive',
      description: "Not sent. The chat is archived. It's kept on the file as a note.",
    });
  });

  it('says a refusal with no reason from the server in the existing words', async () => {
    const send = vi.fn<ArtifactCommentSender['send']>().mockRejectedValue(new CommentSendRefused(null));
    const actions = actionsWith(send);

    await fromBox(actions, comment('c-1', { delivery: 'queued', thread_id: 't-1' }));

    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(toast.mock.calls[0][0].description).toBe("Not sent. It's still queued.");
  });

  it('leaves a send to the chat beside it to that chat, which says its own refusals', async () => {
    const send = vi.fn<ArtifactCommentSender['send']>().mockRejectedValue(new CommentSendRefused('Busy'));
    const actions = actionsWith(send, { threadId: 't-1' });

    await fromBox(actions, comment('c-1'));

    await waitFor(() => expect(send).toHaveBeenCalled());
    await idle();
    expect(toast).not.toHaveBeenCalled();
  });
});

describe("Send now on a comment's card", () => {
  it('leaves a refusal to the card, which stays on screen and says it', async () => {
    const send = vi.fn<ArtifactCommentSender['send']>().mockRejectedValue(new CommentSendRefused('Busy'));
    const actions = actionsWith(send);

    act(() => actions.current.send(comment('c-1'), { threadId: 't-1' }));

    await waitFor(() => expect(actions.current.sendState(comment('c-1'))).toMatchObject({ refusal: 'Busy' }));
    await idle();
    expect(toast).not.toHaveBeenCalled();
  });

  it('names the Drive file it is on, which its row does not', async () => {
    const send = vi.fn<ArtifactCommentSender['send']>().mockResolvedValue({ threadId: 't-1' });
    const actions = actionsWith(send, null, 'entry-1');

    act(() => actions.current.send(comment('c-1'), { threadId: 't-1' }));

    await waitFor(() => expect(send).toHaveBeenCalled());
    expect(send.mock.calls[0][0][0]).toMatchObject({ comment_id: 'c-1', path: 'notes.md', entry_id: 'entry-1' });
  });
});
