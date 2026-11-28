import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { ArtifactComment } from '@/types/api';

const mockPost = vi.hoisted(() => vi.fn());
vi.mock('../../../utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../utils/api/artifacts')>()),
  postArtifactComments: mockPost,
}));

import { CommentSendRefused } from '../../../utils/api/artifacts';
import { postComments } from '../commentPosts';

function comment(id: string, artifactId = 'a-1') {
  return { comment_id: id, artifact_id: artifactId, workspace_id: 'home' } as ArtifactComment & { workspace_id: string };
}

const home = { newThreadIn: 'home' };
let queryClient = new QueryClient();
const post = (c = comment('c-1'), to: { newThreadIn: string } | { threadId: string } = home) =>
  postComments([c], to, { reading: 'file', queryClient });

/** A post that answers when told to. */
function held() {
  let answer: (threadId: string) => void = () => {};
  let refuse: () => void = () => {};
  mockPost.mockImplementationOnce(() => new Promise((resolve, reject) => {
    answer = (threadId) => resolve({ threadId });
    refuse = () => reject(new CommentSendRefused('Busy'));
  }));
  return { answer: (t: string) => answer(t), refuse: () => refuse() };
}

const leaving = () => {
  const event = new Event('beforeunload', { cancelable: true });
  window.dispatchEvent(event);
  return event.defaultPrevented;
};

beforeEach(() => {
  mockPost.mockReset();
  queryClient = new QueryClient();
});

describe('a post to a new thread while another for the same file is out', () => {
  it('joins the thread the first makes rather than making another', async () => {
    const first = held();
    mockPost.mockResolvedValue({ threadId: 'cos-1' });
    const one = post(comment('c-1'));
    const two = post(comment('c-2'));
    await Promise.resolve();
    expect(mockPost).toHaveBeenCalledTimes(1);

    first.answer('cos-1');
    await Promise.all([one, two]);
    expect(mockPost).toHaveBeenCalledTimes(2);
    expect(mockPost.mock.calls[1][1]).toEqual({ threadId: 'cos-1' });
  });

  it("starts its own when the first was refused", async () => {
    const first = held();
    mockPost.mockResolvedValue({ threadId: 'cos-2' });
    const one = post(comment('c-1')).catch(() => null);
    const two = post(comment('c-2'));
    first.refuse();
    await Promise.all([one, two]);
    expect(mockPost.mock.calls[1][1]).toEqual(home);
  });

  it("leaves another file's post, and a post to a thread, to go at once", async () => {
    const first = held();
    mockPost.mockResolvedValue({ threadId: 'cos-3' });
    const one = post(comment('c-1', 'a-1'));
    await post(comment('c-2', 'a-2'));
    await post(comment('c-3', 'a-1'), { threadId: 't-1' });
    expect(mockPost.mock.calls.map((call) => call[1])).toEqual([home, home, { threadId: 't-1' }]);
    first.answer('cos-1');
    await one;
  });
});

describe('a post to a new thread across a sign-out', () => {
  it("is not the next account's: one that waited is not sent, and a new one starts its own", async () => {
    const first = held();
    mockPost.mockResolvedValue({ threadId: 'cos-next' });
    const one = post(comment('c-1', 'a-5'));
    const two = post(comment('c-2', 'a-5')).catch((error: unknown) => error);

    runAuthResets();
    await post(comment('c-3', 'a-5'));
    expect(mockPost.mock.calls[1][1]).toEqual(home);

    first.answer('cos-1');
    await one;
    expect(await two).toBeInstanceOf(CommentSendRefused);
    expect(mockPost).toHaveBeenCalledTimes(2);
  });
});

describe('a post to a new thread once another for the same file has landed', () => {
  /** A view showing where the file's comments go, whose reads answer when told to. */
  function destinationsShown(artifactId: string) {
    const reads: (() => void)[] = [];
    const observer = new QueryObserver(queryClient, {
      queryKey: queryKeys.artifacts.destinations(artifactId, null),
      queryFn: () => new Promise<string>((resolve) => { reads.push(() => resolve('read')); }),
    });
    const stop = observer.subscribe(() => {});
    return { answer: () => reads.splice(0).forEach((read) => read()), stop };
  }
  /** Past the query cache's own batching, which tells its listeners a task later. */
  const settled = async () => {
    for (let turn = 0; turn < 3; turn += 1) await new Promise((resolve) => setTimeout(resolve, 0));
  };

  it('still joins it until the view has read where the file now goes', async () => {
    const shown = destinationsShown('a-9');
    shown.answer();
    await settled();
    mockPost.mockResolvedValue({ threadId: 'cos-1' });

    await post(comment('c-1', 'a-9'));
    await settled();
    // The default on screen is still a new chat: a click now joins the one made.
    await post(comment('c-2', 'a-9'));
    expect(mockPost.mock.calls[1][1]).toEqual({ threadId: 'cos-1' });

    await settled();
    shown.answer();
    await settled();
    // Read again, the default names that chat, so a new chat asked for now is one the person picked.
    await post(comment('c-3', 'a-9'));
    expect(mockPost.mock.calls[2][1]).toEqual(home);
    shown.stop();
  });

  it('still joins it after a read that failed, since the view still offers a new chat', async () => {
    let fail = false;
    const observer = new QueryObserver(queryClient, {
      queryKey: queryKeys.artifacts.destinations('a-7', null),
      queryFn: () => (fail ? Promise.reject(new Error('down')) : Promise.resolve('read')),
      retry: false,
    });
    const stop = observer.subscribe(() => {});
    await settled();
    fail = true;
    mockPost.mockResolvedValue({ threadId: 'cos-1' });

    await post(comment('c-1', 'a-7'));
    await settled();
    expect(queryClient.getQueryState(queryKeys.artifacts.destinations('a-7', null))?.status).toBe('error');
    await post(comment('c-2', 'a-7'));
    expect(mockPost.mock.calls[1][1]).toEqual({ threadId: 'cos-1' });
    stop();
  });

  it('still joins it after a later post that joined it was refused', async () => {
    const shown = destinationsShown('a-6');
    shown.answer();
    await settled();
    mockPost.mockResolvedValueOnce({ threadId: 'cos-1' });
    await post(comment('c-1', 'a-6'));
    const second = held();
    const two = post(comment('c-2', 'a-6')).catch(() => null);
    await settled();
    second.refuse();
    await two;
    await settled();

    mockPost.mockResolvedValue({ threadId: 'cos-1' });
    await post(comment('c-3', 'a-6'));
    expect(mockPost.mock.calls[2][1]).toEqual({ threadId: 'cos-1' });
    shown.stop();
  });

  it('is its own post where no view shows where the file goes', async () => {
    mockPost.mockResolvedValue({ threadId: 'cos-1' });
    await post(comment('c-1', 'a-8'));
    await settled();
    await post(comment('c-2', 'a-8'));
    expect(mockPost.mock.calls[1][1]).toEqual(home);
  });
});

describe('leaving the page while a post is out', () => {
  it('asks until the server has it, and while either of two is out', async () => {
    const first = held();
    const second = held();
    expect(leaving()).toBe(false);

    const one = post(comment('c-1', 'a-1'));
    const two = post(comment('c-2', 'a-2')).catch(() => null);
    expect(leaving()).toBe(true);
    first.answer('cos-1');
    await one;
    expect(leaving()).toBe(true);
    second.refuse();
    await two;
    expect(leaving()).toBe(false);
  });
});
