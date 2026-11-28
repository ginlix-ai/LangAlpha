import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockSend = vi.hoisted(() => vi.fn());
vi.mock('../messages', () => ({ sendChatMessageStream: mockSend }));

import { CommentSendRefused, postArtifactComments } from '../artifacts';

type Options = { onEvent: (event: Record<string, unknown>) => void; onRunIdResolved: (runId: string, threadId: string | null) => void };

const answering = (frame: Record<string, unknown>) => async (_m: string, _ws: string, _t: string, options: Options) => {
  options.onRunIdResolved('run-1', 'th-1');
  options.onEvent(frame);
  return {};
};

const post = () => postArtifactComments(['c-1'], { threadId: 'th-1' }, { reading: 'file', locale: 'en-US', timezone: 'UTC' });

describe('postArtifactComments: what a failure says about the comments', () => {
  beforeEach(() => {
    mockSend.mockReset();
  });

  it('counts them taken once a run holds them, whose Retry carries them on', async () => {
    mockSend.mockImplementation(answering({ event: 'error', recovery: 'retry', run_id: 'run-1', error: 'No computer' }));
    await expect(post()).resolves.toEqual({ threadId: 'th-1' });
  });

  it('says not sent when the failure asks for the send again, and the comments wait', async () => {
    mockSend.mockImplementation(answering({ event: 'error', recovery: 'resend', error: 'No computer' }));
    await expect(post()).rejects.toBeInstanceOf(CommentSendRefused);
  });
});

describe('postArtifactComments: the reason a refusal carries', () => {
  beforeEach(() => {
    mockSend.mockReset();
  });

  it("keeps the words the transport says are the reader's, and none for its own line", async () => {
    mockSend.mockRejectedValueOnce(Object.assign(new Error('Out of turns'), { status: 402, reason: 'Out of turns' }));
    await expect(post()).rejects.toMatchObject({ reason: 'Out of turns' });
    mockSend.mockRejectedValueOnce(Object.assign(new Error('HTTP error! status: 502'), { status: 502 }));
    await expect(post()).rejects.toMatchObject({ reason: null, message: 'The comments were not sent.' });
  });

  it("keeps a refusal frame's words with its code", async () => {
    mockSend.mockImplementation(answering({ event: 'error', code: 'busy', error: 'A reply is still running.' }));
    await expect(post()).rejects.toMatchObject({ reason: 'A reply is still running.', code: 'busy' });
  });
});
