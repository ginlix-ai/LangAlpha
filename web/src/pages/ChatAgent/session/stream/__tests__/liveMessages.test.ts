/**
 * The live transcript's writes. Streamed chunks wait for a frame, and any other
 * write made meanwhile must neither overtake them nor be overtaken by them.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mockFrames, runFrame, settleFrames } from '@/test/frames';
import { createLiveTranscript } from '../liveMessages';

beforeEach(mockFrames);

afterEach(() => {
  settleFrames();
  vi.restoreAllMocks();
});

function setup() {
  const committed: string[][] = [];
  const transcript = createLiveTranscript<string[]>([], (value) => committed.push(value));
  const push = (s: string) => (prev: string[]) => [...prev, s];
  return { transcript, push, committed };
}

describe('createLiveTranscript', () => {
  it('lands a frame of chunks in the store alone', () => {
    const { transcript, push, committed } = setup();
    transcript.queue(push('a'));
    transcript.queue(push('b'));
    runFrame();
    expect(transcript.get()).toEqual(['a', 'b']);
    expect(committed).toEqual([]);
  });

  it('computes an update on the chunks waiting before it', () => {
    const { transcript, push, committed } = setup();
    transcript.queue(push('chunk'));
    transcript.write(push('write'));
    expect(committed).toEqual([['chunk', 'write']]);
    runFrame();
    expect(transcript.get()).toEqual(['chunk', 'write']);
  });

  it('drops the waiting chunks when a value replaces the transcript', () => {
    // A history load or a thread switch: the old turn's text must not land on it.
    const { transcript, push, committed } = setup();
    transcript.queue(push('old turn'));
    transcript.write(['history']);
    runFrame();
    expect(transcript.get()).toEqual(['history']);
    expect(committed).toEqual([['history']]);
  });
});
