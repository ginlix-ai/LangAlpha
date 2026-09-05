import { describe, expect, it } from 'vitest';
import { buildEntries, plainText } from '../minimapEntries';
import type { MessageRecord } from '../messageList/types';

const user = (id: string, content: unknown, extra: Record<string, unknown> = {}): MessageRecord =>
  ({ id, role: 'user', content, ...extra }) as MessageRecord;
const assistant = (id: string, content: string, extra: Record<string, unknown> = {}): MessageRecord =>
  ({ id, role: 'assistant', content, ...extra }) as MessageRecord;

describe('buildEntries', () => {
  it('folds every assistant bubble after a prompt into that turn', () => {
    const entries = buildEntries(
      [user('u1', 'First'), assistant('a1', 'Part one.'), assistant('a2', 'Part two.'), user('u2', 'Second'), assistant('a3', 'Reply.')],
      false,
    );
    expect(entries.map((e) => [e.id, e.reply])).toEqual([
      ['u1', 'Part one. Part two.'],
      ['u2', 'Reply.'],
    ]);
  });

  it('marks only the newest text-free turn pending, and only while a turn is in flight', () => {
    const messages = [user('u1', 'Old'), assistant('a1', ''), user('u2', 'New')];
    expect(buildEntries(messages, true).map((e) => e.pending)).toEqual([false, true]);
    // A failed send leaves the same shape; it must not read as loading.
    expect(buildEntries(messages, false).map((e) => e.pending)).toEqual([false, false]);
    // A reply with text is never pending, even mid-stream.
    expect(buildEntries([user('u1', 'Q'), assistant('a1', 'Tok', { isStreaming: true })], true)[0].pending).toBe(false);
  });

  it('reads text segments in transcript order, falling back to flat content', () => {
    const segmented = assistant('a1', 'fallback', {
      contentSegments: [
        { type: 'text', order: 2, content: 'second' },
        { type: 'tool_call', order: 1 },
        { type: 'text', order: 0, content: 'first ' },
      ],
    });
    expect(buildEntries([user('u1', 'Q'), segmented], false)[0].reply).toBe('first second');
    expect(buildEntries([user('u1', 'Q'), assistant('a1', 'flat')], false)[0].reply).toBe('flat');
  });

  it('names an attachment-only prompt by its files and survives non-string content', () => {
    const entries = buildEntries(
      [
        user('u1', '', { attachments: [{ name: 'q3.pdf', type: 'application/pdf' }] }),
        assistant('a1', 'Reply.'),
        user('u2', ['not', 'a', 'string']),
      ],
      false,
    );
    expect(entries.map((e) => e.prompt)).toEqual(['q3.pdf', '']);
  });

  it('names an attachment-only prompt from either attachment shape', () => {
    // A live send stores the upload-time shape as-is, where the name is on the
    // File; history reloads it into the flat shape.
    const entries = buildEntries(
      [
        user('u1', '', { attachments: [{ file: { name: 'live.csv' }, dataUrl: 'x', type: 'text/csv' }] }),
        assistant('a1', 'Reply.'),
        user('u2', '', { attachments: [{ name: 'history.pdf', type: 'application/pdf' }] }),
      ],
      false,
    );
    expect(entries.map((e) => e.prompt)).toEqual(['live.csv', 'history.pdf']);
  });

  it('does not let a failed steering send absorb the next prompt', () => {
    // A failed steer keeps its bubble, is stamped queueError and never gets an
    // assistant, so merging it would hand the fresh prompt the failed id.
    const entries = buildEntries(
      [
        user('u1', 'Q'),
        assistant('a1', 'Reply.'),
        user('s1', 'steer that failed', { queueError: 'Failed to send steering' }),
        user('u2', 'fresh prompt'),
        assistant('a2', 'Fresh reply.'),
      ],
      false,
    );
    expect(entries.map((e) => [e.id, e.prompt, e.reply])).toEqual([
      ['u1', 'Q', 'Reply.'],
      ['s1', 'steer that failed', ''],
      ['u2', 'fresh prompt', 'Fresh reply.'],
    ]);
  });

  it('splits a failed steer out of the batch it was queued alongside', () => {
    // The continuation answers only what was delivered, so labelling it with an
    // instruction the agent never received would misreport what it was asked.
    const entries = buildEntries(
      [
        user('u1', 'Analyse AMD'),
        assistant('a1', 'Working.'),
        user('s1', 'use Q3 numbers'),
        user('s2', 'and skip the charts', { queueError: 'Failed to send steering' }),
        assistant('a2', 'Continuation.'),
      ],
      false,
    );
    expect(entries.map((e) => [e.id, e.prompt, e.reply])).toEqual([
      ['u1', 'Analyse AMD', 'Working.'],
      ['s1', 'use Q3 numbers', 'Continuation.'],
      ['s2', 'and skip the charts', ''],
    ]);
  });

  it('never shows a failed steer as pending, even while the parent turn runs', () => {
    const entries = buildEntries(
      [
        user('u1', 'Analyse AMD'),
        assistant('a1', 'Working.'),
        user('s1', 'use Q3 numbers', { queueError: 'Failed to send steering' }),
      ],
      true,
    );
    expect(entries.map((e) => [e.id, e.pending])).toEqual([
      ['u1', false],
      ['s1', false],
    ]);
    // The turn that is genuinely still running keeps its skeleton.
    const noReplyYet = buildEntries(
      [user('u1', 'Analyse AMD'), user('s1', 'use Q3 numbers', { queueError: 'Failed to send steering' })],
      true,
    );
    expect(noReplyYet.map((e) => [e.id, e.pending])).toEqual([
      ['u1', true],
      ['s1', false],
    ]);
  });

  it('gives a drained steering batch one tick, anchored and labelled by the whole batch', () => {
    // Everything queued during one tool call is delivered together and answered
    // by a single continuation, so the prompts share a tick rather than leaving
    // all but the last with an empty card.
    const entries = buildEntries(
      [
        user('u1', 'Analyse AMD'),
        assistant('a1', 'Working.'),
        user('s1', 'use Q3 numbers'),
        user('s2', 'and skip the charts'),
        assistant('a2', 'Continuation.'),
      ],
      false,
    );
    expect(entries.map((e) => [e.id, e.prompt, e.reply])).toEqual([
      ['u1', 'Analyse AMD', 'Working.'],
      ['s1', 'use Q3 numbers · and skip the charts', 'Continuation.'],
    ]);
  });

  it('keeps a lone steer on its own tick, and leaves an unanswered batch pending as one', () => {
    // A single steer has an assistant bubble before it, so it still splits.
    const lone = buildEntries(
      [user('u1', 'Q'), assistant('a1', 'Part.'), user('s1', 'actually stop'), assistant('a2', 'OK.')],
      false,
    );
    expect(lone.map((e) => e.id)).toEqual(['u1', 's1']);
    // Mid-flight, a batch with no continuation yet is one pending entry.
    const inFlight = buildEntries(
      [user('u1', 'Q'), assistant('a1', 'Part.'), user('s1', 'one'), user('s2', 'two')],
      true,
    );
    expect(inFlight.map((e) => [e.id, e.pending])).toEqual([
      ['u1', false],
      ['s1', true],
    ]);
  });
});

describe('plainText', () => {
  it('prefers prose over fenced code, but keeps a code-only message readable', () => {
    expect(plainText('Run this:\n```py\nprint(1)\n```\nthen check.')).toBe('Run this: then check.');
    expect(plainText('```py\nprint(1)\n```')).toBe('print(1)');
  });

  it('strips markdown structure to one line', () => {
    expect(plainText('# Title\n\n- **bold** item\n> quote [link](http://x)\n| a | b |')).toBe('Title bold item quote link a b');
  });

  it('unwraps matched delimiters but leaves literal punctuation alone', () => {
    expect(plainText('*em* _also_ ~~gone~~ `code`')).toBe('em also gone code');
    // Research prose the old character-class strip silently rewrote.
    expect(plainText('BRK_B is A * B, about ~5% of AAPL_US')).toBe('BRK_B is A * B, about ~5% of AAPL_US');
  });
});
