/**
 * Scoring a text as an answer runs regexes over it, and the projection is
 * rebuilt on every streamed chunk. A turn still streaming shows every text, so
 * nothing there may score; a collapsed turn scores each text once.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { blockIsProcess, blockVisible, holdsAnswerText, projectMessageContent } from '../contentProjection';
import { answerScore } from '../answerText';
import type { MessageRecord } from '../types';

vi.mock('../answerText', async (importActual) => {
  const actual = await importActual<typeof import('../answerText')>();
  return { ...actual, answerScore: vi.fn(actual.answerScore) };
});

const scored = vi.mocked(answerScore);

function message(): MessageRecord {
  return {
    id: `a-${Math.random()}`,
    role: 'assistant',
    content: '',
    contentType: 'text',
    contentSegments: [
      { type: 'text', content: 'Checking the tape.', order: 0 },
      { type: 'tool_call', toolCallId: 't1', order: 1 },
      { type: 'text', content: '| A | B |\n|---|---|\n| 1 | 2 |', order: 2 },
      { type: 'tool_call', toolCallId: 't2', order: 3 },
      { type: 'text', content: 'Saved.', order: 4 },
    ],
    reasoningProcesses: {},
    toolCallProcesses: {
      t1: { toolName: 'WebSearch', toolCall: { args: {} }, isInProgress: false, isComplete: true, isFailed: false },
      t2: { toolName: 'TodoWrite', toolCall: { args: {} }, isInProgress: false, isComplete: true, isFailed: false },
    },
  } as unknown as MessageRecord;
}

describe('reading a turn\'s answer texts', () => {
  beforeEach(() => { scored.mockClear(); });

  it('keeps a reply\'s last text out of a fold panel when a call lands after it', () => {
    // The wrapper is decided while the text may still be typing: a new one
    // would remount it and skip it to its end.
    const projection = projectMessageContent(message());
    const last = projection.blocks.find((block) => block.key === projection.lastTextKey)!;
    expect(blockIsProcess(projection, last, true)).toBe(false);
    const withCallAfter = projectMessageContent({
      ...message(),
      contentSegments: [
        { type: 'text', content: 'Checking the tape.', order: 0 },
        { type: 'tool_call', toolCallId: 't1', order: 1 },
      ],
    } as unknown as MessageRecord);
    const typing = withCallAfter.blocks.find((block) => block.key === withCallAfter.lastTextKey)!;
    expect(blockIsProcess(withCallAfter, typing, true)).toBe(false);
  });

  it('scores nothing while the turn streams', () => {
    const projection = projectMessageContent(message());
    for (const block of projection.blocks) {
      blockIsProcess(projection, block, true);
      blockVisible(projection, block, 'live', true);
      blockVisible(projection, block, 'expanded', true);
    }
    expect(scored).not.toHaveBeenCalled();
  });

  it('scores each text a collapsed turn could fold, once, and reuses the reading', () => {
    const projection = projectMessageContent(message());
    const shown = () => projection.blocks.filter((block) => blockVisible(projection, block, 'collapsed', true));
    const texts = projection.blocks.filter((block) => block.type === 'text');

    expect(shown().map((block) => block.key)).toEqual(['text-2', 'text-4']);
    // The reply's closing text stands whatever it says, so only the others score.
    expect(scored).toHaveBeenCalledTimes(texts.length - 1);
    shown();
    holdsAnswerText(projection, true);
    expect(scored).toHaveBeenCalledTimes(texts.length - 1);
  });
});
