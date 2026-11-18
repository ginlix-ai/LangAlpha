/**
 * Which inline cards survive a collapsed turn is a product decision, not a
 * consequence of `INLINE_ARTIFACT_MAP`. The two lists sit in different files
 * and a new tool adds to the map, so without this the split drifts silently:
 * a card would start persisting, or stop, because someone registered a
 * renderer. The rule being pinned is that outcomes stay and lookups fold.
 */
import { describe, it, expect } from 'vitest';
import { projectMessageContent } from '../contentProjection';
import { INLINE_ARTIFACT_MAP } from '../../charts/InlineArtifactCards';
import type { MessageRecord } from '../types';

const RETAINED = ['preview_url', 'order_receipt', 'message_delivery', 'artifact_comment'];

/** What a type's card needs to draw at all: a reply names its comment. */
const FIELDS: Record<string, Record<string, unknown>> = {
  artifact_comment: { artifact_id: 'art-1', comment_id: 'cmt-1', number: 1 },
};

/** A settled turn whose only non-text block is one inline artifact card. */
function roleOf(artifactType: string): string | undefined {
  const message = {
    id: 'a0',
    role: 'assistant',
    content: 'Done.',
    contentType: 'text',
    contentSegments: [
      { type: 'tool_call', toolCallId: 't1', order: 0 },
      { type: 'text', content: 'Done.', order: 1 },
    ],
    reasoningProcesses: {},
    toolCallProcesses: {
      t1: {
        toolName: 'some_tool',
        toolCall: { id: 't1', name: 'some_tool', args: {} },
        toolCallResult: { artifact: { type: artifactType, ...FIELDS[artifactType] } },
        order: 0,
      },
    },
  } as unknown as MessageRecord;

  const projection = projectMessageContent(message);
  const block = projection.blocks.find((b) => b.type === 'compact_artifact');
  expect(block, `no compact_artifact block for ${artifactType}`).toBeDefined();
  return projection.roles.get(block!.key);
}

describe('what survives a collapsed turn', () => {
  it.each(RETAINED)('keeps %s, which is an outcome rather than a lookup', (type) => {
    expect(roleOf(type)).toBe('retain');
  });

  // Each notice's part is said where it is drawn (`NOTICES`); this pins the split.
  it.each([
    ['a reply billed to usage credits, which is what it cost', 'retain', { kind: 'planBilled', window: 'five_hour', until: '2026-11-06T20:40:00Z' }],
    ['a turn that opened on another model\'s brief, which says what it was written from', 'retain', { kind: 'handoff', from: 'deepseek-flash', to: 'claude-sonnet-5-5-code' }],
    ['a turn that waited for a Claude Code CLI, which is part of how long it took', 'retain', { kind: 'cliWait', waitId: 'w1', computerId: 'c1', name: 'Mac', cap: 2, settable: true, max: 8, waitedS: 42 }],
    ['a model switch, which folds with the working', 'process', { kind: 'fallback', from: 'model-alpha', to: 'model-beta' }],
  ])('the notice of %s: %s', (_why, role, notice) => {
    const message = {
      id: 'a0',
      role: 'assistant',
      content: 'Done.',
      contentType: 'text',
      contentSegments: [
        { type: 'notification', order: 1, notice },
        { type: 'text', content: 'Done.', order: 2 },
      ],
      reasoningProcesses: {},
      toolCallProcesses: {},
    } as unknown as MessageRecord;
    const projection = projectMessageContent(message);
    const block = projection.blocks.find((b) => b.type === 'notification');
    expect(projection.roles.get(block!.key)).toBe(role);
  });

  it.each(Object.keys(INLINE_ARTIFACT_MAP).filter((t) => !RETAINED.includes(t)))(
    'folds %s away with the rest of the working',
    (type) => {
      expect(roleOf(type)).toBe('process');
    },
  );
});
