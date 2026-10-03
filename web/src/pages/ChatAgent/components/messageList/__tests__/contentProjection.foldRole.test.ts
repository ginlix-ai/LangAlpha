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

const RETAINED = ['preview_url', 'chart_annotation', 'order_receipt', 'message_delivery'];

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
        toolCallResult: { artifact: { type: artifactType } },
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

  it.each(Object.keys(INLINE_ARTIFACT_MAP).filter((t) => !RETAINED.includes(t)))(
    'folds %s away with the rest of the working',
    (type) => {
      expect(roleOf(type)).toBe('process');
    },
  );
});
