import { describe, it, expect } from 'vitest';
import { summarizeCompletedItems } from '../activitySummary';
import type { ActivityItem } from '../activityTypes';

const t = (key: string, opts?: Record<string, unknown>) =>
  opts && 'count' in opts ? `${key}:${String(opts.count)}` : key;

function call(toolName: string, id: string, failed = false): ActivityItem {
  return {
    type: 'tool_call',
    id,
    toolCallId: id,
    toolName,
    toolCall: { args: {} },
    isComplete: true,
    isFailed: failed,
    _liveState: failed ? 'failed' : 'completed',
  } as ActivityItem;
}

describe('summarizeCompletedItems', () => {
  // After the failures, "and more" read as more calls having failed.
  it('ends the capped counts with "and more", before the failure count', () => {
    const items = [
      call('ExecuteCode', 'c-1'),
      call('WebSearch', 'w-1', true),
      call('Glob', 'g-1'),
      call('Read', 'r-1'),
    ];

    const label = summarizeCompletedItems(items, t, { countFailures: true }) ?? '';

    expect(label).toMatch(/ toolArtifact\.andMore · toolArtifact\.categoryCount\.failed:1$/);
  });
});
