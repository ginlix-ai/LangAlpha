import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('../../../utils/api', () => ({
  getSubagentTaskHistory: vi.fn(),
  getSubagentTaskStatus: vi.fn(),
}));

import { getSubagentTaskHistory } from '../../../utils/api';
import { hydrateTaskTranscript } from '../hydrateTaskTranscript';
import { createSubagentHistoryStore, type SubagentHistorySnapshot } from '../historyStore';
import { ZERO_USAGE } from '../../../utils/tokenUsage';

const mockTaskHistory = getSubagentTaskHistory as Mock;

describe('hydrateTaskTranscript', () => {
  beforeEach(() => vi.clearAllMocks());

  it('lands the transcript with its owner in one snapshot, leaving the ghost as it was', async () => {
    mockTaskHistory.mockResolvedValue({
      items: [
        { event: 'message_chunk', data: { agent: 'task:ch1', role: 'assistant', content_type: 'text', content: 'AAPL brief' } },
      ],
    });
    const published: SubagentHistorySnapshot[] = [];
    const subagentHistory = createSubagentHistoryStore((s) => published.push(s));
    // A ghost lane: the owner is known from the workflow run, the transcript is not.
    subagentHistory.putEntries({
      'task:ch1': {
        taskId: 'task:ch1', description: 'AAPL', prompt: '', type: 'research', messages: [],
        status: 'completed', ownerTaskId: 'task:wf1', toolCalls: 0, tokenUsage: ZERO_USAGE, currentTool: '',
      },
    });
    const ghost = subagentHistory.get();
    const ghostEntry = ghost.entries['task:ch1'];
    const rt = { t: (k: string) => k, subagentHistory, subagentStateRefsRef: { current: {} } };

    const view = await hydrateTaskTranscript(rt, 'thread-1', 'task:ch1');

    const landed = subagentHistory.get().entries['task:ch1'];
    expect(view).toEqual({ ...landed, agentId: 'task:ch1' });
    expect(landed.messages.length).toBeGreaterThan(0);
    expect(landed.ownerTaskId).toBe('task:wf1');
    // The ghost snapshot and the hydrated one; neither was edited after it went out.
    expect(published).toHaveLength(2);
    expect(ghost.entries['task:ch1']).toBe(ghostEntry);
    expect(ghostEntry.messages).toHaveLength(0);
    expect(published[1].entries['task:ch1']).toBe(landed);
  });
});
