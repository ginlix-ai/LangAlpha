/**
 * A resume retracts why the task stopped.
 *
 * Card state is keyed by task, and the resume's card shares that key with the
 * one that stopped — so a stale reason would render the credit-stop notice on a
 * task actively working. The retraction rides the reactivation write as an
 * explicit `undefined`, which only clears because the merge is a spread; a
 * refactor to field-by-field copying would silently drop it, which is what this
 * pins.
 */
import { describe, it, expect } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useCardState } from '../useCardState';

const AGENT_ID = 'task:F092rQ';
const CARD_ID = `subagent-${AGENT_ID}`;

/** The card as the credit gate leaves it: settled, carrying the denial. */
function seedStoppedCard(update: ReturnType<typeof useCardState>['updateSubagentCard']): void {
  update(AGENT_ID, {
    agentId: AGENT_ID,
    taskId: AGENT_ID,
    type: 'equity-analyst',
    status: 'cancelled',
    isActive: false,
    messages: [{ role: 'assistant', content: 'partial work' }],
    error: 'Monthly credit limit reached (50/50 credits)',
    errorType: 'credit_stop',
  });
}

/** The reactivation write the `action === 'resume'` branch makes. */
function resumeWrite(update: ReturnType<typeof useCardState>['updateSubagentCard']): void {
  update(AGENT_ID, {
    agentId: AGENT_ID,
    taskId: AGENT_ID,
    type: 'equity-analyst',
    status: 'active',
    isActive: true,
    error: undefined,
    errorType: undefined,
  });
}

describe('useCardState — a resume clears the stop reason', () => {
  it('drops the reason and its type when the task goes live again', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedStoppedCard(result.current.updateSubagentCard));
    expect(result.current.cards[CARD_ID].subagentData?.error).toBe(
      'Monthly credit limit reached (50/50 credits)',
    );

    act(() => resumeWrite(result.current.updateSubagentCard));
    const sd = result.current.cards[CARD_ID].subagentData;
    expect(sd?.error).toBeUndefined();
    expect(sd?.errorType).toBeUndefined();
    expect(sd?.isActive).toBe(true);
  });

  it('keeps the work the stopped run produced, so the resume appends rather than restarts', () => {
    const { result } = renderHook(() => useCardState());

    act(() => seedStoppedCard(result.current.updateSubagentCard));
    act(() => resumeWrite(result.current.updateSubagentCard));

    expect(result.current.cards[CARD_ID].subagentData?.messages).toHaveLength(1);
  });
});
