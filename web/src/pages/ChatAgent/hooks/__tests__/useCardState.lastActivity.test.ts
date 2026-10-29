/**
 * When a task card says it went quiet, which the tray ages it by: the live
 * write that settles it, at the time the write carries or now. A card opened
 * already settled, or refreshed from history, reports a settle from before
 * and must not restart the task's clock.
 */
import { afterEach, describe, it, expect, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useCardState } from '../useCardState';

const AGENT_ID = 'task:Qm3xLp';
const CARD_ID = `subagent-${AGENT_ID}`;
const NOW = 1_700_000_500_000;

function setup() {
  vi.useFakeTimers({ now: NOW, toFake: ['Date'] });
  const { result } = renderHook(() => useCardState());
  const update = (data: Parameters<typeof result.current.updateSubagentCard>[1]) =>
    act(() => result.current.updateSubagentCard(AGENT_ID, { agentId: AGENT_ID, taskId: AGENT_ID, ...data }));
  const stamp = () => result.current.cards[CARD_ID]?.subagentData?.lastActivityMs;
  return { update, stamp };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('useCardState — last activity', () => {
  it('stamps now when a live write settles a running card', () => {
    const { update, stamp } = setup();
    update({ status: 'active', isActive: true, messages: [] });
    expect(stamp()).toBeUndefined();
    update({ status: 'completed', isActive: false });
    expect(stamp()).toBe(NOW);
  });

  it('keeps the time a settling write carries', () => {
    const { update, stamp } = setup();
    update({ status: 'active', isActive: true, messages: [] });
    update({ status: 'error', isActive: false, messages: [], lastActivityMs: 1_700_000_000_000 });
    expect(stamp()).toBe(1_700_000_000_000);
  });

  it('leaves a card opened already settled without a stamp', () => {
    const { update, stamp } = setup();
    update({ status: 'completed', isActive: false, description: 'Pull filings' });
    expect(stamp()).toBeUndefined();
  });

  it('keeps the time a close carries on the card it creates', () => {
    const { update, stamp } = setup();
    update({ status: 'completed', isActive: false, lastActivityMs: 1_700_000_000_000 });
    expect(stamp()).toBe(1_700_000_000_000);
  });

  it('moves a settled card\'s time only when a write settles it again', () => {
    const { update, stamp } = setup();
    update({ status: 'active', isActive: true, messages: [] });
    update({ status: 'completed', isActive: false });
    // A second close for the same run carries its own time.
    update({ status: 'completed', isActive: false, lastActivityMs: NOW + 60_000 });
    expect(stamp()).toBe(NOW);
  });
});
