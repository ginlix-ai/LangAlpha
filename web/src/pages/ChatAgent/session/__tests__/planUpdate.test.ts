// @vitest-environment node
/**
 * A `plan_update` reads into steps, and each update is one snapshot on its
 * bubble: a frame delivered twice lands on the same order and stays one.
 */
import { describe, expect, it } from 'vitest';
import { applyPlanSnapshot, readPlanSteps } from '../planUpdate';
import type { MessageRecord } from '../../hooks/utils/types';

describe('readPlanSteps', () => {
  it('reads a plan_update into steps', () => {
    expect(readPlanSteps('plan_update', { steps: [{ title: 'Pull filings', status: 'in_progress' }, { title: 'Chart' }] })).toEqual([
      { title: 'Pull filings', status: 'in_progress' },
      { title: 'Chart', status: 'pending' },
    ]);
  });

  it('narrows a status it does not know to pending', () => {
    expect(readPlanSteps('plan_update', { steps: [{ title: 'x', status: 'cancelled' }] })?.[0].status).toBe('pending');
  });

  it('is null for any other artifact', () => {
    expect(readPlanSteps('html_widget', { steps: [] })).toBeNull();
  });
});

describe('applyPlanSnapshot', () => {
  const steps = [{ title: 'x', status: 'pending' as const }];

  it('adds the snapshot to its bubble alone, newest last, leaving the messages it was given', () => {
    let messages: MessageRecord[] = [
      { id: 'a1', planSnapshots: [{ order: 2, steps: [] }] },
      { id: 'a2' },
    ];
    const before = messages;
    applyPlanSnapshot((update) => { messages = update(messages); }, 'a1', steps, 7);
    expect(messages[0].planSnapshots).toEqual([{ order: 2, steps: [] }, { order: 7, steps }]);
    expect(messages[1]).toBe(before[1]);
    expect(before[0].planSnapshots).toHaveLength(1);
  });

  it('replaces a snapshot delivered again at its order', () => {
    const done = [{ title: 'x', status: 'completed' as const }];
    let messages: MessageRecord[] = [{ id: 'a1', planSnapshots: [{ order: 2, steps: [] }, { order: 7, steps }] }];
    applyPlanSnapshot((update) => { messages = update(messages); }, 'a1', done, 7);
    expect(messages[0].planSnapshots).toEqual([{ order: 2, steps: [] }, { order: 7, steps: done }]);
  });
});
