// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { AutomationExecution, DeliveryAttempt } from '@/types/automation';
import { DELIVERY_SETTLE_MS, isDeliverySettling } from '../polling';

const ENDED = '2026-10-01T13:02:00Z';
const SOON = Date.parse(ENDED) + 10_000;

const LANDED: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo', via: 'agent', error: null };
const UNCONFIRMED: DeliveryAttempt = {
  method: 'discord',
  success: false,
  address: 'discord:@me',
  name: null,
  via: null,
  error: "Delivery couldn't be confirmed.",
};

function run(over: Partial<AutomationExecution> = {}): AutomationExecution {
  return {
    automation_execution_id: 'exec-1',
    automation_id: 'auto-1',
    status: 'completed',
    conversation_thread_id: null,
    scheduled_at: '2026-10-01T13:00:00Z',
    started_at: '2026-10-01T13:00:01Z',
    completed_at: ENDED,
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: null,
    created_at: '2026-10-01T13:00:00Z',
    excerpt: null,
    dismissed_at: null,
    ...over,
  };
}

describe('isDeliverySettling', () => {
  it('waits for the first record of a run that delivers, and for none of one that does not', () => {
    expect(isDeliverySettling(run(), true, SOON)).toBe(true);
    expect(isDeliverySettling(run(), false, SOON)).toBe(false);
  });

  it('waits while a row reads unconfirmed, failed runs included', () => {
    expect(isDeliverySettling(run({ delivery_result: [LANDED, UNCONFIRMED] }), true, SOON)).toBe(true);
    expect(isDeliverySettling(run({ status: 'failed', delivery_result: [UNCONFIRMED] }), true, SOON)).toBe(true);
  });

  it('takes a record whose every row landed as final', () => {
    expect(isDeliverySettling(run({ delivery_result: [LANDED] }), true, SOON)).toBe(false);
    expect(isDeliverySettling(run({ delivery_result: [] }), true, SOON)).toBe(false);
  });

  it('stops waiting once the asks are over, whatever the record says', () => {
    const late = Date.parse(ENDED) + DELIVERY_SETTLE_MS;
    expect(isDeliverySettling(run({ delivery_result: [UNCONFIRMED] }), true, late - 1)).toBe(true);
    expect(isDeliverySettling(run({ delivery_result: [UNCONFIRMED] }), true, late)).toBe(false);
    expect(isDeliverySettling(run(), true, late)).toBe(false);
  });

  it('leaves a run that has not ended to its own liveness', () => {
    expect(isDeliverySettling(run({ status: 'running', completed_at: null }), true, SOON)).toBe(false);
    expect(isDeliverySettling(run({ status: 'waiting', completed_at: null }), true, SOON)).toBe(false);
  });

  it('settles nothing on an end it cannot read', () => {
    expect(isDeliverySettling(run({ completed_at: 'not a time' }), true, SOON)).toBe(false);
  });
});
