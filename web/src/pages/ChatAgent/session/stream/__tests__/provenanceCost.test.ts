// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { ProvenanceEvent } from '@/types/sse';
import type { ProvenanceRecord } from '@/types/chat';
import type { MessageRecord } from '../../../hooks/utils/types';
import { handleProvenance } from '../mainEventHandlers';

function paidPage(recordId: string, cost: number | null): ProvenanceEvent {
  return {
    event: 'provenance',
    record_id: recordId,
    timestamp: '2026-11-16T00:00:00Z',
    source_type: 'web_fetch',
    identifier: 'https://example.com/report',
    tool_call_id: 'tc-run',
    result_sha256: 'a'.repeat(64),
    cost_credits: cost,
  } as ProvenanceEvent;
}

function listed(events: ProvenanceEvent[]): ProvenanceRecord[] {
  let messages: MessageRecord[] = [{ id: 'a1', role: 'assistant' } as MessageRecord];
  const setMessages = (update: (prev: MessageRecord[]) => MessageRecord[]) => {
    messages = update(messages);
  };
  for (const event of events) {
    handleProvenance({ assistantMessageId: 'a1', event, setMessages: setMessages as never });
  }
  return Object.values(messages[0].provenanceRecords as Record<string, ProvenanceRecord>);
}

describe('handleProvenance cost', () => {
  it('lists one code run calling the same page twice at both calls\' cost', () => {
    const records = listed([paidPage('rec-1', 1), paidPage('rec-2', 1)]);

    expect(records).toHaveLength(1);
    expect(records[0].cost_credits).toBe(2);
  });

  it('counts a redelivered event once', () => {
    const records = listed([paidPage('rec-1', 1), paidPage('rec-2', 1), paidPage('rec-2', 1), paidPage('rec-1', 1)]);

    expect(records[0].cost_credits).toBe(2);
  });

  it('leaves an unpriced source unpriced', () => {
    expect(listed([paidPage('rec-1', null), paidPage('rec-2', null)])[0].cost_credits).toBeNull();
  });
});
