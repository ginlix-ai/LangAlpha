/**
 * A re-raised credit pause has to land back on the card that renders it.
 *
 * The resume settles the card the moment the backend opens a run, because
 * admission is the only signal the client gets — a run opens whether or not the
 * graph goes on to consume the pause. When it does not, the same interrupt id
 * streams again into a fresh `assistant-hitl-*` bubble, and the duplicate-card
 * suppression means that bubble has no segment of its own. Writing the pause
 * there leaves the visible card on `resumed` with nothing left to answer it,
 * and `status` is what history replays, so the dead end survives a reload.
 */
import { describe, it, expect, vi } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

import { projectLiveInterrupt } from '../fromLiveEvent';
import type { StreamRuntime } from '../../runtime';
import type { MessageRecord, SSEEvent, StreamProcessorRefs } from '../../types';

type Ref<T> = { current: T };
const ref = <T,>(current: T): Ref<T> => ({ current });

const PAUSE_ID = 'int-credit-1';

function pauseEvent(): SSEEvent {
  return {
    type: 'interrupt',
    interrupt_id: PAUSE_ID,
    action_requests: [{ type: 'credit_pause', message: 'Out of credits' }],
  } as unknown as SSEEvent;
}

function build(messages: MessageRecord[]) {
  let current = messages;
  const rt = {
    setMessages: ((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
      current = updater(current);
    }) as StreamRuntime['setMessages'],
    setPendingInterrupt: vi.fn(),
    pendingInterruptIdsRef: ref(new Set<string>()),
    renderedInterruptIdsRef: ref(new Set<string>()),
  } as unknown as StreamRuntime;
  const refs = {
    contentOrderCounterRef: ref(0),
  } as unknown as StreamProcessorRefs;
  return { rt, refs, read: () => current };
}

const bubble = (id: string): MessageRecord =>
  ({ id, role: 'assistant', content: '', contentSegments: [] }) as unknown as MessageRecord;

const cardOn = (m: MessageRecord) =>
  (m as AssistantMessage).creditPauses?.[PAUSE_ID];

describe('a credit pause that is raised twice', () => {
  it('renders one card, and re-raising puts that card back to pending', () => {
    const { rt, refs, read } = build([bubble('a-1')]);

    projectLiveInterrupt(rt, pauseEvent(), 'a-1', refs);
    expect(cardOn(read()[0])?.status).toBe('pending');
    expect((read()[0] as AssistantMessage).contentSegments).toHaveLength(1);

    // The resume: admission opened a run, so the card reads resumed.
    let msgs = read();
    msgs = msgs.map((m) => ({
      ...(m as AssistantMessage),
      creditPauses: { [PAUSE_ID]: { ...cardOn(m)!, status: 'resumed' } },
    })) as unknown as MessageRecord[];
    const second = build([...msgs, bubble('a-2')]);
    second.rt.renderedInterruptIdsRef.current.add(PAUSE_ID);

    projectLiveInterrupt(second.rt, pauseEvent(), 'a-2', second.refs);

    const [first, fresh] = second.read();
    expect(cardOn(first)?.status).toBe('pending');
    // No second card: one segment on the original bubble, none on the new one.
    expect((first as AssistantMessage).contentSegments).toHaveLength(1);
    expect((fresh as AssistantMessage).contentSegments || []).toHaveLength(0);
    // ...and nothing unrenderable left behind on the fresh bubble.
    expect(cardOn(fresh)).toBeUndefined();
    // The pause is tracked again, so a later click is not dropped.
    expect(second.rt.pendingInterruptIdsRef.current.has(PAUSE_ID)).toBe(true);
  });
});
