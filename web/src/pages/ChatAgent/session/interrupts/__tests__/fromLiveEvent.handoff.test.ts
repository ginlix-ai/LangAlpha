// @vitest-environment node
/**
 * The approval card reads a hand-off's model and effort off its proposal, so
 * the live interrupt has to carry both there, or a costly level is approved
 * unseen.
 */
import { describe, it, expect, vi } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

import { projectLiveInterrupt } from '../fromLiveEvent';
import { createAnswerBoard } from '../answerBoard';
import { createPauseSlot } from '../pauseSlot';
import type { StreamRuntime } from '../../runtime';
import type { SSEEvent, StreamProcessorRefs } from '../../types';

const ref = <T,>(current: T) => ({ current });

function build(messages: ChatMessage[]) {
  let current = messages;
  const setMessages = ((updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    current = updater(current);
  }) as StreamRuntime['setMessages'];
  const rt = {
    setMessages,
    pause: createPauseSlot(
      vi.fn(),
      createAnswerBoard({ pendingIds: ref(new Set<string>()), sessionEpoch: ref(0), setMessages }),
    ),
    currentRunIdRef: ref<string | null>('run-1'),
    renderedInterruptIdsRef: ref(new Set<string>()),
  } as unknown as StreamRuntime;
  const refs = { contentOrderCounterRef: ref(0) } as unknown as StreamProcessorRefs;
  return { rt, refs, read: () => current };
}

describe('a hand-off asking for approval', () => {
  it('carries the model and effort it names onto its card', () => {
    const { rt, refs, read } = build([
      { id: 'a-1', role: 'assistant', content: '', contentSegments: [] } as unknown as ChatMessage,
    ]);
    const event = {
      type: 'interrupt',
      interrupt_id: 'int-1',
      action_requests: [{ type: 'ptc_agent', question: 'Q', model: 'gpt-6.1-terra', effort: 'max' }],
    } as unknown as SSEEvent;

    projectLiveInterrupt(rt, event, 'a-1', refs);

    const card = (read()[0] as AssistantMessage).ptcAgentProposals?.['int-1'];
    expect(card).toMatchObject({ status: 'pending', model: 'gpt-6.1-terra', effort: 'max' });
  });
});
