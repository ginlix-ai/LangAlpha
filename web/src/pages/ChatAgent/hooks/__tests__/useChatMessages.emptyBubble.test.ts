/**
 * HITL-resume bubble turn-integrity on empty finalize.
 *
 * A HITL resume opens a fresh `assistant-hitl-*` bubble that can settle with
 * nothing on it (content landed elsewhere, or the turn re-interrupted and the
 * re-raise was deduped by interrupt_id). That bubble must STAY in state: a
 * resume is a backend turn, and edit/regenerate map UI position → turn_index
 * by counting non-steering assistant bubbles — pruning it would silently
 * re-target later regenerates at the wrong checkpoint. The visual orphan
 * (bare avatar + action row) is suppressed at render time instead — see
 * MessageList.orphanBubble.test.tsx.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import {
  fetchThreadTurns,
  getWorkflowStatus,
  replayThreadHistory,
  sendChatMessageStream,
  sendHitlResponse,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, ContentSegment } from '@/types/chat';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;
const mockTurns = fetchThreadTurns as Mock;

const planInterrupt = {
  event: 'interrupt',
  interrupt_id: 'plan-1',
  action_requests: [{ name: 'SubmitPlan', description: 'Step 1. Do the thing.' }],
};

const assistantBubbles = (messages: readonly unknown[]): AssistantMessage[] =>
  messages.filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant');

/** Send "make a plan", get a plan_approval interrupt on the main bubble. */
async function sendAndInterrupt() {
  mockSend.mockImplementation(async (...args: unknown[]) => {
    const onEvent = args[5] as (e: Record<string, unknown>) => void;
    onEvent(planInterrupt);
    return { disconnected: false };
  });

  const rendered = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
  await waitFor(() => expect(mockReplay).toHaveBeenCalled());
  await settleMountEffect();

  await act(async () => {
    await rendered.result.current.handleSendMessage('make a plan', false);
  });

  // The plan card is on screen and pending approval.
  await waitFor(() =>
    expect(
      assistantBubbles(rendered.result.current.messages).flatMap(
        (m) => (m.contentSegments as ContentSegment[] | undefined) || [],
      ).filter((s) => s.type === 'plan_approval'),
    ).toHaveLength(1),
  );
  return rendered;
}

describe('useChatMessages — empty HITL-resume bubble stays in state (turn integrity)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // clearAllMocks clears call history but NOT implementations — reset and
    // re-seed so no stream emitter leaks into the next test's mount.
    mockReplay.mockReset();
    mockReplay.mockResolvedValue(undefined);
    mockSend.mockReset();
    mockSendHitl.mockReset();
    mockTurns.mockReset();
    mockTurns.mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    mockStatus.mockReset();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
  });

  it('an empty resume keeps its bubble in state, settled (render hides it)', async () => {
    const { result } = await sendAndInterrupt();

    // Approve → the resume stream emits NOTHING onto its fresh bubble.
    mockSendHitl.mockImplementation(async () => ({ disconnected: false, aborted: false }));

    await act(async () => {
      result.current.handleApproveInterrupt();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalled());
    await settleMountEffect();

    // TWO assistant bubbles: the plan-card turn AND the resume turn. The
    // resume bubble is empty and settled — MessageList's orphan guard hides
    // it — but it must remain so bubble-count → turn_index stays aligned.
    const assistants = assistantBubbles(result.current.messages);
    expect(assistants).toHaveLength(2);
    const resumeBubble = assistants.find((m) => m.id.startsWith('assistant-hitl-'))!;
    expect(resumeBubble).toBeDefined();
    expect(resumeBubble.contentSegments).toHaveLength(0);
    expect(resumeBubble.content).toBe('');
    await waitFor(() => {
      const settled = assistantBubbles(result.current.messages).find((m) => m.id.startsWith('assistant-hitl-'))!;
      expect(settled.isStreaming).toBe(false);
    });
  });

  it('a resume that streamed real content keeps it on its bubble', async () => {
    const { result } = await sendAndInterrupt();

    // Approve → the resume streams a real answer onto its bubble.
    mockSendHitl.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'resumed answer' });
      return { disconnected: false, aborted: false };
    });

    await act(async () => {
      result.current.handleApproveInterrupt();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalled());
    await settleMountEffect();

    await waitFor(() => expect(JSON.stringify(result.current.messages)).toContain('resumed answer'));
    expect(assistantBubbles(result.current.messages)).toHaveLength(2);
  });

  it('edit truncation releases truncated interrupt ids so a re-raised id renders a card again', async () => {
    const { result } = await sendAndInterrupt();

    // Edit the original user message: truncation removes the plan card, and the
    // forked run re-raises the SAME interrupt_id (LangGraph ids are
    // deterministic). A stale rendered-id entry would suppress the new card and
    // strand the interrupt unanswerable.
    mockTurns.mockResolvedValue({
      turns: [{ edit_checkpoint_id: 'ckpt-0', regenerate_checkpoint_id: 'ckpt-0r', turn_index: 0 }],
      retry_checkpoint_id: null,
    });
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent(planInterrupt); // same interrupt_id as the truncated card
      return { disconnected: false };
    });

    const userMsg = result.current.messages.find((m) => m.role === 'user')!;
    await act(async () => {
      await result.current.handleEditMessage(userMsg.id, 'make a better plan');
    });

    // Exactly one plan card — the truncated id was released and re-rendered.
    await waitFor(() => {
      const cards = assistantBubbles(result.current.messages).flatMap(
        (m) => (m.contentSegments as ContentSegment[] | undefined) || [],
      ).filter((s) => s.type === 'plan_approval');
      expect(cards).toHaveLength(1);
    });
  });
});
