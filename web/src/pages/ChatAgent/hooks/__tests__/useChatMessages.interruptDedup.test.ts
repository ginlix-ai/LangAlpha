/**
 * Interrupt-card de-duplication by interrupt_id.
 *
 * LangGraph re-raises an UNANSWERED interrupt with the SAME interrupt_id on every
 * resume. Each re-raise arrives on a later turn's bubble (history replay) or on a
 * fresh `assistant-hitl-*` bubble (live resume), so the per-message maps keyed by
 * interrupt_id never collide and a duplicate CARD used to be appended. Repro from
 * a Slack-driven thread: a still-pending "PTC Agent" (NVIDIA) interrupt showed up
 * twice in the web transcript (3 cards for 2 real interrupts).
 *
 * These tests drive the REAL hook internals (api module mocked) and assert exactly
 * one rendered card survives across both the replay and live-resume paths.
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
  reconnectToWorkflowStream,
  sendChatMessageStream,
  sendHitlResponse,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, ContentSegment } from '@/types/chat';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;
const mockTurns = fetchThreadTurns as Mock;

/** Live/replay-shaped ptc_agent interrupt action payload. */
const ptcAction = (toolCallId: string) => ({
  type: 'ptc_agent',
  workspace_id: 'ws-x',
  workspace_name: 'Analysis',
  question: 'analyze the stock',
  report_back: true,
  tool_call_id: toolCallId,
});

/** Count content segments of a given type across every assistant message. */
function countSegments(messages: readonly unknown[], type: string): number {
  return messages
    .filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant')
    .reduce(
      (n, m) => n + ((m.contentSegments as ContentSegment[] | undefined) || []).filter((s) => s.type === type).length,
      0,
    );
}

describe('useChatMessages — interrupt card de-dup by interrupt_id', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // clearAllMocks clears call history but NOT implementations — reset and
    // re-seed the stream mocks so one test's emitter can't replay into the
    // next test's mount (a leaked replay emitter injects phantom cards and
    // pre-seeds the rendered-interrupt set).
    mockReplay.mockReset();
    mockReplay.mockResolvedValue(undefined);
    mockSend.mockReset();
    mockSendHitl.mockReset();
    mockReconnect.mockReset();
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
    mockTurns.mockReset();
    mockTurns.mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    mockStatus.mockReset();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
  });

  it('history replay: a re-raised interrupt (same id, later turn) renders ONE card', async () => {
    // Turn 0 raises two PTC-agent interrupts (nvidia + amd). The user answers amd
    // on turn 1; nvidia stays pending, so turn 1's response RE-EMITS nvidia with
    // the same interrupt_id on turn 1's bubble. Pre-fix that second emit appended
    // a duplicate nvidia card (the reported 3-cards-for-2-interrupts bug).
    const ptc = (interruptId: string, turn: number, question: string) => ({
      event: 'interrupt',
      turn_index: turn,
      interrupt_id: interruptId,
      action_requests: [{
        type: 'ptc_agent',
        workspace_id: 'ws-x',
        workspace_name: 'Analysis',
        question,
        report_back: true,
        tool_call_id: `tc-${interruptId}`,
      }],
    });

    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'compare nvidia and amd', role: 'user' });
      onEvent(ptc('nvidia', 0, 'analyze nvidia'));
      onEvent(ptc('amd', 0, 'analyze amd'));
      // Resume turn: amd answered, nvidia re-raised (same id) on the new bubble.
      onEvent({ event: 'user_message', turn_index: 1, content: '', role: 'user' });
      onEvent(ptc('nvidia', 1, 'analyze nvidia'));
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    // Two distinct interrupts (nvidia + amd) → two cards, NOT three.
    await waitFor(() => expect(countSegments(result.current.messages, 'ptc_agent')).toBe(2));

    const nvidiaCards = result.current.messages
      .filter((m): m is AssistantMessage => m.role === 'assistant')
      .flatMap((m) => ((m.contentSegments as ContentSegment[] | undefined) || []))
      .filter((s) => s.type === 'ptc_agent' && (s as { proposalId?: string }).proposalId === 'nvidia');
    expect(nvidiaCards).toHaveLength(1);
  });

  it('live resume: a still-pending interrupt re-raised after HITL resume renders ONE card', async () => {
    // Send → a plan_approval interrupt streams in (bubble A). The user approves;
    // the resume stream lands on a fresh `assistant-hitl-*` bubble and RE-EMITS
    // the same interrupt_id (LangGraph re-raising it). Pre-fix that painted a
    // second plan card on the resume bubble.
    const planInterrupt = {
      event: 'interrupt',
      interrupt_id: 'plan-1',
      action_requests: [{ name: 'SubmitPlan', description: 'Step 1. Do the thing.' }],
    };

    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent(planInterrupt);
      return { disconnected: false };
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    await act(async () => {
      await result.current.handleSendMessage('make a plan', false);
    });

    // The first card is on screen and pending approval.
    await waitFor(() => expect(countSegments(result.current.messages, 'plan_approval')).toBe(1));

    // The resume re-raises the same still-pending interrupt on the new bubble.
    mockSendHitl.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      onEvent(planInterrupt);
      return { disconnected: false, aborted: false };
    });

    await act(async () => {
      result.current.handleApproveInterrupt();
      await new Promise((r) => setTimeout(r, 0));
    });

    // The re-raise did NOT paint a twin: still exactly one plan card.
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalled());
    await settleMountEffect();
    expect(countSegments(result.current.messages, 'plan_approval')).toBe(1);
    // ...and the suppression only dropped the CARD: the map write + pending
    // tracking still ran, so the re-raised interrupt stays answerable.
    expect(result.current.pendingInterrupt?.interruptId).toBe('plan-1');
  });

  it('interrupts without an interrupt_id are never deduped', async () => {
    // Guard false-branch: id-less interrupt events skip dedup entirely — every
    // occurrence renders its own card, and nothing is added to the rendered set.
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'interrupt', action_requests: [{ name: 'SubmitPlan', description: 'plan A' }] });
      onEvent({ event: 'interrupt', action_requests: [{ name: 'SubmitPlan', description: 'plan B' }] });
      return { disconnected: false };
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await act(async () => {
      await result.current.handleSendMessage('plan twice', false);
    });

    await waitFor(() => expect(countSegments(result.current.messages, 'plan_approval')).toBe(2));
  });

  it('reconnect: the stripped history card is re-rendered from the reconnect stream', async () => {
    // Opening a thread whose workflow is still active strips the replay-rendered
    // interrupt cards (the reconnect stream is authoritative and re-delivers
    // them). The strip must RELEASE those ids from the rendered set — a stale
    // entry would suppress the re-delivery, leaving the pending interrupt with
    // no card anywhere (strip removed the only other copy) and unanswerable.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'run-1',
      active_tasks: [],
      pending_report_back: false,
    });
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch the agent', role: 'user' });
      onEvent({ event: 'interrupt', turn_index: 0, interrupt_id: 'int-1', action_requests: [ptcAction('tc-int-1')] });
      return Promise.resolve();
    });
    mockReconnect.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[3] as (e: Record<string, unknown>) => void;
      // The active run re-delivers the still-pending interrupt, same id.
      onEvent({ event: 'interrupt', interrupt_id: 'int-1', action_requests: [ptcAction('tc-int-1')] });
      return { disconnected: false, aborted: false };
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await settleMountEffect();

    // Exactly one card: the strip removed the history copy, the reconnect
    // stream repainted it (NOT suppressed by the dedup set).
    await waitFor(() => expect(countSegments(result.current.messages, 'ptc_agent')).toBe(1));
  });

  it('edit of a later turn RETAINS the surviving earlier card id (re-raise stays suppressed)', async () => {
    // Truncation rebuild, retain direction: editing turn 1 keeps turn 0's card
    // on screen, so its id must SURVIVE the rebuild — the fork's re-raise of
    // that id must stay suppressed or the transcript grows a duplicate card.
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch the agent', role: 'user' });
      onEvent({ event: 'interrupt', turn_index: 0, interrupt_id: 'int-keep', action_requests: [ptcAction('tc-keep')] });
      onEvent({ event: 'user_message', turn_index: 1, content: 'and summarize', role: 'user' });
      return Promise.resolve();
    });
    mockTurns.mockResolvedValue({
      turns: [
        { edit_checkpoint_id: 'ckpt-0', regenerate_checkpoint_id: 'ckpt-0r', turn_index: 0 },
        { edit_checkpoint_id: 'ckpt-1', regenerate_checkpoint_id: 'ckpt-1r', turn_index: 1 },
      ],
      retry_checkpoint_id: null,
    });
    // The edit fork re-raises the surviving card's id.
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent({ event: 'interrupt', interrupt_id: 'int-keep', action_requests: [ptcAction('tc-keep')] });
      return { disconnected: false };
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await waitFor(() => expect(countSegments(result.current.messages, 'ptc_agent')).toBe(1));

    const userMsgs = result.current.messages.filter((m) => m.role === 'user');
    const laterUser = userMsgs[userMsgs.length - 1];
    await act(async () => {
      await result.current.handleEditMessage(laterUser.id, 'and compare to AMD');
    });
    await waitFor(() => expect(mockSend).toHaveBeenCalled());

    // Still exactly one card: the surviving turn-0 card owns the id, the
    // fork's re-raise did NOT paint a twin.
    expect(countSegments(result.current.messages, 'ptc_agent')).toBe(1);
  });

  it('thread switch clears the rendered set so the next thread renders its own cards', async () => {
    // Interrupt ids are only unique per thread. After switching threads, the
    // prior thread's rendered set must not suppress the new thread's replay.
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch', role: 'user' });
      onEvent({ event: 'interrupt', turn_index: 0, interrupt_id: 'shared-id', action_requests: [ptcAction('tc-s')] });
      return Promise.resolve();
    });

    let tid = 'th-A';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-x', tid));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await waitFor(() => expect(countSegments(result.current.messages, 'ptc_agent')).toBe(1));

    tid = 'th-B';
    rerender();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await settleMountEffect();

    // Thread B's card rendered — not suppressed by thread A's set.
    await waitFor(() => expect(countSegments(result.current.messages, 'ptc_agent')).toBe(1));
  });
});
