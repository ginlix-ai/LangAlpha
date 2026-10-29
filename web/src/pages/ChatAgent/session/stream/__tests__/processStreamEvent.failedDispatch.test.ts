// @vitest-environment node
/**
 * A hand-off approved on its card whose dispatch then failed says so. The
 * result carries `success: false`, and an unknown outcome also names the
 * thread the run may have started on. Without either written, the card waits
 * on "Starting…" for a run that never comes.
 */
import { describe, expect, it, vi } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const BUBBLE = 'a0';

function setup() {
  // What the approve click leaves: approved, its call awaiting a result.
  let messages: ChatMessage[] = [{
    id: BUBBLE,
    role: 'assistant',
    ptcAgentProposals: {
      'int-1': { status: 'approved', question: 'Job', workspace_name: 'NVDA', tool_call_id: 'call-1' },
    },
  } as unknown as ChatMessage];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const backfill = new Map([['call-1', 'int-1']]);
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: backfill },
    subagentHistory: { toolCalls: new Map() },
  };
  const deps = { clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn() };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const raw = (content: string) =>
    process({ event: 'tool_call_result', tool_call_id: 'call-1', content } as SSEEvent);
  const result = (content: Record<string, unknown>) => raw(JSON.stringify(content));
  const card = () =>
    ((messages.find((m) => m.id === BUBBLE) as unknown as AssistantMessage)
      .ptcAgentProposals as unknown as Record<string, Record<string, unknown>>)['int-1'];
  return { raw, result, card, backfill };
}

describe('live: a hand-off approved on its card', () => {
  it('marks a dispatch a busy thread refused as failed, still approved', () => {
    const { result, card, backfill } = setup();

    result({ success: false, error: 'That thread is still working on an earlier message, so this was not sent.' });

    expect(card()).toMatchObject({ status: 'approved', dispatch_failed: true });
    expect(card().thread_id).toBeUndefined();
    // A refusal is definite: nothing on any thread is this hand-off's run.
    expect(card()).not.toHaveProperty('dispatch_unknown');
    expect(backfill.size).toBe(0);
  });

  it('keeps the thread an unknown outcome names, so the card can follow that run', () => {
    const { result, card } = setup();

    result({
      success: false,
      error: 'dispatch_timeout',
      outcome: 'unknown_retained',
      thread_id: 'analyst-1',
      workspace_id: 'ws-1',
    });

    expect(card()).toMatchObject({
      status: 'approved', dispatch_failed: true, dispatch_unknown: true, thread_id: 'analyst-1', workspace_id: 'ws-1',
    });
  });

  it('marks a call cancelled when the chat changed agents as failed', () => {
    const { raw, card, backfill } = setup();

    raw('Not run: this chat changed agents before the user answered. Ask again if it is still wanted.');

    expect(card()).toMatchObject({ status: 'approved', dispatch_failed: true });
    expect(backfill.size).toBe(0);
  });

  it('gives a dispatch that started its thread and no failure', () => {
    const { result, card } = setup();

    result({ success: true, status: 'dispatched', thread_id: 'analyst-1', workspace_id: 'ws-1', run_id: 'run-1' });

    // The run it started, which the card reads once it is over, whatever the thread runs next.
    expect(card()).toMatchObject({ status: 'approved', thread_id: 'analyst-1', workspace_id: 'ws-1', run_id: 'run-1' });
    expect(card().dispatch_failed).toBeUndefined();
  });

  it('writes whether a report-back is wired, which the switch only asked for', () => {
    const { result, card } = setup();

    result({ success: true, status: 'dispatched', thread_id: 'analyst-1', workspace_id: 'ws-1', report_back: false });

    expect(card()).toMatchObject({ status: 'approved', report_back: false });
  });
});
