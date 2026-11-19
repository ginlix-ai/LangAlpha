// @vitest-environment node
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import { createForkRefusalRecovery, isOthersTurnRefusal, isRecoverableRetryRefusal, type ForkCut } from '../forkRefusal';

// A shared thread's rewrite refused because another member sent a turn from
// that point on: the route's 403, whose detail names itself by its type.
const refused = { status: 403, errorInfo: { type: 'another_members_turn', message: 'Another member sent a turn from here on.' } };

function recovery() {
  const deps = {
    setMessages: vi.fn(),
    setMessageError: vi.fn(),
    requestReload: vi.fn(),
    sessionEpoch: () => 1,
    staleMessage: vi.fn(() => 'stale'),
    returnDraft: vi.fn(),
    busyMessage: vi.fn(() => 'busy'),
    resendMessage: vi.fn(() => 'resend'),
    othersTurnMessage: vi.fn((kind: ForkCut['kind']) => ({ message: `others:${kind}`, kind: 'notice' as const })),
    giveBack: vi.fn(),
    markPersisted: vi.fn(),
  };
  return { deps, recovery: createForkRefusalRecovery(deps) };
}

const transcript = [{ id: 'u-1', role: 'user', content: 'hi' }] as unknown as ChatMessage[];

describe("a rewrite refused for another member's turn", () => {
  it('is read off the detail type, and a Retry recovers from it too', () => {
    expect(isOthersTurnRefusal(refused)).toBe(true);
    expect(isRecoverableRetryRefusal(refused)).toBe(true);
    expect(isOthersTurnRefusal({ status: 403, errorInfo: { type: 'policy_denied', message: 'x' } })).toBe(false);
  });

  it('puts the transcript back, says why, and hands an edit to the composer', () => {
    const { deps, recovery: r } = recovery();
    const draft = { threadId: 't-1', turnIndex: 3, original: 'Lead with memory.', text: 'Lead with memory, then semis.' };
    expect(r.recover(refused, { kind: 'edit', transcript, draft })).toBe(true);
    expect(deps.setMessages).toHaveBeenCalledWith(transcript);
    expect(deps.setMessageError).toHaveBeenCalledWith({ message: 'others:edit', kind: 'notice' });
    expect(deps.giveBack).toHaveBeenCalledWith('Lead with memory, then semis.');
    // Its box would only be refused again.
    expect(deps.returnDraft).not.toHaveBeenCalled();
    expect(deps.requestReload).toHaveBeenCalledOnce();
  });

  it('words a regenerate and a retry as a redo', () => {
    const { deps, recovery: r } = recovery();
    r.recover(refused, { kind: 'regenerate', transcript, draft: null });
    r.recover(refused, { kind: 'retry', transcript, draft: null });
    expect(deps.othersTurnMessage.mock.calls.map(([kind]) => kind)).toEqual(['regenerate', 'retry']);
    expect(deps.giveBack).not.toHaveBeenCalled();
  });
});
