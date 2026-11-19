// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import { sharedBubble, threadTurns, turnOwner, type SharedThreadView } from '../sharedThread';

const ME = { userId: 'u-lee', name: 'Lee Chen' };
const MAYA = { userId: 'u-maya', name: 'Maya Okafor' };

const user = (fields: Record<string, unknown>): ChatMessage =>
  ({ id: `m-${Math.random()}`, role: 'user', content: 'x', contentType: 'text', isStreaming: false, ...fields }) as ChatMessage;
const reply = (turnIndex: number) => ({ id: `a-${turnIndex}`, role: 'assistant', turnIndex, content: '' }) as unknown as ChatMessage;
const view = (over: Partial<SharedThreadView> = {}): SharedThreadView =>
  ({ me: ME, avatars: {}, names: {}, lastOthersTurn: -1, ownTurns: new Set(), ...over });

describe('turnOwner', () => {
  it('goes by the named sender, else counts an unnamed send of this view as mine', () => {
    expect(turnOwner({ role: 'user', sender: MAYA }, ME.userId)).toBe('theirs');
    expect(turnOwner({ role: 'user', sender: ME, isHistory: true }, ME.userId)).toBe('mine');
    expect(turnOwner({ role: 'user' }, ME.userId)).toBe('mine');
    expect(turnOwner({ role: 'user', isHistory: true, sentHere: true }, ME.userId)).toBe('mine');
  });

  it('knows nobody for an unnamed history row or an unnamed agent-sent message', () => {
    expect(turnOwner({ role: 'user', isHistory: true }, ME.userId)).toBe('none');
    expect(turnOwner({ role: 'user', provenance: { kind: 'automation' } }, ME.userId)).toBe('none');
    // A report naming the run it answers counts by that name.
    expect(turnOwner({ role: 'user', provenance: { kind: 'task_report' }, sender: MAYA }, ME.userId)).toBe('theirs');
  });
});

describe('sharedBubble', () => {
  const sender = (v: SharedThreadView, message: Parameters<typeof sharedBubble>[1]) => sharedBubble(v, message, 0).sender;

  it('names the sender either source gave, mine or not, with their picture', () => {
    expect(sender(view({ avatars: { 'u-maya': 'maya.png' } }), { role: 'user', sender: MAYA, isHistory: true }))
      .toEqual({ name: 'Maya Okafor', avatarUrl: 'maya.png', mine: false });
    expect(sender(view(), { role: 'user', sender: ME })).toEqual({ name: 'Lee Chen', avatarUrl: null, mine: true });
  });

  it("keeps a send of this view as the viewer's after its stream ends unnamed", () => {
    expect(sender(view(), { role: 'user', isHistory: true, sentHere: true })).toEqual({ name: 'Lee Chen', avatarUrl: null, mine: true });
    expect(sender(view(), { role: 'user' })).toEqual({ name: 'Lee Chen', avatarUrl: null, mine: true });
  });

  it("names a sender given by id alone from the workspace's people", () => {
    const steer = { role: 'user', sender: { userId: 'u-maya', name: null }, steeringDelivered: true };
    expect(sender(view({ names: { 'u-maya': 'Maya Okafor' } }), steer)).toEqual({ name: 'Maya Okafor', avatarUrl: null, mine: false });
    expect(sender(view({ names: { 'u-maya': 'Maya Okafor' } }), { ...steer, isHistory: true })).toEqual({ name: 'Maya Okafor', avatarUrl: null, mine: false });
  });

  it('draws none for an unnamed history row, an agent-sent message, or a reply', () => {
    expect(sender(view(), { role: 'user', isHistory: true })).toBeUndefined();
    expect(sender(view(), { role: 'user', provenance: { kind: 'automation' }, sender: MAYA })).toBeUndefined();
    expect(sender(view(), { role: 'assistant' })).toBeUndefined();
  });
});

describe('sharedBubble: pauses', () => {
  it("lets only the turn's sender answer its question or approval", () => {
    const v = view({ ownTurns: new Set([1]) });
    expect(sharedBubble(v, reply(1), 1).answersPause).toBe(true);
    expect(sharedBubble(v, reply(2), 2).answersPause).toBe(false);
    expect(sharedBubble(v, reply(0), null).answersPause).toBe(true);
  });
});

describe('threadTurns', () => {
  it("allows rewrites only after another member's latest turn, and reviews only on the viewer's own", () => {
    const thread = [user({ sender: ME, turnIndex: 0 }), user({ sender: MAYA, turnIndex: 1 }), user({ sender: ME, turnIndex: 2 })];
    const turns = threadTurns(thread, ME.userId, false);
    expect(turns.lastOthersTurn).toBe(1);
    expect([...turns.ownTurns].sort()).toEqual([0, 2]);
    const v = view(turns);
    const bubble = (turnIndex: number | null) => sharedBubble(v, { role: 'assistant' }, turnIndex);
    expect([0, 1, 2].map((i) => bubble(i).canFork)).toEqual([false, false, true]);
    expect([0, 1, 2].map((i) => bubble(i).humanReview)).toEqual([true, false, true]);
    expect(bubble(null)).toMatchObject({ canFork: false, humanReview: false });
  });

  it("counts a turn as the viewer's only when its opener names them or is this view's send", () => {
    const report = (sender?: unknown) => user({ turnIndex: 2, isHistory: true, content: '', provenance: { kind: 'task_report' }, ...(sender ? { sender } : {}) });
    const base = [user({ sender: ME, turnIndex: 0, isHistory: true }), reply(0), user({ sender: ME, turnIndex: 1, isHistory: true }), reply(1)];
    const last = (messages: ChatMessage[]) => threadTurns(messages, ME.userId, false).lastOthersTurn;
    expect(last([...base, report(ME), reply(2)])).toBe(-1);
    expect(last([...base, report(MAYA), reply(2)])).toBe(2);
    // A turn whose opener names no one (a wake turn's reply alone): the server
    // counts its run's sender, which the view cannot see.
    expect(last([...base, reply(2)])).toBe(2);
    expect(last([...base, user({ turnIndex: 2 }), reply(2)])).toBe(-1);
    // A steer the viewer sent into another member's turn decides nothing.
    expect(last([...base, user({ turnIndex: 2, sender: MAYA, isHistory: true }), user({ turnIndex: 2, steering: true })])).toBe(2);
  });

  it("names whoever opened the running turn when it is not the viewer", () => {
    const thread = [user({ sender: ME, turnIndex: 0 }), user({ sender: MAYA, turnIndex: 1 })];
    expect(threadTurns(thread, ME.userId, true).othersTurn).toEqual(MAYA);
    expect(threadTurns(thread, ME.userId, false).othersTurn).toBeNull();
    expect(threadTurns([user({ sender: ME, turnIndex: 0 })], ME.userId, true).othersTurn).toBeNull();
  });

  it("skips steers, queued sends and agent-sent messages, and reads an unnamed opener as the viewer's", () => {
    const thread = [user({ sender: MAYA, turnIndex: 1 }), user({ sender: ME, steering: true }), user({ queued: true }), user({ provenance: { kind: 'automation' } })];
    expect(threadTurns(thread, ME.userId, true).othersTurn).toEqual(MAYA);
    expect(threadTurns([user({ turnIndex: 1 })], ME.userId, true).othersTurn).toBeNull();
    expect(threadTurns([user({ turnIndex: 1, isHistory: true })], ME.userId, true).othersTurn).toBeNull();
  });
});
