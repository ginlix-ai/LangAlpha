// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import { readSender, withTurnSender } from '../turnSender';

const ME = { userId: 'u-lee', name: 'Lee Chen' };
const MAYA = { userId: 'u-maya', name: 'Maya Okafor' };

const user = (fields: Record<string, unknown>): ChatMessage =>
  ({ id: `m-${Math.random()}`, role: 'user', content: 'x', contentType: 'text', isStreaming: false, ...fields }) as ChatMessage;

describe('readSender', () => {
  it('reads the replay and start-frame shape, and nothing else', () => {
    expect(readSender({ user_id: 'u-maya', name: 'Maya Okafor' })).toEqual(MAYA);
    expect(readSender({ user_id: 'u-maya', name: '' })).toEqual({ userId: 'u-maya', name: null });
    expect(readSender({ name: 'No id' })).toBeNull();
    expect(readSender(null)).toBeNull();
    expect(readSender('u-maya')).toBeNull();
  });
});

describe('withTurnSender', () => {
  it("names the live turn's opening messages, leaving the others and a named one be", () => {
    const opener = user({ turnIndex: 3 });
    const earlier = user({ turnIndex: 2 });
    const named = user({ turnIndex: 3, sender: ME });
    const agentSent = user({ turnIndex: 3, provenance: { kind: 'automation' } });
    const next = withTurnSender([earlier, opener, named, agentSent], 3, MAYA);
    expect(next.map((m) => (m as { sender?: unknown }).sender)).toEqual([undefined, MAYA, ME, undefined]);
  });

  it('keeps the array when nothing changes, so the transcript does not re-render', () => {
    const messages = [user({ turnIndex: 1, sender: MAYA })];
    expect(withTurnSender(messages, 1, MAYA)).toBe(messages);
  });
});
