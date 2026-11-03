/**
 * A Claude Code refusal on the stream, live or on a reload, reads in the
 * app's words where the app words it, with the link that fixes it; any
 * other keeps the server's.
 */
import { describe, it, expect } from 'vitest';
import { readStreamFailure } from '../streamFailure';

const t = (key: string) => `t:${key}`;
const SERVER = 'Server words.';
const refusal = (reason: string) => ({ event: 'error', error: SERVER, type: 'claude_code_off', reason });

describe('readStreamFailure: a Claude Code refusal', () => {
  it('words a refused setup token in the app, with the link to a new one', () => {
    const failure = readStreamFailure(refusal('token_refused'), SERVER, 'ws-1', t);
    expect(failure?.message).toBe('t:chat.claudeCode.off.token_refused');
    expect(failure?.links).toHaveLength(1);
  });

  it("keeps the server's words, and its link, for a reason the app does not word", () => {
    expect(readStreamFailure(refusal('signed_out'), SERVER, 'ws-1', t)?.message).toBe(SERVER);
    expect(readStreamFailure(refusal('server'), SERVER, 'ws-1', t)).toBeUndefined();
  });
});
