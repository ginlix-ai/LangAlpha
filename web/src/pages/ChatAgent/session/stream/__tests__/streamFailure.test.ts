/**
 * A Claude Code refusal on the stream, live or on a reload, reads in the
 * app's words where the app words it, with the link that fixes it; any
 * other keeps the server's.
 */
import { describe, it, expect } from 'vitest';
import { ANTHROPIC_KEY_URL } from '@/lib/claudeCode';
import { readStreamFailure } from '../streamFailure';

const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key);
const SERVER = 'Server words.';
const refusal = (fields: Record<string, unknown>) => ({ event: 'error', error: SERVER, type: 'claude_code_off', ...fields });

describe('readStreamFailure: a Claude Code refusal', () => {
  it.each(['api_key_refused', 'busy'])('words %s in the app', (reason) => {
    expect(readStreamFailure(refusal({ reason }), SERVER, 'ws-1', t)).toEqual({ message: `chat.claudeCode.off.${reason}` });
  });

  it('names the cap on turns, and links the key a cloud computer runs on', () => {
    expect(readStreamFailure(refusal({ reason: 'too_many_turns', limit: 2 }), SERVER, 'ws-1', t))
      .toEqual({ message: 'chat.claudeCode.off.too_many_turns:{"n":2}' });
    expect(readStreamFailure(refusal({ reason: 'needs_api_key' }), SERVER, 'ws-1', t))
      .toEqual({ message: 'chat.claudeCode.off.needs_api_key', links: [expect.objectContaining({ url: ANTHROPIC_KEY_URL })] });
  });

  it('words a refused setup token in the app, with the link to a new one', () => {
    const failure = readStreamFailure(refusal({ reason: 'token_refused' }), SERVER, 'ws-1', t);
    expect(failure?.message).toBe('chat.claudeCode.off.token_refused');
    expect(failure?.links).toHaveLength(1);
  });

  it("keeps the server's words for a reason the app does not word", () => {
    expect(readStreamFailure(refusal({ reason: 'server' }), SERVER, 'ws-1', t)).toBeUndefined();
  });
});
