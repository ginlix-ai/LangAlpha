/**
 * A Claude Code refusal on the stream, live or on a reload, reads in the
 * app's words where the app words it, with the link that fixes it; any
 * other keeps the server's.
 */
import { describe, it, expect } from 'vitest';
import { readStreamFailure } from '../streamFailure';

const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key);
const SERVER = 'Server words.';
const refusal = (fields: Record<string, unknown>) => ({ event: 'error', error: SERVER, type: 'claude_code_off', ...fields });

describe('readStreamFailure: a Claude Code refusal', () => {
  it.each(['needs_bigger_computer'])('words %s in the app', (reason) => {
    expect(readStreamFailure(refusal({ reason }), SERVER, 'ws-1', t)).toEqual({ message: `chat.claudeCode.off.${reason}` });
  });

  it('names the cap on turns', () => {
    expect(readStreamFailure(refusal({ reason: 'too_many_turns', limit: 2 }), SERVER, 'ws-1', t))
      .toEqual({ message: 'chat.claudeCode.off.too_many_turns:{"n":2}' });
  });

  it('words a refused setup token in the app, with the link to a new one', () => {
    const failure = readStreamFailure(refusal({ reason: 'token_refused' }), SERVER, 'ws-1', t);
    expect(failure?.message).toBe('chat.claudeCode.off.token_refused');
    expect(failure?.links).toHaveLength(1);
  });

  it("keeps the server's words for a reason the app does not word", () => {
    expect(readStreamFailure(refusal({ reason: 'server' }), SERVER, 'ws-1', t)).toBeUndefined();
  });

  it("carries a plan limit's window and reset for the card to word, with nothing to fix", () => {
    const until = '2026-11-06T20:40:00Z';
    expect(readStreamFailure(refusal({ reason: 'plan_stopped', window: 'five_hour', until, credits_billed: true }), SERVER, 'ws-1', t))
      .toEqual({ message: SERVER, planRefusal: { reason: 'plan_stopped', window: 'five_hour', until, creditsBilled: true } });
    expect(readStreamFailure(refusal({ reason: 'plan_limited', window: 'seven_day', until }), SERVER, 'ws-1', t))
      .toEqual({ message: SERVER, planRefusal: { reason: 'plan_limited', window: 'seven_day', until, creditsBilled: false } });
  });

  it("keeps the server's words for a plan limit without a time to word", () => {
    expect(readStreamFailure(refusal({ reason: 'plan_limited', window: 'five_hour' }), SERVER, 'ws-1', t)).toBeUndefined();
  });
});

describe('readStreamFailure: who pays for an upstream failure', () => {
  const upstream = (fields: Record<string, unknown>) => ({ event: 'error', error: SERVER, error_kind: 'upstream', ...fields });

  it('reads the payer and the account the server named', () => {
    const failure = readStreamFailure(upstream({ payer: 'own_key', account: 'DeepSeek' }), SERVER, 'ws-1', t);
    expect(failure?.payer).toBe('own_key');
    expect(failure?.account).toBe('DeepSeek');
  });

  it('drops a payer it does not know, so the card reads as it always did', () => {
    expect(readStreamFailure(upstream({ payer: 'someone' }), SERVER, 'ws-1', t)?.payer).toBeUndefined();
  });
});
