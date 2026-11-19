// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { holdsDraft, othersTurnWords, readTurnRefusal } from '../turnRefusal';

describe('readTurnRefusal', () => {
  it('reads a rule from a route detail and from a stream frame', () => {
    const detail = { type: 'policy_denied', rule: 'trading', set_by: 'organization', message: 'No trading.' };
    expect(readTurnRefusal(detail)).toEqual({
      kind: 'rule',
      refusal: { rule: 'trading', setBy: 'organization', model: undefined, message: 'No trading.' },
    });
    expect(readTurnRefusal({ error_type: 'policy_denied', rule: 'models.allowed', model: 'm1', error: 'Not allowed.' })).toEqual({
      kind: 'rule',
      refusal: { rule: 'models.allowed', setBy: 'deployment', model: 'm1', message: 'Not allowed.' },
    });
  });

  it("reads a suspension with its organization's name, when given", () => {
    expect(readTurnRefusal({ type: 'account_suspended', organization: 'Northfield' })).toEqual({ kind: 'suspended', organization: 'Northfield' });
    expect(readTurnRefusal({ error_type: 'account_suspended', organization: '' })).toEqual({ kind: 'suspended', organization: null });
  });

  it("reads another member's running turn from the admission detail and frame, naming them when it does", () => {
    expect(readTurnRefusal({ code: 'another_member', member: { user_id: 'u-maya', name: 'Maya Okafor' } }))
      .toEqual({ kind: 'others_turn', member: 'Maya Okafor' });
    expect(readTurnRefusal({ error_type: 'admission_conflict', code: 'another_member', member: { user_id: 'u-maya', name: null } }))
      .toEqual({ kind: 'others_turn', member: null });
    expect(readTurnRefusal({ code: 'another_member' })).toEqual({ kind: 'others_turn', member: null });
  });

  it('reads a thread sent under another workspace by its type', () => {
    expect(readTurnRefusal({ type: 'thread_in_another_workspace' })).toEqual({ kind: 'other_workspace' });
  });

  it('is null for anything else', () => {
    expect(readTurnRefusal(null)).toBeNull();
    expect(readTurnRefusal('policy_denied')).toBeNull();
    expect(readTurnRefusal({ code: 'running' })).toBeNull();
    expect(readTurnRefusal({ type: 'policy_denied' })).toBeNull();
    expect(readTurnRefusal({ type: 'service_unavailable' })).toBeNull();
    expect(readTurnRefusal({ type: 'another_members_turn', message: 'x' })).toBeNull();
  });
});

describe('othersTurnWords', () => {
  const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${String(opts.name)}` : key);
  it('names the member the refusal names, else someone', () => {
    expect(othersTurnWords(t, 'Maya Okafor')).toBe('sharedWorkspace.othersTurn.running:Maya Okafor');
    expect(othersTurnWords(t, null)).toBe('sharedWorkspace.othersTurn.running:sharedWorkspace.someone');
  });
});

describe('holdsDraft', () => {
  it("holds the draft for another member's turn only", () => {
    expect(holdsDraft({ kind: 'others_turn', member: null })).toBe(true);
    expect(holdsDraft({ kind: 'other_workspace' })).toBe(false);
    expect(holdsDraft(null)).toBe(false);
  });
});
