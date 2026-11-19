// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { NO_STANDING, type AccountStanding } from '@/lib/accountRules';
import { readTurnFailure } from '../turnFailure';

const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${String(opts.name)}` : key);
const org: AccountStanding = { ...NO_STANDING, organization: { id: 'o1', name: 'Northfield', suspended: false } };
const failure = (errorInfo: Record<string, unknown> | undefined, { status = 403, standing = NO_STANDING }: { status?: number; standing?: AccountStanding } = {}) =>
  readTurnFailure(Object.assign(new Error('Server words.'), { status, errorInfo }), { t, standing, workspaceId: 'ws-1' });
const rule = { rule: 'trading', setBy: 'organization', model: undefined, message: 'No trading.' };

describe('readTurnFailure', () => {
  // What each refusal asks of the composer: its banner, whether it is the
  // server's answer (no failed reply under it), whether the composer holds the
  // draft (and the view reloads), and what is read again.
  it.each([
    ['a rule', { type: 'policy_denied', rule: 'trading', set_by: 'organization', message: 'No trading.' },
      { banner: { message: 'No trading.', ruleStop: { kind: 'rule', refusal: rule } }, answered: true, holdsDraft: false, reread: 'rules' }],
    ['a suspension', { type: 'account_suspended', organization: 'Northfield' },
      { banner: { message: 'Server words.', ruleStop: { kind: 'suspended', organization: 'Northfield' } }, answered: true, holdsDraft: false, reread: null }],
    ['a thread under another workspace', { type: 'thread_in_another_workspace' },
      { banner: { message: 'sharedWorkspace.otherWorkspace', kind: 'notice' }, answered: false, holdsDraft: false, reread: null }],
    ["another member's turn", { code: 'another_member', member: { user_id: 'u-maya', name: 'Maya Okafor' } },
      { banner: { message: 'sharedWorkspace.othersTurn.running:Maya Okafor', kind: 'notice' }, answered: false, holdsDraft: true, reread: null }],
  ])('meets %s', (_name, errorInfo, expected) => {
    expect(failure(errorInfo)).toEqual(expected);
  });

  it("meets a 503 for an organization's account, or one whose rules cannot be read, as the rules unknown", () => {
    const expected = { banner: { message: 'Server words.', ruleStop: { kind: 'rules_unknown' } }, answered: true, holdsDraft: false, reread: 'rules' };
    expect(failure({ type: 'service_unavailable' }, { status: 503, standing: org })).toEqual(expected);
    expect(failure({ type: 'service_unavailable' }, { status: 503, standing: { ...NO_STANDING, unknown: true } })).toEqual(expected);
  });

  it('reads the cleaned preferences again after a removed model', () => {
    expect(failure({ type: 'model_removed', message: 'Gone.', link: { url: '/settings', label: 'Settings' } }).reread).toBe('preferences');
  });

  it("gives a bare failure no banner, and the server's words when it has some", () => {
    expect(failure(undefined, { status: 500 })).toEqual({ banner: null, answered: false, holdsDraft: false, reread: null });
    expect(failure({ message: 'Try later.' }, { status: 500 }).banner).toBe('Try later.');
    expect(failure({ type: 'service_unavailable' }, { status: 503 }).banner).toBeNull();
  });
});
