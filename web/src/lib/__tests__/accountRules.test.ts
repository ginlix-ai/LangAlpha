// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { AccountPolicy, User } from '@/types/api';
import {
  allowedDefault, allows, methodAllowed, modelRefusedBy, modelRuleRowKey, modelsRule, organizationLabel, orgWords, RULE, standingOf, withoutRefusedModels,
} from '../accountRules';

function policy(over: Partial<AccountPolicy> = {}): AccountPolicy {
  return {
    models: { allowed: null },
    own_model_access: { api_keys: true, subscriptions: true },
    public_links: true,
    trading: true,
    own_machines: true,
    user_connectors: true,
    set_by: {},
    ...over,
  };
}

const user = (fields: Partial<User>) => ({ user_id: 'u1', ...fields }) as User;

describe('standingOf', () => {
  it('reads a server that sends no rules as restricting nothing, not as unknown', () => {
    const s = standingOf(user({}));
    expect(s).toEqual({ organization: null, policy: null, unknown: false, suspended: false, teams: [] });
  });

  it("reads the organization's teams, none from a server that sends none, and none while suspended", () => {
    const semis = { id: 't1', name: 'Semis desk', role: 'admin' as const };
    const org = { id: 'o', name: 'Northfield', suspended: false };
    expect(standingOf(user({ organization: org })).teams).toEqual([]);
    expect(standingOf(user({ organization: { ...org, teams: [semis] } })).teams).toEqual([semis]);
    expect(standingOf(user({ organization: { ...org, suspended: true, teams: [semis] } })).teams).toEqual([]);
  });

  it('reads an explicit null policy as rules that cannot be read', () => {
    expect(standingOf(user({ policy: null, organization: null })).unknown).toBe(true);
  });

  it('reads a suspended organization as suspended, never as unknown', () => {
    const s = standingOf(user({ policy: null, organization: { id: 'o', name: 'Northfield', suspended: true } }));
    expect(s.suspended).toBe(true);
    expect(s.unknown).toBe(false);
  });
});

describe('modelRefusedBy', () => {
  const metadata = { mine: { is_custom_model: true }, sub: { access_type: 'oauth' }, cc: { sdk: 'claude-code' } };

  it('allows everything without rules', () => {
    expect(modelRefusedBy(null, metadata, 'mine')).toBeNull();
  });

  it('checks the allow-list first, and a model the user added never passes it', () => {
    const p = policy({ models: { allowed: ['a', 'mine'] }, own_model_access: { api_keys: false, subscriptions: false } });
    expect(modelRefusedBy(p, metadata, 'a')).toBeNull();
    expect(modelRefusedBy(p, metadata, 'b')).toBe(RULE.modelsAllowed);
    expect(modelRefusedBy(p, metadata, 'mine')).toBe(RULE.modelsAllowed);
  });

  it("then own keys for a model the user added, then own sign-ins for a subscription's or Claude Code's", () => {
    const p = policy({ own_model_access: { api_keys: false, subscriptions: false } });
    expect(modelRefusedBy(p, metadata, 'mine')).toBe(RULE.ownApiKeys);
    expect(modelRefusedBy(p, metadata, 'sub')).toBe(RULE.ownSubscriptions);
    expect(modelRefusedBy(p, metadata, 'cc')).toBe(RULE.ownSubscriptions);
    expect(modelRefusedBy(p, metadata, 'platform')).toBeNull();
  });
});

describe('modelRuleRowKey', () => {
  it('words a model the user added apart under the allow-list, else by the rule', () => {
    expect(modelRuleRowKey(RULE.modelsAllowed, { is_custom_model: true })).toBe('organization.picker.row.custom');
    expect(modelRuleRowKey(RULE.modelsAllowed, undefined)).toBe('organization.picker.row.allowList');
    expect(modelRuleRowKey(RULE.ownApiKeys, { is_custom_model: true })).toBe('organization.picker.row.ownApiKeys');
  });
});

describe('withoutRefusedModels', () => {
  const models = { anthropic: { models: ['a', 'b'] }, mine: { models: ['c'] } };
  const meta = { c: { is_custom_model: true }, b: { sdk: 'claude-code' } };

  it('returns the same object when nothing restricts models', () => {
    expect(withoutRefusedModels(models, null, meta)).toBe(models);
    expect(withoutRefusedModels(models, policy({ trading: false }), meta)).toBe(models);
  });

  it('drops refused models and the groups left empty', () => {
    const p = policy({ own_model_access: { api_keys: false, subscriptions: false } });
    expect(withoutRefusedModels(models, p, meta)).toEqual({ anthropic: { models: ['a'] } });
  });
});

describe('allowedDefault', () => {
  const p = policy({ models: { allowed: ['x', 'y', 'z'] } });
  const reachable = (m: string) => m !== 'x';

  it('keeps a saved default no rule refuses', () => {
    expect(allowedDefault(p, 'y', reachable, {}, 'd')).toBe('y');
    expect(allowedDefault(null, 'q', reachable, {}, 'd')).toBe('q');
  });

  it('falls to the first allowed model the user can reach', () => {
    expect(allowedDefault(p, 'q', reachable, {}, 'd')).toBe('y');
  });

  it('falls to the deployment default when allowed, else to none', () => {
    const narrow = policy({ models: { allowed: ['x'] } });
    expect(allowedDefault(narrow, 'q', reachable, {}, 'x')).toBe('x');
    expect(allowedDefault(narrow, 'q', reachable, {}, 'd')).toBeNull();
    const own = policy({ own_model_access: { api_keys: false, subscriptions: true } });
    expect(allowedDefault(own, 'mine', reachable, { mine: { is_custom_model: true } }, 'd')).toBe('d');
  });
});

describe('model and connection rules', () => {
  it('names the rule that narrows models most', () => {
    expect(modelsRule(null)).toBeNull();
    expect(modelsRule(policy())).toBeNull();
    expect(modelsRule(policy({ models: { allowed: [] }, own_model_access: { api_keys: false, subscriptions: true } })))
      .toBe(RULE.modelsAllowed);
    expect(modelsRule(policy({ own_model_access: { api_keys: true, subscriptions: false } }))).toBe(RULE.ownSubscriptions);
  });

  it('opens a sign-in under the subscriptions rule and everything else under the keys rule', () => {
    const p = policy({ own_model_access: { api_keys: false, subscriptions: true } });
    expect(methodAllowed(p, 'oauth')).toBe(true);
    expect(methodAllowed(p, 'api_key')).toBe(false);
    expect(methodAllowed(p, 'local')).toBe(false);
    expect(methodAllowed(null, 'api_key')).toBe(true);
  });

  it('allows what no rule restricts', () => {
    expect(allows(null, RULE.trading)).toBe(true);
    expect(allows(policy({ public_links: false }), RULE.publicLinks)).toBe(false);
  });
});

describe('naming the organization', () => {
  const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key);

  it("names it in the key's own wording, or takes the unnamed variant", () => {
    expect(orgWords(t, 'Northfield', 'organization.share.membersCanView')).toBe('organization.share.membersCanView:{"organization":"Northfield"}');
    expect(orgWords(t, null, 'organization.share.membersCanView')).toBe('organization.share.membersCanViewUnnamed');
    expect(orgWords(t, null, 'k', { count: 2 })).toBe('kUnnamed:{"count":2}');
  });

  it('falls back to "your organization" where the wording takes a name either way', () => {
    expect(organizationLabel(t, 'Northfield')).toBe('Northfield');
    expect(organizationLabel(t, null)).toBe('organization.yourOrganization');
  });
});
