// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { readRuleRefusal, ruleRefusalCopy } from '../ruleRefusal';

// Echoes the key and the values it was given, so a test reads which copy was
// picked and who it names without depending on the catalog's wording.
const t = (key: string, values?: Record<string, unknown>) =>
  values ? `${key}|${values.who ?? ''}|${values.model ?? ''}` : key;
const name = (m: string) => `name:${m}`;

describe('readRuleRefusal', () => {
  it('reads a route refusal', () => {
    expect(readRuleRefusal({
      type: 'policy_denied', rule: 'models.allowed', set_by: 'organization', model: 'gpt-x', message: 'No.',
    })).toEqual({ rule: 'models.allowed', setBy: 'organization', model: 'gpt-x', message: 'No.' });
  });

  it('reads a stream frame, whose sentence is under error', () => {
    expect(readRuleRefusal({ error_type: 'policy_denied', rule: 'trading', error: 'Trading is off.' }))
      .toEqual({ rule: 'trading', setBy: 'deployment', model: undefined, message: 'Trading is off.' });
  });

  it('reads anything else as no refusal by rule', () => {
    expect(readRuleRefusal(null)).toBeNull();
    expect(readRuleRefusal('policy_denied')).toBeNull();
    expect(readRuleRefusal({ type: 'model_not_on_plan', rule: 'models.allowed' })).toBeNull();
    expect(readRuleRefusal({ type: 'policy_denied' })).toBeNull();
  });
});

describe('ruleRefusalCopy', () => {
  it('names the organization and offers a model choice for a refused model', () => {
    const c = ruleRefusalCopy(t, { rule: 'models.allowed', setBy: 'organization', model: 'm', message: '' }, 'Northfield', name);
    expect(c.title).toBe('organization.refusal.modelNotAllowed|Northfield|name:m');
    expect(c.body).toBe('organization.refusal.pickAllowed.organization');
    expect(c.action).toBe('choose_model');
  });

  it('names the deployment when it set the rule', () => {
    const c = ruleRefusalCopy(t, { rule: 'public_links', setBy: 'deployment', message: '' }, 'Northfield', name);
    expect(c.title).toBe('organization.refusal.publicLinks|organization.who.deploymentStart|');
    expect(c.action).toBeNull();
  });

  it('offers nothing to choose for a rule on the account rather than a model', () => {
    const c = ruleRefusalCopy(t, { rule: 'own_model_access.api_keys', setBy: 'organization', message: '' }, null, name);
    expect(c.title).toBe('organization.refusal.ownApiKeys|organization.who.yourOrganizationStart|');
    expect(c.action).toBeNull();
  });

  it("shows the server's sentence for a rule this client has no words for", () => {
    const c = ruleRefusalCopy(t, { rule: 'future.rule', setBy: 'organization', message: 'Not today.' }, null, name);
    expect(c).toEqual({ title: 'Not today.', body: null, action: null });
  });
});
