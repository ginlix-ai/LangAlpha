// @vitest-environment node
/**
 * Who pays for a turn on a model, as the server's ladder decides it: a
 * subscription's model runs only on its sign-in; any other model on the
 * user's own key when one serves it and their keys are on; else on the
 * deployment, which is credits on a hosted account and its own key self-hosted.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { payerLabel, payerOfBilling, payerResolver, type PayerContext } from '../modelPayer';

const mode = vi.hoisted(() => ({ platform: true }));
vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return mode.platform; },
}));

const key = (provider: string, extra: Partial<ConfiguredProvider> = {}): ConfiguredProvider => ({
  provider, display_name: provider, access_type: 'api_key', masked_key: 'sk-...1234', ...extra,
});
const served = (provider: string) => key(provider, { masked_key: '(server)' });
const signIn = (provider: string) => key(provider, { access_type: 'oauth', masked_key: null });

function resolver(ctx: Partial<PayerContext> = {}) {
  return payerResolver({
    platform: { model_tier: 1, byok_providers: [], oauth_providers: [] },
    configured: [],
    byokEnabled: true,
    keyCandidates: {
      // A coding plan's key is issued apart from its brand's: it serves the
      // brand's models only when saved with a base URL.
      'z-ai': { slugs: ['z-ai', 'z-ai-coding'], shared: ['z-ai'] },
      // A regional variant shares its brand's key.
      dashscope: { slugs: ['dashscope', 'dashscope-intl'], shared: ['dashscope', 'dashscope-intl'] },
    },
    ...ctx,
  });
}

describe('payerResolver', () => {
  beforeEach(() => { mode.platform = true; });

  it("puts a subscription's model on its sign-in, whatever keys the user holds", () => {
    const payerOf = resolver({ configured: [key('anthropic')] });
    expect(payerOf({ provider: 'claude-oauth', access_type: 'oauth' }, 'claude-oauth')).toBe('subscription:claude-oauth');
  });

  it("puts a model on the user's key when one serves it, and on credits otherwise", () => {
    const payerOf = resolver({ configured: [key('anthropic')] });
    expect(payerOf({ provider: 'anthropic', tier: 1 }, 'anthropic')).toBe('own_key');
    expect(payerOf({ provider: 'openai', tier: 1 }, 'openai')).toBe('credits');
  });

  it('runs nothing on a saved key while the user has their keys switched off', () => {
    const payerOf = resolver({ configured: [key('anthropic')], byokEnabled: false });
    expect(payerOf({ provider: 'anthropic', tier: 1 }, 'anthropic')).toBe('credits');
  });

  it('finds a key the vendor issues as one with the provider, and one saved with a base URL', () => {
    expect(resolver({ configured: [key('dashscope-intl')] })({ provider: 'dashscope' }, 'dashscope')).toBe('own_key');
    expect(resolver({ configured: [key('z-ai-coding')] })({ provider: 'z-ai' }, 'z-ai')).toBe('credits');
    expect(
      resolver({ configured: [key('z-ai-coding', { base_url: 'https://gateway.example/v1' })] })({ provider: 'z-ai' }, 'z-ai'),
    ).toBe('own_key');
  });

  it("has no payer for a model above the plan, or for a local server", () => {
    const payerOf = resolver();
    expect(payerOf({ provider: 'openai', tier: 2 }, 'openai')).toBeUndefined();
    expect(payerOf({ provider: 'ollama', access_type: 'local' }, 'ollama')).toBeUndefined();
  });

  it("puts a model the user added on their key, or on the sign-in it was added under", () => {
    const payerOf = resolver({ configured: [signIn('codex-oauth'), key('my-gateway')] });
    expect(payerOf({ provider: 'my-gateway' }, 'my-gateway', 'gw-model')).toBe('own_key');
    expect(payerOf({ provider: 'codex-oauth' }, 'codex-oauth', 'codex-model')).toBe('subscription:codex-oauth');
  });

  // The server builds no client for one: the turn is refused for a key.
  it('has no payer for a model the user added while no key of theirs serves it', () => {
    expect(resolver()({ provider: 'my-gateway' }, 'my-gateway', 'gw-model')).toBeUndefined();
    const switchedOff = resolver({ configured: [key('my-gateway')], byokEnabled: false });
    expect(switchedOff({ provider: 'my-gateway' }, 'my-gateway', 'gw-model')).toBeUndefined();
    // A key saved under the model's own name serves it too.
    expect(resolver({ configured: [key('gw-model')] })({ provider: 'my-gateway' }, 'my-gateway', 'gw-model')).toBe('own_key');
  });

  it("puts a model the user added on its own route, not the built-in's it shadows", () => {
    const payerOf = resolver({ configured: [key('ollama', { access_type: 'local', masked_key: null }), key('my-gateway')] });
    const shadowed = { provider: 'claude-oauth', access_type: 'oauth' as const };
    expect(payerOf(shadowed, 'my-gateway', 'claude-opus-5-5-oauth')).toBe('own_key');
    expect(payerOf({ provider: 'ollama' }, 'ollama', 'llama-local')).toBeUndefined();
  });

  it("puts a self-hosted model on the deployment's key when it holds one", () => {
    mode.platform = false;
    const payerOf = resolver({ platform: null, configured: [served('openai'), key('xai')] });
    expect(payerOf({ provider: 'openai' }, 'openai')).toBe('server_key');
    expect(payerOf({ provider: 'xai' }, 'xai')).toBe('own_key');
    expect(payerOf({ provider: 'gemini' }, 'gemini')).toBeUndefined();
  });
});

describe('payerOfBilling', () => {
  it('reads who paid for a call as the badge names it', () => {
    mode.platform = true;
    expect([payerOfBilling('oauth', { provider: 'claude-oauth' }), payerOfBilling('byok'), payerOfBilling('platform'), payerOfBilling(undefined)])
      .toEqual(['subscription:claude-oauth', 'own_key', 'credits', undefined]);
    mode.platform = false;
    expect(payerOfBilling('platform')).toBe('server_key');
    // A local server's call goes keyless, which the wire calls "platform".
    expect(payerOfBilling('platform', { provider: 'ollama', access_type: 'local' })).toBeUndefined();
  });
});

describe('payerLabel', () => {
  const t = (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key);

  // "Subscription" beside "Credits" reads as the user's LangAlpha plan.
  it('names a subscription by the plan its sign-in draws on, and an unknown one generically', () => {
    expect(payerLabel(t, 'subscription:codex-oauth')).toBe('payer.plan:{"plan":"ChatGPT"}');
    expect(payerLabel(t, 'subscription:')).toBe('payer.subscription');
  });

  it("names a model the user added under a sign-in by that sign-in's plan", () => {
    const payer = resolver({ configured: [signIn('claude-oauth')] })({ provider: 'claude-oauth' }, 'claude-oauth', 'claude-model');
    expect(payerLabel(t, payer!)).toBe('payer.plan:{"plan":"Claude"}');
  });
});
