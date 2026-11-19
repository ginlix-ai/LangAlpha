import { describe, expect, it } from 'vitest';
import { defaultChoices, type DefaultChoicesInput } from '../defaultChoices';
import { connectedModel } from '../connectedDefault';

const input = (over: Partial<DefaultChoicesInput>): DefaultChoicesInput => ({
  models: 'subscription_or_key',
  offered: {},
  slotModels: [],
  payers: {},
  ownProviders: [],
  metadata: {},
  customModels: [],
  connectionOf: (p) => p,
  systemDefault: null,
  systemBackground: null,
  preferred: null,
  ...over,
});

const rated = {
  'gpt-astra': { intelligence: 5, speed: 2 },
  'gpt-sol': { intelligence: 5, speed: 3 },
  'gpt-terra': { intelligence: 4, speed: 3 },
  'gpt-luna': { intelligence: 3, speed: 5 },
};

describe('defaultChoices', () => {
  it('recommends the strongest model for conversations and the fastest other one for Background', () => {
    const choices = defaultChoices(input({
      offered: { 'codex-oauth': { models: ['gpt-astra', 'gpt-terra', 'gpt-sol', 'gpt-luna'] }, platform: { models: ['house'] } },
      slotModels: ['gpt-astra', 'gpt-terra', 'gpt-sol', 'gpt-luna', 'house'],
      ownProviders: ['codex-oauth'],
      metadata: rated,
    }));
    expect(choices.recommended).toEqual({
      primary: 'gpt-sol', primaryWhy: 'strongestFastest', background: 'gpt-luna', backgroundWhy: 'fastest',
    });
    expect(choices.initial).toBe('gpt-sol');
    expect(choices.list).toEqual(['gpt-sol', 'gpt-astra', 'gpt-terra', 'gpt-luna']);
  });

  it('opens on a default already saved, with the recommended one still listed', () => {
    const choices = defaultChoices(input({
      offered: { 'codex-oauth': { models: ['gpt-astra', 'gpt-terra', 'gpt-sol', 'gpt-luna'] } },
      slotModels: ['gpt-astra', 'gpt-terra', 'gpt-sol', 'gpt-luna'],
      ownProviders: ['codex-oauth'],
      metadata: rated,
      preferred: 'gpt-terra',
    }));
    expect(choices.initial).toBe('gpt-terra');
    expect(choices.list.slice(0, 2)).toEqual(['gpt-terra', 'gpt-sol']);
    expect(choices.recommended.primary).toBe('gpt-sol');
  });

  it('offers the deployment\'s models with its own picks recommended', () => {
    const choices = defaultChoices(input({
      models: 'langalpha_models',
      offered: { platform: { models: ['a', 'b', 'c'] }, 'codex-oauth': { models: ['mine'] } },
      slotModels: ['a', 'b', 'c', 'mine'],
      payers: { a: 'credits', b: 'credits', c: 'credits', mine: 'subscription:codex-oauth' },
      systemDefault: 'b',
      systemBackground: 'c',
      preferred: 'a',
    }));
    expect(choices.list).toEqual(['b', 'a', 'c']);
    expect(choices.recommended).toEqual({ primary: 'b', primaryWhy: 'deployment', background: 'c', backgroundWhy: 'deployment' });
  });

  it('recommends no Background for LangAlpha\'s models that a key the user connected would pay for', () => {
    const choices = defaultChoices(input({
      models: 'langalpha_models',
      offered: { platform: { models: ['a', 'b'] }, openai: { models: ['keyed'] } },
      slotModels: ['a', 'b', 'keyed'],
      payers: { a: 'credits', b: 'credits', keyed: 'own_key' },
      metadata: { a: { speed: 2 }, b: { speed: 4 } },
      systemDefault: 'a',
      systemBackground: 'keyed',
    }));
    expect(choices.recommended).toMatchObject({ primary: 'a', background: 'b', backgroundWhy: 'fastest' });
  });

  it('leaves Claude Code\'s Background to the deployment, which runs it', () => {
    const choices = defaultChoices(input({
      models: 'claude_code',
      offered: { 'claude-code': { models: ['cc-opus', 'cc-sonnet', 'cc-haiku'] } },
      slotModels: ['house-fast'],
      metadata: {
        'cc-opus': { model_id: 'claude-opus-5', intelligence: 5, speed: 3 },
        'cc-sonnet': { model_id: 'claude-sonnet-5', intelligence: 5, speed: 4 },
        'cc-haiku': { model_id: 'claude-haiku-5', intelligence: 3, speed: 5 },
      },
      systemBackground: 'house-fast',
    }));
    expect(choices.recommended).toEqual({ primary: 'cc-sonnet', primaryWhy: 'claudeCode', background: null, backgroundWhy: 'claudeCode' });
  });

  it('says Claude Code\'s Background bills credits where the deployment\'s model does', () => {
    const choices = defaultChoices(input({
      models: 'claude_code',
      offered: { 'claude-code': { models: ['cc-sonnet'] } },
      slotModels: ['house-fast'],
      payers: { 'house-fast': 'credits' },
      systemBackground: 'house-fast',
    }));
    expect(choices.recommended).toMatchObject({ background: null, backgroundWhy: 'claudeCodeCredits' });
  });

  it('recommends Claude Code\'s newest Sonnet, which suits any Claude plan, over an Opus or Fable rated higher', () => {
    const choices = defaultChoices(input({
      models: 'claude_code',
      offered: { 'claude-code': { models: ['cc-fable', 'cc-opus', 'cc-sonnet-new', 'cc-sonnet-old', 'cc-haiku'] } },
      metadata: {
        'cc-fable': { model_id: 'claude-fable-5-2', intelligence: 5, speed: 3 },
        'cc-opus': { model_id: 'claude-opus-5-5', intelligence: 5, speed: 3 },
        'cc-sonnet-new': { model_id: 'claude-sonnet-5-5', intelligence: 4, speed: 4 },
        'cc-sonnet-old': { model_id: 'claude-sonnet-5', intelligence: 4, speed: 4 },
        'cc-haiku': { model_id: 'claude-haiku-5-5', intelligence: 2, speed: 5 },
      },
    }));
    expect(choices.recommended).toMatchObject({ primary: 'cc-sonnet-new', primaryWhy: 'claudeCode' });
    expect(choices.initial).toBe('cc-sonnet-new');
  });

  it('starts Claude Code on its own pick where the manifest rates none', () => {
    const choices = defaultChoices(input({
      models: 'claude_code',
      offered: { 'claude-code': { models: ['cc-opus', 'cc-sonnet'] } },
      metadata: { 'cc-opus': { model_id: 'claude-opus-5' }, 'cc-sonnet': { model_id: 'claude-sonnet-5' } },
    }));
    expect(choices.initial).toBe('cc-sonnet');
    expect(choices.recommended.primaryWhy).toBe('claudeCode');
  });

  it('pairs a lone model with itself for Background', () => {
    const choices = defaultChoices(input({
      offered: { deepseek: { models: ['ds'] } },
      slotModels: ['ds'],
      ownProviders: ['deepseek'],
    }));
    expect(choices.recommended).toMatchObject({ primary: 'ds', background: 'ds', backgroundWhy: 'only' });
  });

  it('offers nothing where nothing is connected', () => {
    expect(defaultChoices(input({})).list).toEqual([]);
    expect(defaultChoices(input({})).initial).toBeNull();
  });

  it('finds a native variant\'s models where the list files them under the brand, and a plan\'s picked ones', () => {
    const choices = defaultChoices(input({
      offered: { moonshot: { models: ['kimi-own', 'kimi-plan'] }, 'z-ai-coding': { models: ['glm-a'] } },
      metadata: { 'kimi-own': { provider: 'moonshot' }, 'kimi-plan': { provider: 'moonshot-coding' }, 'glm-a': { provider: 'z-ai' } },
      ownProviders: ['moonshot-coding'],
      preferred: 'kimi-plan',
    }));
    expect(choices.list).toEqual(['kimi-plan']);
    const plan = defaultChoices(input({
      offered: { 'z-ai': { models: ['glm-a'] } },
      metadata: { 'glm-a': { provider: 'z-ai' } },
      customModels: [{ name: 'glm-a', model_id: 'glm-a', provider: 'z-ai-coding' }],
      ownProviders: ['z-ai-coding'],
    }));
    expect(plan.list).toEqual(['glm-a']);
  });
});

describe('connectedModel', () => {
  it('saves the model the default step recommends, so the two agree', () => {
    const models = { 'codex-oauth': { models: ['gpt-astra', 'gpt-terra', 'gpt-sol', 'gpt-luna'] } };
    expect(connectedModel('codex-oauth', models, rated, null, [], (p) => p)).toBe('gpt-sol');
  });

  it('keeps a Claude Code Default already saved', () => {
    const models = { 'claude-code': { models: ['cc-opus', 'cc-sonnet'] } };
    const metadata = {
      'cc-opus': { model_id: 'claude-opus-5', sdk: 'claude-code', intelligence: 5, speed: 3 },
      'cc-sonnet': { model_id: 'claude-sonnet-5', sdk: 'claude-code', intelligence: 5, speed: 4 },
    };
    expect(connectedModel('claude-code', models, metadata, null, [], (p) => p)).toBe('cc-sonnet');
    expect(connectedModel('claude-code', models, metadata, 'cc-opus', [], (p) => p)).toBe('cc-opus');
  });

  it('saves Claude Code\'s Sonnet, as the default step recommends, over an Opus rated higher', () => {
    const models = { 'claude-code': { models: ['cc-opus', 'cc-sonnet'] } };
    const metadata = {
      'cc-opus': { model_id: 'claude-opus-5-5', sdk: 'claude-code', intelligence: 5, speed: 3 },
      'cc-sonnet': { model_id: 'claude-sonnet-5-5', sdk: 'claude-code', intelligence: 4, speed: 4 },
    };
    expect(connectedModel('claude-code', models, metadata, null, [], (p) => p)).toBe('cc-sonnet');
  });
});
