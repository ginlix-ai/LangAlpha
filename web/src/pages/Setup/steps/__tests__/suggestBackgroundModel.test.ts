// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { suggestBackgroundModel } from '../suggestBackgroundModel';

const models = {
  anthropic: { models: ['claude-big', 'claude-small'] },
  solo: { models: ['solo-model'] },
  openai: { models: ['gpt-sol', 'gpt-terra'] },
};

describe('suggestBackgroundModel', () => {
  it("pairs the primary with another model of its provider", () => {
    expect(suggestBackgroundModel(models, 'claude-big')).toBe('claude-small');
    expect(suggestBackgroundModel(models, 'claude-small')).toBe('claude-big');
  });

  it('pairs a primary alone in its provider with itself', () => {
    expect(suggestBackgroundModel(models, 'solo-model')).toBe('solo-model');
  });

  it('falls back to the first listed model for a primary the list does not carry', () => {
    expect(suggestBackgroundModel(models, 'gone-model')).toBe('claude-big');
    expect(suggestBackgroundModel({ empty: {}, openai: { models: ['gpt-sol'] } }, 'gone-model')).toBe('gpt-sol');
  });

  it('suggests nothing from an empty list', () => {
    expect(suggestBackgroundModel({}, 'claude-big')).toBeUndefined();
  });
});
