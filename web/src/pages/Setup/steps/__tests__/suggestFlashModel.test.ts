// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { suggestFlashModel } from '../suggestFlashModel';

const models = {
  anthropic: { models: ['claude-big', 'claude-small'] },
  solo: { models: ['solo-model'] },
  openai: { models: ['gpt-sol', 'gpt-terra'] },
};

describe('suggestFlashModel', () => {
  it("pairs the primary with another model of its provider", () => {
    expect(suggestFlashModel(models, 'claude-big')).toBe('claude-small');
    expect(suggestFlashModel(models, 'claude-small')).toBe('claude-big');
  });

  it('pairs a primary alone in its provider with itself', () => {
    expect(suggestFlashModel(models, 'solo-model')).toBe('solo-model');
  });

  it('falls back to the first listed model for a primary the list does not carry', () => {
    expect(suggestFlashModel(models, 'gone-model')).toBe('claude-big');
    expect(suggestFlashModel({ empty: {}, openai: { models: ['gpt-sol'] } }, 'gone-model')).toBe('gpt-sol');
  });

  it('suggests nothing from an empty list', () => {
    expect(suggestFlashModel({}, 'claude-big')).toBeUndefined();
  });
});
