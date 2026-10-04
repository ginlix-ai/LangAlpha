// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { derivePrimaryModels } from '../chat-input.models';

describe('derivePrimaryModels', () => {
  it('lists the thread models, then the current selection', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'claude-sonnet'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'claude-sonnet', 'gpt-5']);
  });

  it('dedupes a selection the thread already used', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'claude-opus',
        threadModels: ['claude-opus'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus']);
  });

  it('drops a thread model the user can no longer reach', () => {
    // A model can be revoked (BYOK key removed, plan downgrade) long after a
    // turn used it. Leaving it clickable fails only once the user sends.
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'revoked-model'],
        validModelNames: new Set(['claude-opus', 'gpt-5']),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('keeps the current selection even when it is not in the valid set', () => {
    // The trigger renders the selection; gating it would leave the menu unable
    // to show what is currently selected.
    expect(
      derivePrimaryModels({
        selectedModel: 'not-yet-loaded',
        threadModels: ['claude-opus'],
        validModelNames: new Set(['claude-opus']),
      }),
    ).toEqual(['claude-opus', 'not-yet-loaded']);
  });

  it('skips the availability gate while the model list is still loading', () => {
    expect(
      derivePrimaryModels({
        selectedModel: null,
        threadModels: ['claude-opus', 'gpt-5'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('offers thread models from any provider, not just the selection\'s', () => {
    // The point of the change: history spanning providers stays reachable.
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'glm-5.2'],
        validModelNames: new Set(['claude-opus', 'glm-5.2', 'gpt-5']),
      }),
    ).toEqual(['claude-opus', 'glm-5.2', 'gpt-5']);
  });

  it('returns an empty list with no selection and no history', () => {
    expect(
      derivePrimaryModels({
        selectedModel: null,
        threadModels: [],
        validModelNames: new Set(),
      }),
    ).toEqual([]);
  });

  it('drops malformed thread-model entries', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: [123, '', null, 'claude-opus'] as unknown as string[],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });
});
