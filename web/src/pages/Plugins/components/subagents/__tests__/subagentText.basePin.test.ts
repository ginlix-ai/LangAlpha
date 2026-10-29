import { describe, expect, it } from 'vitest';
import { offerBasePin } from '../subagentText';

/**
 * A base's own model runs whatever the plan, and picking it drops the user's
 * key, so the picker offers it even where the plan holds that model back.
 */

const models = { openai: { display_name: 'OpenAI', models: ['basic-model'] } };

describe('offerBasePin', () => {
  it("moves the base's pin out of the locked list and into its provider's choices", () => {
    const locked = { openai: { display_name: 'OpenAI', models: ['pinned-model', 'other-locked'] } };
    expect(offerBasePin(models, locked, 'pinned-model')).toEqual({
      models: { openai: { display_name: 'OpenAI', models: ['basic-model', 'pinned-model'] } },
      locked: { openai: { display_name: 'OpenAI', models: ['other-locked'] } },
    });
  });

  it('adds the provider from its locked entry, and drops a locked entry left empty', () => {
    const locked = { anthropic: { display_name: 'Anthropic', models: ['pinned-model'] } };
    expect(offerBasePin(models, locked, 'pinned-model')).toEqual({
      models: { ...models, anthropic: { display_name: 'Anthropic', models: ['pinned-model'] } },
      locked: {},
    });
  });

  it('leaves both lists as they are when the pin is not held back, or there is none', () => {
    const locked = { openai: { models: ['other-locked'] } };
    for (const pin of ['basic-model', 'not-listed', null]) {
      const out = offerBasePin(models, locked, pin);
      expect(out.models).toBe(models);
      expect(out.locked).toBe(locked);
    }
  });
});
