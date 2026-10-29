import { describe, expect, it } from 'vitest';
import type { ListedModel } from '@/api/model';
import { entryForListed, withListedSpecs } from '../listedEntries';

const ladder = { efforts: ['none', 'low', 'high', 'max'], default: 'high', write: 'parameters.reasoning.effort' };

describe('effort levels from a provider list', () => {
  it('a pick is saved with the levels the list gives it', () => {
    const listed: ListedModel = { id: 'deepseek/deepseek-v4-flash', context_window: 128000, reasoning: ladder };
    expect(entryForListed(listed, 'openrouter', new Set())).toEqual({
      name: 'deepseek/deepseek-v4-flash',
      model_id: 'deepseek/deepseek-v4-flash',
      provider: 'openrouter',
      context: 128000,
      reasoning: ladder,
    });
    expect(entryForListed({ id: 'plain/chat' }, 'openrouter', new Set())).not.toHaveProperty('reasoning');
  });

  it('a pick whose provider-prefixed name is taken too is numbered, not saved over it', () => {
    expect(entryForListed({ id: 'foo' }, 'openai', new Set(['foo', 'openai/foo', 'openai/foo-2'])).name).toBe('openai/foo-3');
  });

  it('a saved pick takes the levels listed now, and one the list does not name stays as saved', () => {
    const old = { name: 'a', model_id: 'deepseek/deepseek-v4-flash', provider: 'openrouter' };
    const manual = { name: 'b', model_id: 'acme/house', provider: 'openrouter', reasoning: { efforts: ['low'], write: 'parameters.reasoning.effort' } };
    const listed: ListedModel[] = [{ id: 'deepseek/deepseek-v4-flash', reasoning: ladder }, { id: 'acme/house' }];
    expect(withListedSpecs([old, manual], listed)).toEqual([{ ...old, reasoning: ladder }, manual]);
  });

  it('a saved pick takes the window listed now, so a lowered limit is the one sent', () => {
    const saved = { name: 'a', model_id: 'deepseek/deepseek-v4-flash', provider: 'openrouter', context: 256000 };
    expect(withListedSpecs([saved], [{ id: 'deepseek/deepseek-v4-flash', context_window: 128000 }]))
      .toEqual([{ ...saved, context: 128000 }]);
    expect(withListedSpecs([saved], [{ id: 'deepseek/deepseek-v4-flash' }])).toEqual([saved]);
  });

  it('a saved pick takes the inputs listed now, and keeps its own when the list names none', () => {
    const saved = { name: 'a', model_id: 'acme/vision', provider: 'openrouter', input_modalities: ['text', 'image'] };
    const [textOnly] = withListedSpecs([saved], [{ id: 'acme/vision', input_modalities: ['text'] }]);
    expect(textOnly).not.toHaveProperty('input_modalities');
    expect(withListedSpecs([saved], [{ id: 'acme/vision', input_modalities: ['text', 'pdf'] }]))
      .toEqual([{ ...saved, input_modalities: ['text', 'pdf'] }]);
    expect(withListedSpecs([saved], [{ id: 'acme/vision' }])).toEqual([saved]);
    // OpenRouter's word for a PDF is `file`.
    expect(withListedSpecs([saved], [{ id: 'acme/vision', input_modalities: ['text', 'file'] }]))
      .toEqual([{ ...saved, input_modalities: ['text', 'pdf'] }]);
  });

  it("a gateway's pick is always named under the gateway, so a built-in of its id is never shadowed", () => {
    expect(entryForListed({ id: 'qwen9', route: 'gw-chat' }, 'gw', new Set()).name).toBe('gw/qwen9');
    expect(entryForListed({ id: 'qwen9' }, 'gw', new Set()).name).toBe('qwen9');
  });

  it("a saved gateway pick moves whole when the gateway's catalog serves it on another API", () => {
    const saved = {
      name: 'gw/m', model_id: 'm', provider: 'gw-chat',
      parameters: { max_tokens: 1 }, reasoning: { efforts: ['low'], write: 'parameters.reasoning_effort' },
    };
    const ladder = { efforts: ['low', 'high'], write: 'parameters.output_config.effort' };
    const [moved] = withListedSpecs([saved], [
      { id: 'm', described: true, route: 'gw-anthropic', parameters: { thinking: { type: 'adaptive' } }, reasoning: ladder },
    ]);
    expect(moved).toMatchObject({ provider: 'gw-anthropic', parameters: { thinking: { type: 'adaptive' } }, reasoning: ladder });
    // Undescribed (the catalog unreadable), the list's word on the route is a fallback's: it stays.
    expect(withListedSpecs([saved], [{ id: 'm', route: 'gw-chat2' }])).toEqual([saved]);
    // Described without settings: none of the old API's are sent on the new one.
    const [bare] = withListedSpecs([saved], [{ id: 'm', described: true, route: 'gw-anthropic' }]);
    expect(bare).not.toHaveProperty('parameters');
    expect(bare).not.toHaveProperty('reasoning');
  });
});
